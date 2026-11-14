"""
Build the curated dataset: daily trip counts per taxi type (Jan-Jun 2024).

For each source file (read remotely over HTTPS, nothing saved to raw):
  - map that taxi type's own column names to one common schema
  - clean: drop rows outside the file's month, bad timestamps,
    negative distance
  - aggregate to one row per day in a single remote scan
  - save data/curated/daily_trips/<taxi_type>_<YYYY-MM>.parquet

Incremental: a file whose curated output already exists is skipped,
so re-running only processes new or previously failed months.
Use --force to rebuild everything.

Outputs:
  data/curated/daily_trips/*.parquet   one per source file
  data/curated/daily_trips_all.parquet all months combined
  reports/curation_log.csv             rows kept / dropped per file, status
"""
import sys
import time
from datetime import date

import duckdb
import pandas as pd

from config.datasets import DATASETS, CURATED_DIR, REPORTS_DIR
from src.data_quality import COLUMNS, connect, with_retry

DAILY_DIR = CURATED_DIR / "daily_trips"
COMBINED_FILE = CURATED_DIR / "daily_trips_all.parquet"
LOG_FILE = REPORTS_DIR / "curation_log.csv"
WAIT_BETWEEN_FILES = 3


def month_range(year, month):
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def output_path(dataset):
    return DAILY_DIR / f"{dataset['taxi_type']}_{dataset['year']}-{dataset['month']:02d}.parquet"


def aggregate_file(con, dataset):
    """One remote scan: label every row with a clean/drop reason, then group by day."""
    pickup, dropoff, distance = COLUMNS[dataset["taxi_type"]]
    start, end = month_range(dataset["year"], dataset["month"])
    dist_expr = f'"{distance}"' if distance else "NULL::DOUBLE"

    query = f"""
        WITH labelled AS (
            SELECT
                CAST("{pickup}" AS DATE) AS trip_date,
                {dist_expr}              AS distance,
                CASE
                    WHEN "{pickup}" IS NULL
                      OR "{pickup}" <  TIMESTAMP '{start}'
                      OR "{pickup}" >= TIMESTAMP '{end}'   THEN 'out_of_period'
                    WHEN "{dropoff}" IS NULL
                      OR "{dropoff}" < "{pickup}"          THEN 'bad_timestamp'
                    WHEN {dist_expr} < 0                   THEN 'negative_distance'
                    ELSE 'kept'
                END AS reason
            FROM read_parquet(?)
        )
        SELECT trip_date, reason, COUNT(*) AS trips, SUM(distance) AS total_miles
        FROM labelled
        GROUP BY trip_date, reason
    """
    return con.execute(query, [dataset["url"]]).fetchdf()


def curate_one(con, dataset):
    grouped = with_retry(lambda: aggregate_file(con, dataset), "aggregate")

    kept = grouped[grouped["reason"] == "kept"].copy()
    daily = (
        kept.groupby("trip_date", as_index=False)[["trips", "total_miles"]].sum(min_count=1)
        .assign(taxi_type=dataset["taxi_type"],
                source_file=dataset["filename"])
        .sort_values("trip_date")
    )
    daily = daily[["trip_date", "taxi_type", "trips", "total_miles", "source_file"]]

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(output_path(dataset), index=False)

    by_reason = grouped.groupby("reason")["trips"].sum()
    return {
        "raw_rows": int(by_reason.sum()),
        "kept_rows": int(by_reason.get("kept", 0)),
        "dropped_out_of_period": int(by_reason.get("out_of_period", 0)),
        "dropped_bad_timestamp": int(by_reason.get("bad_timestamp", 0)),
        "dropped_negative_distance": int(by_reason.get("negative_distance", 0)),
        "days": len(daily),
    }


def combine():
    """Merge every per-file output into one curated table."""
    files = sorted(DAILY_DIR.glob("*.parquet"))
    if not files:
        return None
    con = duckdb.connect()
    try:
        pattern = str(DAILY_DIR / "*.parquet").replace("\\", "/")
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{pattern}')
                ORDER BY trip_date, taxi_type
            ) TO '{str(COMBINED_FILE).replace(chr(92), "/")}' (FORMAT parquet)
        """)
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()[0]
    finally:
        con.close()


def run_curation(force=False):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    con = connect()
    log = []

    try:
        for dataset in DATASETS:
            filename = dataset["filename"]
            record = {"taxi_type": dataset["taxi_type"], "filename": filename}

            if output_path(dataset).exists() and not force:
                print(f"SKIP (already curated): {filename}")
                record.update(status="SKIPPED", error="")
                log.append(record)
                continue

            print(f"\nCurating: {filename}")
            try:
                record.update(curate_one(con, dataset))
                record.update(status="SUCCESS", error="")
                print(f"SUCCESS: {filename} | kept {record['kept_rows']:,} "
                      f"of {record['raw_rows']:,} rows")
            except Exception as error:
                record.update(status="FAILED", error=str(error))
                print(f"FAILED: {filename}\nReason: {error}")

            log.append(record)
            time.sleep(WAIT_BETWEEN_FILES)
    finally:
        con.close()

    # keep stats from earlier runs for skipped files
    new_log = pd.DataFrame(log)
    if LOG_FILE.exists():
        old = pd.read_csv(LOG_FILE)
        old = old[old["status"] == "SUCCESS"]
        skipped = new_log[new_log["status"] == "SKIPPED"]
        reused = old[old["filename"].isin(skipped["filename"])]
        new_log = pd.concat([
            reused,
            skipped[~skipped["filename"].isin(reused["filename"])],
            new_log[new_log["status"] != "SKIPPED"],
        ])
        order = {d["filename"]: i for i, d in enumerate(DATASETS)}
        new_log = new_log.sort_values("filename", key=lambda s: s.map(order))
    new_log.to_csv(LOG_FILE, index=False)

    rows = combine()
    print("\n========== CURATION SUMMARY ==========")
    print(new_log.drop(columns="error").to_string(index=False))
    print(f"\nCurated table: {COMBINED_FILE} ({rows} daily rows)")
    print(f"Log: {LOG_FILE}")


if __name__ == "__main__":
    run_curation(force="--force" in sys.argv)