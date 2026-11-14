"""
Data quality checks on NYC TLC parquet files, read remotely over HTTPS
(no local download, no AWS).

Strategy (to avoid HTTP 403 from CloudFront on large fhvhv files):
  1. Metadata-first: read only the parquet footer via parquet_metadata().
     Row count, null counts and min values per column come from the
     row-group statistics, so no trip rows are transferred.
  2. Scan fallback: only when statistics are missing, or when
     min(distance) < 0 (so we need the exact negative count), scan just
     that one column, with retries and backoff.
"""
import time
from pathlib import Path

import duckdb
import pandas as pd

from config.datasets import DATASETS

OUTPUT_FILE = Path("reports/data_quality_results.csv")

# column names differ per taxi type
COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"),
    "green":  ("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance"),
    "fhv":    ("pickup_datetime", "dropOff_datetime", None),
    "fhvhv":  ("pickup_datetime", "dropoff_datetime", "trip_miles"),
}

MAX_ATTEMPTS = 4
WAIT_BETWEEN_FILES = 3      # seconds
BACKOFF_BASE = 15           # seconds: waits 15, 30, 60 between retries


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # fewer parallel range requests = less likely to be throttled
    con.execute("SET threads = 2")
    con.execute("SET http_retries = 5")
    con.execute("SET http_retry_wait_ms = 2000")
    con.execute("SET http_retry_backoff = 2")
    return con


def with_retry(fn, label):
    """Run fn(); on HTTP 403/429/5xx wait and try again."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as error:
            msg = str(error)
            retryable = any(code in msg for code in ("403", "429", "500", "502", "503", "504"))
            if not retryable or attempt == MAX_ATTEMPTS:
                raise
            wait = BACKOFF_BASE * 2 ** (attempt - 1)
            print(f"  {label}: attempt {attempt} failed ({msg.splitlines()[0][:80]}), retry in {wait}s")
            time.sleep(wait)


def column_stats(con, url, columns):
    """Per-column null count and min value from the parquet footer only."""
    cols = [c for c in columns if c]
    placeholders = ", ".join("?" for _ in cols)
    df = con.execute(f"""
        SELECT
            path_in_schema                              AS col,
            SUM(stats_null_count)                       AS null_count,
            COUNT(*) FILTER (WHERE stats_null_count IS NULL) AS missing_stats,
            MIN(TRY_CAST(stats_min_value AS DOUBLE))    AS min_value
        FROM parquet_metadata(?)
        WHERE path_in_schema IN ({placeholders})
        GROUP BY path_in_schema
    """, [url, *cols]).fetchdf()
    return {row.col: row for row in df.itertuples()}


def row_count(con, url):
    return con.execute("""
        SELECT SUM(num_rows) FROM parquet_file_metadata(?)
    """, [url]).fetchone()[0]


def scan_count(con, url, condition):
    """Fallback: scan one column. DuckDB only fetches the columns used."""
    return con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {condition}) FROM read_parquet(?)
    """, [url]).fetchone()[0]


def check_file(con, dataset):
    url = dataset["url"]
    pickup, dropoff, distance = COLUMNS[dataset["taxi_type"]]

    total = with_retry(lambda: row_count(con, url), "row count")
    stats = with_retry(lambda: column_stats(con, url, (pickup, dropoff, distance)), "metadata")

    method = "metadata"

    def nulls(col):
        nonlocal method
        s = stats.get(col)
        if s is not None and s.missing_stats == 0:
            return int(s.null_count)
        method = "metadata+scan"
        return with_retry(lambda: scan_count(con, url, f'"{col}" IS NULL'), f"scan {col}")

    null_pickup = nulls(pickup)
    null_dropoff = nulls(dropoff)

    negative = None
    if distance:
        s = stats.get(distance)
        if s is not None and s.min_value is not None and s.min_value >= 0:
            negative = 0              # smallest value in every row group is >= 0
        else:
            method = "metadata+scan"
            negative = with_retry(lambda: scan_count(con, url, f'"{distance}" < 0'), f"scan {distance}")

    return {
        "total_rows": total,
        "null_pickup": null_pickup,
        "null_dropoff": null_dropoff,
        "negative_distance": negative,
        "check_method": method,
    }


MAX_CONSECUTIVE_BLOCKS = 2   # stop early if the source keeps refusing us


def load_previous_success():
    """Results from earlier runs that already passed -> no need to re-request."""
    if not OUTPUT_FILE.exists():
        return {}
    prev = pd.read_csv(OUTPUT_FILE)
    prev = prev[prev["status"] == "SUCCESS"]
    return {row["filename"]: row.to_dict() for _, row in prev.iterrows()}


def run_quality_checks(force=False):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    previous = {} if force else load_previous_success()
    con = connect()
    results = []
    consecutive_blocks = 0
    stopped = False

    try:
        for dataset in DATASETS:
            filename = dataset["filename"]
            record = {"taxi_type": dataset["taxi_type"], "filename": filename}

            if filename in previous:
                print(f"SKIP (already checked): {filename}")
                results.append(previous[filename])
                continue

            if stopped:
                record.update(status="NOT_RUN", error="skipped: source blocked this run")
                results.append(record)
                continue

            print(f"\nChecking: {filename}")
            try:
                record.update(check_file(con, dataset))
                record.update(status="SUCCESS", error="")
                print(f"SUCCESS ({record['check_method']}): {filename}")
                consecutive_blocks = 0
            except Exception as error:
                record.update(status="FAILED", error=str(error))
                print(f"FAILED: {filename}\nReason: {error}")
                if "403" in str(error):
                    consecutive_blocks += 1
                    if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                        print("\nSource is blocking requests (HTTP 403). "
                              "Stopping now; re-run later and finished files will be skipped.")
                        stopped = True

            results.append(record)
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)  # save progress
            time.sleep(WAIT_BETWEEN_FILES)

        df = pd.DataFrame(results)
        print("\n========== DATA QUALITY SUMMARY ==========")
        print(df.drop(columns="error").to_string(index=False))
        print("\nSuccess:", (df["status"] == "SUCCESS").sum(), "/", len(df))
        print(f"Saved: {OUTPUT_FILE}")
    finally:
        con.close()


if __name__ == "__main__":
    import sys
    run_quality_checks(force="--force" in sys.argv)