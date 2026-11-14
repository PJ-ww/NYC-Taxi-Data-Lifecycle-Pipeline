
import duckdb
import logging
from config.datasets import DATASETS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("reports/ingestion.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def test_all_datasets():
    con = duckdb.connect()

    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        results = []

        for dataset in DATASETS:
            taxi_type = dataset["taxi_type"]
            filename = dataset["filename"]
            url = dataset["url"]

            try:
                row = con.execute(f"""
                    SELECT COUNT(*) AS total_rows
                    FROM read_parquet('{url}')
                """).fetchone()

                total_rows = row[0]

                results.append({
                    "taxi_type": taxi_type,
                    "filename": filename,
                    "total_rows": total_rows,
                    "status": "SUCCESS"
                })

                logging.info(
                    f"SUCCESS | {filename} | {total_rows:,} rows"
                )

            except Exception as error:
                results.append({
                    "taxi_type": taxi_type,
                    "filename": filename,
                    "total_rows": 0,
                    "status": "FAILED"
                })

                logging.error(f"FAILED | {filename} | {error}")

        print("\n========== DATASET CHECK RESULTS ==========")

        for result in results:
            print(
                f"{result['status']:7} | "
                f"{result['taxi_type']:6} | "
                f"{result['filename']} | "
                f"{result['total_rows']:,} rows"
            )

        success = sum(
            result["status"] == "SUCCESS"
            for result in results
        )

        print("\nTotal datasets:", len(results))
        print("Successful:", success)
        print("Failed:", len(results) - success)

    finally:
        con.close()


if __name__ == "__main__":
    test_all_datasets()