
import duckdb
import pandas as pd

from config.datasets import DATASETS

def profile_datasets():
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    results = []

    try:
        for dataset in DATASETS:
            taxi_type = dataset["taxi_type"]
            filename = dataset["filename"]
            url = dataset["url"]

            print(f"Profiling: {filename}")

            row_count = con.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('{url}')
            """).fetchone()[0]

            column_info = con.execute(f"""
                DESCRIBE
                SELECT *
                FROM read_parquet('{url}')
            """).fetchdf()

            results.append({
                "taxi_type": taxi_type,
                "filename": filename,
                "row_count": row_count,
                "column_count": len(column_info)
            })

        df = pd.DataFrame(results)

        df.to_csv(
            "reports/data_profile.csv",
            index=False
        )

        print("\n========== DATA PROFILE ==========")
        print(df.to_string(index=False))
        print("\nSaved: reports/data_profile.csv")

    finally:
        con.close()


if __name__ == "__main__":
    profile_datasets()