from pathlib import Path

# Project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
CURATED_DIR = BASE_DIR / "data" / "curated"
REPORTS_DIR = BASE_DIR / "reports"

# Dataset period
YEAR = 2024
MONTHS = range(1, 7)

# Original NYC TLC source
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Taxi types and original filename prefixes
TAXI_TYPES = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "fhvhv": "fhvhv_tripdata"
}

# Generate all dataset URLs
DATASETS = []

for taxi_type, prefix in TAXI_TYPES.items():
    for month in MONTHS:
        filename = f"{prefix}_{YEAR}-{month:02d}.parquet"

        DATASETS.append({
            "taxi_type": taxi_type,
            "year": YEAR,
            "month": month,
            "filename": filename,
            "url": f"{BASE_URL}/{filename}",
            "output_path": RAW_DIR / filename
        })

# Create required directories
for directory in [
    RAW_DIR,
    PROCESSED_DIR,
    CURATED_DIR,
    REPORTS_DIR
]:
    directory.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    print("NYC Taxi Dataset Configuration")
    print("-" * 40)
    print(f"Year: {YEAR}")
    print(f"Total datasets: {len(DATASETS)}")

    for dataset in DATASETS:
        print(
            dataset["taxi_type"],
            dataset["filename"]
        )