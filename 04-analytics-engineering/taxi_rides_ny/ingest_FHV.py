import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

def download_and_convert_files():
    data_dir = Path("data") / 'fhv'
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020, 2021]:
        for month in range(1, 13):
            if year == 2021 and month > 7:
                continue

            parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            # Check if parquet file exists and is valid
            if parquet_filepath.exists():
                # Verify the parquet file is valid
                try:
                    con_test = duckdb.connect()
                    con_test.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_filepath.as_posix()}')")
                    con_test.close()
                    print(f"Skipping {parquet_filename} (already exists and valid)")
                    continue
                except Exception as e:
                    print(f"Found corrupted {parquet_filename}, will re-download: {e}")
                    parquet_filepath.unlink()  # Delete corrupted file

            # Download CSV.gz file
            csv_gz_filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            print(f"Downloading {csv_gz_filename}...")
            response = requests.get(f"{BASE_URL}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto(
                        '{csv_gz_filepath.as_posix()}',
                        strict_mode=false,
                        ignore_errors=true
                    )
                )
                TO '{parquet_filepath.as_posix()}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')

if __name__ == "__main__":
    # Update .gitignore to exclude data directory
    update_gitignore()

    download_and_convert_files()

    print("Creating DuckDB table...")
    con = duckdb.connect("fhv.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    con.execute(f"""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)

    con.close()
    print("Done!")