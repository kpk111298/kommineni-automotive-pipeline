"""
ingest_bronze.py
Loads raw CSVs into DuckDB bronze layer.
No transforms here - just raw data + audit metadata.
"""

import duckdb
import pandas as pd
import os
from datetime import datetime

DB_PATH = "kommineni_automotive.duckdb"
RAW_DATA_PATH = "../data/raw"


def get_connection():
    """Connect to DuckDB. Creates the file if it does not exist."""
    return duckdb.connect(DB_PATH)


def create_bronze_schema(conn):
    """Create the bronze schema if it does not exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    print("Bronze schema ready.")


def load_table(conn, table_name, csv_file):
    """Load a single CSV file into a bronze table."""
    file_path = os.path.join(RAW_DATA_PATH, csv_file)

    if not os.path.exists(file_path):
        print(f"  WARNING: {file_path} not found, skipping.")
        return 0

    df = pd.read_csv(file_path)
    df["_ingested_at"] = datetime.now()
    df["_source_file"] = csv_file

    conn.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
    conn.execute(f"CREATE TABLE bronze.{table_name} AS SELECT * FROM df")

    count = conn.execute(
        f"SELECT COUNT(*) FROM bronze.{table_name}"
    ).fetchone()[0]

    return count


def verify_bronze(conn):
    """Verify all tables exist and show row counts."""
    print("\nBronze Layer Verification:")
    print("-" * 40)

    tables = [
        "locations",
        "employees",
        "vehicles",
        "sales_transactions",
        "service_jobs"
    ]

    for table in tables:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM bronze.{table}"
            ).fetchone()[0]
            print(f"  bronze.{table}: {count} rows")
        except Exception as e:
            print(f"  bronze.{table}: ERROR - {e}")

    print("-" * 40)
    
def main():
    print("Kommineni Automotive - Bronze Ingestion Starting...")
    print(f"Database: {DB_PATH}")
    print(f"Source:   {RAW_DATA_PATH}")
    print("")

    conn = get_connection()
    create_bronze_schema(conn)

    tables_to_load = {
        "locations":          "locations.csv",
        "employees":          "employees.csv",
        "vehicles":           "vehicles.csv",
        "sales_transactions": "sales_transactions.csv",
        "service_jobs":       "service_jobs.csv"
    }

    print("Loading tables into bronze layer...")
    for table_name, csv_file in tables_to_load.items():
        count = load_table(conn, table_name, csv_file)
        print(f"  Loaded bronze.{table_name}: {count} rows")

    verify_bronze(conn)
    conn.close()

    print("\nBronze ingestion complete!")
    print(f"Database saved at: {DB_PATH}")


if __name__ == "__main__":
    main()
# refreshed
# refreshed
