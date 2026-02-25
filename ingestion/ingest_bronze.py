#"""
ingest_bronze.py
Loads raw CSVs into DuckDB bronze layer.
No transforms here - just raw data + audit metadata.
"""

import duckdb
import pandas as pd
import os
from datetime import datetime

DB_PATH = "../kommineni_automotive.duckdb"
RAW_DATA_PATH = "../data/raw"


def get_connection():
    """Connect to DuckDB, creates file if missing."""
def get_connection():
    """Connect to DuckDB. Creates the file if it does not exist."""
    return duckdb.connect(DB_PATH)


def create_bronze_schema(conn):
    """
    Create the bronze schema if it does not exist.
    
    A schema is like a folder inside the database.
    We have three schemas: bronze, silver, gold.
    Bronze = raw data exactly as it came from source.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    print("Bronze schema ready.")


def load_table(conn, table_name, csv_file):
    """
    Load a single CSV file into a bronze table.
    
    We use CREATE OR REPLACE so every time this runs
    it refreshes the data. This simulates a full reload
    which is how many ingestion tools like Fivetran work.
    """
    file_path = os.path.join(RAW_DATA_PATH, csv_file)
    
    if not os.path.exists(file_path):
        print(f"  WARNING: {csv_file} not found, skipping.")
        return 0
    
    # This is the magic of DuckDB - it can read CSV files
    # directly with a single SQL statement
    # read_csv_auto means DuckDB figures out the column types automatically
    conn.execute(f"""
        CREATE OR REPLACE TABLE bronze.{table_name} AS
        SELECT 
            *,
            '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' AS _ingested_at,
            '{csv_file}' AS _source_file
        FROM read_csv_auto('{file_path}')
    """)
    
    # Count how many rows were loaded
    count = conn.execute(
        f"SELECT COUNT(*) FROM bronze.{table_name}"
    ).fetchone()[0]
    
    return count


def verify_bronze(conn):
    """
    After loading, verify all tables exist and show row counts.
    This is data quality checking at the ingestion layer.
    """
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
    
    # Connect to DuckDB
    conn = get_connection()
    
    # Create bronze schema
    create_bronze_schema(conn)
    
    # Define which CSV maps to which table name
    tables_to_load = {
        "locations":          "locations.csv",
        "employees":          "employees.csv",
        "vehicles":           "vehicles.csv",
        "sales_transactions": "sales_transactions.csv",
        "service_jobs":       "service_jobs.csv"
    }
    
    # Load each table
    print("Loading tables into bronze layer...")
    for table_name, csv_file in tables_to_load.items():
        count = load_table(conn, table_name, csv_file)
        print(f"  Loaded bronze.{table_name}: {count} rows")
    
    # Verify everything loaded correctly
    verify_bronze(conn)
    
    # Close the connection
    conn.close()
    
    print("\nBronze ingestion complete!")
    print(f"Database saved at: {DB_PATH}")


if __name__ == "__main__":
    main()