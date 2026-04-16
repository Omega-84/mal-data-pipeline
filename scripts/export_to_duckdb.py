"""
Export mart tables from BigQuery to a local DuckDB file.
Run this before tearing down GCP infrastructure.

Usage:
    python scripts/export_to_duckdb.py
"""

import os
from pathlib import Path

import duckdb
from google.cloud import bigquery

PROJECT_ID = "de-zoomcamp-485104"
DATASET = "mal_pipeline"
DB_PATH = Path(__file__).parent.parent / "data" / "mal.duckdb"

TABLES = [
    "mart_anime",
    "mart_episodes",
    "mart_characters",
]


def main():
    bq = bigquery.Client(project=PROJECT_ID)
    con = duckdb.connect(str(DB_PATH))

    for table in TABLES:
        print(f"Exporting {table}...")
        df = bq.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table}`").to_dataframe()
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        print(f"  {len(df):,} rows written")

    con.close()
    print(f"\nDone. DuckDB saved to {DB_PATH}")


if __name__ == "__main__":
    main()
