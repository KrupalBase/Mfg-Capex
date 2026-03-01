"""
Run a BigQuery query against Odoo source dataset.
Uses Application Default Credentials (run: gcloud auth application-default login).
"""
from __future__ import annotations

import os

from google.cloud import bigquery


DEFAULT_ODOO_SOURCE_PROJECT = "gtm-analytics-447201"
DEFAULT_ODOO_SOURCE_DATASET = "odoo_public"
PROJECT_ID = os.environ.get("ODOO_SOURCE_PROJECT", DEFAULT_ODOO_SOURCE_PROJECT)
DATASET = os.environ.get("ODOO_SOURCE_DATASET", DEFAULT_ODOO_SOURCE_DATASET)
QUERY_PROJECT = os.environ.get("BQ_QUERY_PROJECT", PROJECT_ID)
TABLE = "account_account"
LIMIT = 1000


def main() -> None:
    client = bigquery.Client(project=QUERY_PROJECT)
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        LIMIT {LIMIT}
    """
    print(f"Running: {query.strip()}")
    print()
    df = client.query(query).to_dataframe()
    print(df.to_string())


if __name__ == "__main__":
    main()
