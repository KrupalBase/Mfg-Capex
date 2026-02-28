"""
Run a BigQuery query against gtm-analytics-447201.odoo_public.
Uses Application Default Credentials (run: gcloud auth application-default login).
"""
from __future__ import annotations

from google.cloud import bigquery


PROJECT_ID = "gtm-analytics-447201"
DATASET = "odoo_public"
TABLE = "account_account"
LIMIT = 1000


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
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
