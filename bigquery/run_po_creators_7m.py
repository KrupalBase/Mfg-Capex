"""
Fetch POs created by listed team members in the last 7 months.
Cleans data and exports CSV without: date_approve, project_analytic_id,
assigned_project_id, dest_address_id, origin, currency_id, company_id,
po_updated_date, po_created_date.
"""
from __future__ import annotations

import pathlib

from google.cloud import bigquery

from po_export_utils import clean_po_dataframe


PROJECT_ID = "gtm-analytics-447201"
SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_creators_last_7m.sql"
OUT_CSV = pathlib.Path(__file__).resolve().parent / "po_creators_last_7m.csv"

COLUMNS_TO_DROP = [
    "date_approve",
    "project_analytic_id",
    "assigned_project_id",
    "dest_address_id",
    "origin",
    "currency_id",
    "company_id",
    "po_updated_date",
    "po_created_date",
]


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")

    print("Running POs by creators (last 7 months)...")
    df = client.query(query_text).to_dataframe()
    print(f"Rows: {len(df)}")
    if df.empty:
        print("No POs found.")
        return

    df = clean_po_dataframe(df)
    for col in COLUMNS_TO_DROP:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Exported to: {OUT_CSV}")
    print("Columns:", list(df.columns))


if __name__ == "__main__":
    main()
