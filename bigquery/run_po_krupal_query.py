"""
Run the "POs by Krupal Patel" BigQuery query and print or export results.
Uses Application Default Credentials (run: gcloud auth application-default login).
Exports cleaned/formatted CSV (project names, dates, numbers, no HTML in notes).
"""
from __future__ import annotations

import os
import pathlib

from google.cloud import bigquery

from po_export_utils import clean_po_dataframe


DEFAULT_ODOO_SOURCE_PROJECT = "gtm-analytics-447201"
DEFAULT_ODOO_SOURCE_DATASET = "odoo_public"
ODOO_SOURCE_PROJECT = os.environ.get("ODOO_SOURCE_PROJECT", DEFAULT_ODOO_SOURCE_PROJECT)
ODOO_SOURCE_DATASET = os.environ.get("ODOO_SOURCE_DATASET", DEFAULT_ODOO_SOURCE_DATASET)
QUERY_PROJECT = os.environ.get("BQ_QUERY_PROJECT", ODOO_SOURCE_PROJECT)
SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_krupal_patel.sql"


def main() -> None:
    client = bigquery.Client(project=QUERY_PROJECT)
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")
    query_text = query_text.replace("{odoo_source}", f"{ODOO_SOURCE_PROJECT}.{ODOO_SOURCE_DATASET}")

    print(f"Running PO-by-Krupal-Patel query on {ODOO_SOURCE_PROJECT}.{ODOO_SOURCE_DATASET}...")
    df = client.query(query_text).to_dataframe()
    print(f"Rows: {len(df)}")
    if df.empty:
        print("No POs found for Krupal Patel (check res_users/res_partner name match).")
        return
    df = clean_po_dataframe(df)
    print()
    print(df.to_string())

    out_csv = pathlib.Path(__file__).resolve().parent / "po_krupal_patel.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\nExported to: {out_csv}")


if __name__ == "__main__":
    main()
