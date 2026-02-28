"""
Fetch a single PO by number from BigQuery, clean/format, and export to CSV.
Usage: python run_po_by_number.py [PO_NUMBER]
Example: python run_po_by_number.py PO12060
"""
from __future__ import annotations

import pathlib
import sys

from google.cloud import bigquery

from po_export_utils import clean_po_dataframe


PROJECT_ID = "gtm-analytics-447201"
SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_number.sql"


def main() -> None:
    po_number = (sys.argv[1] if len(sys.argv) > 1 else "PO12060").strip().upper()
    if not po_number.startswith("PO"):
        po_number = f"PO{po_number}"

    client = bigquery.Client(project=PROJECT_ID)
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")
    query_text = query_text.replace("'PO12060'", f"'{po_number}'")

    print(f"Fetching {po_number}...")
    df = client.query(query_text).to_dataframe()
    if df.empty:
        print(f"No data found for {po_number}.")
        return

    print(f"Rows (lines): {len(df)}")
    df = clean_po_dataframe(df)
    out_csv = pathlib.Path(__file__).resolve().parent / f"po_{po_number.lower()}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"Exported to: {out_csv}")
    print()
    print(df.to_string())


if __name__ == "__main__":
    main()
