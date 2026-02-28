"""
CAPEX Pipeline -- single entry point for the entire data refresh cycle.

Usage:
    python capex_pipeline.py              # full refresh (re-queries BigQuery)
    python capex_pipeline.py --skip-bq    # skip BigQuery, reprocess existing CSV
    python capex_pipeline.py --dashboard  # run pipeline then launch dashboard
    python capex_pipeline.py --review     # run pipeline then launch review UI
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

import storage_backend as store
from mfg_subcategory import classify_dataframe as classify_mfg_subcategories
from po_export_utils import (
    apply_overrides,
    auto_map_stations,
    classify_item_bucket,
    classify_line_type,
    clean_po_dataframe,
    extract_part_numbers,
    load_and_normalize_ramp,
    load_bf1_stations,
    merge_section_headers,
    split_product_category,
    tag_capex_flag,
)

PROJECT_ID = "gtm-analytics-447201"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = store.local_data_dir()
SQL_FILE = BASE_DIR / "po_by_creators_last_7m.sql"
RAMP_CSV = BASE_DIR.parent / "Ramp" / "Ramp Data Andy Org.csv"
EXCEL_FILE = BASE_DIR.parent / "Gen3 Mfg BF1.xlsx"

COLUMNS_TO_DROP = [
    "date_approve", "project_analytic_id", "assigned_project_id",
    "dest_address_id", "origin", "currency_id", "company_id",
    "po_updated_date", "po_created_date",
]


def _step(num: int, msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  Step {num}: {msg}")
    print(f"{'='*60}")


RAMP_ACCOUNTING_SQL = """
SELECT
  payment_state,
  COUNT(*) AS entry_count,
  SUM(ABS(amount_total_signed)) AS total_amount,
  SUM(GREATEST(ABS(amount_total_signed) - ABS(amount_residual_signed), 0)) AS amount_paid,
  SUM(ABS(amount_residual_signed)) AS amount_open
FROM `gtm-analytics-447201.odoo_public.account_move`
WHERE x_para_ramp_external_id IS NOT NULL
  AND move_type IN ('in_invoice', 'in_refund')
  AND IFNULL(_fivetran_deleted, FALSE) = FALSE
  AND state = 'posted'
GROUP BY payment_state
""".strip()


def step1_pull_bigquery() -> pd.DataFrame:
    """Pull fresh Odoo PO data from BigQuery."""
    _step(1, "Pull fresh Odoo data from BigQuery")
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")

    print("  Executing query...")
    df = client.query(query_text).to_dataframe()
    print(f"  Rows pulled: {len(df)}")

    raw_path = store.write_csv("po_creators_last_7m.csv", df)
    print(f"  Saved raw: {raw_path}")

    _pull_ramp_accounting(client)

    return df


def _pull_ramp_accounting(client: "bigquery.Client") -> None:
    """Pull Ramp-linked accounting entries from account_move and save summary."""
    print("  Pulling Ramp accounting data (account_move)...")
    rows = client.query(RAMP_ACCOUNTING_SQL).to_dataframe()

    summary: dict[str, float | int] = {
        "available": True,
        "total_entries": 0,
        "total_amount": 0.0,
        "amount_paid": 0.0,
        "amount_open": 0.0,
        "by_state": {},
    }
    for _, r in rows.iterrows():
        state = str(r["payment_state"] or "unknown")
        count = int(r["entry_count"])
        total = float(r["total_amount"] or 0)
        paid = float(r["amount_paid"] or 0)
        amount_open = float(r["amount_open"] or 0)
        summary["total_entries"] += count
        summary["total_amount"] += total
        summary["amount_paid"] += paid
        summary["amount_open"] += amount_open
        summary["by_state"][state] = {
            "count": count,
            "amount": total,
            "paid": paid,
            "open": amount_open,
        }

    total = summary["total_amount"]
    summary["paid_pct"] = round(summary["amount_paid"] / total * 100, 1) if total else 0.0

    dest = store.write_json("ramp_accounting.json", summary)
    print(f"  Ramp accounting: {summary['total_entries']} entries, "
          f"${summary['total_amount']:,.0f} total, "
          f"${summary['amount_paid']:,.0f} paid ({summary['paid_pct']}%), "
          f"${summary['amount_open']:,.0f} open -> {dest}")


def step1_load_existing() -> pd.DataFrame:
    """Load existing Odoo CSV (skip BigQuery)."""
    _step(1, "Load existing Odoo CSV (--skip-bq)")
    df = store.read_csv("po_creators_last_7m.csv")
    if df.empty:
        old_path = BASE_DIR / "po_creators_last_7m.csv"
        if old_path.exists():
            print(f"  Migrating from {old_path}")
            df = pd.read_csv(old_path, encoding="utf-8-sig")
            store.write_csv("po_creators_last_7m.csv", df)
            return df
        print("  ERROR: No existing CSV found. Run without --skip-bq first.")
        sys.exit(1)
    print(f"  Loaded: {len(df)} rows")
    return df


def step2_load_ramp() -> pd.DataFrame:
    """Load and normalize Ramp CC data."""
    _step(2, "Load + filter Ramp CSV")
    if not RAMP_CSV.exists():
        print(f"  WARNING: Ramp CSV not found at {RAMP_CSV}, skipping.")
        return pd.DataFrame()
    ramp = load_and_normalize_ramp(RAMP_CSV)
    print(f"  Ramp rows after filter: {len(ramp)}")
    return ramp


def step3_load_stations() -> tuple[list[dict], list[dict]]:
    """Load BF1 stations from Excel."""
    _step(3, "Load BF1 stations from Excel")
    if not EXCEL_FILE.exists():
        print(f"  WARNING: Excel not found at {EXCEL_FILE}.")
        cached = store.read_json("bf1_stations.json")
        if isinstance(cached, dict):
            stations = cached.get("stations", [])
            cost_breakdown = cached.get("cost_breakdown", [])
            if isinstance(stations, list) and isinstance(cost_breakdown, list) and stations:
                print(f"  Using cached bf1_stations.json ({len(stations)} stations, {len(cost_breakdown)} cost rows).")
                return stations, cost_breakdown
        print("  No cached station metadata found; station mapping and forecast seeding will be empty.")
        return [], []
    stations, cost_breakdown = load_bf1_stations(EXCEL_FILE)
    print(f"  Stations: {len(stations)}, Cost breakdown rows: {len(cost_breakdown)}")

    store.write_json("bf1_stations.json", {"stations": stations, "cost_breakdown": cost_breakdown})
    return stations, cost_breakdown


def step4_clean_odoo(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Odoo data: format, split categories, merge headers, extract parts."""
    _step(4, "Clean Odoo (split categories, merge headers, extract part numbers)")
    df = clean_po_dataframe(df)
    for col in COLUMNS_TO_DROP:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df["source"] = "odoo"
    df = split_product_category(df)
    df = merge_section_headers(df)
    df["part_numbers"] = df["item_description"].apply(extract_part_numbers)
    print(f"  Odoo rows after cleaning: {len(df)}")
    return df


def step5_normalize_ramp(ramp: pd.DataFrame) -> pd.DataFrame:
    """Ensure Ramp has all needed columns and part numbers."""
    _step(5, "Normalize Ramp into Odoo schema")
    if ramp.empty:
        return ramp
    ramp["part_numbers"] = "[]"
    ramp["line_type"] = "spend"
    print(f"  Ramp rows: {len(ramp)}")
    return ramp


def step6_concatenate(odoo: pd.DataFrame, ramp: pd.DataFrame) -> pd.DataFrame:
    """Concatenate Odoo + Ramp into unified DataFrame."""
    _step(6, "Concatenate Odoo + Ramp")
    for col in odoo.columns:
        if pd.api.types.is_integer_dtype(odoo[col]):
            odoo[col] = odoo[col].astype("object")
        elif pd.api.types.is_float_dtype(odoo[col]):
            odoo[col] = odoo[col].astype("object")
    unified = pd.concat([odoo, ramp], ignore_index=True, sort=False)
    unified = unified.fillna("")
    print(f"  Unified rows: {len(unified)} (Odoo: {len(odoo)}, Ramp: {len(ramp)})")
    return unified


def step7_map_stations(
    df: pd.DataFrame,
    stations: list[dict],
    cost_breakdown: list[dict],
) -> pd.DataFrame:
    """Run 3-tier station mapping agent."""
    _step(7, "Run 3-tier station mapping agent")
    df = classify_line_type(df)
    df = tag_capex_flag(df)
    if stations:
        df = auto_map_stations(df, stations, cost_breakdown)
    else:
        df["station_id"] = ""
        df["station_name"] = ""
        df["mapping_confidence"] = "none"
        df["mapping_reason"] = "no station data loaded"

    spend = df[df["line_type"] == "spend"]
    for conf in ("high", "medium", "low", "none"):
        count = len(spend[spend["mapping_confidence"] == conf])
        sub = spend[spend["mapping_confidence"] == conf]["price_subtotal"]
        total = pd.to_numeric(sub, errors="coerce").sum()
        print(f"  {conf:>6}: {count:>5} lines  (${total:>14,.2f})")
    return df


def step8_apply_overrides(
    df: pd.DataFrame,
    stations: list[dict],
) -> pd.DataFrame:
    """Apply human corrections from station_overrides.json."""
    _step(8, "Apply station_overrides.json (human corrections)")
    overrides_dict = store.read_json("station_overrides.json")
    if not isinstance(overrides_dict, dict):
        overrides_dict = {}
    df = apply_overrides(df, overrides_dict, stations)
    n = len(overrides_dict)
    if n:
        print(f"  Applied {n} human overrides")
    else:
        print("  No overrides found (first run)")
    return df


def step9_classify_subcategories(df: pd.DataFrame) -> pd.DataFrame:
    """Assign manufacturing sub-categories to each spend line."""
    _step(9, "Classify manufacturing sub-categories")
    df = classify_mfg_subcategories(df)

    spend = df[df["line_type"] == "spend"]
    by_sc = spend.groupby("mfg_subcategory")["price_subtotal"].apply(
        lambda x: pd.to_numeric(x, errors="coerce").sum()
    ).sort_values(ascending=False)
    for sc, total in by_sc.items():
        count = len(spend[spend["mfg_subcategory"] == sc])
        print(f"  {sc:>40}: {count:>5} lines  (${total:>14,.2f})")

    mfg = spend[spend["is_mfg"] == True]
    mfg_total = pd.to_numeric(mfg["price_subtotal"], errors="coerce").sum()
    print(f"\n  Manufacturing spend: ${mfg_total:>14,.2f}")
    return df


def _load_existing_manual_rows() -> pd.DataFrame:
    """Load existing manual rows from capex_clean.csv so re-exports preserve them."""
    existing = store.read_csv("capex_clean.csv")
    if existing.empty or "source" not in existing.columns:
        return pd.DataFrame()
    manual = existing[existing["source"].astype(str) == "manual"].copy()
    if "line_type" in manual.columns:
        manual = manual[manual["line_type"] == "spend"]
    return manual.fillna("")


def _load_forecast_overrides() -> dict[str, float]:
    """Load forecast overrides keyed by normalized station_id."""
    raw = store.read_json("forecast_overrides.json")
    if not isinstance(raw, dict):
        return {}
    overrides: dict[str, float] = {}
    for sid, value in raw.items():
        sid_key = str(sid).strip().upper()
        if not sid_key:
            continue
        try:
            overrides[sid_key] = float(value)
        except (TypeError, ValueError):
            continue
    return overrides


def step10_export(df: pd.DataFrame, stations: list[dict]) -> None:
    """Export all CSVs to data/ directory."""
    _step(10, "Export all CSVs")

    spend = df[df["line_type"] == "spend"].copy()

    confirmed_states = {"purchase", "sent"}
    if "po_state" in spend.columns:
        spend = spend[spend["po_state"].isin(confirmed_states) | (spend["source"] == "ramp")]

    manual_existing = _load_existing_manual_rows()
    if not manual_existing.empty:
        for col in manual_existing.columns:
            if col not in spend.columns:
                spend[col] = ""
        for col in spend.columns:
            if col not in manual_existing.columns:
                manual_existing[col] = ""
        spend = pd.concat([spend, manual_existing[spend.columns]], ignore_index=True, sort=False)
        if "line_id" in spend.columns:
            spend = spend.drop_duplicates(subset=["line_id"], keep="last")
        print(f"  Preserved manual rows: {len(manual_existing)}")

    col_order = [
        "source", "po_number", "date_order", "po_state", "po_invoice_status", "po_receipt_status",
        "vendor_name", "vendor_ref",
        "product_category", "item_description", "is_capex",
        "station_id", "station_name", "mapping_confidence", "mapping_reason", "mapping_status",
        "mfg_subcategory", "subcat_confidence", "subcat_reason", "is_mfg",
        "product_id", "product_qty", "qty_received", "product_uom",
        "price_unit", "price_subtotal", "price_tax", "price_total",
        "bill_count", "bill_amount_total", "bill_amount_paid", "bill_amount_open", "bill_payment_status",
        "project_name", "created_by_name",
        "po_amount_total", "po_notes", "part_numbers", "line_id",
    ]
    available = [c for c in col_order if c in spend.columns]
    extra = [c for c in spend.columns if c not in col_order]
    spend = spend[available + extra]

    clean_dest = store.write_csv("capex_clean.csv", spend)
    print(f"  capex_clean.csv: {len(spend)} rows -> {clean_dest}")

    # --- capex_by_station.csv ---
    station_name_map = {s["station_id"]: s["process_name"] for s in stations}
    station_owner_map = {s["station_id"]: s["owner"] for s in stations}
    station_forecast_map = {s["station_id"]: s["forecasted_cost"] for s in stations}

    mapped = spend[spend["station_id"] != ""].copy()
    mapped["_subtotal"] = pd.to_numeric(mapped["price_subtotal"], errors="coerce").fillna(0)
    mapped["_total"] = pd.to_numeric(mapped["price_total"], errors="coerce").fillna(0)

    if not mapped.empty:
        by_station = mapped.groupby("station_id").agg(
            line_count=("_subtotal", "size"),
            actual_spend=("_subtotal", "sum"),
            actual_with_tax=("_total", "sum"),
            odoo_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "odoo"].sum()),
            ramp_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "ramp"].sum()),
            manual_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "manual"].sum()),
        ).reset_index()
    else:
        by_station = pd.DataFrame(columns=[
            "station_id", "line_count", "actual_spend", "actual_with_tax",
            "odoo_spend", "ramp_spend", "manual_spend",
        ])

    all_sids = set(s["station_id"] for s in stations)
    mapped_sids = set(by_station["station_id"]) if not by_station.empty else set()
    missing = all_sids - mapped_sids
    if missing:
        missing_rows = pd.DataFrame([{
            "station_id": sid, "line_count": 0, "actual_spend": 0,
            "actual_with_tax": 0, "odoo_spend": 0, "ramp_spend": 0, "manual_spend": 0,
        } for sid in missing])
        by_station = pd.concat([by_station, missing_rows], ignore_index=True)

    by_station["station_name"] = by_station["station_id"].map(station_name_map).fillna("")
    by_station["owner"] = by_station["station_id"].map(station_owner_map).fillna("")
    by_station["forecasted_cost"] = by_station["station_id"].map(station_forecast_map).fillna(0)
    forecast_overrides = _load_forecast_overrides()
    if forecast_overrides:
        sid_keys = by_station["station_id"].fillna("").astype(str).str.strip().str.upper()
        applied_rows = 0
        for sid_key, override_value in forecast_overrides.items():
            mask = sid_keys == sid_key
            if mask.any():
                by_station.loc[mask, "forecasted_cost"] = override_value
                applied_rows += int(mask.sum())
        print(f"  Applied forecast overrides: {applied_rows}")
    by_station["variance"] = by_station["actual_spend"] - by_station["forecasted_cost"]
    by_station["variance_pct"] = (
        by_station["variance"] / by_station["forecasted_cost"].replace(0, float("nan")) * 100
    ).round(1).fillna(0)

    by_station = by_station.sort_values("station_id")
    col_order_station = [
        "station_id", "station_name", "owner",
        "forecasted_cost", "actual_spend", "variance", "variance_pct",
        "odoo_spend", "ramp_spend", "manual_spend", "actual_with_tax", "line_count",
    ]
    by_station = by_station[[c for c in col_order_station if c in by_station.columns]]

    station_dest = store.write_csv("capex_by_station.csv", by_station)
    print(f"  capex_by_station.csv: {len(by_station)} stations -> {station_dest}")

    # --- spares_catalog.csv (Odoo + Ramp) ---
    catalog_spend = spend[spend["item_description"] != ""].copy()
    catalog_spend["_subtotal"] = pd.to_numeric(catalog_spend["price_subtotal"], errors="coerce").fillna(0)
    catalog_spend["_qty"] = pd.to_numeric(catalog_spend["product_qty"], errors="coerce").fillna(0)
    catalog_spend["_unit"] = pd.to_numeric(catalog_spend["price_unit"], errors="coerce").fillna(0)

    def _po_or_contact(group: pd.DataFrame) -> str:
        """PO numbers for Odoo rows, contact names for Ramp rows."""
        odoo_pos = sorted(set(
            r["po_number"] for _, r in group.iterrows()
            if r["source"] == "odoo" and r["po_number"]
        ))
        ramp_contacts = sorted(set(
            r["created_by_name"] for _, r in group.iterrows()
            if r["source"] == "ramp" and r.get("created_by_name")
        ))
        parts = []
        if odoo_pos:
            parts.extend(odoo_pos)
        if ramp_contacts:
            parts.extend(f"(Ramp: {c})" for c in ramp_contacts)
        return ", ".join(parts)

    if not catalog_spend.empty:
        spares = catalog_spend.groupby("item_description").agg(
            product_category=("product_category", "first"),
            mfg_subcategory=("mfg_subcategory", "first"),
            mfg_subcategories=("mfg_subcategory", lambda x: ", ".join(sorted(set(s for s in x if str(s).strip())))),
            source=("source", lambda x: ", ".join(sorted(set(x)))),
            vendor_names=("vendor_name", lambda x: ", ".join(sorted(set(x)))),
            station_ids=("station_id", lambda x: ", ".join(sorted(set(s for s in x if s)))),
            total_qty_ordered=("_qty", "sum"),
            avg_unit_price=("_unit", "mean"),
            total_spend=("_subtotal", "sum"),
            last_order_date=("date_order", "max"),
            part_numbers=("part_numbers", "first"),
        ).reset_index()

        po_contact = catalog_spend.groupby("item_description").apply(
            _po_or_contact, include_groups=False,
        ).rename("po_or_contact")
        spares = spares.merge(po_contact, on="item_description", how="left")

        spares["avg_unit_price"] = spares["avg_unit_price"].round(2)
        spares["item_bucket"] = spares.apply(
            lambda r: classify_item_bucket(
                r["item_description"],
                r["product_category"],
                r["avg_unit_price"],
                r["total_spend"],
            ),
            axis=1,
        )
        spares = spares.sort_values("total_spend", ascending=False)

        bucket_counts = spares["item_bucket"].value_counts()
        for bucket, count in bucket_counts.items():
            bucket_spend = spares.loc[spares["item_bucket"] == bucket, "total_spend"].sum()
            print(f"    {bucket:>25}: {count:>4} items  (${bucket_spend:>14,.2f})")

        odoo_only = len(spares[spares["source"] == "odoo"])
        ramp_only = len(spares[spares["source"] == "ramp"])
        both = len(spares[spares["source"].str.contains(",")])
        print(f"    Sources: {odoo_only} odoo-only, {ramp_only} ramp-only, {both} both")
    else:
        spares = pd.DataFrame()

    spares_dest = store.write_csv("spares_catalog.csv", spares)
    print(f"  spares_catalog.csv: {len(spares)} items -> {spares_dest}")


def step11_summary(df: pd.DataFrame) -> None:
    """Print final summary statistics."""
    _step(11, "Summary")
    spend = df[df["line_type"] == "spend"]
    total_sub = pd.to_numeric(spend["price_subtotal"], errors="coerce").sum()

    odoo_count = len(spend[spend["source"] == "odoo"])
    ramp_count = len(spend[spend["source"] == "ramp"])
    manual_count = len(spend[spend["source"] == "manual"])

    auto_mapped = spend[spend["mapping_status"] == "auto"]
    confirmed = spend[spend["mapping_status"] == "confirmed"]
    unmapped = spend[spend["mapping_status"] == "unmapped"]
    non_prod = spend[spend["mapping_reason"].str.contains("non_prod|pilot_npi", case=False, na=False)]

    auto_spend = pd.to_numeric(auto_mapped["price_subtotal"], errors="coerce").sum()
    confirmed_spend = pd.to_numeric(confirmed["price_subtotal"], errors="coerce").sum()
    unmapped_spend = pd.to_numeric(unmapped["price_subtotal"], errors="coerce").sum()
    non_prod_spend = pd.to_numeric(non_prod["price_subtotal"], errors="coerce").sum()

    print(f"""
=== CAPEX Pipeline Complete ===
Total spend lines:    {len(spend):>6}  (${total_sub:>14,.2f})
  Odoo PO lines:      {odoo_count:>6}
  Ramp CC lines:       {ramp_count:>6}
  Manual PO lines:     {manual_count:>6}

Station mapping:
  auto-mapped:         {len(auto_mapped):>6}  (${auto_spend:>14,.2f})
  human-confirmed:     {len(confirmed):>6}  (${confirmed_spend:>14,.2f})
  needs review:        {len(unmapped):>6}  (${unmapped_spend:>14,.2f})
  non_prod/pilot:      {len(non_prod):>6}  (${non_prod_spend:>14,.2f})

Exported to: {DATA_DIR}/
  - capex_clean.csv
  - capex_by_station.csv
  - spares_catalog.csv
""")


def main() -> None:
    parser = argparse.ArgumentParser(description="CAPEX Pipeline")
    parser.add_argument("--skip-bq", action="store_true",
                        help="Skip BigQuery pull, reprocess existing CSV")
    parser.add_argument("--dashboard", action="store_true",
                        help="Launch dashboard after pipeline")
    parser.add_argument("--review", action="store_true",
                        help="Launch review UI after pipeline")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)

    if args.skip_bq:
        odoo_raw = step1_load_existing()
    else:
        odoo_raw = step1_pull_bigquery()

    ramp_raw = step2_load_ramp()
    stations, cost_breakdown = step3_load_stations()
    odoo = step4_clean_odoo(odoo_raw)
    ramp = step5_normalize_ramp(ramp_raw)
    unified = step6_concatenate(odoo, ramp)
    unified = step7_map_stations(unified, stations, cost_breakdown)
    unified = step8_apply_overrides(unified, stations)
    unified = step9_classify_subcategories(unified)
    step10_export(unified, stations)
    step11_summary(unified)

    if args.dashboard:
        print("Launching dashboard on http://localhost:5050 ...")
        subprocess.Popen(
            [sys.executable, str(BASE_DIR / "capex_dashboard.py")],
            cwd=str(BASE_DIR),
        )
    if args.review:
        print("Launching review UI on http://localhost:5051 ...")
        subprocess.Popen(
            [sys.executable, str(BASE_DIR / "station_review_app.py")],
            cwd=str(BASE_DIR),
        )


if __name__ == "__main__":
    main()
