"""
CAPEX Analytics Dashboard -- Flask app with Plotly.js charts and DataTables.

Run: python capex_dashboard.py
Open: http://localhost:5050
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

import storage_backend as store
from auth import get_google_access_token, init_auth

app = Flask(__name__)
init_auth(app)


# ---------------------------------------------------------------------------
# Data loading -- delegates to storage_backend (local or GCS)
# ---------------------------------------------------------------------------

def _load_csv(name: str) -> pd.DataFrame:
    return store.read_csv(name)


def _load_stations_json() -> list[dict]:
    data = store.read_json("bf1_stations.json")
    return data.get("stations", []) if isinstance(data, dict) else []


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

import re as _re

_CELL_TO_MOD = {
    "CELL1": "MOD1",
    "CELL2": "MOD2",
    "CELL3": "MOD3",
}


def _extract_mod(sid: str) -> str:
    """Raw module extraction -- CELL stays as CELL."""
    m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", str(sid))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _extract_line(sid: str) -> str:
    """Nest CELLs under their parent MOD for budget/line grouping.

    BASE1-CELL1 → BASE1-MOD1, BASE1-CELL2 → BASE1-MOD2, etc.
    INV stays as-is.
    """
    m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", str(sid))
    if not m:
        return ""
    base, unit = m.group(1), m.group(2)
    parent = _CELL_TO_MOD.get(unit, unit)
    return f"{base}-{parent}"


# Sentinel labels for non-production buckets in the line filter (must match URL param).
LINE_PILOT_NPI = "Pilot / NPI"
LINE_NON_PROD = "Non-Prod"
LINE_UNMAPPED = "Needs review"


def _apply_line_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows by the ?lines= query param (comma-separated line list).

    Uses _extract_line so filtering by BASE1-MOD1 also includes BASE1-CELL1.
    Includes Pilot/NPI, Non-Prod, and Needs review as selectable lines.
    """
    raw = request.args.get("lines", "")
    if not raw:
        return df
    allowed = {s.strip() for s in raw.split(",") if s.strip()}
    if not allowed:
        return df
    df = df.copy()
    reason = df.get("mapping_reason", pd.Series([""] * len(df))).fillna("").astype(str)
    status = df.get("mapping_status", pd.Series([""] * len(df))).fillna("").astype(str)
    lines = []
    for idx in df.index:
        sid = str(df.at[idx, "station_id"])
        ln = _extract_line(sid)
        if ln:
            lines.append(ln)
        elif status.at[idx] == "pilot_npi" or "pilot_npi" in reason.at[idx]:
            lines.append(LINE_PILOT_NPI)
        elif status.at[idx] == "non_prod" or "non_prod" in reason.at[idx]:
            lines.append(LINE_NON_PROD)
        else:
            lines.append(LINE_UNMAPPED)
    df["_line"] = lines
    return df[df["_line"].isin(allowed)].drop(columns=["_line"])


def _all_lines(df: pd.DataFrame) -> list[str]:
    """Unique lines for the filter: production lines (CELLs rolled into MODs) plus Pilot/NPI, Non-Prod, Needs review.
    Order: non-BASE first (Pilot/NPI, Non-Prod, Needs review), then sorted BASE* lines.
    """
    production = sorted({_extract_line(str(sid)) for sid in df["station_id"] if _extract_line(str(sid))})
    reason = df.get("mapping_reason", pd.Series([""] * len(df))).fillna("").astype(str)
    status = df.get("mapping_status", pd.Series([""] * len(df))).fillna("").astype(str)
    empty_station = df["station_id"].fillna("").astype(str).str.strip() == ""
    has_pilot = ((status == "pilot_npi") | reason.str.contains("pilot_npi", na=False)).any()
    has_non_prod = ((status == "non_prod") | reason.str.contains("non_prod", na=False)).any()
    has_unmapped = (empty_station & (status != "pilot_npi") & ~reason.str.contains("pilot_npi", na=False)
                   & (status != "non_prod") & ~reason.str.contains("non_prod", na=False)).any()
    result = []
    if has_pilot:
        result.append(LINE_PILOT_NPI)
    if has_non_prod:
        result.append(LINE_NON_PROD)
    if has_unmapped:
        result.append(LINE_UNMAPPED)
    result.extend(production)
    return result


DEFAULT_BF1_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1FEEF_ev62ttt-SRZAIG3i824_jHyySuPN0IE_6BNWb0/"
    "edit?gid=657859777#gid=657859777"
)
DEFAULT_BF2_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1ngtchHeES3R3QmffpizlD8l4j3U8LtJO8BYTClqvq3E/"
    "edit?gid=657859777#gid=657859777"
)


def _build_forecasting_rows(by_station: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Build grouped station forecast rows for the Forecasting tab."""
    if by_station.empty:
        return [], []
    work = by_station.copy()
    if "station_id" not in work.columns:
        return [], []

    work["station_id"] = work["station_id"].fillna("").astype(str).str.strip()
    work = work[work["station_id"] != ""]
    if work.empty:
        return [], []

    work["_line"] = work["station_id"].apply(_extract_line)
    work["_line"] = work["_line"].replace("", "Other")
    work["forecasted_cost"] = pd.to_numeric(work.get("forecasted_cost", 0), errors="coerce").fillna(0.0)
    work["actual_spend"] = pd.to_numeric(work.get("actual_spend", 0), errors="coerce").fillna(0.0)
    work["variance"] = work["actual_spend"] - work["forecasted_cost"]
    work["_base"] = work["station_id"].str.extract(r"^(BASE\d+)-", expand=False).fillna("")

    raw_lines = request.args.get("lines", "")
    if raw_lines:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        work = work[work["_line"].isin(allowed)]

    overrides = store.read_json("forecast_overrides.json")
    locked_keys = {str(k).strip().upper() for k in (overrides or {})} if isinstance(overrides, dict) else set()

    work = work.sort_values(["_line", "station_id"])
    rows = [{
        "line": str(r["_line"]),
        "base": str(r["_base"]),
        "station_id": str(r["station_id"]),
        "station_name": str(r.get("station_name", "")),
        "owner": str(r.get("owner", "")),
        "forecasted_cost": float(r["forecasted_cost"]),
        "actual_spend": float(r["actual_spend"]),
        "variance": float(r["variance"]),
        "is_locked": str(r["station_id"]).strip().upper() in locked_keys,
    } for _, r in work.iterrows()]

    grp = work.groupby("_line").agg(
        station_count=("station_id", "size"),
        total_forecast=("forecasted_cost", "sum"),
        total_actual=("actual_spend", "sum"),
    ).reset_index().sort_values("_line")
    groups = [{
        "line": str(r["_line"]),
        "station_count": int(r["station_count"]),
        "total_forecast": float(r["total_forecast"]),
        "total_actual": float(r["total_actual"]),
        "total_variance": float(r["total_actual"] - r["total_forecast"]),
    } for _, r in grp.iterrows()]
    return rows, groups


def _apply_forecast_updates(
    update_values: dict[str, float],
    *,
    update_overrides: bool = True,
    locked_station_ids: set[str] | None = None,
) -> dict[str, object]:
    """Persist forecast updates into overrides JSON and capex_by_station.csv."""
    if not update_values:
        return {
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "locked_skipped_station_ids": [],
        }

    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty or "station_id" not in by_station.columns:
        unmatched = sorted({str(s).strip().upper() for s in update_values.keys() if str(s).strip()})
        return {
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": unmatched,
            "locked_skipped_station_ids": [],
        }

    station_lookup: dict[str, list[int]] = {}
    station_display: dict[str, str] = {}
    for idx, sid in by_station["station_id"].items():
        sid_str = str(sid).strip()
        sid_key = sid_str.upper()
        if not sid_key:
            continue
        station_lookup.setdefault(sid_key, []).append(idx)
        station_display[sid_key] = sid_str

    locked_keys = {str(s).strip().upper() for s in (locked_station_ids or set()) if str(s).strip()}
    updated_keys: set[str] = set()
    unmatched_keys: set[str] = set()
    locked_skipped_keys: set[str] = set()

    for sid, raw_val in update_values.items():
        sid_key = str(sid).strip().upper()
        if not sid_key:
            continue
        if sid_key in locked_keys:
            locked_skipped_keys.add(sid_key)
            continue
        if sid_key not in station_lookup:
            unmatched_keys.add(sid_key)
            continue

        value = float(raw_val)
        idx_list = station_lookup[sid_key]
        actual = pd.to_numeric(by_station.loc[idx_list, "actual_spend"], errors="coerce").fillna(0.0)
        by_station.loc[idx_list, "forecasted_cost"] = value
        by_station.loc[idx_list, "variance"] = actual - value
        if value == 0:
            by_station.loc[idx_list, "variance_pct"] = 0.0
        else:
            by_station.loc[idx_list, "variance_pct"] = ((actual - value) / value * 100).round(1)
        updated_keys.add(sid_key)

    if updated_keys:
        store.write_csv("capex_by_station.csv", by_station)
        if update_overrides:
            overrides = store.read_json("forecast_overrides.json")
            if not isinstance(overrides, dict):
                overrides = {}
            for sid_key in sorted(updated_keys):
                canonical_sid = station_display.get(sid_key, sid_key)
                idx = station_lookup[sid_key][0]
                val = float(by_station.at[idx, "forecasted_cost"])
                overrides[canonical_sid] = val
            store.write_json("forecast_overrides.json", overrides)

    updated_station_ids = [station_display.get(k, k) for k in sorted(updated_keys)]
    unmatched_station_ids = [station_display.get(k, k) for k in sorted(unmatched_keys)]
    locked_skipped_station_ids = [station_display.get(k, k) for k in sorted(locked_skipped_keys)]
    return {
        "updated_count": len(updated_station_ids),
        "updated_station_ids": updated_station_ids,
        "unmatched_station_ids": unmatched_station_ids,
        "locked_skipped_station_ids": locked_skipped_station_ids,
    }


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/modules")
def api_modules():
    """Return the list of production lines for the global filter (CELLs nested under MODs).

    Includes lines from both capex_clean.csv (spend) and capex_by_station.csv
    (forecasted) so that lines with budget but no spend yet still appear.
    """
    df = _load_csv("capex_clean.csv")
    lines = _all_lines(df)
    by_station = _load_csv("capex_by_station.csv")
    if not by_station.empty and "station_id" in by_station.columns:
        budget_lines = {_extract_line(str(sid)) for sid in by_station["station_id"] if _extract_line(str(sid))}
        existing = set(lines)
        for bl in sorted(budget_lines):
            if bl not in existing:
                lines.append(bl)
    return jsonify(lines)


@app.route("/api/summary")
def api_summary():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({})
    df = _apply_line_filter(df)

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_total"] = pd.to_numeric(df["price_total"], errors="coerce").fillna(0)

    total_committed = float(df["_sub"].sum())
    odoo_total = float(df.loc[df["source"] == "odoo", "_sub"].sum())
    ramp_total = float(df.loc[df["source"] == "ramp", "_sub"].sum())
    active_pos = int(df["po_number"].nunique())
    _vendor_col = next((c for c in df.columns if c.strip().lower() == "vendor_name"), None)
    unique_vendors = int(df[_vendor_col].nunique()) if _vendor_col else 0
    odoo_count = int((df["source"] == "odoo").sum())
    ramp_count = int((df["source"] == "ramp").sum())

    by_station = _load_csv("capex_by_station.csv")
    raw_lines = request.args.get("lines", "")
    if raw_lines and not by_station.empty:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        by_station = by_station[by_station["station_id"].apply(_extract_line).isin(allowed)]
    forecasted = float(pd.to_numeric(by_station["forecasted_cost"], errors="coerce").sum()) if "forecasted_cost" in by_station.columns else 0
    variance = total_committed - forecasted
    pct_spent = (total_committed / forecasted * 100) if forecasted else 0

    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    dated = df.dropna(subset=["_date"]).copy()
    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)
    monthly = dated.groupby(["_month", "source"])["_sub"].sum().reset_index()
    monthly_data: dict[str, dict] = {}
    for _, r in monthly.iterrows():
        m = r["_month"]
        if m not in monthly_data:
            monthly_data[m] = {"month": m, "odoo": 0, "ramp": 0}
        monthly_data[m][r["source"]] = float(r["_sub"])
    undated_ramp = float(df.loc[(df["source"] == "ramp") & df["_date"].isna(), "_sub"].sum())
    monthly_list = sorted(monthly_data.values(), key=lambda x: x["month"])

    dated["_line"] = dated["station_id"].apply(_extract_line)
    monthly_by_line: dict[str, dict[str, float]] = {}
    for ln in sorted(set(l for l in dated["_line"] if l)):
        sub = dated[dated["_line"] == ln].groupby("_month")["_sub"].sum()
        monthly_by_line[ln] = {str(m): float(v) for m, v in sub.items()}

    cat_spend = df.groupby("product_category")["_sub"].sum().reset_index()
    cat_spend = cat_spend[cat_spend["product_category"] != ""]
    cat_spend = cat_spend.sort_values("_sub", ascending=False)
    cat_data = [{"category": r["product_category"], "spend": float(r["_sub"])} for _, r in cat_spend.iterrows()]

    subcat_col = "mfg_subcategory" if "mfg_subcategory" in df.columns else None
    subcat_data: list[dict] = []
    mfg_total = 0.0
    non_mfg_total = 0.0
    if subcat_col:
        sc_spend = df[df[subcat_col] != ""].groupby(subcat_col)["_sub"].sum().reset_index()
        sc_spend = sc_spend.sort_values("_sub", ascending=False)
        subcat_data = [{"subcategory": r[subcat_col], "spend": float(r["_sub"])} for _, r in sc_spend.iterrows()]
        is_mfg = df.get("is_mfg")
        if is_mfg is not None:
            mfg_total = float(df.loc[df["is_mfg"] == True, "_sub"].sum())
            non_mfg_total = float(df.loc[df["is_mfg"] != True, "_sub"].sum())

    # Top 15 vendors (use column if present; show "(No name)" for blank so chart has data)
    vendor_col = next((c for c in df.columns if c.strip().lower() == "vendor_name"), None)
    if vendor_col and not df.empty:
        _vn = df[vendor_col].fillna("").astype(str).str.strip().replace("", "(No name)")
        vendor_spend = df.assign(_vn=_vn).groupby("_vn")["_sub"].sum().reset_index().rename(columns={"_vn": "vendor_name"}).sort_values("_sub", ascending=False).head(15)
        vendor_data = [{"vendor": r["vendor_name"], "spend": float(r["_sub"])} for _, r in vendor_spend.iterrows()]
    else:
        vendor_data = []

    conf_counts = df["mapping_confidence"].value_counts().to_dict()

    mapping_detail: dict[str, list] = {}
    for conf_level in ["high", "medium", "low", "none"]:
        sub = df[df["mapping_confidence"] == conf_level].copy()
        if sub.empty:
            continue
        by_proj = sub.groupby("project_name")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        mapping_detail[conf_level] = [
            {"project": r["project_name"] or "(no project)", "spend": float(r["_sub"]), "count": int(sub[sub["project_name"] == r["project_name"]].shape[0])}
            for _, r in by_proj.iterrows()
        ]

    line_data = []
    if not by_station.empty and "station_id" in by_station.columns:
        by_station["_mod"] = by_station["station_id"].apply(lambda s: _extract_line(s) or "Other")
        mod_agg = by_station.groupby("_mod").agg(
            forecasted=("forecasted_cost", lambda x: pd.to_numeric(x, errors="coerce").sum()),
            actual=("actual_spend", lambda x: pd.to_numeric(x, errors="coerce").sum()),
        ).reset_index()
        line_data = [{"line": r["_mod"], "forecasted": float(r["forecasted"]), "actual": float(r["actual"])} for _, r in mod_agg.iterrows()]

    source_compare = {
        "odoo": {"total": odoo_total, "count": odoo_count, "avg": odoo_total / odoo_count if odoo_count else 0},
        "ramp": {"total": ramp_total, "count": ramp_count, "avg": ramp_total / ramp_count if ramp_count else 0},
    }
    odoo_cats = df[df["source"] == "odoo"].groupby("product_category")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(8)
    ramp_cats = df[df["source"] == "ramp"].groupby("product_category")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(8)
    source_compare["odoo_categories"] = [{"cat": r["product_category"] or "(none)", "spend": float(r["_sub"])} for _, r in odoo_cats.iterrows()]
    source_compare["ramp_categories"] = [{"cat": r["product_category"] or "(none)", "spend": float(r["_sub"])} for _, r in ramp_cats.iterrows()]
    if subcat_col:
        odoo_sc = df[df["source"] == "odoo"].groupby(subcat_col)["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        ramp_sc = df[df["source"] == "ramp"].groupby(subcat_col)["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        source_compare["odoo_subcats"] = [{"cat": r[subcat_col], "spend": float(r["_sub"])} for _, r in odoo_sc.iterrows()]
        source_compare["ramp_subcats"] = [{"cat": r[subcat_col], "spend": float(r["_sub"])} for _, r in ramp_sc.iterrows()]

    payment_summary: dict = {"available": False}
    odoo_df = df[df["source"] == "odoo"].copy()
    if not odoo_df.empty and "bill_payment_status" in odoo_df.columns:
        pay_state = odoo_df["bill_payment_status"].fillna("").astype(str).str.strip().replace("", "no_bill")
        paid_spend = float(odoo_df.loc[pay_state == "paid", "_sub"].sum())
        partial_spend = float(odoo_df.loc[pay_state == "partial", "_sub"].sum())
        unpaid_spend = float(odoo_df.loc[pay_state == "unpaid", "_sub"].sum())
        no_bill_spend = float(odoo_df.loc[pay_state == "no_bill", "_sub"].sum())
        mixed_spend = float(odoo_df.loc[pay_state == "mixed", "_sub"].sum())
        billed_spend = paid_spend + partial_spend + unpaid_spend + mixed_spend
        open_spend = unpaid_spend + partial_spend + mixed_spend
        odoo_committed = float(odoo_df["_sub"].sum())
        payment_summary = {
            "available": True,
            "odoo_committed": odoo_committed,
            "paid_spend": paid_spend,
            "partial_spend": partial_spend,
            "unpaid_spend": unpaid_spend,
            "no_bill_spend": no_bill_spend,
            "mixed_spend": mixed_spend,
            "open_spend": open_spend,
            "billed_spend": billed_spend,
            "paid_spend_pct": (paid_spend / odoo_committed * 100) if odoo_committed else 0.0,
            "billed_spend_pct": (billed_spend / odoo_committed * 100) if odoo_committed else 0.0,
        }

    # Spend by employee (use column if present; show "(No name)" for blank so chart has data)
    emp_col = next((c for c in df.columns if c.strip().lower() == "created_by_name"), None)
    if emp_col and not df.empty:
        _en = df[emp_col].fillna("").astype(str).str.strip().replace("", "(No name)")
        emp_agg = df.assign(_en=_en).groupby("_en").agg(
            spend=("_sub", "sum"), count=("_sub", "size"), pos=("po_number", "nunique"),
        ).reset_index().rename(columns={"_en": "created_by_name"}).sort_values("spend", ascending=False).head(15)
        emp_data = [{"name": r["created_by_name"], "spend": float(r["spend"]), "count": int(r["count"]), "pos": int(r["pos"])} for _, r in emp_agg.iterrows()]
    else:
        emp_data = []

    ramp_df = df[df["source"] == "ramp"].copy()
    ramp_payment: dict = {"available": False}
    if not ramp_df.empty:
        ramp_spend = float(ramp_df["_sub"].sum())
        ramp_txn_count = int(ramp_df["po_number"].nunique())
        ramp_payment = {
            "available": True,
            "total_amount": ramp_spend,
            "txn_count": ramp_txn_count,
            "paid_pct": 100.0,
            "card_charged": ramp_spend,
        }

    return jsonify({
        "total_committed": total_committed,
        "odoo_total": odoo_total,
        "ramp_total": ramp_total,
        "forecasted_budget": forecasted,
        "variance": variance,
        "pct_spent": round(pct_spent, 1),
        "active_pos": active_pos,
        "unique_vendors": unique_vendors,
        "monthly_trend": monthly_list,
        "monthly_by_line": monthly_by_line,
        "undated_ramp": undated_ramp,
        "category_spend": cat_data,
        "subcategory_spend": subcat_data,
        "mfg_total": mfg_total,
        "non_mfg_total": non_mfg_total,
        "top_vendors": vendor_data,
        "top_employees": emp_data,
        "mapping_quality": conf_counts,
        "mapping_detail": mapping_detail,
        "budget_vs_actual": line_data,
        "source_compare": source_compare,
        "payment": payment_summary,
        "ramp_payment": ramp_payment,
    })


@app.route("/api/stations")
def api_stations():
    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty:
        return jsonify([])
    raw_lines = request.args.get("lines", "")
    if raw_lines:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        by_station = by_station[by_station["station_id"].apply(_extract_line).isin(allowed)]
    return jsonify(by_station.to_dict(orient="records"))


@app.route("/api/forecasting")
def api_forecasting():
    """Return station rows grouped by line for the Forecasting tab."""
    by_station = _load_csv("capex_by_station.csv")
    rows, groups = _build_forecasting_rows(by_station)
    return jsonify({"rows": rows, "groups": groups})


@app.route("/api/station/<station_id>")
def api_station_detail(station_id: str):
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"lines": [], "vendors": [], "timeline": []})

    sub = df[df["station_id"] == station_id].copy()
    sub["price_subtotal"] = pd.to_numeric(sub["price_subtotal"], errors="coerce").fillna(0)

    lines = sub.to_dict(orient="records")

    vendor_agg = sub.groupby("vendor_name")["price_subtotal"].sum().reset_index()
    vendors = [{"vendor": r["vendor_name"], "spend": float(r["price_subtotal"])} for _, r in vendor_agg.iterrows()]

    sub["_date"] = pd.to_datetime(sub["date_order"], errors="coerce")
    timeline = sub.dropna(subset=["_date"]).sort_values("_date")
    timeline_data = [{
        "date": row["_date"].strftime("%Y-%m-%d"),
        "po": str(row.get("po_number", "")),
        "desc": str(row.get("item_description", ""))[:60],
        "vendor": str(row.get("vendor_name", "")),
        "amount": float(row["price_subtotal"]),
    } for _, row in timeline.iterrows()]

    by_station = _load_csv("capex_by_station.csv")
    station_row = by_station[by_station["station_id"] == station_id]
    meta = station_row.iloc[0].to_dict() if not station_row.empty else {}

    return jsonify({
        "meta": meta,
        "lines": lines[:500],
        "vendors": vendors,
        "timeline": timeline_data,
    })


@app.route("/api/vendors")
def api_vendors():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)

    vendor_agg = df.groupby("vendor_name").agg(
        spend=("_sub", "sum"),
        po_count=("po_number", "nunique"),
        stations=("station_id", lambda x: ", ".join(sorted(set(s for s in x if s)))),
    ).reset_index().sort_values("spend", ascending=False)

    return jsonify(vendor_agg.to_dict(orient="records"))


@app.route("/api/vendor/<vendor_name>")
def api_vendor_detail(vendor_name: str):
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    sub = df[df["vendor_name"] == vendor_name]
    return jsonify(sub.head(500).to_dict(orient="records"))


@app.route("/api/spares")
def api_spares():
    df = _load_csv("spares_catalog.csv")
    if df.empty:
        return jsonify([])
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/transactions")
def api_transactions():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    df = _apply_line_filter(df)
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/timeline")
def api_timeline():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"weekly": [], "monthly_cat": [], "cumulative": [], "monthly_source": []})
    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    dated = df.dropna(subset=["_date"]).copy()

    dated["_week"] = dated["_date"].dt.isocalendar().week.astype(int)
    dated["_year"] = dated["_date"].dt.year
    weekly = dated.groupby(["_year", "_week"]).agg(spend=("_sub", "sum"), count=("_sub", "size")).reset_index()
    weekly["label"] = weekly.apply(lambda r: f"{int(r['_year'])}-W{int(r['_week']):02d}", axis=1)
    weekly_data = [{"week": r["label"], "spend": float(r["spend"]), "count": int(r["count"])} for _, r in weekly.iterrows()]

    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)
    monthly_cat = dated.groupby(["_month", "product_category"])["_sub"].sum().reset_index()
    mc_data = [{"month": r["_month"], "category": r["product_category"], "spend": float(r["_sub"])} for _, r in monthly_cat.iterrows()]

    daily = dated.groupby(dated["_date"].dt.date)["_sub"].sum().sort_index().cumsum()
    cum_data = [{"date": str(d), "cumulative": float(v)} for d, v in daily.items()]

    monthly_src = dated.groupby(["_month", "source"])["_sub"].sum().reset_index()
    ms_data = [{"month": r["_month"], "source": r["source"], "spend": float(r["_sub"])} for _, r in monthly_src.iterrows()]

    msc_data: list[dict] = []
    if "mfg_subcategory" in dated.columns:
        monthly_sc = dated.groupby(["_month", "mfg_subcategory"])["_sub"].sum().reset_index()
        msc_data = [{"month": r["_month"], "subcategory": r["mfg_subcategory"], "spend": float(r["_sub"])} for _, r in monthly_sc.iterrows()]

    undated_spend = float(df.loc[df["_date"].isna(), "_sub"].sum())

    return jsonify({
        "weekly": weekly_data,
        "monthly_cat": mc_data,
        "monthly_subcat": msc_data,
        "cumulative": cum_data,
        "monthly_source": ms_data,
        "undated_spend": undated_spend,
    })


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mfg Budgeting App</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
<style>
:root{--bg:#1A1A1A;--surface:#242422;--surface2:#32312F;--text:#F0EEEB;--muted:#9E9C98;--accent:#B2DD79;--accent-dark:#1A1A1A;--green:#B2DD79;--green-bright:#D0F585;--yellow:#F7C33C;--red:#D1531D;--blue:#048EE5;--border:#3E3D3A;--disabled:#32312F;--secondary:#3E3D3A}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh}
.sidebar{width:230px;background:var(--surface);border-right:1px solid var(--border);flex-shrink:0;position:fixed;height:100vh;overflow-y:auto;display:flex;flex-direction:column}
.sidebar-brand{padding:20px 18px 16px;border-bottom:1px solid var(--border)}
.sidebar-brand h2{font-size:15px;font-weight:700;color:var(--green);letter-spacing:.5px}
.sidebar-brand .sub{font-size:10px;color:var(--muted);margin-top:2px;text-transform:uppercase;letter-spacing:1px}
.nav-item{display:flex;align-items:center;gap:10px;padding:11px 18px;color:var(--muted);text-decoration:none;font-size:13px;cursor:pointer;border-left:3px solid transparent;transition:all .15s}
.nav-item:hover{background:var(--surface2);color:var(--text)}
.nav-item.active{color:var(--green);border-left-color:var(--green);background:rgba(178,221,121,.06);font-weight:600}
.nav-item .icon{font-size:16px;width:20px;text-align:center}
.main{margin-left:230px;flex:1;padding:28px 32px;min-width:0}
.page{display:none;animation:fadeIn .2s ease}
.page.active{display:block}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.page-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}
.page-title{font-size:20px;font-weight:700;color:var(--text)}
.page-subtitle{font-size:12px;color:var(--muted);margin-top:2px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:24px}
.kpi{background:var(--surface);border-radius:10px;padding:18px 20px;border:1px solid var(--border);transition:border-color .15s}
.kpi:hover{border-color:var(--green)}
.kpi .label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600}
.kpi .value{font-size:24px;font-weight:700;margin-top:6px;font-variant-numeric:tabular-nums}
.kpi .sub{font-size:11px;color:var(--muted);margin-top:3px}
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}
.chart-card{background:var(--surface);border-radius:10px;padding:18px;border:1px solid var(--border);overflow:hidden}
.chart-card.full{grid-column:1/-1}
.chart-card h3{font-size:12px;margin-bottom:14px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;font-weight:600}
.filter-bar{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;align-items:center}
.filter-bar select,.filter-bar input{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px 14px;font-size:13px;outline:none;transition:border-color .15s}
.filter-bar select:focus,.filter-bar input:focus{border-color:var(--green)}
.filter-bar label{font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px}
table.dataTable{color:var(--text)!important;background:var(--surface)!important;border-collapse:collapse!important;width:100%!important;font-size:12px!important}
table.dataTable{table-layout:auto!important}
table.dataTable thead th{background:var(--surface2)!important;color:var(--muted)!important;border-bottom:1px solid var(--border)!important;font-size:11px!important;padding:10px 8px!important;text-transform:uppercase;letter-spacing:.3px;font-weight:600;overflow:hidden;min-width:60px}
table.dataTable tbody td{border-bottom:1px solid rgba(62,61,58,.5)!important;padding:8px!important;max-width:350px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
table.dataTable tbody tr:hover{background:rgba(178,221,121,.05)!important}
.dataTables_wrapper .dataTables_filter input{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important;border-radius:6px;padding:6px 10px}
.dataTables_wrapper .dataTables_length select{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important}
.dataTables_wrapper .dataTables_info,.dataTables_wrapper .dataTables_paginate{color:var(--muted)!important;font-size:11px!important}
.dataTables_wrapper .dataTables_paginate .paginate_button{color:var(--muted)!important}
.dataTables_wrapper .dataTables_paginate .paginate_button.current{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;border-radius:4px;font-weight:700}
.dataTables_wrapper .dataTables_paginate .paginate_button:hover{background:var(--surface2)!important;color:var(--text)!important}
dt.buttons-csv{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;font-weight:600!important;border-radius:4px!important}
.station-select{min-width:350px}
.sidebar-footer{padding:16px 18px;margin-top:auto;border-top:1px solid var(--border)}
.btn-refresh{width:100%;padding:10px;background:var(--green);color:var(--accent-dark);border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700;letter-spacing:.3px;transition:all .15s}
.btn-refresh:hover{background:var(--green-bright)}
.btn-refresh:disabled{background:var(--disabled);color:var(--muted);cursor:wait}
.toast{position:fixed;bottom:24px;right:24px;background:var(--green);color:var(--accent-dark);padding:12px 24px;border-radius:8px;font-weight:700;font-size:13px;display:none;z-index:999;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.forecast-input{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;width:110px;font-size:12px;font-variant-numeric:tabular-nums;text-align:right}
.forecast-input:focus{border-color:var(--green);outline:none}
.forecast-save{background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:700;cursor:pointer;margin-left:4px}
.forecast-save:hover{background:var(--green-bright)}
.forecast-saved{color:var(--green);font-size:11px;font-weight:600;margin-left:6px}
.forecast-lock{background:transparent;color:var(--green);border:1px solid var(--green);border-radius:4px;padding:1px 8px;font-size:10px;font-weight:700;cursor:pointer;margin-left:6px}
.forecast-lock:hover{background:rgba(178,221,121,.12)}
.forecast-unlock{background:transparent;color:var(--yellow);border:1px solid var(--yellow);border-radius:4px;padding:1px 8px;font-size:10px;font-weight:700;cursor:pointer;margin-left:6px}
.forecast-unlock:hover{background:rgba(247,195,60,.12)}
.dollar{font-variant-numeric:tabular-nums}
.dollar-positive{color:var(--green)}
.dollar-negative{color:var(--red)}
.drill-panel{background:var(--surface2);border-radius:8px;padding:16px;margin-top:12px;display:none;max-height:300px;overflow-y:auto}
.drill-panel h4{font-size:12px;color:var(--green);margin-bottom:10px;text-transform:uppercase}
.drill-row{display:flex;justify-content:space-between;padding:4px 0;font-size:12px;border-bottom:1px solid rgba(62,61,58,.3)}
.drill-row .dr-name{color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.drill-row .dr-val{color:var(--muted);margin-left:12px;white-space:nowrap}
.source-badge{display:inline-block;padding:2px 8px;border-radius:3px;font-size:10px;font-weight:700;text-transform:uppercase}
.source-badge.odoo{background:rgba(178,221,121,.15);color:var(--green)}
.source-badge.ramp{background:rgba(4,142,229,.15);color:var(--blue)}
table.dataTable tfoot th{padding:4px 4px!important;background:var(--surface2)!important}
table.dataTable tfoot input{width:100%;padding:4px 6px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px;font-size:10px;outline:none;box-sizing:border-box}
table.dataTable tfoot input:focus{border-color:var(--green)}
table.dataTable tfoot input::placeholder{color:var(--muted);font-size:10px}
.dt-resizable thead th{position:relative;overflow:visible!important}
.col-resizer{position:absolute;top:0;right:0;width:14px;height:100%;cursor:col-resize;z-index:3;touch-action:none}
.col-resizer:hover{background:rgba(178,221,121,.18)}
body.col-resize-active{cursor:col-resize;user-select:none}
.asset-mode-btn{background:var(--surface2);color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:4px 12px;font-size:11px;font-weight:600;cursor:pointer;transition:all .15s}
.asset-mode-btn.active{background:rgba(178,221,121,.15);color:var(--green);border-color:var(--green)}
.asset-mode-btn:hover{border-color:var(--green)}
.asset-date{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:3px;padding:2px 4px;width:105px;font-size:10px;cursor:pointer}
.asset-date:focus{border-color:var(--green);outline:none}
.asset-date::-webkit-calendar-picker-indicator{filter:invert(.7)}
.export-heading{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600;margin-bottom:8px}
.export-row{display:flex;gap:6px}
.btn-export{flex:1;padding:8px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;font-size:10px;font-weight:700;cursor:pointer}
.btn-export:hover{border-color:var(--green);color:var(--green)}
.btn-export:disabled{opacity:.6;cursor:wait}
.export-note{margin-top:6px;font-size:10px;color:var(--muted);line-height:1.2;min-height:24px}
.about-hero{background:linear-gradient(135deg,rgba(178,221,121,.14),rgba(4,142,229,.12));border:1px solid var(--border);border-radius:12px;padding:20px 22px;margin-bottom:16px}
.about-hero h3{font-size:18px;color:var(--text);margin-bottom:8px}
.about-hero p{font-size:13px;color:var(--text);line-height:1.5;max-width:980px}
.about-pill-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.about-pill{display:inline-block;padding:5px 10px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.2px}
.about-pill.ok{background:rgba(178,221,121,.2);color:var(--green)}
.about-pill.warn{background:rgba(247,195,60,.18);color:var(--yellow)}
.about-pill.note{background:rgba(4,142,229,.18);color:var(--blue)}
.about-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:16px}
.about-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}
.about-card h4{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
.about-card ul{margin:0;padding-left:18px}
.about-card li{font-size:12px;line-height:1.45;color:var(--text);margin:5px 0}
.about-timeline{display:grid;gap:8px}
.about-step{display:flex;align-items:flex-start;gap:10px;padding:10px 12px;background:var(--surface);border:1px solid var(--border);border-radius:8px}
.about-step .num{width:24px;height:24px;border-radius:50%;background:rgba(178,221,121,.2);color:var(--green);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex-shrink:0}
.about-step .body{font-size:12px;color:var(--text);line-height:1.45}
.about-step .body strong{display:block;color:var(--text);margin-bottom:2px}
.about-rules{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.about-flow{display:grid;gap:10px}
.about-flow-row{display:flex;align-items:stretch;gap:8px;flex-wrap:wrap}
.about-node{min-width:180px;flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-node h5{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.about-node p{font-size:12px;line-height:1.4}
.about-arrow{align-self:center;color:var(--green);font-weight:700;font-size:16px;padding:0 2px}
.about-tech-table{width:100%;border-collapse:collapse}
.about-tech-table th,.about-tech-table td{border-bottom:1px solid var(--border);padding:8px 6px;text-align:left;vertical-align:top}
.about-tech-table th{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px}
.about-tech-table td{font-size:12px;line-height:1.4}
.about-code{display:block;font-family:Consolas,monospace;font-size:11px;color:var(--green);margin-top:3px;word-break:break-word}
.about-metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:16px}
.about-metric{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-metric .k{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px}
.about-metric .v{font-size:18px;font-weight:700;color:var(--text);margin-top:4px}
.about-metric .d{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.3}
.about-score-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.about-score-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px}
.about-score-card h4{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
.about-bar-row{margin:8px 0}
.about-bar-row .lbl{font-size:11px;color:var(--text);margin-bottom:4px}
.about-bar{height:10px;border-radius:999px;background:var(--surface2);overflow:hidden;border:1px solid var(--border)}
.about-bar span{display:block;height:100%;background:linear-gradient(90deg,var(--green),var(--blue))}
.about-journey{display:flex;align-items:stretch;gap:8px;flex-wrap:wrap}
.about-stage{min-width:190px;flex:1;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-stage h5{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.about-stage p{font-size:12px;line-height:1.4}
.about-connector{align-self:center;color:var(--green);font-weight:700;font-size:16px}
.about-details{margin-top:10px;background:var(--surface);border:1px solid var(--border);border-radius:10px}
.about-details summary{cursor:pointer;padding:10px 12px;font-size:12px;color:var(--green);font-weight:700;list-style:none}
.about-details summary::-webkit-details-marker{display:none}
.about-details-body{padding:0 12px 12px}
@media(max-width:1200px){.about-rules{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="sidebar">
    <div class="sidebar-brand"><h2>MFG BUDGETING</h2><div class="sub">Base Power Company</div></div>
    <a class="nav-item active" onclick="showPage('executive',this)"><span class="icon">&#9632;</span> Executive Summary</a>
    <a class="nav-item" onclick="showPage('source',this)"><span class="icon">&#8644;</span> Odoo vs Ramp</a>
    <a class="nav-item" onclick="showPage('stations',this)"><span class="icon">&#9881;</span> Station Drill-Down</a>
    <a class="nav-item" onclick="showPage('forecasting',this)"><span class="icon">&#128202;</span> Forecasting</a>
    <a class="nav-item" onclick="showPage('vendors',this)"><span class="icon">&#9733;</span> Vendor Analysis</a>
    <a class="nav-item" onclick="showPage('assets',this)"><span class="icon">&#9878;</span> Asset Tracking</a>
    <a class="nav-item" onclick="showPage('spares',this)"><span class="icon">&#9776;</span> Materials / Spares</a>
    <a class="nav-item" onclick="showPage('detail',this)"><span class="icon">&#9783;</span> Full Transactions</a>
    <a class="nav-item" onclick="showPage('timeline',this)"><span class="icon">&#9202;</span> Spend Timeline</a>
    <a class="nav-item" onclick="showPage('projects',this)"><span class="icon">&#9670;</span> Other Projects</a>
    <a class="nav-item" onclick="showPage('uniteco',this)"><span class="icon">&#9879;</span> Unit Economics</a>
    <a class="nav-item" onclick="showPage('settings',this)"><span class="icon">&#9881;</span> Settings</a>
    <a class="nav-item" onclick="showPage('about',this)"><span class="icon">&#8505;</span> About This Tool</a>
    <div style="padding:12px 18px;border-top:1px solid var(--border)">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600;margin-bottom:8px">Filter by Line</div>
        <div id="line-filter-checks" style="max-height:180px;overflow-y:auto"></div>
        <div style="margin-top:6px;display:flex;gap:6px">
            <button onclick="toggleAllLines(true)" style="flex:1;padding:4px;background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">All</button>
            <button onclick="toggleAllLines(false)" style="flex:1;padding:4px;background:var(--secondary);color:var(--text);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">None</button>
        </div>
    </div>
    <div class="sidebar-footer">
        <div class="export-heading">Export Current Tab Data</div>
        <div class="export-row">
            <button class="btn-export" id="export-csv-btn" onclick="exportCurrentPage('csv')">CSV</button>
            <button class="btn-export" id="export-xlsx-btn" onclick="exportCurrentPage('xlsx')">Excel</button>
        </div>
        <div class="export-note" id="export-note">Graphs + tables for active tab</div>
    </div>
</div>

<div class="main">

<!-- EXECUTIVE -->
<div class="page active" id="page-executive">
    <div class="page-header"><div><div class="page-title">Executive Summary</div><div class="page-subtitle">BF1 Manufacturing CAPEX Overview</div></div></div>
    <div class="kpi-row" id="kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Budget vs Actual by Module</h3><div id="chart-budget"></div></div>
        <div class="chart-card"><h3>Monthly Spend Trend (Odoo + Ramp)</h3><div id="chart-monthly"></div></div>
        <div class="chart-card"><h3>Spend by Mfg Sub-Category</h3><div id="chart-subcategory"></div></div>
        <div class="chart-card"><h3>Top 15 Vendors</h3><div id="chart-vendors"></div></div>
        <div class="chart-card"><h3>Odoo PO Payment Status</h3><div id="chart-payment-status"></div></div>
        <div class="chart-card"><h3>Ramp CC Payment Status</h3><div id="chart-ramp-payment"></div></div>
        <div class="chart-card full"><h3>Spend by Employee</h3><div id="chart-employees"></div></div>
    </div>
</div>

<!-- ABOUT -->
<div class="page" id="page-about">
    <div class="page-header"><div><div class="page-title">About This Tool</div><div class="page-subtitle">Current-state overview of the CAPEX data pipeline and cleanup rules</div></div></div>

    <div class="about-hero">
        <h3>What this dashboard is doing today</h3>
        <p>
            This dashboard combines manufacturing CAPEX spend from Odoo purchase orders and Ramp card transactions, then applies station mapping, cleanup rules, and forecast logic before showing the final results.
            This page documents the current implementation as it exists now.
        </p>
        <div class="about-pill-row">
            <span class="about-pill warn">Processing model: Batch / Manual run</span>
            <span class="about-pill note">Data window: Last 7 months (Odoo SQL pull)</span>
            <span class="about-pill ok">Outputs: capex_clean, capex_by_station, spares_catalog</span>
        </div>
    </div>

    <div class="about-metric-grid">
        <div class="about-metric"><div class="k">Execution Trigger</div><div class="v">Manual CLI</div><div class="d"><code>python capex_pipeline.py</code> or <code>--skip-bq</code></div></div>
        <div class="about-metric"><div class="k">Freshness Model</div><div class="v">Snapshot-Based</div><div class="d">UI reads exported CSV snapshots, not live source systems.</div></div>
        <div class="about-metric"><div class="k">Real-Time Boundary</div><div class="v">No Streaming Job</div><div class="d">No Pub/Sub, Dataflow, or event trigger in current pipeline path.</div></div>
        <div class="about-metric"><div class="k">Core Output Contracts</div><div class="v">3 Primary Files</div><div class="d"><code>capex_clean.csv</code>, <code>capex_by_station.csv</code>, <code>spares_catalog.csv</code></div></div>
    </div>

    <div class="chart-card full" style="margin-bottom:14px">
        <h3>Block Diagram: End-to-End Data Path</h3>
        <div class="about-flow">
            <div class="about-flow-row">
                <div class="about-node"><h5>Source Layer</h5><p>Odoo via BigQuery SQL<br/>Ramp transaction CSV<br/>BF1/BF2 station metadata + forecast/override JSON</p></div>
                <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Batch Orchestrator</h5><p><code>capex_pipeline.py</code><br/>steps 1..11 executed sequentially</p></div>
                <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Rule Engine</h5><p>Cleanup + type tagging + station mapping + subcategory classification + dedupe + validations</p></div>
                <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Output Artifacts</h5><p>CSV exports + settings/override files written to storage backend (local/GCS)</p></div>
            </div>
            <div class="about-flow-row">
                <div class="about-node"><h5>Consumer Layer</h5><p>Dashboard APIs load exported CSVs<br/>Forecasting tab applies per-station overrides<br/>Review app applies human mapping corrections</p></div>
                <div class="about-arrow">&#8592;</div>
                <div class="about-node"><h5>Feedback Loop</h5><p>Human corrections are persisted and become inputs for next pipeline run.</p></div>
            </div>
        </div>
    </div>

    <div class="chart-card full" style="margin-bottom:14px">
        <h3>Technical Execution Map (Function-Level)</h3>
        <div class="about-timeline">
            <div class="about-step"><div class="num">1</div><div class="body"><strong>Extract</strong>Run <code>step1_pull_bigquery()</code> or <code>step1_load_existing()</code>; SQL source query is <code>po_by_creators_last_7m.sql</code>.</div></div>
            <div class="about-step"><div class="num">2</div><div class="body"><strong>Ingest Ramp</strong><code>step2_load_ramp()</code> uses <code>load_and_normalize_ramp()</code> to map category, shape columns, and generate stable IDs.</div></div>
            <div class="about-step"><div class="num">3</div><div class="body"><strong>Station Metadata</strong><code>step3_load_stations()</code> loads station master/cost rows and falls back to cached JSON if workbook is missing.</div></div>
            <div class="about-step"><div class="num">4</div><div class="body"><strong>Cleanup</strong><code>step4_clean_odoo()</code> applies text/date/amount standardization, category splitting, section merge, and part-number extraction.</div></div>
            <div class="about-step"><div class="num">5</div><div class="body"><strong>Unify + Map</strong><code>step6_concatenate()</code> creates a unified frame; <code>step7_map_stations()</code> assigns line type, CAPEX flags, and station/confidence.</div></div>
            <div class="about-step"><div class="num">6</div><div class="body"><strong>Human Control + Enrichment</strong><code>step8_apply_overrides()</code> enforces human station overrides; <code>step9_classify_subcategories()</code> sets manufacturing subcategories.</div></div>
            <div class="about-step"><div class="num">7</div><div class="body"><strong>Export + Metrics</strong><code>step10_export()</code> writes output contracts and computes station variance; <code>step11_summary()</code> logs run totals and mapping status.</div></div>
        </div>
    </div>

    <div class="about-rules">
        <div class="about-card">
            <h4>Decision Tree: Agent Station Mapping</h4>
            <div class="about-flow">
                <div class="about-flow-row">
                    <div class="about-node"><h5>Gate A</h5><p><strong>Is line_type == spend?</strong><br/>No: no station assignment<br/>Yes: continue</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Gate B</h5><p><strong>Non-prod/pilot?</strong><br/>Route to non_prod/pilot buckets<br/>else continue</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Gate C</h5><p><strong>CIP direct map available?</strong><br/><code>CIP-BF1-*</code> -> direct/prefix station mapping</p></div>
                </div>
                <div class="about-flow-row">
                    <div class="about-node"><h5>Gate D</h5><p><strong>Tier-2 scored ranking</strong><br/>Build candidate set and sort by score; tie-break prefers matching line prefix family.</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Guardrails + Status</h5><p>BASE2 requires explicit BASE2/BF2 project intent; then <code>apply_overrides()</code> sets final status (confirmed/skipped/non_prod/pilot_npi/auto/unmapped).</p></div>
                </div>
            </div>
        </div>

        <div class="about-card">
            <h4>Real-Time Boundary Diagram</h4>
            <div class="about-flow">
                <div class="about-flow-row">
                    <div class="about-node"><h5>Source Change</h5><p>New PO in Odoo or new Ramp transaction</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Pending State</h5><p>No UI update until pipeline run occurs</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Pipeline Run</h5><p>Batch pull + transform + export snapshot files</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>UI Visible</h5><p>Dashboard APIs reflect new snapshot immediately after export</p></div>
                </div>
            </div>
        </div>
    </div>

    <div class="chart-card full" style="margin-top:14px">
        <h3>Scoring and Bucketing Visuals</h3>
        <div class="about-score-grid">
            <div class="about-score-card">
                <h4>Station Mapping Weights</h4>
                <div class="about-bar-row">
                    <div class="lbl">Vendor match (+3)</div>
                    <div class="about-bar"><span style="width:100%"></span></div>
                </div>
                <div class="about-bar-row">
                    <div class="lbl">Project-line match (+2)</div>
                    <div class="about-bar"><span style="width:67%"></span></div>
                </div>
                <div class="about-bar-row">
                    <div class="lbl">Keyword match (+1)</div>
                    <div class="about-bar"><span style="width:34%"></span></div>
                </div>
                <div class="about-pill-row">
                    <span class="about-pill ok">High >= 5</span>
                    <span class="about-pill note">Medium >= 3</span>
                    <span class="about-pill warn">Low >= 1</span>
                </div>
            </div>
            <div class="about-score-card">
                <h4>Subcategory Agent Priority (First Hit Wins)</h4>
                <div class="about-flow-row">
                    <div class="about-node"><h5>1</h5><p>Line override</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>2</h5><p>Vendor rules</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>3</h5><p>Keyword/project/card hints</p></div>
                </div>
                <div class="about-flow-row">
                    <div class="about-node"><h5>4</h5><p>Category fallback</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>5</h5><p>Price heuristic</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>6</h5><p>Default bucket</p></div>
                </div>
            </div>
            <div class="about-score-card">
                <h4>Spares Bucket Priority</h4>
                <div class="about-flow-row">
                    <div class="about-node"><h5>Step A</h5><p>Description regex rules</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Step B</h5><p>Category map fallback</p></div>
                </div>
                <div class="about-flow-row">
                    <div class="about-node"><h5>Step C</h5><p>Capital threshold (>= 50k)</p></div>
                    <div class="about-arrow">&#8594;</div>
                    <div class="about-node"><h5>Step D</h5><p>Parts / Materials default</p></div>
                </div>
            </div>
        </div>
        <details class="about-details">
            <summary>Show technical scoring matrix</summary>
            <div class="about-details-body">
                <table class="about-tech-table">
                    <thead>
                        <tr>
                            <th>Engine</th>
                            <th>Signal / Rule</th>
                            <th>Weight / Priority</th>
                            <th>Implementation Notes</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr><td>Station Mapping</td><td>Vendor signal match</td><td>+3 score</td><td>Fuzzy vendor match from cost-breakdown vendors; constrained by line-family when project maps to a known line.</td></tr>
                        <tr><td>Station Mapping</td><td>Project-to-line signal</td><td>+2 score</td><td>Project mapping adds candidates for allowed line prefixes.</td></tr>
                        <tr><td>Station Mapping</td><td>Keyword signal</td><td>+1 score</td><td>Station keyword map + integration keywords add candidate boosts.</td></tr>
                        <tr><td>Station Mapping</td><td>Confidence band</td><td>high >= 5, medium >= 3, low >= 1, none &lt; 1</td><td>Best-score station wins; tie-break favors matching project line prefix.</td></tr>
                        <tr><td>Subcategory Agent</td><td>Priority chain</td><td>Ordered first-hit wins</td><td>line override -> split vendor -> specialist vendor -> distributor logic -> keywords -> project/card hints -> category fallback -> price heuristic -> default.</td></tr>
                        <tr><td>Spares Bucket</td><td>Bucket priority</td><td>Regex -> category map -> threshold -> default</td><td><code>classify_item_bucket()</code> applies keyword rules first; unmatched rows fall back to category/price/default.</td></tr>
                    </tbody>
                </table>
            </div>
        </details>
    </div>

    <div class="chart-card full" style="margin-top:14px">
        <h3>Concrete Example: Transaction Journey</h3>
        <div class="about-journey">
            <div class="about-stage">
                <h5>Raw Input</h5>
                <p>Odoo line enters with project <code>CIP-BF1-MOD1-ST22000</code>, vendor <code>Precitec</code>, and machinery description.</p>
            </div>
            <div class="about-connector">&#8594;</div>
            <div class="about-stage">
                <h5>Normalize</h5>
                <p>Category/description split and numeric formatting produce clean fields for downstream mapping.</p>
            </div>
            <div class="about-connector">&#8594;</div>
            <div class="about-stage">
                <h5>Map Station</h5>
                <p>CIP direct mapping assigns <code>BASE1-MOD1-ST22000</code> with high confidence.</p>
            </div>
            <div class="about-connector">&#8594;</div>
            <div class="about-stage">
                <h5>Classify</h5>
                <p>Subcategory agent assigns manufacturing subcategory + confidence + reason fields.</p>
            </div>
            <div class="about-connector">&#8594;</div>
            <div class="about-stage">
                <h5>Bucket + Export</h5>
                <p>Bucket logic labels catalog item, then row flows into clean/station/spares outputs.</p>
            </div>
        </div>
        <details class="about-details">
            <summary>Show field-level before/after details</summary>
            <div class="about-details-body">
                <table class="about-tech-table">
                    <thead>
                        <tr><th>Stage</th><th>Input / Rule Trigger</th><th>Output Fields</th></tr>
                    </thead>
                    <tbody>
                        <tr><td>Raw Input</td><td><span class="about-code">project_name=CIP-BF1-MOD1-ST22000</span><span class="about-code">line_description=Non-Inventory: Machinery &gt;$2k TruFiber laser welding unit</span><span class="about-code">vendor_name=Precitec Inc.; price_subtotal=128500</span></td><td>Raw PO line from SQL extract.</td></tr>
                        <tr><td>Normalization</td><td><span class="about-code">split_product_category(); _to_single_line(); _format_currency()</span></td><td><span class="about-code">product_category=Non-Inventory: Machinery &gt;$2k</span><span class="about-code">item_description=TruFiber laser welding unit</span><span class="about-code">price_subtotal=128500.00</span></td></tr>
                        <tr><td>Station Mapping</td><td><span class="about-code">CIP-BF1-* -> BASE1-*</span></td><td><span class="about-code">station_id=BASE1-MOD1-ST22000</span><span class="about-code">mapping_confidence=high</span></td></tr>
                        <tr><td>Subcategory Agent</td><td><span class="about-code">classify_mfg_subcategory()</span></td><td><span class="about-code">mfg_subcategory=Process Equipment</span><span class="about-code">subcat_reason=kw(...) or vendor=...</span></td></tr>
                        <tr><td>Spares Bucketing</td><td><span class="about-code">classify_item_bucket()</span></td><td><span class="about-code">item_bucket=Capital Equipment</span></td></tr>
                        <tr><td>Final Export</td><td><span class="about-code">step10_export()</span></td><td>Row contributes to <code>capex_clean.csv</code>, <code>capex_by_station.csv</code>, and <code>spares_catalog.csv</code>.</td></tr>
                    </tbody>
                </table>
            </div>
        </details>
    </div>

    <div class="chart-card full" style="margin-top:14px">
        <h3>Rules Matrix (Specific Implementation)</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:8px">
            Keep this collapsed for presentation mode; expand when you need exact implementation-level references.
        </p>
        <details class="about-details">
            <summary>Show full rules matrix</summary>
            <div class="about-details-body">
                <table class="about-tech-table">
                    <thead>
                        <tr>
                            <th>Layer</th>
                            <th>Rule</th>
                            <th>Behavior</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>SQL Extract</td>
                            <td>Creator Allowlist + 7-Month Window<span class="about-code">creators CTE; WHERE po.date_order &gt;= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)</span></td>
                            <td>Limits source scope to configured users and recent period only.</td>
                        </tr>
                        <tr>
                            <td>SQL Extract</td>
                            <td>Bill Dedup + Payment Rollup<span class="about-code">bill_links_dedup; bill_status_by_line</span></td>
                            <td>Removes duplicate bill links and computes paid/partial/unpaid/mixed/no_bill states.</td>
                        </tr>
                        <tr>
                            <td>Normalization</td>
                            <td>Text/HTML/Locale Cleanup<span class="about-code">_to_single_line; _strip_html; _extract_en_us_name</span></td>
                            <td>Converts multiline and rich text into clean analysis-ready strings.</td>
                        </tr>
                        <tr>
                            <td>Normalization</td>
                            <td>Money/Qty/Date Standardization<span class="about-code">_format_currency; _format_qty; _format_ts</span></td>
                            <td>Enforces numeric/date consistency across Odoo and Ramp rows.</td>
                        </tr>
                        <tr>
                            <td>Classification</td>
                            <td>Line-Type + CAPEX Flag<span class="about-code">classify_line_type; tag_capex_flag</span></td>
                            <td>Separates true spend lines from non-spend text/terms rows.</td>
                        </tr>
                        <tr>
                            <td>Mapping</td>
                            <td>3-Tier Station Assignment<span class="about-code">auto_map_stations</span></td>
                            <td>Assigns station IDs using deterministic rules and score-based candidate ranking.</td>
                        </tr>
                        <tr>
                            <td>Human Control</td>
                            <td>Override Precedence<span class="about-code">apply_overrides; mapping_status field</span></td>
                            <td>Human-confirmed routing overrides automatic mapping outputs.</td>
                        </tr>
                        <tr>
                            <td>Export Integrity</td>
                            <td>Spend-State Filter + Dedupe<span class="about-code">confirmed_states={'purchase','sent'}; drop_duplicates(subset=['line_id'])</span></td>
                            <td>Keeps financially relevant spend rows and removes duplicate line artifacts.</td>
                        </tr>
                        <tr>
                            <td>Forecast</td>
                            <td>Station Override + Variance Math<span class="about-code">_load_forecast_overrides; variance = actual - forecast</span></td>
                            <td>Applies per-station forecast overrides and recomputes variance metrics.</td>
                        </tr>
                        <tr>
                            <td>Manual Entry</td>
                            <td>Payload Validation + Upsert<span class="about-code">_validate_manual_payload; _upsert_manual_po</span></td>
                            <td>Requires valid required fields/date/numeric values and deterministic manual IDs.</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </details>
    </div>
</div>

<!-- ODOO vs RAMP -->
<div class="page" id="page-source">
    <div class="page-header"><div><div class="page-title">Odoo vs Ramp Comparison</div><div class="page-subtitle">Purchase Orders vs Credit Card spend breakdown</div></div></div>
    <div class="kpi-row" id="source-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Odoo PO - Sub-Categories</h3><div id="chart-odoo-subcats"></div></div>
        <div class="chart-card"><h3>Ramp CC - Sub-Categories</h3><div id="chart-ramp-subcats"></div></div>
        <div class="chart-card"><h3>Odoo PO Billing</h3><div id="chart-src-odoo-billing"></div></div>
        <div class="chart-card"><h3>Ramp CC Accounting</h3><div id="chart-src-ramp-billing"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Source</h3><div id="chart-source-monthly"></div></div>
    </div>
</div>

<!-- STATIONS -->
<div class="page" id="page-stations">
    <div class="page-header"><div><div class="page-title">Station Drill-Down</div><div class="page-subtitle">Select a station to view detailed spend and materials</div></div></div>
    <div class="filter-bar">
        <label>Station:</label>
        <select class="station-select" id="stationSelect" onchange="loadStationDetail()"></select>
    </div>
    <div class="kpi-row" id="station-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Vendor Breakdown</h3><div id="chart-station-vendors"></div></div>
        <div class="chart-card"><h3>Order Timeline</h3><div id="chart-station-timeline"></div></div>
        <div class="chart-card full"><h3>Materials / BOM</h3><div id="station-bom-table"></div></div>
    </div>
</div>

<!-- FORECASTING -->
<div class="page" id="page-forecasting">
    <div class="page-header"><div><div class="page-title">Forecasting</div><div class="page-subtitle">Edit station forecasts grouped by line and refresh from BF1/BF2 Google Sheets</div></div></div>
    <div class="chart-card" style="margin-bottom:12px">
        <h3>Forecast Sources</h3>
        <div style="display:grid;grid-template-columns:120px 1fr;gap:8px;align-items:center">
            <label style="font-size:12px;color:var(--muted)">BF1 Sheet URL</label>
            <input id="forecast-bf1-url" class="forecast-input" style="width:100%;text-align:left" placeholder="https://docs.google.com/spreadsheets/d/..."/>
            <label style="font-size:12px;color:var(--muted)">BF2 Sheet URL</label>
            <input id="forecast-bf2-url" class="forecast-input" style="width:100%;text-align:left" placeholder="https://docs.google.com/spreadsheets/d/..."/>
        </div>
        <div style="margin-top:12px;display:flex;gap:10px;align-items:center">
            <button class="btn-refresh" id="forecast-refresh-btn" style="width:auto;padding:10px 24px" onclick="refreshForecastFromSheets()">Refresh from Sheets</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="reauthGoogleForSheets()">Re-auth Google</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px" onclick="saveForecastingBulk()">Save Forecasts</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="lockAllForecastOverrides()">Lock All</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="unlockAllForecastOverrides()">Unlock All</button>
            <span id="forecast-ok" class="forecast-saved" style="display:none">Forecasts saved</span>
        </div>
        <div id="forecast-refresh-msg" style="margin-top:10px;font-size:12px;color:var(--muted)"></div>
    </div>
    <div id="forecast-table-wrap"></div>
</div>

<!-- VENDORS -->
<div class="page" id="page-vendors">
    <div class="page-header"><div><div class="page-title">Vendor Analysis</div><div class="page-subtitle">Spend concentration and vendor-station relationships</div></div></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Top Vendor Concentration</h3><div id="chart-vendor-conc"></div></div>
        <div class="chart-card"><h3>Vendor-Station Spend (Top 10 Vendors x Top Stations)</h3><div id="chart-vendor-heatmap"></div></div>
        <div class="chart-card full"><h3>All Vendors</h3><div id="vendor-table-wrap"></div></div>
    </div>
</div>

<!-- ASSETS -->
<div class="page" id="page-assets">
    <div class="page-header"><div><div class="page-title">Asset Tracking</div><div class="page-subtitle">Station-level capital asset register — physical equipment on the floor</div></div></div>
    <div class="filter-bar" id="asset-filters">
        <label>Owner:</label><select id="assetOwnerFilter" onchange="filterAssets()"><option value="">All Owners</option></select>
        <label>Status:</label><select id="assetStatusFilter" onchange="filterAssets()"><option value="">All</option><option value="Ordered">Ordered</option><option value="Shipped">Shipped</option><option value="Received">Received</option><option value="Installed">Installed</option><option value="Commissioned">Commissioned</option></select>
        <label>Vendor:</label><select id="assetVendorFilter" onchange="filterAssets()"><option value="">All Vendors</option></select>
        <label>Show:</label>
        <span style="display:inline-flex;gap:2px">
            <button class="asset-mode-btn active" id="assetModeAsset" onclick="setAssetMode('asset')">Asset Value</button>
            <button class="asset-mode-btn" id="assetModeTotal" onclick="setAssetMode('total')">Total Investment</button>
        </span>
    </div>
    <div id="asset-subcat-chips" style="margin-bottom:8px;display:flex;flex-wrap:wrap;gap:6px"></div>
    <div class="kpi-row" id="asset-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Station Investment</h3><div id="chart-asset-bars"></div></div>
        <div class="chart-card"><h3>Station Status</h3><div id="chart-asset-delivery"></div></div>
        <div class="chart-card"><h3>Spend Composition</h3><div id="chart-asset-composition"></div></div>
    </div>
    <div class="chart-card full" style="margin-top:12px"><h3>Station Asset Register</h3><div id="asset-table-wrap"></div></div>
</div>

<!-- SPARES -->
<div class="page" id="page-spares">
    <div class="page-header"><div><div class="page-title">Materials / Spares Catalog</div><div class="page-subtitle">Deduplicated items with part numbers and sourcing info</div></div></div>
    <div class="filter-bar" id="spares-filters">
        <label>Bucket:</label><select id="sparesBucketFilter" onchange="filterSpares()"><option value="">All Buckets</option></select>
        <label>Station:</label><select id="sparesStationFilter" onchange="filterSpares()"><option value="">All Stations</option></select>
        <label>Sub-Category:</label><select id="sparesSubcatFilter" onchange="filterSpares()"><option value="">All Sub-Categories</option></select>
        <label>Category:</label><select id="sparesCatFilter" onchange="filterSpares()"><option value="">All Categories</option></select>
        <label>Vendor:</label><select id="sparesVendorFilter" onchange="filterSpares()"><option value="">All Vendors</option></select>
    </div>
    <div id="spares-bucket-summary" style="margin-bottom:16px"></div>
    <div id="spares-table-wrap"></div>
</div>

<!-- DETAIL -->
<div class="page" id="page-detail">
    <div class="page-header"><div><div class="page-title">Full Transaction Detail</div><div class="page-subtitle">All CAPEX line items from Odoo POs and Ramp credit card</div></div></div>
    <div id="detail-table-wrap"></div>
</div>

<!-- TIMELINE -->
<div class="page" id="page-timeline">
    <div class="page-header"><div><div class="page-title">Spend Timeline</div><div class="page-subtitle">Temporal patterns and cumulative spend tracking</div></div></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Cumulative Spend (S-Curve)</h3><div id="chart-cumulative"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Source</h3><div id="chart-timeline-source"></div></div>
        <div class="chart-card full"><h3>Weekly Spend (bar height = total, color = intensity)</h3><div id="chart-weekly"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Sub-Category</h3><div id="chart-monthly-subcat"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by GL Category (legacy)</h3><div id="chart-monthly-cat"></div></div>
    </div>
</div>

<!-- OTHER PROJECTS -->
<div class="page" id="page-projects">
    <div class="page-header"><div><div class="page-title">Other Projects</div><div class="page-subtitle">NPI, Pilot, Facilities, Quality, IT, Maintenance, and unmapped spend</div></div></div>
    <div class="kpi-row" id="proj-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Spend by Project</h3><div id="chart-proj-breakdown"></div></div>
        <div class="chart-card"><h3>Top Vendors (Non-Production)</h3><div id="chart-proj-vendors"></div></div>
        <div class="chart-card full"><h3>Monthly Non-Production Spend</h3><div id="chart-proj-monthly"></div></div>
        <div class="chart-card full"><h3>Transaction Detail</h3><div id="proj-detail-wrap"></div></div>
    </div>
</div>

<!-- UNIT ECONOMICS -->
<div class="page" id="page-uniteco">
    <div class="page-header"><div><div class="page-title">Unit Economics</div><div class="page-subtitle">$/GWh and ft&sup2;/GWh by production line (configure capacities in Settings)</div></div></div>
    <div class="kpi-row" id="ue-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>$/GWh by Line (Forecast)</h3><div id="chart-ue-dollar"></div></div>
        <div class="chart-card"><h3>Forecast Spend by Line</h3><div id="chart-ue-compare"></div></div>
        <div class="chart-card"><h3>ft&sup2;/GWh by Line</h3><div id="chart-ue-sqft"></div></div>
        <div class="chart-card"><h3>$/GWh Composition (Forecast)</h3><div id="chart-ue-stack"></div></div>
        <div class="chart-card full"><h3>Line Detail (Forecast Basis)</h3><div id="ue-table-wrap"></div></div>
    </div>
</div>

<!-- SETTINGS -->
<div class="page" id="page-settings">
    <div class="page-header"><div><div class="page-title">Settings</div><div class="page-subtitle">Configure line capacities and floor area for unit economics calculations</div></div></div>
    <div class="chart-card" style="max-width:800px">
        <h3>Line Capacity &amp; Floor Area</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:16px">Enter the GWh capacity and floor area (ft&sup2;) for each production line. These values are used to compute $/GWh and ft&sup2;/GWh metrics on the Unit Economics page.</p>
        <div id="settings-lines"></div>
        <div style="margin-top:16px;display:flex;gap:10px;align-items:center">
            <button class="btn-refresh" style="width:auto;padding:10px 24px" onclick="saveSettings()">Save Settings</button>
            <span id="settings-ok" class="forecast-saved" style="display:none">Settings saved</span>
        </div>
    </div>
</div>

</div>
<div id="drill-overlay" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.6);z-index:900" onclick="closeDrill()"></div>
<div id="drill-panel" style="display:none;position:fixed;top:40px;right:20px;bottom:40px;width:65%;max-width:1000px;background:var(--surface);border:1px solid var(--border);border-radius:12px;z-index:901;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,.5);flex-direction:column">
    <div style="padding:16px 20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;flex-shrink:0">
        <div><div id="drill-title" style="font-size:16px;font-weight:700;color:var(--green)"></div><div id="drill-sub" style="font-size:11px;color:var(--muted);margin-top:2px"></div></div>
        <button onclick="closeDrill()" style="background:var(--surface2);color:var(--text);border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:12px;font-weight:600">Close</button>
    </div>
    <div style="flex:1;overflow:auto;padding:16px 20px" id="drill-body"></div>
</div>
<div class="toast" id="toast"></div>

<script>
const C={green:'#B2DD79',greenBright:'#D0F585',red:'#D1531D',yellow:'#F7C33C',blue:'#048EE5',surface:'#242422',surface2:'#32312F',text:'#F0EEEB',muted:'#9E9C98',border:'#3E3D3A'};
const PL={paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}};
const PC={responsive:true,displayModeBar:false};
let dtI={}, summaryCache=null, sparesData=[];
let forecastOriginal={};
let allModules=[], activeModules=new Set();
const DEFAULT_UNCHECKED_LINES=new Set(['pilot / npi','needs review','non-prod']);

async function initLineFilter(){
    const res=await fetch('/api/modules');
    allModules=await res.json();
    // Default: uncheck BASE2 and non-production buckets.
    activeModules=new Set(allModules.filter(m=>{
        if(typeof m!=='string')return false;
        const normalized=m.trim().toLowerCase();
        if(m.startsWith('BASE2-'))return false;
        if(DEFAULT_UNCHECKED_LINES.has(normalized))return false;
        return true;
    }));
    renderLineChecks();
}
function renderLineChecks(){
    const wrap=document.getElementById('line-filter-checks');
    wrap.innerHTML=allModules.map(m=>{
        const checked=activeModules.has(m)?'checked':'';
        return `<label style="display:flex;align-items:center;gap:6px;padding:3px 0;font-size:12px;color:var(--text);cursor:pointer"><input type="checkbox" ${checked} onchange="toggleLine('${m}',this.checked)" style="accent-color:var(--green)"/>${m}</label>`;
    }).join('');
}
function toggleLine(mod,on){
    if(on)activeModules.add(mod);else activeModules.delete(mod);
    reloadCurrentPage();
}
function toggleAllLines(on){
    if(on)activeModules=new Set(allModules);else activeModules.clear();
    renderLineChecks();
    reloadCurrentPage();
}
function lineQS(){
    if(activeModules.size===0)return'lines=__none__'; // None filter: show no data
    if(activeModules.size===allModules.length)return'';
    return'lines='+[...activeModules].join(',');
}
function apiUrl(path){const qs=lineQS();return qs?path+'?'+qs:path;}
function reloadCurrentPage(){
    summaryCache=null;
    const active=document.querySelector('.page.active');
    if(!active)return;
    const id=active.id.replace('page-','');
    if(id==='executive')loadExecutive();
    else if(id==='source')loadSource();
    else if(id==='stations')loadStations();
    else if(id==='forecasting')loadForecasting();
    else if(id==='vendors')loadVendors();
    else if(id==='assets')loadAssets();
    else if(id==='spares')loadSpares();
    else if(id==='detail')loadDetail();
    else if(id==='timeline')loadTimeline();
    else if(id==='projects')loadProjects();
}

function fmt$(v){if(v==null||isNaN(v))return'$0';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e6)return s+'$'+(a/1e6).toFixed(2)+'M';if(a>=1e3)return s+'$'+(a/1e3).toFixed(1)+'K';return s+'$'+a.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}

function dtOpts(base){
    const baseInit=base&&typeof base.initComplete==='function'?base.initComplete:null;
    return Object.assign({scrollX:true,autoWidth:false},base,{
        initComplete:function(){
            const api=this.api();
            api.columns().every(function(){
            const col=this;const th=$(col.footer());
            if(!th.length)return;
            $('<input type="text" placeholder="Filter..."/>').appendTo(th.empty()).on('keyup change',function(){if(col.search()!==this.value)col.search(this.value).draw();});
            });
            const tableNode=api.table().node();
            if(tableNode&&tableNode.id){
                enableTableColumnResize(api,tableNode.id);
                if(!tableNode.dataset.resizeBound){
                    api.on('draw',()=>enableTableColumnResize(api,tableNode.id));
                    tableNode.dataset.resizeBound='1';
                }
            }
            if(baseInit)baseInit.call(this);
        }
    });
}

function enableTableColumnResize(dtApi,tableId){
    const table=document.getElementById(tableId);
    if(!table)return;
    const wrap=table.closest('.dataTables_wrapper');
    if(!wrap)return;
    const headTable=wrap.querySelector('.dataTables_scrollHead table');
    const bodyTable=wrap.querySelector('.dataTables_scrollBody table');
    if(!headTable||!bodyTable)return;

    const headers=[...headTable.querySelectorAll('thead th')];
    if(!headers.length)return;
    headTable.classList.add('dt-resizable');

    const headCols=[...headTable.querySelectorAll('colgroup col')];
    const bodyCols=[...bodyTable.querySelectorAll('colgroup col')];
    const STORAGE_KEY='dashboard_table_col_widths_'+tableId;

    const applyWidth=(idx,w)=>{
        const width=Math.max(70,Math.round(w));
        if(headCols[idx])headCols[idx].style.width=width+'px';
        if(bodyCols[idx])bodyCols[idx].style.width=width+'px';
        if(headers[idx])headers[idx].style.width=width+'px';
        const n=idx+1;
        headTable.querySelectorAll(`thead th:nth-child(${n})`).forEach(el=>{
            el.style.width=width+'px';
            el.style.minWidth=width+'px';
            el.style.maxWidth=width+'px';
        });
        bodyTable.querySelectorAll(`thead th:nth-child(${n}), tbody td:nth-child(${n}), tfoot th:nth-child(${n})`).forEach(el=>{
            el.style.width=width+'px';
            el.style.minWidth=width+'px';
            el.style.maxWidth=width+'px';
        });
    };
    const saveWidths=()=>{
        const payload=headers.map(h=>Math.round(h.getBoundingClientRect().width));
        localStorage.setItem(STORAGE_KEY,JSON.stringify(payload));
    };

    try{
        const saved=JSON.parse(localStorage.getItem(STORAGE_KEY)||'[]');
        if(Array.isArray(saved)&&saved.length){
            saved.forEach((w,i)=>{if(Number.isFinite(w)&&w>0)applyWidth(i,w);});
        }
    }catch(_){}

    headers.forEach((th,idx)=>{
        th.querySelectorAll('.col-resizer').forEach(el=>el.remove());
        const startResize=(startX,startW)=>{
            document.body.classList.add('col-resize-active');
            const onMove=(ev)=>applyWidth(idx,startW+(ev.clientX-startX));
            const onUp=()=>{
                document.removeEventListener('mousemove',onMove);
                document.removeEventListener('mouseup',onUp);
                document.body.classList.remove('col-resize-active');
                saveWidths();
            };
            document.addEventListener('mousemove',onMove);
            document.addEventListener('mouseup',onUp);
        };
        const handle=document.createElement('span');
        handle.className='col-resizer';
        handle.title='Drag to resize column';
        handle.addEventListener('mousedown',(e)=>{
            e.preventDefault();
            e.stopPropagation();
            startResize(e.clientX,th.getBoundingClientRect().width);
        });
        th.addEventListener('mousedown',(e)=>{
            if(e.target!==th)return;
            const rect=th.getBoundingClientRect();
            if((rect.right-e.clientX)<=14){
                e.preventDefault();
                startResize(e.clientX,rect.width);
            }
        });
        th.appendChild(handle);
    });
}
function fmtF$(v){if(v==null||isNaN(v))return'$0.00';return(v<0?'-':'')+'$'+Math.abs(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtPct(v){return(v||0).toFixed(1)+'%'}
function vc(v){return v>0?'dollar-negative':'dollar-positive'}

function safeFileName(name){
    return String(name||'export').replace(/[^a-z0-9_-]+/gi,'_').replace(/^_+|_+$/g,'').toLowerCase()||'export';
}
function safeSheetName(name){
    const cleaned=String(name||'Sheet1').replace(/[\\/*?:\[\]]/g,' ').replace(/\s+/g,' ').trim();
    return (cleaned||'Sheet1').slice(0,31);
}
function normalizeRows(rows){
    if(!Array.isArray(rows))return [];
    if(!rows.length)return [];
    if(typeof rows[0]==='object'&&rows[0]!==null&&!Array.isArray(rows[0]))return rows;
    return rows.map((v,i)=>({index:i,value:v}));
}
function rowsToCsv(rows){
    const data=normalizeRows(rows);
    if(!data.length)return 'no_data\n';
    const keys=[...new Set(data.flatMap(r=>Object.keys(r)))];
    const esc=(v)=>{
        if(v===null||v===undefined)return '';
        const s=String(v);
        if(/[",\n]/.test(s))return '"'+s.replace(/"/g,'""')+'"';
        return s;
    };
    const out=[keys.join(',')];
    data.forEach(r=>out.push(keys.map(k=>esc(r[k])).join(',')));
    return out.join('\n')+'\n';
}
function flattenToDatasets(prefix,payload){
    const datasets=[];
    if(Array.isArray(payload)){
        datasets.push({name:prefix,rows:normalizeRows(payload)});
        return datasets;
    }
    if(payload&&typeof payload==='object'){
        const metaRows=[];
        Object.entries(payload).forEach(([k,v])=>{
            if(Array.isArray(v)){
                datasets.push({name:prefix+'_'+k,rows:normalizeRows(v)});
            }else if(v&&typeof v==='object'){
                const objRows=Object.entries(v).map(([kk,vv])=>({key:kk,value:typeof vv==='object'?JSON.stringify(vv):vv}));
                datasets.push({name:prefix+'_'+k,rows:objRows});
            }else{
                metaRows.push({key:k,value:v});
            }
        });
        if(metaRows.length)datasets.push({name:prefix+'_meta',rows:metaRows});
        return datasets;
    }
    datasets.push({name:prefix+'_value',rows:[{value:payload}]});
    return datasets;
}
function downloadBlob(fileName,blob){
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;
    a.download=fileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function setExportBusy(isBusy,msg){
    const csvBtn=document.getElementById('export-csv-btn');
    const xlsxBtn=document.getElementById('export-xlsx-btn');
    const note=document.getElementById('export-note');
    if(csvBtn)csvBtn.disabled=!!isBusy;
    if(xlsxBtn)xlsxBtn.disabled=!!isBusy;
    if(note&&msg)note.textContent=msg;
}
function endpointBaseName(path){
    const clean=String(path||'data').split('?')[0].replace(/^\/+|\/+$/g,'');
    return safeFileName(clean.replace(/\//g,'_'));
}
async function collectExportDatasets(pageId){
    const endpoints=[];
    const sidEl=document.getElementById('stationSelect');
    const stationId=sidEl?(sidEl.value||'').trim():'';
    if(pageId==='executive'||pageId==='source')endpoints.push(apiUrl('/api/summary'));
    else if(pageId==='stations'){
        endpoints.push(apiUrl('/api/stations'));
        if(stationId)endpoints.push('/api/station/'+encodeURIComponent(stationId));
    }else if(pageId==='forecasting'){
        endpoints.push(apiUrl('/api/forecasting'));
        endpoints.push('/api/settings');
    }else if(pageId==='vendors'){
        endpoints.push(apiUrl('/api/vendors'));
        endpoints.push(apiUrl('/api/transactions'));
    }else if(pageId==='assets'){
        endpoints.push(apiUrl('/api/assets'));
        endpoints.push('/api/asset-status');
    }else if(pageId==='spares')endpoints.push(apiUrl('/api/spares'));
    else if(pageId==='detail')endpoints.push(apiUrl('/api/transactions'));
    else if(pageId==='timeline')endpoints.push(apiUrl('/api/timeline'));
    else if(pageId==='projects')endpoints.push('/api/projects');
    else if(pageId==='uniteco')endpoints.push('/api/unit-economics');
    else if(pageId==='settings'){
        endpoints.push('/api/settings');
        endpoints.push('/api/modules');
    }else{
        endpoints.push(apiUrl('/api/summary'));
    }

    const datasets=[];
    for(const ep of endpoints){
        const res=await fetch(ep);
        if(!res.ok)throw new Error('Failed to fetch '+ep+' ('+res.status+')');
        const payload=await res.json();
        flattenToDatasets(endpointBaseName(ep),payload).forEach(ds=>datasets.push(ds));
    }
    return datasets.filter(ds=>Array.isArray(ds.rows));
}
async function exportCurrentPage(format){
    const active=document.querySelector('.page.active');
    const pageId=active?active.id.replace('page-',''):'executive';
    try{
        setExportBusy(true,'Preparing '+format.toUpperCase()+' export...');
        const datasets=await collectExportDatasets(pageId);
        if(!datasets.length){
            setExportBusy(false,'No data available to export for this tab.');
            showToast('No data to export');
            return;
        }
        const stamp=new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
        const base='capex_'+safeFileName(pageId)+'_'+stamp;
        if(format==='xlsx'){
            if(typeof XLSX==='undefined')throw new Error('Excel library unavailable');
            const wb=XLSX.utils.book_new();
            datasets.forEach(ds=>{
                const rows=normalizeRows(ds.rows);
                const ws=XLSX.utils.json_to_sheet(rows.length?rows:[{info:'No rows'}]);
                XLSX.utils.book_append_sheet(wb,ws,safeSheetName(ds.name));
            });
            const out=XLSX.write(wb,{bookType:'xlsx',type:'array'});
            downloadBlob(base+'.xlsx',new Blob([out],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}));
            setExportBusy(false,'Excel export ready: '+datasets.length+' sheet(s).');
            showToast('Excel export complete');
            return;
        }
        if(typeof JSZip==='undefined')throw new Error('CSV zip library unavailable');
        const zip=new JSZip();
        datasets.forEach(ds=>zip.file(safeFileName(ds.name)+'.csv',rowsToCsv(ds.rows)));
        const blob=await zip.generateAsync({type:'blob'});
        downloadBlob(base+'_csv.zip',blob);
        setExportBusy(false,'CSV export ready: '+datasets.length+' file(s).');
        showToast('CSV export complete');
    }catch(err){
        setExportBusy(false,'Export failed. Please retry.');
        showToast('Export failed');
    }
}

/* ====== DRILL-DOWN ====== */
let drillDT=null;
async function openDrill(title,params){
    const qs=Object.entries(params).filter(([k,v])=>v).map(([k,v])=>k+'='+encodeURIComponent(v)).join('&');
    const res=await fetch('/api/drilldown?'+qs);
    const d=await res.json();
    document.getElementById('drill-title').textContent=title;
    document.getElementById('drill-sub').textContent=d.count+' items | '+fmtF$(d.total)+' total';
    const cols=['source','po_number','date_order','vendor_name','mfg_subcategory','item_description','station_id','project_name','mapping_confidence','bill_payment_status','price_subtotal','created_by_name'];
    const labels=['Src','PO','Date','Vendor','Sub-Cat','Description','Station','Project','Conf','Pay','Subtotal','By'];
    let html='<table id="drill-tbl" class="display compact" style="width:100%"><thead><tr>';
    labels.forEach(l=>{html+='<th>'+l+'</th>';});
    html+='</tr></thead><tfoot><tr>';
    labels.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    (d.rows||[]).forEach(r=>{
        html+='<tr>';
        cols.forEach(c=>{
            const v=r[c]!=null?r[c]:'';
            if(c==='price_subtotal')html+='<td class="dollar">'+fmtF$(parseFloat(v)||0)+'</td>';
            else if(c==='source')html+='<td><span class="source-badge '+v+'">'+v+'</span></td>';
            else html+='<td>'+v+'</td>';
        });
        html+='</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('drill-body').innerHTML=html;
    if(drillDT)drillDT.destroy();
    drillDT=$('#drill-tbl').DataTable(dtOpts({pageLength:25,order:[[10,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
    document.getElementById('drill-overlay').style.display='block';
    document.getElementById('drill-panel').style.display='flex';
}
function closeDrill(){
    document.getElementById('drill-overlay').style.display='none';
    document.getElementById('drill-panel').style.display='none';
    if(drillDT){drillDT.destroy();drillDT=null;}
}

function showPage(id,el){
    document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
    document.getElementById('page-'+id).classList.add('active');
    if(el)el.classList.add('active');
    const exportNote=document.getElementById('export-note');
    if(exportNote)exportNote.textContent='Graphs + tables for '+id+' tab';
    if(id==='about')loadAbout();
    if(id==='source')loadSource();
    if(id==='stations'&&!document.getElementById('stationSelect').value)loadStations();
    if(id==='forecasting')loadForecasting();
    if(id==='vendors')loadVendors();
    if(id==='assets')loadAssets();
    if(id==='spares')loadSpares();
    if(id==='detail')loadDetail();
    if(id==='timeline')loadTimeline();
    if(id==='projects')loadProjects();
    if(id==='uniteco')loadUnitEconomics();
    if(id==='settings')loadSettings();
}

function loadAbout(){
    // Static page today; hook kept for future lightweight dynamic badges if needed.
}

/* ====== EXECUTIVE ====== */
async function loadExecutive(){
    const res=await fetch(apiUrl('/api/summary'));const d=await res.json();
    if(!d.total_committed)return;
    summaryCache=d;
    const pay=d.payment||{};
    const rpay=d.ramp_payment||{};

    document.getElementById('kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Committed</div><div class="value dollar">${fmt$(d.total_committed)}</div><div class="sub"><span class="source-badge odoo">Odoo ${fmt$(d.odoo_total)}</span> <span class="source-badge ramp">Ramp ${fmt$(d.ramp_total)}</span></div></div>
        <div class="kpi"><div class="label">Forecasted Budget</div><div class="value dollar">${fmt$(d.forecasted_budget)}</div></div>
        <div class="kpi"><div class="label">Variance</div><div class="value ${vc(d.variance)}">${fmt$(d.variance)}</div><div class="sub">${d.variance>0?'Over':'Under'} budget</div></div>
        <div class="kpi"><div class="label">% Budget Spent</div><div class="value">${fmtPct(d.pct_spent)}</div></div>
        <div class="kpi"><div class="label">Mfg Spend</div><div class="value dollar" style="color:var(--green)">${fmt$(d.mfg_total||0)}</div><div class="sub">Non-Mfg: ${fmt$(d.non_mfg_total||0)}</div></div>
        <div class="kpi"><div class="label">Odoo Billed</div><div class="value">${fmtPct(pay.billed_spend_pct||0)}</div><div class="sub">Paid: ${fmt$(pay.paid_spend||0)} &middot; Open: ${fmt$(pay.open_spend||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC</div><div class="value dollar" style="color:var(--blue)">${fmt$(rpay.total_amount||0)}</div><div class="sub">${rpay.available?(rpay.txn_count||0)+' transactions &middot; Card Charged':'N/A'}</div></div>
        <div class="kpi"><div class="label">Active POs</div><div class="value">${(d.active_pos||0).toLocaleString()}</div></div>`;

    // Budget vs Actual -- grouped by MOD with proper left margin
    if(d.budget_vs_actual&&d.budget_vs_actual.length){
        const lines=d.budget_vs_actual.sort((a,b)=>a.line.localeCompare(b.line));
        const maxLabel=Math.max(...lines.map(l=>l.line.length));
        Plotly.newPlot('chart-budget',[
            {y:lines.map(l=>l.line),x:lines.map(l=>l.forecasted),type:'bar',orientation:'h',name:'Forecasted',marker:{color:C.surface2},text:lines.map(l=>fmt$(l.forecasted)),textposition:'outside',textfont:{color:C.muted,size:10}},
            {y:lines.map(l=>l.line),x:lines.map(l=>l.actual),type:'bar',orientation:'h',name:'Actual',marker:{color:C.green},text:lines.map(l=>fmt$(l.actual)),textposition:'outside',textfont:{color:C.green,size:10}},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'group',height:Math.max(300,lines.length*50),margin:{l:Math.max(140,maxLabel*8),r:80,t:30,b:40},legend:{font:{color:C.muted},x:0.7,y:1.1,orientation:'h'},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}},PC);
        document.getElementById('chart-budget').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const ln=ev.points[0].y;openDrill('Line: '+ln,{line:ln});}});
    }

    // Monthly trend with line overlay
    if(d.monthly_trend&&d.monthly_trend.length){
        const months=d.monthly_trend.map(m=>m.month);
        const odooY=d.monthly_trend.map(m=>m.odoo||0);
        const rampY=d.monthly_trend.map(m=>m.ramp||0);
        const traces=[
            {x:months,y:odooY,type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:rampY,type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ];
        const lineColors=['#E8A838','#D0F585','#048EE5','#D1531D','#9B7ED8','#F06292','#4DD0E1'];
        if(d.monthly_by_line){
            Object.entries(d.monthly_by_line).sort().forEach(([ln,mData],i)=>{
                traces.push({x:months,y:months.map(m=>mData[m]||0),type:'scatter',mode:'lines+markers',name:ln,line:{width:2,color:lineColors[i%lineColors.length],dash:i<3?'solid':'dash'},marker:{size:5},hovertemplate:'%{x}<br>'+ln+': %{y:$,.0f}<extra></extra>'});
            });
        }
        Plotly.newPlot('chart-monthly',traces,{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'stack',height:350,legend:{font:{color:C.muted,size:10},x:0,y:1.2,orientation:'h'},margin:{l:65,r:15,t:60,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,tickangle:-45}},PC);
        document.getElementById('chart-monthly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const m=p.x;const params={month:m};if(p.data.name&&p.data.name.startsWith('BASE'))params.line=p.data.name;else if(p.data.name==='Ramp CC')params.source='ramp';else if(p.data.name==='Odoo PO')params.source='odoo';openDrill(m+(params.line?' | '+params.line:params.source?' | '+params.source:''),params);}});
    } else {
        document.getElementById('chart-monthly').innerHTML='<p style="color:var(--muted);padding:20px">No monthly data available.</p>';
    }

    // Sub-category treemap
    const scData=d.subcategory_spend||d.category_spend||[];
    if(scData.length){
        const isSubcat=!!d.subcategory_spend;
        const items=scData.slice(0,13);
        const scColors={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
        Plotly.newPlot('chart-subcategory',[{
            type:'treemap',
            labels:items.map(c=>isSubcat?c.subcategory:c.category.replace('Non-Inventory: ','')),
            parents:items.map(()=>''),values:items.map(c=>c.spend),
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            marker:{colors:items.map(c=>isSubcat?(scColors[c.subcategory]||'#555'):`hsl(${90+items.indexOf(c)*20},55%,52%)`)}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8}},PC);
        document.getElementById('chart-subcategory').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].label;openDrill('Sub-Category: '+lbl,isSubcat?{subcategory:lbl}:{category:scData.find(c=>c.category.replace('Non-Inventory: ','')==lbl)?.category||lbl});}});
    }

    // Odoo payment status donut (linked vendor-bill states)
    if(pay.available){
        const labels=['Paid','Partial','Unpaid','No Bill','Mixed'];
        const values=[pay.paid_spend||0,pay.partial_spend||0,pay.unpaid_spend||0,pay.no_bill_spend||0,pay.mixed_spend||0];
        const colors=[C.green,C.yellow,C.red,'#666','#9B7ED8'];
        Plotly.newPlot('chart-payment-status',[{
            labels,values,type:'pie',hole:.5,marker:{colors},
            textinfo:'label+percent',textfont:{color:C.text,size:11},
            hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8}},PC);
        document.getElementById('chart-payment-status').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const label=(ev.points[0].label||'').toLowerCase();
            const map={'paid':'paid','partial':'partial','unpaid':'unpaid','no bill':'no_bill','mixed':'mixed'};
            openDrill('Payment: '+ev.points[0].label,{source:'odoo',payment_status:map[label]||''});
        });
    }else{
        document.getElementById('chart-payment-status').innerHTML='<p style="color:var(--muted);padding:20px">Payment status not available in this dataset.</p>';
    }

    // Ramp CC payment -- all CC charges are paid at swipe
    if(rpay.available&&rpay.total_amount>0){
        const rLabels=['Card Charged'];
        const rValues=[rpay.card_charged||0];
        const rColors=[C.blue];
        Plotly.newPlot('chart-ramp-payment',[{
            labels:rLabels,values:rValues,type:'pie',hole:.5,marker:{colors:rColors},
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            textfont:{color:C.text,size:12},
            hovertemplate:'%{label}<br>%{value:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:(rpay.txn_count||0)+' txns',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
    }else{
        document.getElementById('chart-ramp-payment').innerHTML='<p style="color:var(--muted);padding:20px">No Ramp transactions in this dataset.</p>';
    }

    // Top vendors -- generous left margin
    if(d.top_vendors&&d.top_vendors.length){
        const v=[...d.top_vendors].reverse();
        const maxLen=Math.max(...v.map(x=>x.vendor.length));
        Plotly.newPlot('chart-vendors',[{
            y:v.map(x=>x.vendor),x:v.map(x=>x.spend),
            type:'bar',orientation:'h',marker:{color:C.green},
            text:v.map(x=>fmt$(x.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,v.length*32),margin:{l:Math.max(180,maxLen*7),r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-vendors').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=ev.points[0].y;openDrill('Vendor: '+vn,{vendor:vn});}});
    }

    // Spend by Employee
    if(d.top_employees&&d.top_employees.length){
        const e=[...d.top_employees].reverse();
        Plotly.newPlot('chart-employees',[
            {y:e.map(x=>x.name),x:e.map(x=>x.spend),type:'bar',orientation:'h',name:'Spend',marker:{color:C.green},text:e.map(x=>fmt$(x.spend)+' ('+x.pos+' POs)'),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>Spend: %{x:$,.0f}<extra></extra>'},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,e.length*30),margin:{l:160,r:100,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-employees').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const nm=ev.points[0].y;openDrill('Employee: '+nm,{employee:nm});}});
    }
}

/* ====== ODOO vs RAMP ====== */
async function loadSource(){
    if(!summaryCache){const r=await fetch(apiUrl('/api/summary'));summaryCache=await r.json();}
    const d=summaryCache;const sc=d.source_compare||{};
    const pay=d.payment||{};
    const rpay=d.ramp_payment||{};
    document.getElementById('source-kpis').innerHTML=`
        <div class="kpi"><div class="label">Odoo PO Total</div><div class="value dollar">${fmt$(d.odoo_total)}</div><div class="sub">${(sc.odoo?.count||0).toLocaleString()} line items &middot; avg ${fmtF$(sc.odoo?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC Total</div><div class="value dollar" style="color:var(--blue)">${fmt$(d.ramp_total)}</div><div class="sub">${(sc.ramp?.count||0).toLocaleString()} transactions &middot; avg ${fmtF$(sc.ramp?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Odoo Share</div><div class="value">${fmtPct(d.odoo_total/(d.total_committed||1)*100)}</div></div>
        <div class="kpi"><div class="label">Ramp Share</div><div class="value">${fmtPct(d.ramp_total/(d.total_committed||1)*100)}</div></div>
        <div class="kpi"><div class="label">Odoo Billed</div><div class="value dollar">${fmt$(pay.billed_spend||0)}</div><div class="sub">Paid: ${fmt$(pay.paid_spend||0)} &middot; Open: ${fmt$(pay.open_spend||0)} &middot; No Bill: ${fmt$(pay.no_bill_spend||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC Txns</div><div class="value" style="color:var(--blue)">${(rpay.txn_count||0).toLocaleString()}</div><div class="sub">${fmt$(rpay.total_amount||0)} &middot; Card Charged</div></div>`;

    const osc=sc.odoo_subcats||sc.odoo_categories||[];
    const isOSC=!!sc.odoo_subcats;
    if(osc.length){
        const ocR=[...osc].reverse();
        Plotly.newPlot('chart-odoo-subcats',[{y:ocR.map(c=>isOSC?c.cat:c.cat.replace('Non-Inventory: ','')),x:ocR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.green},text:ocR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,osc.length*35),margin:{l:210,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-odoo-subcats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;openDrill('Odoo: '+lbl,isOSC?{source:'odoo',subcategory:lbl}:{source:'odoo',category:osc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl)?.cat||lbl});}});
    }
    const rsc=sc.ramp_subcats||sc.ramp_categories||[];
    const isRSC=!!sc.ramp_subcats;
    if(rsc.length){
        const rcR=[...rsc].reverse();
        Plotly.newPlot('chart-ramp-subcats',[{y:rcR.map(c=>isRSC?c.cat:c.cat.replace('Non-Inventory: ','')),x:rcR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.blue},text:rcR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,rsc.length*35),margin:{l:210,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-ramp-subcats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;openDrill('Ramp: '+lbl,isRSC?{source:'ramp',subcategory:lbl}:{source:'ramp',category:rsc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl)?.cat||lbl});}});
    }

    // Odoo PO billing donut on source page
    if(pay.available){
        Plotly.newPlot('chart-src-odoo-billing',[{
            labels:['Paid','Partial','Unpaid','No Bill','Mixed'],
            values:[pay.paid_spend||0,pay.partial_spend||0,pay.unpaid_spend||0,pay.no_bill_spend||0,pay.mixed_spend||0],
            type:'pie',hole:.5,marker:{colors:[C.green,C.yellow,C.red,'#666','#9B7ED8']},
            textinfo:'label+percent',textfont:{color:C.text,size:11},
            hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:fmtPct(pay.paid_spend_pct||0)+' paid',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
        document.getElementById('chart-src-odoo-billing').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const label=(ev.points[0].label||'').toLowerCase();
            const map={'paid':'paid','partial':'partial','unpaid':'unpaid','no bill':'no_bill','mixed':'mixed'};
            openDrill('Odoo Payment: '+ev.points[0].label,{source:'odoo',payment_status:map[label]||''});
        });
    }

    // Ramp CC accounting -- all CC charges are paid at swipe
    if(rpay.available&&rpay.total_amount>0){
        Plotly.newPlot('chart-src-ramp-billing',[{
            labels:['Card Charged'],
            values:[rpay.card_charged||0],
            type:'pie',hole:.5,marker:{colors:[C.blue]},
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            textfont:{color:C.text,size:12},
            hovertemplate:'%{label}<br>%{value:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:(rpay.txn_count||0)+' transactions',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
    }else{
        document.getElementById('chart-src-ramp-billing').innerHTML='<p style="color:var(--muted);padding:20px">No Ramp transactions in this dataset.</p>';
    }

    if(d.monthly_trend&&d.monthly_trend.length){
        const months=d.monthly_trend.map(m=>m.month);
        Plotly.newPlot('chart-source-monthly',[
            {x:months,y:d.monthly_trend.map(m=>m.odoo||0),type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:d.monthly_trend.map(m=>m.ramp||0),type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'group',height:350,legend:{font:{color:C.muted},x:0,y:1.15,orientation:'h'},margin:{l:65,r:15,t:40,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-source-monthly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const src=p.data.name==='Ramp CC'?'ramp':'odoo';openDrill(p.x+' | '+p.data.name,{month:p.x,source:src});}});
    }
}

/* ====== STATIONS ====== */
async function loadStations(){
    const res=await fetch(apiUrl('/api/stations'));const stations=await res.json();
    const sel=document.getElementById('stationSelect');
    sel.innerHTML='<option value="">-- select a station --</option>';

    // Group: BASE -> MOD/CELL/INV -> stations
    const tree={};
    stations.forEach(s=>{
        const sid=s.station_id||'';
        const m=sid.match(/^(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)/);
        const mod=m?m[1]+'-'+m[2]:'Other';
        if(!tree[mod])tree[mod]=[];
        tree[mod].push(s);
    });
    // Sort: CELL under MOD grouping
    Object.keys(tree).sort().forEach(mod=>{
        const og=document.createElement('optgroup');
        og.label=mod;
        tree[mod].sort((a,b)=>(a.station_id||'').localeCompare(b.station_id||'')).forEach(s=>{
            const o=document.createElement('option');
            o.value=s.station_id;
            o.textContent=s.station_id+(s.station_name?' - '+s.station_name:'');
            og.appendChild(o);
        });
        sel.appendChild(og);
    });
}

async function loadStationDetail(){
    const sid=document.getElementById('stationSelect').value;
    if(!sid)return;
    const res=await fetch('/api/station/'+encodeURIComponent(sid));const d=await res.json();
    const m=d.meta||{};
    const forecast=parseFloat(m.forecasted_cost)||0;
    const actual=parseFloat(m.actual_spend)||0;
    const variance=actual-forecast;

    document.getElementById('station-kpis').innerHTML=`
        <div class="kpi"><div class="label">Station</div><div class="value" style="font-size:16px">${sid}</div><div class="sub">${m.station_name||''} &middot; Owner: ${m.owner||'--'}</div></div>
        <div class="kpi"><div class="label">Forecasted</div><div class="value dollar">${fmtF$(forecast)}</div>
            <div class="sub"><input class="forecast-input" id="fc-edit" type="number" step="100" value="${forecast.toFixed(0)}"/><button class="forecast-save" onclick="saveForecast('${sid}')">Save</button><span class="forecast-saved" id="fc-ok" style="display:none">Saved</span></div></div>
        <div class="kpi"><div class="label">Actual Spend</div><div class="value dollar">${fmtF$(actual)}</div></div>
        <div class="kpi"><div class="label">Variance</div><div class="value ${vc(variance)}">${fmtF$(variance)}</div><div class="sub">${variance>0?'Over':'Under'} budget</div></div>
        <div class="kpi"><div class="label">Line Items</div><div class="value">${(m.line_count||0).toLocaleString()}</div></div>`;

    if(d.vendors&&d.vendors.length){
        const vSorted=[...d.vendors].sort((a,b)=>a.spend-b.spend);
        Plotly.newPlot('chart-station-vendors',[{
            y:vSorted.map(v=>v.vendor.length>30?v.vendor.substring(0,30)+'...':v.vendor),
            x:vSorted.map(v=>v.spend),type:'bar',orientation:'h',marker:{color:C.green},
            text:vSorted.map(v=>fmt$(v.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,vSorted.length*30),margin:{l:Math.max(160,Math.max(...vSorted.map(v=>v.vendor.length))*6),r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
    } else { document.getElementById('chart-station-vendors').innerHTML='<p style="color:var(--muted);padding:20px">No vendor data for this station.</p>'; }

    if(d.timeline&&d.timeline.length){
        Plotly.newPlot('chart-station-timeline',[{
            x:d.timeline.map(t=>t.date),y:d.timeline.map(t=>t.amount),
            text:d.timeline.map(t=>(t.po||'')+': '+(t.vendor||'')+' - '+(t.desc||'')),
            type:'scatter',mode:'markers+lines',
            marker:{size:10,color:C.green,line:{color:C.surface,width:1}},
            line:{color:'rgba(178,221,121,0.3)',width:1},
            hovertemplate:'%{x}<br>%{text}<br>%{y:$,.2f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:70,r:20,t:20,b:50},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}},PC);
        document.getElementById('chart-station-timeline').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const m=p.x.substring(0,7);openDrill(sid+' | '+p.x,{station:sid,month:m});}});
    } else { document.getElementById('chart-station-timeline').innerHTML='<p style="color:var(--muted);padding:20px">No dated orders for this station.</p>'; }

    let bom='<table id="station-bom" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Sub-Category</th><th>Vendor</th><th>Qty</th><th>Unit Price</th><th>Subtotal</th><th>PO</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    (d.lines||[]).forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        bom+=`<tr><td>${r.item_description||''}</td><td>${r.mfg_subcategory||(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_name||''}</td><td>${r.product_qty||''}</td><td>${fmtF$(parseFloat(r.price_unit)||0)}</td><td>${fmtF$(parseFloat(r.price_subtotal)||0)}</td><td>${r.po_number||''}</td><td>${parts}</td></tr>`;
    });
    bom+='</tbody></table>';
    document.getElementById('station-bom-table').innerHTML=bom;
    if(dtI['station-bom'])dtI['station-bom'].destroy();
    dtI['station-bom']=$('#station-bom').DataTable(dtOpts({pageLength:25,order:[[5,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

async function saveForecast(sid){
    const val=parseFloat(document.getElementById('fc-edit').value);
    if(isNaN(val))return;
    const res=await fetch('/api/forecast',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({station_id:sid,forecasted_cost:val})});
    const d=await res.json();
    if(d.ok){document.getElementById('fc-ok').style.display='inline';setTimeout(()=>{document.getElementById('fc-ok').style.display='none';},2000);showToast('Forecast saved');}
}

/* ====== FORECASTING ====== */
function setForecastRefreshMessage(msg,isError){
    const el=document.getElementById('forecast-refresh-msg');
    if(!el)return;
    el.textContent=msg||'';
    el.style.color=isError?C.red:C.muted;
}

async function loadForecasting(){
    const [dataRes,settingsRes]=await Promise.all([
        fetch(apiUrl('/api/forecasting')),
        fetch('/api/settings'),
    ]);
    const payload=await dataRes.json();
    const settings=await settingsRes.json();
    const rows=payload.rows||[];
    const groups=payload.groups||[];
    document.getElementById('forecast-bf1-url').value=settings.bf1_sheet_url||'';
    document.getElementById('forecast-bf2-url').value=settings.bf2_sheet_url||'';

    forecastOriginal={};
    if(!rows.length){
        document.getElementById('forecast-table-wrap').innerHTML='<p style="color:var(--muted);padding:18px">No stations found for the current line filter.</p>';
        return;
    }

    const groupMap={};
    groups.forEach(g=>{groupMap[g.line]=g;});

    let html='<table id="forecast-table" class="display compact" style="width:100%"><thead><tr><th style="min-width:170px">Station</th><th>Station Name</th><th>Owner</th><th>Forecast</th><th>Actual</th><th>Variance</th></tr></thead><tbody>';
    let currentLine='';
    rows.forEach(r=>{
        const line=r.line||'Other';
        if(line!==currentLine){
            currentLine=line;
            const g=groupMap[line]||{};
            html+=`<tr><td colspan="6" style="background:var(--surface2);color:var(--green);font-weight:700;padding:8px 10px">${line} &middot; Stations: ${(g.station_count||0).toLocaleString()} &middot; Forecast: ${fmtF$(g.total_forecast||0)} &middot; Actual: ${fmtF$(g.total_actual||0)}</td></tr>`;
        }
        const sid=r.station_id||'';
        const forecast=parseFloat(r.forecasted_cost)||0;
        const actual=parseFloat(r.actual_spend)||0;
        const variance=parseFloat(r.variance)||actual-forecast;
        forecastOriginal[sid]=forecast;
        const lockHtml=r.is_locked
            ? `<span title="Locked by manual override. Sheets refresh will not change this value." style="display:inline-block;margin-left:6px;font-size:12px;color:#E8A838">&#128274;</span><button class="forecast-unlock" type="button" data-station-id="${sid}" onclick="unlockForecastOverride(this.dataset.stationId)">Unlock</button>`
            : `<button class="forecast-lock" type="button" data-station-id="${sid}" onclick="lockForecastOverride(this.dataset.stationId)">Lock</button>`;
        html+=`<tr>
            <td style="font-weight:600;color:var(--green)">${sid}${lockHtml}</td>
            <td>${r.station_name||''}</td>
            <td>${r.owner||''}</td>
            <td><input class="forecast-input forecast-edit" data-station-id="${sid}" type="number" min="0" step="100" value="${forecast.toFixed(2)}"/></td>
            <td class="dollar">${fmtF$(actual)}</td>
            <td class="${vc(variance)}">${fmtF$(variance)}</td>
        </tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('forecast-table-wrap').innerHTML=html;
}

async function lockForecastOverride(stationId){
    const sid=(stationId||'').trim();
    if(!sid)return;
    try{
        const res=await fetch('/api/forecast/lock',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_id:sid}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to lock override',true);
            return;
        }
        setForecastRefreshMessage(`Locked override for ${sid}. Sheets refresh will skip this row.`,false);
        showToast('Forecast override locked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to lock override',true);
    }
}

function forecastTableStationIds(){
    return [...document.querySelectorAll('#forecast-table-wrap input.forecast-edit')]
        .map(inp=>(inp.dataset.stationId||'').trim())
        .filter(Boolean);
}

async function lockAllForecastOverrides(){
    const stationIds=forecastTableStationIds();
    if(!stationIds.length){
        showToast('No forecast rows to lock');
        return;
    }
    if(!window.confirm(`Lock overrides for ${stationIds.length} row${stationIds.length===1?'':'s'}?`))return;
    try{
        const res=await fetch('/api/forecast/lock_all',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_ids:stationIds}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to lock all overrides',true);
            return;
        }
        let msg=`Locked ${d.locked_count||0} row${(d.locked_count||0)===1?'':'s'}.`;
        if((d.not_found_count||0)>0)msg+=` ${d.not_found_count} row${d.not_found_count===1?' was':'s were'} not found.`;
        setForecastRefreshMessage(msg,false);
        showToast('Forecast overrides locked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to lock all overrides',true);
    }
}

async function unlockForecastOverride(stationId){
    const sid=(stationId||'').trim();
    if(!sid)return;
    if(!window.confirm(`Unlock forecast override for ${sid}?`))return;
    try{
        const res=await fetch('/api/forecast/unlock',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_id:sid}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to unlock override',true);
            return;
        }
        setForecastRefreshMessage(`Unlocked override for ${sid}. Sheets refresh can update this row again.`,false);
        showToast('Forecast override unlocked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to unlock override',true);
    }
}

async function unlockAllForecastOverrides(){
    const stationIds=forecastTableStationIds();
    if(!stationIds.length){
        showToast('No forecast rows to unlock');
        return;
    }
    if(!window.confirm(`Unlock overrides for ${stationIds.length} row${stationIds.length===1?'':'s'}?`))return;
    try{
        const res=await fetch('/api/forecast/unlock_all',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_ids:stationIds}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to unlock all overrides',true);
            return;
        }
        let msg=`Unlocked ${d.removed_count||0} row${(d.removed_count||0)===1?'':'s'}.`;
        if((d.not_found_count||0)>0)msg+=` ${d.not_found_count} row${d.not_found_count===1?' had':'s had'} no lock.`;
        setForecastRefreshMessage(msg,false);
        showToast('Forecast overrides unlocked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to unlock all overrides',true);
    }
}

async function saveForecastingBulk(){
    const inputs=[...document.querySelectorAll('#forecast-table-wrap input.forecast-edit')];
    if(!inputs.length){
        showToast('No forecast rows to save');
        return;
    }
    const updates=[];
    let invalid=0;
    inputs.forEach(inp=>{
        const sid=inp.dataset.stationId||'';
        const val=parseFloat(inp.value);
        if(!sid||isNaN(val)||val<0){
            invalid+=1;
            return;
        }
        const before=forecastOriginal[sid];
        if(before==null||Math.abs(before-val)>0.0001){
            updates.push({station_id:sid,forecasted_cost:val});
        }
    });
    if(!updates.length){
        showToast(invalid?'No valid forecast edits found':'No forecast changes to save');
        return;
    }
    const res=await fetch('/api/forecast/bulk',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({updates}),
    });
    const d=await res.json();
    if(!d.ok){
        setForecastRefreshMessage(d.error||'Failed to save forecasts',true);
        return;
    }
    document.getElementById('forecast-ok').style.display='inline';
    setTimeout(()=>{document.getElementById('forecast-ok').style.display='none';},2500);
    setForecastRefreshMessage(`Saved ${d.updated_count||0} updates (${d.skipped_count||0} skipped).`,false);
    showToast('Forecasts saved');
    await loadForecasting();
}

async function refreshForecastFromSheets(){
    const bf1=document.getElementById('forecast-bf1-url').value.trim();
    const bf2=document.getElementById('forecast-bf2-url').value.trim();
    const btn=document.getElementById('forecast-refresh-btn');
    if(!bf1||!bf2){
        setForecastRefreshMessage('Please provide both BF1 and BF2 sheet URLs.',true);
        return;
    }
    btn.disabled=true;
    btn.textContent='Refreshing...';
    setForecastRefreshMessage('Refreshing forecast values from Google Sheets...',false);
    try{
        const res=await fetch('/api/forecast/refresh',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({bf1_sheet_url:bf1,bf2_sheet_url:bf2}),
        });
        const d=await res.json();
        const errs=(d.errors||[]);
        if(!res.ok||!d.ok){
            const msg=errs.length?errs.join(' | '):(d.error||'Refresh failed');
            setForecastRefreshMessage(msg,true);
            return;
        }
        const bf1Applied=d.bf1&&d.bf1.applied_updates?d.bf1.applied_updates:0;
        const bf2Applied=d.bf2&&d.bf2.applied_updates?d.bf2.applied_updates:0;
        const lockedSkipped=d.locked_skipped_count||0;
        let msg=`Updated ${d.updated_count||0} stations (BF1: ${bf1Applied}, BF2: ${bf2Applied}).`;
        if(lockedSkipped)msg+=` Preserved ${lockedSkipped} locked override${lockedSkipped===1?'':'s'}.`;
        if(errs.length)msg+=` Warnings: ${errs.join(' | ')}`;
        setForecastRefreshMessage(msg,errs.length>0);
        showToast('Forecast refresh complete');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Refresh failed. Please sign in again and confirm sheet access.',true);
    }finally{
        btn.disabled=false;
        btn.textContent='Refresh from Sheets';
    }
}

function reauthGoogleForSheets(){
    window.location.href='/auth/logout';
}

/* ====== VENDORS ====== */
async function loadVendors(){
    const res=await fetch(apiUrl('/api/vendors'));const vendors=await res.json();
    if(!vendors.length)return;

    const top5=vendors.slice(0,5);
    const t5=[...top5].reverse();
    Plotly.newPlot('chart-vendor-conc',[{
        y:t5.map(v=>v.vendor_name.length>30?v.vendor_name.substring(0,30)+'...':v.vendor_name),
        x:t5.map(v=>v.spend),type:'bar',orientation:'h',marker:{color:C.green},
        text:t5.map(v=>fmt$(v.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
    }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,t5.length*45),margin:{l:200,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
    document.getElementById('chart-vendor-conc').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=top5.find(v=>v.vendor_name.startsWith(ev.points[0].y.replace('...','')));openDrill('Vendor: '+(vn?vn.vendor_name:ev.points[0].y),{vendor:vn?vn.vendor_name:ev.points[0].y});}});

    // Heatmap: top 10 vendors x top 10 stations with actual spend
    const txRes=await fetch(apiUrl('/api/transactions'));const txData=await txRes.json();
    const topV=vendors.slice(0,10).map(v=>v.vendor_name);
    const stationSpend={};
    txData.forEach(t=>{if(t.station_id)stationSpend[t.station_id]=(stationSpend[t.station_id]||0)+(parseFloat(t.price_subtotal)||0);});
    const topS=Object.entries(stationSpend).sort((a,b)=>b[1]-a[1]).slice(0,12).map(e=>e[0]);
    if(topV.length&&topS.length){
        const z=topV.map(v=>topS.map(s=>txData.filter(t=>t.vendor_name===v&&t.station_id===s).reduce((sum,t)=>sum+(parseFloat(t.price_subtotal)||0),0)));
        const maxZ=Math.max(...z.flat().filter(v=>v>0),1);
        Plotly.newPlot('chart-vendor-heatmap',[{
            z,x:topS,y:topV.map(v=>v.length>28?v.substring(0,28)+'...':v),
            type:'heatmap',
            colorscale:[[0,'#1A1A1A'],[0.01,'#2a3520'],[0.15,'#3d5a28'],[0.4,'#5a8a35'],[0.7,'#8abb55'],[1,'#D0F585']],
            hoverongaps:false,
            hovertemplate:'%{y}<br>%{x}<br>%{z:$,.0f}<extra></extra>',
            zmin:0,zmax:maxZ,
            colorbar:{tickprefix:'$',tickformat:',.0s',tickfont:{color:C.muted},outlinewidth:0}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,topV.length*35),margin:{l:220,r:20,t:20,b:100},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickangle:-45}},PC);
        document.getElementById('chart-vendor-heatmap').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const vIdx=p.pointIndex[0];const sIdx=p.pointIndex[1];const vn=topV[vIdx]||'';const sn=topS[sIdx]||'';openDrill(vn.substring(0,25)+' x '+sn,{vendor:vn,station:sn});}});
    }

    let html='<table id="vendor-tbl" class="display compact" style="width:100%"><thead><tr><th>Vendor</th><th>Spend</th><th>PO Count</th><th>Stations</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    vendors.forEach(v=>{html+=`<tr><td>${v.vendor_name}</td><td class="dollar">${fmtF$(v.spend)}</td><td>${v.po_count}</td><td>${v.stations||''}</td></tr>`;});
    html+='</tbody></table>';
    document.getElementById('vendor-table-wrap').innerHTML=html;
    if(dtI['vendor-tbl'])dtI['vendor-tbl'].destroy();
    dtI['vendor-tbl']=$('#vendor-tbl').DataTable(dtOpts({pageLength:25,order:[[1,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== ASSETS ====== */
let assetsData=[],assetMode='asset',assetSubcatActive=new Set();
const scColorMap={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
const statusColors={Ordered:'#78909C',Shipped:'#048EE5',Received:'#E8A838',Installed:'#B2DD79',Commissioned:'#4DD0E1'};
function setAssetMode(mode){
    assetMode=mode;
    document.getElementById('assetModeAsset').classList.toggle('active',mode==='asset');
    document.getElementById('assetModeTotal').classList.toggle('active',mode==='total');
    if(assetsData.length)filterAssets();
}
function toggleAssetSubcat(sc){
    if(assetSubcatActive.has(sc))assetSubcatActive.delete(sc);else assetSubcatActive.add(sc);
    renderSubcatChips();filterAssets();
}
function clearAssetSubcats(){assetSubcatActive.clear();renderSubcatChips();filterAssets();}
function renderSubcatChips(){
    const allSc=new Set();
    assetsData.forEach(r=>(r.sc_breakdown||[]).forEach(b=>{if(b.subcategory)allSc.add(b.subcategory);}));
    let html='';
    [...allSc].sort().forEach(sc=>{
        const active=assetSubcatActive.has(sc);const col=scColorMap[sc]||'#555';
        html+=`<span onclick="toggleAssetSubcat('${sc}')" style="cursor:pointer;display:inline-block;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid ${active?col:'var(--border)'};background:${active?'rgba(178,221,121,.12)':'var(--surface)'};color:${active?col:'var(--muted)'};transition:all .15s">${sc}</span>`;
    });
    if(assetSubcatActive.size)html+=`<span onclick="clearAssetSubcats()" style="cursor:pointer;display:inline-block;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid var(--border);color:var(--muted)">Clear All</span>`;
    document.getElementById('asset-subcat-chips').innerHTML=html;
}
async function saveAssetDate(sid,milestone,val){
    const res=await fetch('/api/asset-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({station_id:sid,milestone:milestone,date:val})});
    const d=await res.json();
    if(d.ok){
        const row=assetsData.find(r=>r.station_id===sid);
        if(row){row.status=d.status;row['date_'+milestone]=val;}
        showToast(sid+' '+milestone+' saved');
        filterAssets();
    }
}
async function loadAssets(){
    const res=await fetch(apiUrl('/api/assets'));const d=await res.json();
    if(!d.stations||!d.stations.length){document.getElementById('asset-kpis').innerHTML='<div class="kpi"><div class="label">No Data</div></div>';return;}
    assetsData=d.stations;
    const k=d.kpis;

    const owners=new Set(),vendors=new Set();
    d.stations.forEach(r=>{if(r.owner)owners.add(r.owner);if(r.primary_vendor)vendors.add(r.primary_vendor);});
    const oSel=document.getElementById('assetOwnerFilter');
    const vSel=document.getElementById('assetVendorFilter');
    oSel.innerHTML='<option value="">All Owners</option>';
    vSel.innerHTML='<option value="">All Vendors</option>';
    [...owners].sort().forEach(o=>{const el=document.createElement('option');el.value=o;el.textContent=o;oSel.appendChild(el);});
    [...vendors].sort().forEach(v=>{const el=document.createElement('option');el.value=v;el.textContent=v;vSel.appendChild(el);});

    const sc=k.status_counts||{};
    let statusKpis='';
    ['Ordered','Shipped','Received','Installed','Commissioned'].forEach(s=>{
        const cnt=sc[s]||0;if(cnt||s==='Ordered')statusKpis+=`<div class="kpi"><div class="label">${s}</div><div class="value" style="color:${statusColors[s]||'var(--muted)'}">${cnt}</div></div>`;
    });
    document.getElementById('asset-kpis').innerHTML=`
        <div class="kpi"><div class="label">Stations Tracked</div><div class="value">${k.station_count}</div></div>
        <div class="kpi"><div class="label">Total Asset Value</div><div class="value dollar" style="color:var(--green)">${fmt$(k.total_asset_value)}</div><div class="sub">Physical equipment only</div></div>
        <div class="kpi"><div class="label">Total Investment</div><div class="value dollar">${fmt$(k.total_investment)}</div><div class="sub">Incl. services, shipping, etc.</div></div>
        <div class="kpi"><div class="label">Services</div><div class="value dollar" style="color:#9B7ED8">${fmt$(k.services_total)}</div></div>
        ${statusKpis}`;

    renderSubcatChips();filterAssets();
}
function filterAssets(){
    const of=document.getElementById('assetOwnerFilter').value;
    const sf=document.getElementById('assetStatusFilter').value;
    const vf=document.getElementById('assetVendorFilter').value;
    let data=assetsData;
    if(of)data=data.filter(r=>r.owner===of);
    if(sf)data=data.filter(r=>r.status===sf);
    if(vf)data=data.filter(r=>r.primary_vendor===vf);
    if(assetSubcatActive.size){
        data=data.filter(r=>{
            const scs=new Set((r.sc_breakdown||[]).map(b=>b.subcategory));
            for(const sc of assetSubcatActive){if(scs.has(sc))return true;}return false;
        });
    }
    renderAssets(data);
}
function renderAssets(data){
    const valKey=assetMode==='asset'?'asset_value':'total_investment';
    const valLabel=assetMode==='asset'?'Asset Value':'Total Investment';

    const sorted=[...data].sort((a,b)=>b[valKey]-a[valKey]);
    const top=sorted.slice(0,30);
    if(top.length){
        const labels=top.map(r=>(r.station_id.replace(/^BASE\d+-\w+-/,'')+' '+r.station_name).substring(0,35));
        const maxLbl=Math.max(...labels.map(l=>l.length));
        const linePalette=['#B2DD79','#048EE5','#E8A838','#9B7ED8','#F06292','#4DD0E1','#FFB74D','#78909C','#CE93D8','#A1887F'];
        const lines=[...new Set(top.map(r=>r.line||'Unknown'))].sort();
        const lineColor={};
        lines.forEach((ln,i)=>{lineColor[ln]=linePalette[i%linePalette.length];});
        Plotly.newPlot('chart-asset-bars',[
            {
                y:labels,
                x:top.map(r=>r[valKey]),
                customdata:top.map(r=>r.line||'Unknown'),
                type:'bar',
                orientation:'h',
                name:valLabel,
                marker:{color:top.map(r=>lineColor[r.line||'Unknown']),line:{color:'rgba(0,0,0,.35)',width:0.5}},
                text:top.map(r=>fmt$(r[valKey])),
                textposition:'outside',
                textfont:{color:C.muted,size:10},
                hovertemplate:'%{y}<br>Line: %{customdata}<br>%{x:$,.0f}<extra></extra>'
            }
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:10},height:Math.max(400,top.length*28),margin:{l:Math.max(180,maxLbl*7),r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-asset-bars').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const idx=ev.points[0].pointIndex;const sid=top[idx].station_id;openDrill('Station: '+sid,{station:sid});}});
    }

    const stCounts={};
    data.forEach(r=>{stCounts[r.status]=(stCounts[r.status]||0)+1;});
    const stLabels=Object.keys(stCounts),stVals=Object.values(stCounts);
    Plotly.newPlot('chart-asset-delivery',[{
        type:'pie',labels:stLabels,values:stVals,hole:.5,
        marker:{colors:stLabels.map(s=>statusColors[s]||'#555')},
        textinfo:'label+value',textfont:{size:11},
        hovertemplate:'%{label}: %{value} stations<extra></extra>'
    }],{paper_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:20,r:20,t:20,b:20},showlegend:false},PC);

    const totAsset=data.reduce((s,r)=>s+r.asset_value,0);
    const totSvc=data.reduce((s,r)=>s+r.services_cost,0);
    const totShip=data.reduce((s,r)=>s+r.shipping_cost,0);
    const totCons=data.reduce((s,r)=>s+r.consumables_cost,0);
    const totAll=data.reduce((s,r)=>s+r.total_investment,0);
    const totOther=totAll-totAsset-totSvc-totShip-totCons;
    Plotly.newPlot('chart-asset-composition',[{
        type:'pie',labels:['Physical Asset','Services & Labor','Shipping','Consumables','Other'],
        values:[totAsset,totSvc,totShip,totCons,totOther>0?totOther:0],hole:.45,
        marker:{colors:[C.green,'#9B7ED8','#78909C','#FFB74D','#555']},
        textinfo:'label+percent',textfont:{size:11},
        hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
    }],{paper_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:20,r:20,t:20,b:20},showlegend:false},PC);

    let html='<table id="asset-tbl" class="display compact" style="width:100%"><thead><tr>';
    const cols=['Station','Name','Line','Owner','OEM Vendor',''+valLabel,'Services','POs','Status','Ordered','Shipped','Received','Installed','Commissioned','Sub-Categories'];
    cols.forEach(c=>{html+='<th>'+c+'</th>';});
    html+='</tr></thead><tfoot><tr>';
    cols.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    data.forEach(r=>{
        const sc=statusColors[r.status]||'var(--muted)';
        const scTags=(r.sc_breakdown||[]).map(b=>{const col=scColorMap[b.subcategory]||'#555';return `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:600;background:${col}22;color:${col};margin:1px 2px;white-space:nowrap">${b.subcategory} ${fmt$(b.spend)}</span>`;}).join('');
        const sid=r.station_id;
        const dtCell=(ms)=>`<input class="asset-date" type="date" value="${r['date_'+ms]||''}" onchange="saveAssetDate('${sid}','${ms}',this.value)"/>`;
        html+=`<tr>
            <td>${sid}</td>
            <td>${r.station_name}</td>
            <td>${r.line}</td>
            <td>${r.owner}</td>
            <td>${r.primary_vendor}</td>
            <td class="dollar">${fmtF$(r[valKey])}</td>
            <td class="dollar">${fmtF$(r.services_cost)}</td>
            <td>${r.po_count}</td>
            <td style="color:${sc};font-weight:700">${r.status}</td>
            <td>${dtCell('ordered')}</td>
            <td>${dtCell('shipped')}</td>
            <td>${dtCell('received')}</td>
            <td>${dtCell('installed')}</td>
            <td>${dtCell('commissioned')}</td>
            <td>${scTags}</td>
        </tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('asset-table-wrap').innerHTML=html;
    if(dtI['asset-tbl'])dtI['asset-tbl'].destroy();
    dtI['asset-tbl']=$('#asset-tbl').DataTable(dtOpts({pageLength:50,order:[[0,'asc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== SPARES ====== */
const SPARES_DEFAULT_BUCKET='';
async function loadSpares(){
    const res=await fetch('/api/spares');sparesData=await res.json();
    if(!sparesData.length){document.getElementById('spares-table-wrap').innerHTML='<p style="color:var(--muted)">No spares data.</p>';return;}

    const buckets=new Set(),stations=new Set(),cats=new Set(),subcats=new Set(),vendors=new Set();
    sparesData.forEach(r=>{
        if(r.item_bucket)buckets.add(r.item_bucket);
        if(r.station_ids)(r.station_ids+'').split(',').forEach(s=>{s=s.trim();if(s)stations.add(s);});
        if(r.product_category)cats.add(r.product_category);
        if(r.mfg_subcategory)subcats.add(r.mfg_subcategory);
        if(r.mfg_subcategories)(r.mfg_subcategories+'').split(',').forEach(sc=>{sc=sc.trim();if(sc)subcats.add(sc);});
        if(r.vendor_names)(r.vendor_names+'').split(',').forEach(v=>{v=v.trim();if(v)vendors.add(v);});
    });
    const bSel=document.getElementById('sparesBucketFilter');
    const sSel=document.getElementById('sparesStationFilter');
    const scSel=document.getElementById('sparesSubcatFilter');
    const cSel=document.getElementById('sparesCatFilter');
    const vSel=document.getElementById('sparesVendorFilter');
    bSel.innerHTML='<option value="">All Buckets</option>';
    sSel.innerHTML='<option value="">All Stations</option>';
    scSel.innerHTML='<option value="">All Sub-Categories</option>';
    cSel.innerHTML='<option value="">All Categories</option>';
    vSel.innerHTML='<option value="">All Vendors</option>';
    [...buckets].sort().forEach(b=>{const o=document.createElement('option');o.value=b;o.textContent=b;if(b===SPARES_DEFAULT_BUCKET)o.selected=true;bSel.appendChild(o);});
    [...stations].sort().forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sSel.appendChild(o);});
    [...subcats].sort().forEach(sc=>{const o=document.createElement('option');o.value=sc;o.textContent=sc;scSel.appendChild(o);});
    [...cats].sort().forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c.replace('Non-Inventory: ','');cSel.appendChild(o);});
    [...vendors].sort().forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;vSel.appendChild(o);});

    if(SPARES_DEFAULT_BUCKET && buckets.has(SPARES_DEFAULT_BUCKET))bSel.value=SPARES_DEFAULT_BUCKET;
    renderBucketSummary();
    filterSpares();
}
function renderBucketSummary(){
    const summary={};
    sparesData.forEach(r=>{
        const primary=(r.mfg_subcategory||'').trim();
        const fallback=(r.mfg_subcategories||'').split(',').map(s=>s.trim()).filter(Boolean)[0]||'Uncategorized';
        const sc=primary||fallback;
        if(!summary[sc])summary[sc]={count:0,spend:0};
        summary[sc].count++;
        summary[sc].spend+=(r.total_spend||0);
    });
    const sorted=Object.entries(summary).sort((a,b)=>b[1].spend-a[1].spend);
    const scColors={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64','Uncategorized':'#555'};
    let html='<div style="display:flex;flex-wrap:wrap;gap:8px">';
    sorted.forEach(([sc,d])=>{
        const col=scColors[sc]||'#555';
        const active=document.getElementById('sparesSubcatFilter').value;
        const sel=active===sc;
        html+=`<div onclick="document.getElementById('sparesSubcatFilter').value='${sc}';filterSpares();renderBucketSummary();" style="cursor:pointer;padding:8px 14px;background:${sel?'rgba(178,221,121,.12)':'var(--surface)'};border:1px solid ${sel?col:'var(--border)'};border-radius:8px;min-width:130px;transition:all .15s">`;
        html+=`<div style="font-size:10px;color:${col};font-weight:700;text-transform:uppercase;letter-spacing:.5px">${sc}</div>`;
        html+=`<div style="font-size:16px;font-weight:700;margin-top:2px">${fmtF$(d.spend)}</div>`;
        html+=`<div style="font-size:11px;color:var(--muted)">${d.count} items</div></div>`;
    });
    html+='<div onclick="document.getElementById(\'sparesSubcatFilter\').value=\'\';filterSpares();renderBucketSummary();" style="cursor:pointer;padding:8px 14px;background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;align-items:center"><div style="font-size:11px;color:var(--muted);font-weight:600">Show All</div></div>';
    html+='</div>';
    document.getElementById('spares-bucket-summary').innerHTML=html;
}
function filterSpares(){
    const bf=document.getElementById('sparesBucketFilter').value;
    const sf=document.getElementById('sparesStationFilter').value;
    const scf=document.getElementById('sparesSubcatFilter').value;
    const cf=document.getElementById('sparesCatFilter').value;
    const vf=document.getElementById('sparesVendorFilter').value;
    let data=sparesData;
    if(bf)data=data.filter(r=>r.item_bucket===bf);
    if(sf)data=data.filter(r=>(r.station_ids+'').includes(sf));
    if(scf)data=data.filter(r=>(r.mfg_subcategory===scf)||((r.mfg_subcategories+'').includes(scf)));
    if(cf)data=data.filter(r=>r.product_category===cf);
    if(vf)data=data.filter(r=>(r.vendor_names+'').includes(vf));
    renderBucketSummary();
    renderSpares(data);
}
function renderSpares(data){
    let html='<table id="spares-tbl" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Bucket</th><th>Sub-Category</th><th>Source</th><th>Category</th><th>Vendors</th><th>Stations</th><th>Qty</th><th>Avg Price</th><th>Total Spend</th><th>PO / Contact</th><th>Last Order</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    data.forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        const src=(r.source||'').split(',').map(s=>s.trim()).map(s=>`<span class="source-badge ${s}">${s}</span>`).join(' ');
        const sc=(r.mfg_subcategories||r.mfg_subcategory||'');
        html+=`<tr><td>${r.item_description||''}</td><td>${r.item_bucket||''}</td><td>${sc}</td><td>${src}</td><td>${(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_names||''}</td><td>${r.station_ids||''}</td><td>${r.total_qty_ordered||''}</td><td class="dollar">${fmtF$(r.avg_unit_price)}</td><td class="dollar">${fmtF$(r.total_spend)}</td><td>${r.po_or_contact||''}</td><td>${r.last_order_date||''}</td><td>${parts}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('spares-table-wrap').innerHTML=html;
    if(dtI['spares-tbl'])dtI['spares-tbl'].destroy();
    dtI['spares-tbl']=$('#spares-tbl').DataTable(dtOpts({pageLength:25,order:[[9,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== DETAIL ====== */
async function loadDetail(){
    const res=await fetch(apiUrl('/api/transactions'));const data=await res.json();
    if(!data.length){document.getElementById('detail-table-wrap').innerHTML='<p style="color:var(--muted)">No data.</p>';return;}
    const cols=['source','po_number','date_order','vendor_name','mfg_subcategory','item_description','station_id','mapping_confidence','price_subtotal','price_total','project_name','created_by_name'];
    const labels=['Source','PO','Date','Vendor','Sub-Category','Description','Station','Confidence','Subtotal','Total','Project','Created By'];
    let html='<table id="detail-tbl" class="display compact" style="width:100%"><thead><tr>';
    labels.forEach(l=>{html+=`<th>${l}</th>`;});
    html+='</tr></thead><tfoot><tr>';
    labels.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    data.forEach(r=>{
        html+='<tr>';
        cols.forEach(c=>{
            const v=r[c]!==undefined?r[c]:'';
            if(c==='price_subtotal'||c==='price_total')html+=`<td class="dollar">${fmtF$(parseFloat(v)||0)}</td>`;
            else if(c==='source')html+=`<td><span class="source-badge ${v}">${v}</span></td>`;
            else html+=`<td>${v}</td>`;
        });
        html+='</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('detail-table-wrap').innerHTML=html;
    if(dtI['detail-tbl'])dtI['detail-tbl'].destroy();
    dtI['detail-tbl']=$('#detail-tbl').DataTable(dtOpts({pageLength:50,order:[[8,'desc']],dom:'Bfrtip',buttons:['csv','excel'],scrollX:true}));
}

/* ====== TIMELINE ====== */
async function loadTimeline(){
    const res=await fetch(apiUrl('/api/timeline'));const d=await res.json();

    const L=function(h,m){return{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:h,margin:m,yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}};};

    if(d.cumulative&&d.cumulative.length){
        Plotly.newPlot('chart-cumulative',[{
            x:d.cumulative.map(c=>c.date),y:d.cumulative.map(c=>c.cumulative),
            type:'scatter',mode:'lines',fill:'tozeroy',
            line:{color:C.green,width:2.5},fillcolor:'rgba(178,221,121,0.08)',
            hovertemplate:'%{x}<br>%{y:$,.0f}<extra></extra>'
        }],L(350,{l:80,r:20,t:20,b:50}),PC);
    }

    if(d.monthly_source&&d.monthly_source.length){
        const months=[...new Set(d.monthly_source.map(m=>m.month))].sort();
        const odooY=months.map(m=>{const r=d.monthly_source.find(x=>x.month===m&&x.source==='odoo');return r?r.spend:0;});
        const rampY=months.map(m=>{const r=d.monthly_source.find(x=>x.month===m&&x.source==='ramp');return r?r.spend:0;});
        const lay=L(300,{l:70,r:20,t:40,b:50});lay.barmode='group';lay.legend={font:{color:C.muted},x:0,y:1.15,orientation:'h'};
        Plotly.newPlot('chart-timeline-source',[
            {x:months,y:odooY,type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:rampY,type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ],lay,PC);
        document.getElementById('chart-timeline-source').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const src=p.data.name==='Ramp CC'?'ramp':'odoo';openDrill(p.x+' | '+p.data.name,{month:p.x,source:src});}});
    }

    if(d.weekly&&d.weekly.length){
        const lay=L(300,{l:70,r:20,t:20,b:60});lay.xaxis.tickangle=-45;
        Plotly.newPlot('chart-weekly',[{
            x:d.weekly.map(w=>w.week),y:d.weekly.map(w=>w.spend),type:'bar',
            marker:{color:d.weekly.map(w=>w.spend>500000?C.red:w.spend>200000?C.yellow:w.spend>50000?C.blue:C.green)},
            text:d.weekly.map(w=>w.count+' items'),hovertemplate:'%{x}<br>%{y:$,.0f}<br>%{text}<extra></extra>'
        }],lay,PC);
        document.getElementById('chart-weekly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const wk=ev.points[0].x;openDrill('Week: '+wk,{week:wk});}});
    }

    const scColors2={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
    if(d.monthly_subcat&&d.monthly_subcat.length){
        const scs=[...new Set(d.monthly_subcat.map(m=>m.subcategory))];
        const scMonths=[...new Set(d.monthly_subcat.map(m=>m.month))].sort();
        const scTraces=scs.map((sc,i)=>({
            x:scMonths,y:scMonths.map(m=>{const row=d.monthly_subcat.find(r=>r.month===m&&r.subcategory===sc);return row?row.spend:0;}),
            name:sc,type:'bar',marker:{color:scColors2[sc]||`hsl(${90+i*25},55%,52%)`}
        }));
        const scLay=L(400,{l:70,r:20,t:20,b:50});scLay.barmode='stack';scLay.legend={font:{color:C.muted,size:10}};
        Plotly.newPlot('chart-monthly-subcat',scTraces,scLay,PC);
        document.getElementById('chart-monthly-subcat').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];openDrill(p.x+' | '+p.data.name,{month:p.x,subcategory:p.data.name});}});
    }

    if(d.monthly_cat&&d.monthly_cat.length){
        const cats=[...new Set(d.monthly_cat.map(m=>m.category))];
        const months=[...new Set(d.monthly_cat.map(m=>m.month))].sort();
        const traces=cats.slice(0,10).map((cat,i)=>({
            x:months,y:months.map(m=>{const row=d.monthly_cat.find(r=>r.month===m&&r.category===cat);return row?row.spend:0;}),
            name:cat.replace('Non-Inventory: ',''),type:'bar',marker:{color:`hsl(${90+i*25},55%,${50+i*2}%)`}
        }));
        const lay=L(400,{l:70,r:20,t:20,b:50});lay.barmode='stack';lay.legend={font:{color:C.muted,size:10}};
        Plotly.newPlot('chart-monthly-cat',traces,lay,PC);
        document.getElementById('chart-monthly-cat').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const catName=p.data.name;const fullCat=d.monthly_cat.find(r=>r.category.replace('Non-Inventory: ','')==catName);openDrill(p.x+' | '+catName,{month:p.x,category:fullCat?fullCat.category:catName});}});
    }
}

/* ====== OTHER PROJECTS ====== */
async function loadProjects(){
    const res=await fetch('/api/projects');const d=await res.json();
    if(!d.projects||!d.projects.length){document.getElementById('proj-kpis').innerHTML='<p style="color:var(--muted)">No non-production project data.</p>';return;}

    document.getElementById('proj-kpis').innerHTML=`
        <div class="kpi"><div class="label">Non-Production Spend</div><div class="value dollar">${fmt$(d.total_spend)}</div><div class="sub">${(d.total_lines||0).toLocaleString()} line items</div></div>
        <div class="kpi"><div class="label">Project Categories</div><div class="value">${d.projects.length}</div></div>
        <div class="kpi"><div class="label">Largest Project</div><div class="value" style="font-size:14px">${d.projects[0].name}</div><div class="sub">${fmt$(d.projects[0].spend)}</div></div>`;

    // Breakdown bar chart
    const p=d.projects.slice(0,12);
    Plotly.newPlot('chart-proj-breakdown',[{
        y:p.map(x=>x.name.length>35?x.name.substring(0,35)+'...':x.name).reverse(),
        x:p.map(x=>x.spend).reverse(),
        type:'bar',orientation:'h',marker:{color:C.green},
        text:p.map(x=>fmt$(x.spend)+' ('+x.count+' items)').reverse(),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
    }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(300,p.length*35),margin:{l:250,r:100,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
    document.getElementById('chart-proj-breakdown').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;const proj=d.projects.find(x=>x.name.startsWith(lbl.replace('...','')));const pn=proj?proj.name:lbl;openDrill('Project: '+pn,{project:pn});}});

    if(d.top_vendors&&d.top_vendors.length){
        const v=[...d.top_vendors].reverse();
        Plotly.newPlot('chart-proj-vendors',[{
            y:v.map(x=>x.vendor.length>28?x.vendor.substring(0,28)+'...':x.vendor),
            x:v.map(x=>x.spend),type:'bar',orientation:'h',marker:{color:C.blue},
            text:v.map(x=>fmt$(x.spend)),textposition:'outside',textfont:{color:C.muted,size:10}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,v.length*30),margin:{l:200,r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-proj-vendors').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=d.top_vendors.find(x=>x.vendor.startsWith(ev.points[0].y.replace('...','')));openDrill('Vendor: '+(vn?vn.vendor:ev.points[0].y),{vendor:vn?vn.vendor:ev.points[0].y});}});
    }

    // Monthly
    if(d.monthly&&d.monthly.length){
        const pm=d.monthly.sort((a,b)=>a.month.localeCompare(b.month));
        Plotly.newPlot('chart-proj-monthly',[{
            x:pm.map(m=>m.month),y:pm.map(m=>m.spend),
            type:'bar',marker:{color:C.yellow},
            text:pm.map(m=>fmt$(m.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{x}<br>%{y:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:300,margin:{l:70,r:60,t:30,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,type:'category',tickangle:-45}},PC);
    }

    // Detail table
    if(d.details&&d.details.length){
        const cols=['source','po_number','date_order','vendor_name','item_description','station_id','project_name','price_subtotal','created_by_name'];
        const labels=['Source','PO','Date','Vendor','Description','Category','Project','Subtotal','Created By'];
        let html='<table id="proj-tbl" class="display compact" style="width:100%"><thead><tr>';
        labels.forEach(l=>{html+=`<th>${l}</th>`;});
        html+='</tr></thead><tfoot><tr>';
        labels.forEach(()=>{html+='<th></th>';});
        html+='</tr></tfoot><tbody>';
        d.details.forEach(r=>{
            html+='<tr>';
            cols.forEach(c=>{
                const v=r[c]!==undefined?r[c]:'';
                if(c==='price_subtotal')html+=`<td class="dollar">${fmtF$(parseFloat(v)||0)}</td>`;
                else if(c==='source')html+=`<td><span class="source-badge ${v}">${v}</span></td>`;
                else html+=`<td>${v}</td>`;
            });
            html+='</tr>';
        });
        html+='</tbody></table>';
        document.getElementById('proj-detail-wrap').innerHTML=html;
        if(dtI['proj-tbl'])dtI['proj-tbl'].destroy();
        dtI['proj-tbl']=$('#proj-tbl').DataTable(dtOpts({pageLength:25,order:[[7,'desc']],dom:'Bfrtip',buttons:['csv','excel'],scrollX:true}));
    }
}

/* ====== SETTINGS ====== */
let savedSettings={};
async function loadSettings(){
    const res=await fetch('/api/settings');savedSettings=await res.json();
    const mRes=await fetch('/api/modules');const mods=await mRes.json();
    const caps=savedSettings.line_capacities||{};
    const sqfts=savedSettings.line_sqft||{};
    let html='<table style="width:100%;border-collapse:collapse"><thead><tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Line</th><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Capacity (GWh)</th><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Floor Area (ft&sup2;)</th></tr></thead><tbody>';
    mods.forEach(m=>{
        html+=`<tr style="border-bottom:1px solid rgba(62,61,58,.3)"><td style="padding:8px;font-size:13px;font-weight:600;color:var(--green)">${m}</td><td style="padding:8px"><input class="forecast-input" id="cap-${m}" type="number" step="0.1" min="0" value="${caps[m]||''}" placeholder="0" style="width:120px"/></td><td style="padding:8px"><input class="forecast-input" id="sqft-${m}" type="number" step="100" min="0" value="${sqfts[m]||''}" placeholder="0" style="width:120px"/></td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('settings-lines').innerHTML=html;
}
async function saveSettings(){
    const mRes=await fetch('/api/modules');const mods=await mRes.json();
    const caps={},sqfts={};
    mods.forEach(m=>{
        const cv=parseFloat(document.getElementById('cap-'+m).value);
        const sv=parseFloat(document.getElementById('sqft-'+m).value);
        if(!isNaN(cv)&&cv>0)caps[m]=cv;
        if(!isNaN(sv)&&sv>0)sqfts[m]=sv;
    });
    const body={line_capacities:caps,line_sqft:sqfts};
    const res=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await res.json();
    if(d.ok){document.getElementById('settings-ok').style.display='inline';setTimeout(()=>{document.getElementById('settings-ok').style.display='none';},2500);showToast('Settings saved');}
}

/* ====== UNIT ECONOMICS ====== */
async function loadUnitEconomics(){
    const res=await fetch('/api/unit-economics');const d=await res.json();
    const t=d.totals||{};
    const lines=d.lines||[];
    const hasData=lines.some(l=>l.gwh>0);

    document.getElementById('ue-kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Forecasted Spend</div><div class="value dollar">${fmt$(t.total_spend)}</div></div>
        <div class="kpi"><div class="label">Hub Capacity (max MOD vs INV)</div><div class="value">${(t.total_gwh||0).toFixed(1)} GWh</div><div style="font-size:11px;color:var(--muted);margin-top:6px">Line sum: ${(t.total_line_gwh||0).toFixed(1)} GWh</div></div>
        <div class="kpi"><div class="label">Avg Forecast $/GWh</div><div class="value dollar">${t.avg_dollar_per_gwh?fmt$(t.avg_dollar_per_gwh):'<span style=\"color:var(--muted);font-size:14px\">Set capacities in Settings</span>'}</div></div>
        <div class="kpi"><div class="label">Total Floor Area</div><div class="value">${t.total_sqft?(t.total_sqft).toLocaleString()+' ft&sup2;':'--'}</div></div>
        <div class="kpi"><div class="label">Avg ft&sup2;/GWh</div><div class="value">${t.avg_sqft_per_gwh?(t.avg_sqft_per_gwh).toLocaleString(undefined,{maximumFractionDigits:0})+' ft&sup2;':'--'}</div></div>`;

    if(!hasData){
        ['chart-ue-dollar','chart-ue-compare','chart-ue-sqft','chart-ue-stack'].forEach(id=>{document.getElementById(id).innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">Configure line capacities in Settings to see unit economics.</p>';});
        document.getElementById('ue-table-wrap').innerHTML='';
        return;
    }

    const configured=lines.filter(l=>l.gwh>0).sort((a,b)=>a.line.localeCompare(b.line));

    const UL=function(m,extra){const o={paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:350,margin:m||{l:70,r:20,t:20,b:90},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickangle:-30}};if(extra)Object.assign(o,extra);return o;};

    // $/GWh bar chart
    Plotly.newPlot('chart-ue-dollar',[{
        x:configured.map(l=>l.line),y:configured.map(l=>l.forecast_per_gwh||0),
        type:'bar',marker:{color:C.green},
        text:configured.map(l=>l.forecast_per_gwh?fmt$(l.forecast_per_gwh):''),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{x}<br>%{y:$,.0f}/GWh<extra></extra>'
    }],UL({l:70,r:60,t:20,b:90}),PC);

    // Forecast spend by line
    const cmpLay=UL({l:70,r:20,t:20,b:90});
    Plotly.newPlot('chart-ue-compare',[
        {x:configured.map(l=>l.line),y:configured.map(l=>l.forecasted||0),type:'bar',name:'Forecasted Spend',marker:{color:C.surface2},text:configured.map(l=>fmt$(l.forecasted||0)),textposition:'outside',textfont:{color:C.muted,size:10}},
    ],cmpLay,PC);

    // ft²/GWh
    const withSqft=configured.filter(l=>l.sqft_per_gwh);
    if(withSqft.length){
        const sqLay=UL({l:70,r:60,t:20,b:90});sqLay.yaxis.tickprefix='';sqLay.yaxis.tickformat=',.0f';
        Plotly.newPlot('chart-ue-sqft',[{
            x:withSqft.map(l=>l.line),y:withSqft.map(l=>l.sqft_per_gwh),
            type:'bar',marker:{color:C.blue},
            text:withSqft.map(l=>(l.sqft_per_gwh||0).toLocaleString(undefined,{maximumFractionDigits:0})+' ft\u00B2'),textposition:'outside',textfont:{color:C.muted,size:10}
        }],sqLay,PC);
    } else {
        document.getElementById('chart-ue-sqft').innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">Set floor area in Settings to see ft\u00B2/GWh.</p>';
    }

    // Forecast-only $/GWh composition
    const stkLay=UL({l:70,r:20,t:20,b:90});
    Plotly.newPlot('chart-ue-stack',[
        {x:configured.map(l=>l.line),y:configured.map(l=>(l.forecast_per_gwh||0)),type:'bar',name:'Forecast $/GWh',marker:{color:C.green},text:configured.map(l=>l.forecast_per_gwh?fmt$(l.forecast_per_gwh):''),textposition:'outside',textfont:{color:C.muted,size:10}},
    ],stkLay,PC);

    // Detail table
    let html='<table id="ue-tbl" class="display compact" style="width:100%"><thead><tr><th>Line</th><th>GWh</th><th>Forecasted Spend</th><th>$/GWh (Forecast)</th><th>ft&sup2;</th><th>ft&sup2;/GWh</th><th>Stations</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    configured.forEach(l=>{
        html+=`<tr><td style="font-weight:600;color:var(--green)">${l.line}</td><td>${l.gwh.toFixed(1)}</td><td class="dollar">${fmtF$(l.forecasted)}</td><td class="dollar">${l.forecast_per_gwh?fmtF$(l.forecast_per_gwh):'--'}</td><td>${l.sqft?(l.sqft).toLocaleString():''}</td><td>${l.sqft_per_gwh?(l.sqft_per_gwh).toLocaleString(undefined,{maximumFractionDigits:0}):''}</td><td>${l.station_count}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('ue-table-wrap').innerHTML=html;
    if(dtI['ue-tbl'])dtI['ue-tbl'].destroy();
    dtI['ue-tbl']=$('#ue-tbl').DataTable(dtOpts({pageLength:25,order:[[3,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

function showToast(msg){const t=document.getElementById('toast');t.textContent=msg;t.style.display='block';setTimeout(()=>{t.style.display='none';},3000);}
function initFromHash(){
    const hash=window.location.hash.slice(1);
    if(hash&&['executive','about','source','stations','forecasting','vendors','spares','detail','timeline','projects','uniteco','settings'].includes(hash)){
        const navItem=document.querySelector(`.nav-item[onclick*="showPage('${hash}')"]`);
        showPage(hash,navItem);
    }else{
        loadExecutive();
    }
}
window.addEventListener('hashchange',initFromHash);
// Wait for line filter default selection before loading data
initLineFilter().then(function(){ initFromHash(); });
</script>
</body>
</html>"""


@app.route("/api/projects")
def api_projects():
    """Return spend data for non-production projects (pilot, NPI, facilities, etc.)."""
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"projects": [], "monthly": [], "top_vendors": [], "details": []})

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_mod"] = df["station_id"].apply(_extract_mod)

    non_prod = df[
        (df["_mod"] == "") &
        (df["station_id"] != "")
    ].copy()

    prod_unmapped = df[
        (df["_mod"] == "") &
        (df["station_id"] == "")
    ].copy()

    project_groups = {}
    for _, row in non_prod.iterrows():
        key = row["station_id"]
        if key not in project_groups:
            project_groups[key] = {"name": key, "spend": 0, "count": 0, "vendors": set()}
        project_groups[key]["spend"] += row["_sub"]
        project_groups[key]["count"] += 1
        if row["vendor_name"]:
            project_groups[key]["vendors"].add(row["vendor_name"])

    by_proj_name = prod_unmapped.groupby("project_name").agg(
        spend=("_sub", "sum"), count=("_sub", "size"),
    ).reset_index().sort_values("spend", ascending=False)
    for _, row in by_proj_name.iterrows():
        pn = row["project_name"] or "(unmapped)"
        if pn not in project_groups:
            project_groups[pn] = {"name": pn, "spend": 0, "count": 0, "vendors": set()}
        project_groups[pn]["spend"] += row["spend"]
        project_groups[pn]["count"] += int(row["count"])

    projects = sorted(project_groups.values(), key=lambda x: -x["spend"])
    for p in projects:
        p["vendors"] = len(p["vendors"]) if isinstance(p["vendors"], set) else 0

    all_other = pd.concat([non_prod, prod_unmapped], ignore_index=True)
    all_other["_date"] = pd.to_datetime(all_other["date_order"], errors="coerce")
    dated = all_other.dropna(subset=["_date"]).copy()
    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)

    monthly = dated.groupby("_month")["_sub"].sum().reset_index()
    monthly_data = [{"month": r["_month"], "spend": float(r["_sub"])} for _, r in monthly.iterrows()]

    vendor_agg = all_other.groupby("vendor_name")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
    vendor_data = [{"vendor": r["vendor_name"], "spend": float(r["_sub"])} for _, r in vendor_agg.iterrows()]

    detail_cols = ["source", "po_number", "date_order", "vendor_name", "item_description",
                   "station_id", "project_name", "price_subtotal", "created_by_name"]
    details = all_other.sort_values("_sub", ascending=False).head(500)
    detail_data = [{c: row.get(c, "") for c in detail_cols} for _, row in details.iterrows()]

    return jsonify({
        "projects": projects,
        "monthly": monthly_data,
        "top_vendors": vendor_data,
        "details": detail_data,
        "total_spend": float(all_other["_sub"].sum()),
        "total_lines": len(all_other),
    })


@app.route("/api/drilldown")
def api_drilldown():
    """Flexible drill-down: filter transactions by any field combo via query params."""
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    df["_line"] = df["station_id"].apply(_extract_line)
    df["_month"] = df["_date"].dt.to_period("M").astype(str)

    vendor = request.args.get("vendor", "")
    station = request.args.get("station", "")
    line = request.args.get("line", "")
    month = request.args.get("month", "")
    category = request.args.get("category", "")
    source = request.args.get("source", "")
    confidence = request.args.get("confidence", "")
    project = request.args.get("project", "")
    week = request.args.get("week", "")
    employee = request.args.get("employee", "")
    subcategory = request.args.get("subcategory", "")
    payment_status = request.args.get("payment_status", "")

    if vendor:
        df = df[df["vendor_name"] == vendor]
    if station:
        df = df[df["station_id"] == station]
    if line:
        df = df[df["_line"] == line]
    if month:
        df = df[df["_month"] == month]
    if category:
        df = df[df["product_category"] == category]
    if source:
        df = df[df["source"] == source]
    if confidence:
        df = df[df["mapping_confidence"] == confidence]
    if project:
        df = df[df["project_name"] == project]
    if employee:
        df = df[df["created_by_name"] == employee]
    if subcategory and "mfg_subcategory" in df.columns:
        df = df[df["mfg_subcategory"] == subcategory]
    if payment_status and "bill_payment_status" in df.columns:
        bps = df["bill_payment_status"].fillna("").astype(str).str.strip().replace("", "no_bill")
        df = df[bps == payment_status]
    if week:
        df["_week"] = df["_date"].dt.isocalendar().apply(
            lambda r: f"{int(r['year'])}-W{int(r['week']):02d}" if pd.notna(r["year"]) else "", axis=1
        )
        df = df[df["_week"] == week]

    total = float(df["_sub"].sum())
    count = len(df)

    cols = ["source", "po_number", "date_order", "vendor_name", "mfg_subcategory",
            "item_description", "station_id", "project_name", "mapping_confidence",
            "bill_payment_status", "price_subtotal", "price_total", "created_by_name"]
    rows = df.sort_values("_sub", ascending=False).head(200)
    records = [{c: row.get(c, "") for c in cols} for _, row in rows.iterrows()]

    return jsonify({"total": total, "count": count, "rows": records})


@app.route("/api/forecast", methods=["POST"])
def api_forecast_update():
    """Save a forecast override for a station."""
    body = request.get_json(force=True)
    station_id = body.get("station_id", "")
    new_forecast = body.get("forecasted_cost")
    if not station_id or new_forecast is None:
        return jsonify({"ok": False, "error": "station_id and forecasted_cost required"}), 400

    try:
        value = float(new_forecast)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "forecasted_cost must be numeric"}), 400
    if value < 0:
        return jsonify({"ok": False, "error": "forecasted_cost must be >= 0"}), 400

    result = _apply_forecast_updates({str(station_id): value})
    if result["updated_count"] == 0:
        return jsonify({
            "ok": False,
            "error": "station_id not found in capex_by_station.csv",
            "unmatched_station_ids": result["unmatched_station_ids"],
        }), 404
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/bulk", methods=["POST"])
def api_forecast_bulk_update():
    """Save many station forecast overrides in one request."""
    body = request.get_json(force=True)
    raw_updates = body.get("updates", [])
    if not isinstance(raw_updates, list):
        return jsonify({"ok": False, "error": "updates must be an array"}), 400

    parsed_updates: dict[str, float] = {}
    skipped_invalid = 0
    for item in raw_updates:
        if not isinstance(item, dict):
            skipped_invalid += 1
            continue
        sid = str(item.get("station_id", "")).strip()
        raw_val = item.get("forecasted_cost")
        if not sid or raw_val is None:
            skipped_invalid += 1
            continue
        try:
            val = float(raw_val)
        except (TypeError, ValueError):
            skipped_invalid += 1
            continue
        if val < 0:
            skipped_invalid += 1
            continue
        parsed_updates[sid] = val

    if not parsed_updates:
        return jsonify({"ok": False, "error": "No valid updates supplied."}), 400

    result = _apply_forecast_updates(parsed_updates)
    skipped_total = skipped_invalid + len(result["unmatched_station_ids"])
    return jsonify({
        "ok": True,
        **result,
        "skipped_count": skipped_total,
        "invalid_count": skipped_invalid,
    })


def _normalize_station_ids(raw_station_ids: object) -> list[str]:
    if not isinstance(raw_station_ids, list):
        return []
    norm: list[str] = []
    seen: set[str] = set()
    for sid in raw_station_ids:
        key = str(sid).strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        norm.append(key)
    return norm


def _lock_forecast_overrides(station_ids: list[str] | None = None) -> dict[str, object]:
    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty or "station_id" not in by_station.columns:
        return {
            "locked_count": 0,
            "not_found_count": len(station_ids or []),
            "locked_station_ids": [],
            "not_found_station_ids": station_ids or [],
        }

    station_map: dict[str, tuple[str, float]] = {}
    for _, row in by_station.iterrows():
        sid = str(row.get("station_id", "")).strip()
        if not sid:
            continue
        sid_key = sid.upper()
        forecast_value = float(pd.to_numeric(row.get("forecasted_cost", 0), errors="coerce") or 0.0)
        station_map[sid_key] = (sid, forecast_value)

    targets = set(station_ids or station_map.keys())
    overrides = store.read_json("forecast_overrides.json")
    if not isinstance(overrides, dict):
        overrides = {}

    # Keep one canonical key per station_id when lock state is updated.
    for existing_key in list(overrides.keys()):
        if str(existing_key).strip().upper() in targets:
            overrides.pop(existing_key, None)

    locked_station_ids: list[str] = []
    not_found_station_ids: list[str] = []
    for sid_key in sorted(targets):
        data = station_map.get(sid_key)
        if not data:
            not_found_station_ids.append(sid_key)
            continue
        canonical_sid, forecast_value = data
        overrides[canonical_sid] = forecast_value
        locked_station_ids.append(canonical_sid)

    store.write_json("forecast_overrides.json", overrides)
    return {
        "locked_count": len(locked_station_ids),
        "not_found_count": len(not_found_station_ids),
        "locked_station_ids": locked_station_ids,
        "not_found_station_ids": not_found_station_ids,
    }


def _unlock_forecast_overrides(station_ids: list[str] | None = None) -> dict[str, object]:
    overrides = store.read_json("forecast_overrides.json")
    if not isinstance(overrides, dict):
        overrides = {}

    if station_ids is None:
        removed_station_ids = sorted(str(k).strip() for k in overrides.keys() if str(k).strip())
        removed_count = len(removed_station_ids)
        overrides = {}
        store.write_json("forecast_overrides.json", overrides)
        return {
            "removed_count": removed_count,
            "not_found_count": 0,
            "removed_station_ids": removed_station_ids,
            "not_found_station_ids": [],
        }

    targets = set(station_ids)
    removed_station_ids: list[str] = []
    for existing_key in list(overrides.keys()):
        if str(existing_key).strip().upper() in targets:
            removed_station_ids.append(str(existing_key).strip())
            overrides.pop(existing_key, None)

    removed_keys = {sid.upper() for sid in removed_station_ids}
    not_found_station_ids = [sid for sid in sorted(targets) if sid not in removed_keys]
    store.write_json("forecast_overrides.json", overrides)
    return {
        "removed_count": len(removed_station_ids),
        "not_found_count": len(not_found_station_ids),
        "removed_station_ids": removed_station_ids,
        "not_found_station_ids": not_found_station_ids,
    }


@app.route("/api/forecast/unlock", methods=["POST"])
def api_forecast_unlock():
    """Remove a manual forecast override lock for a station."""
    body = request.get_json(force=True)
    station_id = str(body.get("station_id", "")).strip()
    if not station_id:
        return jsonify({"ok": False, "error": "station_id required"}), 400

    result = _unlock_forecast_overrides([station_id.upper()])
    if result["removed_count"] == 0:
        return jsonify({"ok": False, "error": f"No override lock found for {station_id}"}), 404
    return jsonify({
        "ok": True,
        "station_id": station_id,
        "removed_count": result["removed_count"],
    })


@app.route("/api/forecast/lock", methods=["POST"])
def api_forecast_lock():
    """Create/refresh a manual forecast override lock for a station."""
    body = request.get_json(force=True)
    station_id = str(body.get("station_id", "")).strip()
    if not station_id:
        return jsonify({"ok": False, "error": "station_id required"}), 400

    result = _lock_forecast_overrides([station_id.upper()])
    if result["locked_count"] == 0:
        return jsonify({"ok": False, "error": f"station_id not found: {station_id}"}), 404
    canonical_sid = result["locked_station_ids"][0]
    by_station = _load_csv("capex_by_station.csv")
    forecast_series = by_station.loc[
        by_station["station_id"].fillna("").astype(str).str.strip().str.upper() == canonical_sid.upper(),
        "forecasted_cost",
    ]
    forecast_value = float(pd.to_numeric(forecast_series.iloc[0], errors="coerce") or 0.0) if not forecast_series.empty else 0.0
    return jsonify({
        "ok": True,
        "station_id": canonical_sid,
        "forecasted_cost": forecast_value,
    })


@app.route("/api/forecast/lock_all", methods=["POST"])
def api_forecast_lock_all():
    """Lock many forecast rows (defaults to all rows when station_ids is omitted)."""
    body = request.get_json(silent=True) or {}
    station_ids_raw = body.get("station_ids")
    station_ids = _normalize_station_ids(station_ids_raw)
    if station_ids_raw is not None and not station_ids:
        return jsonify({"ok": False, "error": "station_ids must be a non-empty array"}), 400
    result = _lock_forecast_overrides(station_ids if station_ids else None)
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/unlock_all", methods=["POST"])
def api_forecast_unlock_all():
    """Unlock many forecast rows (defaults to all locks when station_ids is omitted)."""
    body = request.get_json(silent=True) or {}
    station_ids_raw = body.get("station_ids")
    station_ids = _normalize_station_ids(station_ids_raw)
    if station_ids_raw is not None and not station_ids:
        return jsonify({"ok": False, "error": "station_ids must be a non-empty array"}), 400
    result = _unlock_forecast_overrides(station_ids if station_ids else None)
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/refresh", methods=["POST"])
def api_forecast_refresh():
    """Refresh station forecast values from configured BF1/BF2 Google Sheets."""
    body = request.get_json(silent=True) or {}
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}

    bf1_url = str(body.get("bf1_sheet_url") or settings.get("bf1_sheet_url") or DEFAULT_BF1_SHEET_URL).strip()
    bf2_url = str(body.get("bf2_sheet_url") or settings.get("bf2_sheet_url") or DEFAULT_BF2_SHEET_URL).strip()
    settings["bf1_sheet_url"] = bf1_url
    settings["bf2_sheet_url"] = bf2_url
    store.write_json("dashboard_settings.json", settings)

    try:
        from sheets_forecast_import import SheetImportError, import_forecast_updates
    except Exception as exc:  # pragma: no cover - defensive
        return jsonify({
            "ok": False,
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "errors": [f"Sheets importer unavailable: {exc}"],
            "bf1": {"ok": False, "candidate_updates": 0},
            "bf2": {"ok": False, "candidate_updates": 0},
        }), 500

    access_token = get_google_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "errors": [
                "Google OAuth token unavailable. Please sign out/in again to grant Sheets access."
            ],
            "bf1": {"ok": False, "candidate_updates": 0},
            "bf2": {"ok": False, "candidate_updates": 0},
        }), 401

    errors: list[str] = []
    sheet_results: dict[str, dict] = {}
    per_sheet_updates: dict[str, dict[str, float]] = {"bf1": {}, "bf2": {}}

    for key, url, prefix in (
        ("bf1", bf1_url, "BASE1-"),
        ("bf2", bf2_url, "BASE2-"),
    ):
        try:
            imported = import_forecast_updates(url, access_token=access_token)
            updates = imported.get("updates", {})
            if not isinstance(updates, dict):
                updates = {}
            typed_updates = {str(sid).strip().upper(): float(val) for sid, val in updates.items()}
            scoped = {sid: val for sid, val in typed_updates.items() if sid.startswith(prefix)}
            out_of_scope = sorted([sid for sid in typed_updates.keys() if not sid.startswith(prefix)])
            per_sheet_updates[key] = scoped
            sheet_results[key] = {
                "ok": True,
                "candidate_updates": len(scoped),
                "out_of_scope_count": len(out_of_scope),
                "out_of_scope_samples": out_of_scope[:20],
                "diagnostics": imported.get("diagnostics", {}),
            }
        except SheetImportError as exc:
            errors.append(f"{key.upper()} import failed: {exc}")
            sheet_results[key] = {"ok": False, "error": str(exc), "candidate_updates": 0}
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(f"{key.upper()} import failed: {exc}")
            sheet_results[key] = {"ok": False, "error": str(exc), "candidate_updates": 0}

    merged_updates: dict[str, float] = {}
    merged_updates.update(per_sheet_updates["bf1"])
    merged_updates.update(per_sheet_updates["bf2"])
    overrides = store.read_json("forecast_overrides.json")
    locked_station_ids = {str(k).strip().upper() for k in (overrides or {})} if isinstance(overrides, dict) else set()
    apply_result = _apply_forecast_updates(
        merged_updates,
        update_overrides=False,
        locked_station_ids=locked_station_ids,
    ) if merged_updates else {
        "updated_count": 0,
        "updated_station_ids": [],
        "unmatched_station_ids": [],
        "locked_skipped_station_ids": [],
    }
    updated_set = set(str(s).strip().upper() for s in apply_result["updated_station_ids"])
    bf1_set = set(per_sheet_updates["bf1"].keys())
    bf2_set = set(per_sheet_updates["bf2"].keys())
    sheet_results.setdefault("bf1", {})
    sheet_results.setdefault("bf2", {})
    sheet_results["bf1"]["applied_updates"] = len(updated_set & bf1_set)
    sheet_results["bf2"]["applied_updates"] = len(updated_set & bf2_set)

    ok = bool(apply_result["updated_count"] > 0) or (
        not errors and bool(merged_updates)
    )
    return jsonify({
        "ok": ok,
        "updated_count": apply_result["updated_count"],
        "updated_station_ids": apply_result["updated_station_ids"],
        "unmatched_station_ids": apply_result["unmatched_station_ids"],
        "locked_skipped_station_ids": apply_result["locked_skipped_station_ids"],
        "locked_skipped_count": len(apply_result["locked_skipped_station_ids"]),
        "errors": errors,
        "bf1": sheet_results["bf1"],
        "bf2": sheet_results["bf2"],
    })


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    """Return all dashboard settings (line capacities, sq ft, etc.)."""
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}
    settings.setdefault("bf1_sheet_url", DEFAULT_BF1_SHEET_URL)
    settings.setdefault("bf2_sheet_url", DEFAULT_BF2_SHEET_URL)
    return jsonify(settings)


@app.route("/api/settings", methods=["POST"])
def api_settings_save():
    """Save dashboard settings."""
    body = request.get_json(force=True)
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}
    settings.update(body)
    store.write_json("dashboard_settings.json", settings)
    return jsonify({"ok": True})


@app.route("/api/unit-economics")
def api_unit_economics():
    """Compute $/GWh and ft²/GWh per line using saved settings."""
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}
    raw_caps = settings.get("line_capacities", {})
    raw_sqft = settings.get("line_sqft", {})

    def _resolve_settings(raw: dict) -> dict:
        """Roll old CELL keys into their parent MOD line."""
        resolved: dict[str, float] = {}
        for key, val in raw.items():
            parent = _extract_line(key + "-ST0") if _re.match(r"BASE\d+-(CELL|MOD|INV)", key) else key
            if not parent:
                parent = key
            resolved[parent] = resolved.get(parent, 0) + float(val)
        return resolved

    line_caps = _resolve_settings(raw_caps)
    line_sqft = _resolve_settings(raw_sqft)

    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty:
        return jsonify({"lines": [], "totals": {}})

    by_station["_mod"] = by_station["station_id"].apply(lambda s: _extract_line(s) or "Other")
    mod_agg = by_station.groupby("_mod").agg(
        forecasted=("forecasted_cost", lambda x: pd.to_numeric(x, errors="coerce").sum()),
        stations=("station_id", "count"),
    ).reset_index()

    lines = []
    total_spend = 0.0
    total_line_gwh = 0.0
    total_sqft = 0.0
    hub_caps: dict[str, dict[str, float]] = {}
    for _, row in mod_agg.iterrows():
        mod = row["_mod"]
        if mod == "Other":
            continue
        forecasted = float(row["forecasted"])
        spend = forecasted
        gwh = float(line_caps.get(mod, 0))
        sqft = float(line_sqft.get(mod, 0))
        entry = {
            "line": mod,
            "actual_spend": spend,
            "forecasted": forecasted,
            "gwh": gwh,
            "sqft": sqft,
            "dollar_per_gwh": spend / gwh if gwh > 0 else None,
            "forecast_per_gwh": forecasted / gwh if gwh > 0 else None,
            "sqft_per_gwh": sqft / gwh if gwh > 0 else None,
            "station_count": int(row["stations"]),
        }
        lines.append(entry)
        total_spend += spend
        total_line_gwh += gwh
        total_sqft += sqft

        # Hub capacity is computed per BASE as max(sum(MOD/CELL), sum(INV)).
        m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", mod)
        if m:
            base, unit = m.group(1), m.group(2)
            if base not in hub_caps:
                hub_caps[base] = {"mod_gwh": 0.0, "inv_gwh": 0.0}
            if unit.startswith("INV"):
                hub_caps[base]["inv_gwh"] += gwh
            else:
                hub_caps[base]["mod_gwh"] += gwh

    total_hub_gwh = 0.0
    hubs = []
    for base in sorted(hub_caps.keys()):
        mod_gwh = hub_caps[base]["mod_gwh"]
        inv_gwh = hub_caps[base]["inv_gwh"]
        hub_gwh = max(mod_gwh, inv_gwh)
        total_hub_gwh += hub_gwh
        hubs.append({
            "hub": base,
            "mod_gwh": mod_gwh,
            "inv_gwh": inv_gwh,
            "hub_gwh": hub_gwh,
        })

    totals = {
        "total_spend": total_spend,
        "total_gwh": total_hub_gwh,
        "total_line_gwh": total_line_gwh,
        "total_sqft": total_sqft,
        "avg_dollar_per_gwh": total_spend / total_hub_gwh if total_hub_gwh > 0 else None,
        "avg_sqft_per_gwh": total_sqft / total_hub_gwh if total_hub_gwh > 0 else None,
        "spend_basis": "forecasted",
        "capacity_method": "sum(max(sum_mod_gwh, sum_inv_gwh)) by BASE hub",
    }

    return jsonify({"lines": lines, "totals": totals, "hubs": hubs})


ASSET_MILESTONES = ["ordered", "shipped", "received", "installed", "commissioned"]


def _derive_status(dates: dict) -> str:
    """Derive station status from the latest milestone with a date."""
    for ms in reversed(ASSET_MILESTONES):
        if dates.get(ms):
            return ms.capitalize()
    return "Ordered"


@app.route("/api/asset-status", methods=["GET"])
def api_asset_status_get():
    data = store.read_json("asset_status.json")
    if not isinstance(data, dict):
        data = {}
    return jsonify(data)


@app.route("/api/asset-status", methods=["POST"])
def api_asset_status_save():
    body = request.get_json(force=True)
    station_id = body.get("station_id", "")
    milestone = body.get("milestone", "")
    date_val = body.get("date", "")
    if not station_id or milestone not in ASSET_MILESTONES:
        return jsonify({"ok": False, "error": "station_id and valid milestone required"}), 400

    data = store.read_json("asset_status.json")
    if not isinstance(data, dict):
        data = {}
    if station_id not in data:
        data[station_id] = {}
    data[station_id][milestone] = date_val if date_val else None
    data[station_id]["status"] = _derive_status(data[station_id])
    store.write_json("asset_status.json", data)
    return jsonify({"ok": True, "status": data[station_id]["status"]})


ASSET_SUBCATEGORIES: set[str] = {
    "Process Equipment",
    "Controls & Electrical",
    "Mechanical & Structural",
    "Quality & Metrology",
}


@app.route("/api/assets")
def api_assets():
    """Station-level asset register with spend split by mfg_subcategory."""
    df = _load_csv("capex_clean.csv")
    stations_json = _load_stations_json()
    if df.empty:
        return jsonify({"stations": [], "kpis": {}})

    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_total"] = pd.to_numeric(df["price_total"], errors="coerce").fillna(0)
    df["_qty"] = pd.to_numeric(df["product_qty"], errors="coerce").fillna(0)
    df["_qty_recv"] = pd.to_numeric(df["qty_received"], errors="coerce").fillna(0)

    mapped = df[df["station_id"].str.startswith("BASE", na=False)].copy()
    if mapped.empty:
        return jsonify({"stations": [], "kpis": {}})

    has_subcat = "mfg_subcategory" in mapped.columns
    if has_subcat:
        mapped["_is_asset"] = mapped["mfg_subcategory"].isin(ASSET_SUBCATEGORIES)
    else:
        mapped["_is_asset"] = True

    station_meta = {s["station_id"]: s for s in stations_json}

    groups = mapped.groupby("station_id")
    rows: list[dict] = []
    for sid, grp in groups:
        meta = station_meta.get(sid, {})
        asset_grp = grp[grp["_is_asset"]] if has_subcat else grp
        svc_grp = grp[grp["mfg_subcategory"].isin({
            "Design & Engineering Services", "Integration & Commissioning",
        })] if has_subcat else pd.DataFrame()
        ship_grp = grp[grp["mfg_subcategory"] == "Shipping & Freight"] if has_subcat else pd.DataFrame()
        consum_grp = grp[grp["mfg_subcategory"] == "Consumables"] if has_subcat else pd.DataFrame()

        total_ordered_value = float((grp["_qty"] * pd.to_numeric(grp["price_unit"], errors="coerce").fillna(0)).sum())
        total_received_value = float((grp["_qty_recv"] * pd.to_numeric(grp["price_unit"], errors="coerce").fillna(0)).sum())
        pct_recv = (total_received_value / total_ordered_value * 100) if total_ordered_value > 0 else 0.0
        if pct_recv > 100:
            pct_recv = 100.0

        if pct_recv >= 99:
            delivery = "Complete"
        elif pct_recv > 0:
            delivery = "In Progress"
        else:
            delivery = "Not Started"

        forecasted = float(meta.get("forecasted_cost", 0) or 0)
        actual = float(grp["_sub"].sum())
        asset_val = float(asset_grp["_sub"].sum()) if not asset_grp.empty else 0.0
        variance = actual - forecasted

        conf_mode = ""
        if "mapping_confidence" in grp.columns:
            conf_counts = grp["mapping_confidence"].value_counts()
            conf_mode = str(conf_counts.index[0]) if not conf_counts.empty else ""

        # Sub-category breakdown for this station
        sc_breakdown: list[dict] = []
        if has_subcat:
            for sc, sc_grp in grp.groupby("mfg_subcategory"):
                if sc:
                    sc_breakdown.append({"subcategory": str(sc), "spend": float(sc_grp["_sub"].sum())})
            sc_breakdown.sort(key=lambda x: x["spend"], reverse=True)

        rows.append({
            "station_id": sid,
            "station_name": meta.get("process_name", ""),
            "line": _extract_line(sid),
            "owner": meta.get("owner", ""),
            "primary_vendor": meta.get("vendor", ""),
            "forecasted": forecasted,
            "total_investment": actual,
            "asset_value": asset_val,
            "services_cost": float(svc_grp["_sub"].sum()) if not svc_grp.empty else 0.0,
            "consumables_cost": float(consum_grp["_sub"].sum()) if not consum_grp.empty else 0.0,
            "shipping_cost": float(ship_grp["_sub"].sum()) if not ship_grp.empty else 0.0,
            "variance": variance,
            "variance_pct": round(variance / forecasted * 100, 1) if forecasted else 0.0,
            "po_count": int(grp["po_number"].nunique()),
            "line_count": len(grp),
            "vendor_count": int(grp["vendor_name"].nunique()),
            "pct_received": round(pct_recv, 1),
            "delivery_status": delivery,
            "odoo_spend": float(grp.loc[grp["source"] == "odoo", "_sub"].sum()),
            "ramp_spend": float(grp.loc[grp["source"] == "ramp", "_sub"].sum()),
            "mapping_confidence": conf_mode,
            "sc_breakdown": sc_breakdown,
        })

    rows.sort(key=lambda r: r["total_investment"], reverse=True)

    status_data = store.read_json("asset_status.json")
    if not isinstance(status_data, dict):
        status_data = {}
    for row in rows:
        sid = row["station_id"]
        sd = status_data.get(sid, {})
        row["status"] = sd.get("status", "Ordered")
        for ms in ASSET_MILESTONES:
            row[f"date_{ms}"] = sd.get(ms) or ""

    total_asset_value = sum(r["asset_value"] for r in rows)
    total_investment = sum(r["total_investment"] for r in rows)
    status_counts: dict[str, int] = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    kpis = {
        "station_count": len(rows),
        "total_asset_value": total_asset_value,
        "total_investment": total_investment,
        "services_total": sum(r["services_cost"] for r in rows),
        "status_counts": status_counts,
    }

    return jsonify({"stations": rows, "kpis": kpis})


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    print("Mfg Budgeting App: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
