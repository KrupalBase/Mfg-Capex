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
from auth import init_auth

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


def _apply_line_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows by the ?lines= query param (comma-separated line list).

    Uses _extract_line so filtering by BASE1-MOD1 also includes BASE1-CELL1.
    """
    raw = request.args.get("lines", "")
    if not raw:
        return df
    allowed = {s.strip() for s in raw.split(",") if s.strip()}
    if not allowed:
        return df
    df = df.copy()
    df["_line"] = df["station_id"].apply(_extract_line)
    return df[df["_line"].isin(allowed)].drop(columns=["_line"])


def _all_lines(df: pd.DataFrame) -> list[str]:
    """Unique production lines (with CELLs rolled into MODs)."""
    lines = set()
    for sid in df["station_id"]:
        ln = _extract_line(str(sid))
        if ln:
            lines.add(ln)
    return sorted(lines)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/modules")
def api_modules():
    """Return the list of production lines for the global filter (CELLs nested under MODs)."""
    df = _load_csv("capex_clean.csv")
    return jsonify(_all_lines(df))


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
    unique_vendors = int(df["vendor_name"].nunique())
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

    vendor_spend = df.groupby("vendor_name")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(15)
    vendor_data = [{"vendor": r["vendor_name"], "spend": float(r["_sub"])} for _, r in vendor_spend.iterrows()]

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

    emp_agg = df.groupby("created_by_name").agg(
        spend=("_sub", "sum"), count=("_sub", "size"), pos=("po_number", "nunique"),
    ).reset_index().sort_values("spend", ascending=False).head(15)
    emp_data = [{"name": r["created_by_name"], "spend": float(r["spend"]), "count": int(r["count"]), "pos": int(r["pos"])} for _, r in emp_agg.iterrows()]

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
        "top_vendors": vendor_data,
        "top_employees": emp_data,
        "mapping_quality": conf_counts,
        "mapping_detail": mapping_detail,
        "budget_vs_actual": line_data,
        "source_compare": source_compare,
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

    undated_spend = float(df.loc[df["_date"].isna(), "_sub"].sum())

    return jsonify({
        "weekly": weekly_data,
        "monthly_cat": mc_data,
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
</style>
</head>
<body>

<div class="sidebar">
    <div class="sidebar-brand"><h2>MFG BUDGETING</h2><div class="sub">Base Power Company</div></div>
    <a class="nav-item active" onclick="showPage('executive',this)"><span class="icon">&#9632;</span> Executive Summary</a>
    <a class="nav-item" onclick="showPage('source',this)"><span class="icon">&#8644;</span> Odoo vs Ramp</a>
    <a class="nav-item" onclick="showPage('stations',this)"><span class="icon">&#9881;</span> Station Drill-Down</a>
    <a class="nav-item" onclick="showPage('vendors',this)"><span class="icon">&#9733;</span> Vendor Analysis</a>
    <a class="nav-item" onclick="showPage('spares',this)"><span class="icon">&#9776;</span> Materials / Spares</a>
    <a class="nav-item" onclick="showPage('detail',this)"><span class="icon">&#9783;</span> Full Transactions</a>
    <a class="nav-item" onclick="showPage('timeline',this)"><span class="icon">&#9202;</span> Spend Timeline</a>
    <a class="nav-item" onclick="showPage('projects',this)"><span class="icon">&#9670;</span> Other Projects</a>
    <a class="nav-item" onclick="showPage('uniteco',this)"><span class="icon">&#9879;</span> Unit Economics</a>
    <a class="nav-item" onclick="showPage('settings',this)"><span class="icon">&#9881;</span> Settings</a>
    <div style="padding:12px 18px;border-top:1px solid var(--border)">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600;margin-bottom:8px">Filter by Line</div>
        <div id="line-filter-checks" style="max-height:180px;overflow-y:auto"></div>
        <div style="margin-top:6px;display:flex;gap:6px">
            <button onclick="toggleAllLines(true)" style="flex:1;padding:4px;background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">All</button>
            <button onclick="toggleAllLines(false)" style="flex:1;padding:4px;background:var(--secondary);color:var(--text);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">None</button>
        </div>
    </div>
    <div class="sidebar-footer"><button class="btn-refresh" onclick="refreshPipeline()">Refresh Data</button></div>
</div>

<div class="main">

<!-- EXECUTIVE -->
<div class="page active" id="page-executive">
    <div class="page-header"><div><div class="page-title">Executive Summary</div><div class="page-subtitle">BF1 Manufacturing CAPEX Overview</div></div></div>
    <div class="kpi-row" id="kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Budget vs Actual by Module</h3><div id="chart-budget"></div></div>
        <div class="chart-card"><h3>Monthly Spend Trend (Odoo + Ramp)</h3><div id="chart-monthly"></div></div>
        <div class="chart-card"><h3>Mapping Quality (click to drill down)</h3><div id="chart-mapping"></div><div class="drill-panel" id="mapping-drill"></div></div>
        <div class="chart-card"><h3>Spend by Category</h3><div id="chart-category"></div></div>
        <div class="chart-card"><h3>Top 15 Vendors</h3><div id="chart-vendors"></div></div>
        <div class="chart-card full"><h3>Spend by Employee</h3><div id="chart-employees"></div></div>
    </div>
</div>

<!-- ODOO vs RAMP -->
<div class="page" id="page-source">
    <div class="page-header"><div><div class="page-title">Odoo vs Ramp Comparison</div><div class="page-subtitle">Purchase Orders vs Credit Card spend breakdown</div></div></div>
    <div class="kpi-row" id="source-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Odoo PO - Top Categories</h3><div id="chart-odoo-cats"></div></div>
        <div class="chart-card"><h3>Ramp CC - Top Categories</h3><div id="chart-ramp-cats"></div></div>
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

<!-- VENDORS -->
<div class="page" id="page-vendors">
    <div class="page-header"><div><div class="page-title">Vendor Analysis</div><div class="page-subtitle">Spend concentration and vendor-station relationships</div></div></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Top Vendor Concentration</h3><div id="chart-vendor-conc"></div></div>
        <div class="chart-card"><h3>Vendor-Station Spend (Top 10 Vendors x Top Stations)</h3><div id="chart-vendor-heatmap"></div></div>
        <div class="chart-card full"><h3>All Vendors</h3><div id="vendor-table-wrap"></div></div>
    </div>
</div>

<!-- SPARES -->
<div class="page" id="page-spares">
    <div class="page-header"><div><div class="page-title">Materials / Spares Catalog</div><div class="page-subtitle">Deduplicated items with part numbers and sourcing info</div></div></div>
    <div class="filter-bar" id="spares-filters">
        <label>Station:</label><select id="sparesStationFilter" onchange="filterSpares()"><option value="">All Stations</option></select>
        <label>Category:</label><select id="sparesCatFilter" onchange="filterSpares()"><option value="">All Categories</option></select>
        <label>Vendor:</label><select id="sparesVendorFilter" onchange="filterSpares()"><option value="">All Vendors</option></select>
    </div>
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
        <div class="chart-card full"><h3>Monthly Spend by Category</h3><div id="chart-monthly-cat"></div></div>
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
        <div class="chart-card"><h3>$/GWh by Line (Actual)</h3><div id="chart-ue-dollar"></div></div>
        <div class="chart-card"><h3>$/GWh Actual vs Forecasted</h3><div id="chart-ue-compare"></div></div>
        <div class="chart-card"><h3>ft&sup2;/GWh by Line</h3><div id="chart-ue-sqft"></div></div>
        <div class="chart-card"><h3>Spend Composition per GWh</h3><div id="chart-ue-stack"></div></div>
        <div class="chart-card full"><h3>Line Detail</h3><div id="ue-table-wrap"></div></div>
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
let allModules=[], activeModules=new Set();

async function initLineFilter(){
    const res=await fetch('/api/modules');
    allModules=await res.json();
    activeModules=new Set(allModules);
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
    if(activeModules.size===0||activeModules.size===allModules.length)return'';
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
    else if(id==='vendors')loadVendors();
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

/* ====== DRILL-DOWN ====== */
let drillDT=null;
async function openDrill(title,params){
    const qs=Object.entries(params).filter(([k,v])=>v).map(([k,v])=>k+'='+encodeURIComponent(v)).join('&');
    const res=await fetch('/api/drilldown?'+qs);
    const d=await res.json();
    document.getElementById('drill-title').textContent=title;
    document.getElementById('drill-sub').textContent=d.count+' items | '+fmtF$(d.total)+' total';
    const cols=['source','po_number','date_order','vendor_name','product_category','item_description','station_id','project_name','mapping_confidence','price_subtotal','created_by_name'];
    const labels=['Src','PO','Date','Vendor','Category','Description','Station','Project','Conf','Subtotal','By'];
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
    drillDT=$('#drill-tbl').DataTable(dtOpts({pageLength:25,order:[[9,'desc']]}));
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
    if(id==='source')loadSource();
    if(id==='stations'&&!document.getElementById('stationSelect').value)loadStations();
    if(id==='vendors')loadVendors();
    if(id==='spares')loadSpares();
    if(id==='detail')loadDetail();
    if(id==='timeline')loadTimeline();
    if(id==='projects')loadProjects();
    if(id==='uniteco')loadUnitEconomics();
    if(id==='settings')loadSettings();
}

/* ====== EXECUTIVE ====== */
async function loadExecutive(){
    const res=await fetch(apiUrl('/api/summary'));const d=await res.json();
    if(!d.total_committed)return;
    summaryCache=d;

    document.getElementById('kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Committed</div><div class="value dollar">${fmt$(d.total_committed)}</div><div class="sub"><span class="source-badge odoo">Odoo ${fmt$(d.odoo_total)}</span> <span class="source-badge ramp">Ramp ${fmt$(d.ramp_total)}</span></div></div>
        <div class="kpi"><div class="label">Forecasted Budget</div><div class="value dollar">${fmt$(d.forecasted_budget)}</div></div>
        <div class="kpi"><div class="label">Variance</div><div class="value ${vc(d.variance)}">${fmt$(d.variance)}</div><div class="sub">${d.variance>0?'Over':'Under'} budget</div></div>
        <div class="kpi"><div class="label">% Budget Spent</div><div class="value">${fmtPct(d.pct_spent)}</div></div>
        <div class="kpi"><div class="label">Active POs</div><div class="value">${(d.active_pos||0).toLocaleString()}</div></div>
        <div class="kpi"><div class="label">Unique Vendors</div><div class="value">${(d.unique_vendors||0).toLocaleString()}</div></div>`;

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

    // Mapping quality -- clickable pie
    if(d.mapping_quality){
        const labels=Object.keys(d.mapping_quality);
        const values=Object.values(d.mapping_quality);
        const colorMap={high:C.green,medium:C.yellow,low:C.red,none:'#555'};
        Plotly.newPlot('chart-mapping',[{labels,values,type:'pie',hole:.55,marker:{colors:labels.map(l=>colorMap[l]||'#555')},textinfo:'label+percent+value',textfont:{color:C.text,size:11},hovertemplate:'%{label}: %{value} lines<br>%{percent}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:10,r:10,t:10,b:10}},PC);
        document.getElementById('chart-mapping').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const level=ev.points[0].label;
            openDrill('Confidence: '+level,{confidence:level});
        });
    }

    // Category
    if(d.category_spend&&d.category_spend.length){
        const cats=d.category_spend.slice(0,12);
        Plotly.newPlot('chart-category',[{
            type:'treemap',labels:cats.map(c=>c.category.replace('Non-Inventory: ','')),
            parents:cats.map(()=>''),values:cats.map(c=>c.spend),
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            marker:{colors:cats.map((_,i)=>`hsl(${90+i*20},55%,${50+i*2}%)`)}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8}},PC);
        document.getElementById('chart-category').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].label;const cat=d.category_spend.find(c=>c.category.replace('Non-Inventory: ','')==lbl);openDrill('Category: '+lbl,{category:cat?cat.category:lbl});}});
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
    document.getElementById('source-kpis').innerHTML=`
        <div class="kpi"><div class="label">Odoo PO Total</div><div class="value dollar">${fmt$(d.odoo_total)}</div><div class="sub">${(sc.odoo?.count||0).toLocaleString()} line items &middot; avg ${fmtF$(sc.odoo?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC Total</div><div class="value dollar" style="color:var(--blue)">${fmt$(d.ramp_total)}</div><div class="sub">${(sc.ramp?.count||0).toLocaleString()} transactions &middot; avg ${fmtF$(sc.ramp?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Odoo Share</div><div class="value">${fmtPct(d.odoo_total/(d.total_committed||1)*100)}</div></div>
        <div class="kpi"><div class="label">Ramp Share</div><div class="value">${fmtPct(d.ramp_total/(d.total_committed||1)*100)}</div></div>`;

    const oc=sc.odoo_categories||[];
    if(oc.length){
        const ocR=[...oc].reverse();
        Plotly.newPlot('chart-odoo-cats',[{y:ocR.map(c=>c.cat.replace('Non-Inventory: ','')),x:ocR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.green},text:ocR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,oc.length*35),margin:{l:180,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-odoo-cats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;const cat=oc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl);openDrill('Odoo: '+lbl,{source:'odoo',category:cat?cat.cat:lbl});}});
    }
    const rc=sc.ramp_categories||[];
    if(rc.length){
        const rcR=[...rc].reverse();
        Plotly.newPlot('chart-ramp-cats',[{y:rcR.map(c=>c.cat.replace('Non-Inventory: ','')),x:rcR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.blue},text:rcR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,rc.length*35),margin:{l:180,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-ramp-cats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;const cat=rc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl);openDrill('Ramp: '+lbl,{source:'ramp',category:cat?cat.cat:lbl});}});
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

    let bom='<table id="station-bom" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Category</th><th>Vendor</th><th>Qty</th><th>Unit Price</th><th>Subtotal</th><th>PO</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    (d.lines||[]).forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        bom+=`<tr><td>${r.item_description||''}</td><td>${(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_name||''}</td><td>${r.product_qty||''}</td><td>${fmtF$(parseFloat(r.price_unit)||0)}</td><td>${fmtF$(parseFloat(r.price_subtotal)||0)}</td><td>${r.po_number||''}</td><td>${parts}</td></tr>`;
    });
    bom+='</tbody></table>';
    document.getElementById('station-bom-table').innerHTML=bom;
    if(dtI['station-bom'])dtI['station-bom'].destroy();
    dtI['station-bom']=$('#station-bom').DataTable(dtOpts({pageLength:25,order:[[5,'desc']],dom:'Bfrtip',buttons:['csv']}));
}

async function saveForecast(sid){
    const val=parseFloat(document.getElementById('fc-edit').value);
    if(isNaN(val))return;
    const res=await fetch('/api/forecast',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({station_id:sid,forecasted_cost:val})});
    const d=await res.json();
    if(d.ok){document.getElementById('fc-ok').style.display='inline';setTimeout(()=>{document.getElementById('fc-ok').style.display='none';},2000);showToast('Forecast saved');}
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
    dtI['vendor-tbl']=$('#vendor-tbl').DataTable(dtOpts({pageLength:25,order:[[1,'desc']],dom:'Bfrtip',buttons:['csv']}));
}

/* ====== SPARES ====== */
async function loadSpares(){
    const res=await fetch('/api/spares');sparesData=await res.json();
    if(!sparesData.length){document.getElementById('spares-table-wrap').innerHTML='<p style="color:var(--muted)">No spares data.</p>';return;}

    // Populate filter dropdowns
    const stations=new Set(),cats=new Set(),vendors=new Set();
    sparesData.forEach(r=>{
        if(r.station_ids)(r.station_ids+'').split(',').forEach(s=>{s=s.trim();if(s)stations.add(s);});
        if(r.product_category)cats.add(r.product_category);
        if(r.vendor_names)(r.vendor_names+'').split(',').forEach(v=>{v=v.trim();if(v)vendors.add(v);});
    });
    const sSel=document.getElementById('sparesStationFilter');
    const cSel=document.getElementById('sparesCatFilter');
    const vSel=document.getElementById('sparesVendorFilter');
    [...stations].sort().forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sSel.appendChild(o);});
    [...cats].sort().forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c.replace('Non-Inventory: ','');cSel.appendChild(o);});
    [...vendors].sort().forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;vSel.appendChild(o);});
    renderSpares(sparesData);
}
function filterSpares(){
    const sf=document.getElementById('sparesStationFilter').value;
    const cf=document.getElementById('sparesCatFilter').value;
    const vf=document.getElementById('sparesVendorFilter').value;
    let data=sparesData;
    if(sf)data=data.filter(r=>(r.station_ids+'').includes(sf));
    if(cf)data=data.filter(r=>r.product_category===cf);
    if(vf)data=data.filter(r=>(r.vendor_names+'').includes(vf));
    renderSpares(data);
}
function renderSpares(data){
    let html='<table id="spares-tbl" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Category</th><th>Vendors</th><th>Stations</th><th>Qty</th><th>Avg Price</th><th>Total Spend</th><th>Last Order</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    data.forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        html+=`<tr><td>${r.item_description||''}</td><td>${(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_names||''}</td><td>${r.station_ids||''}</td><td>${r.total_qty_ordered||''}</td><td class="dollar">${fmtF$(r.avg_unit_price)}</td><td class="dollar">${fmtF$(r.total_spend)}</td><td>${r.last_order_date||''}</td><td>${parts}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('spares-table-wrap').innerHTML=html;
    if(dtI['spares-tbl'])dtI['spares-tbl'].destroy();
    dtI['spares-tbl']=$('#spares-tbl').DataTable(dtOpts({pageLength:25,order:[[6,'desc']],dom:'Bfrtip',buttons:['csv']}));
}

/* ====== DETAIL ====== */
async function loadDetail(){
    const res=await fetch(apiUrl('/api/transactions'));const data=await res.json();
    if(!data.length){document.getElementById('detail-table-wrap').innerHTML='<p style="color:var(--muted)">No data.</p>';return;}
    const cols=['source','po_number','date_order','vendor_name','product_category','item_description','station_id','mapping_confidence','price_subtotal','price_total','project_name','created_by_name'];
    const labels=['Source','PO','Date','Vendor','Category','Description','Station','Confidence','Subtotal','Total','Project','Created By'];
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
    dtI['detail-tbl']=$('#detail-tbl').DataTable(dtOpts({pageLength:50,order:[[8,'desc']],dom:'Bfrtip',buttons:['csv'],scrollX:true}));
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
        dtI['proj-tbl']=$('#proj-tbl').DataTable(dtOpts({pageLength:25,order:[[7,'desc']],dom:'Bfrtip',buttons:['csv'],scrollX:true}));
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
        <div class="kpi"><div class="label">Total Production Spend</div><div class="value dollar">${fmt$(t.total_spend)}</div></div>
        <div class="kpi"><div class="label">Hub Capacity (max MOD vs INV)</div><div class="value">${(t.total_gwh||0).toFixed(1)} GWh</div><div style="font-size:11px;color:var(--muted);margin-top:6px">Line sum: ${(t.total_line_gwh||0).toFixed(1)} GWh</div></div>
        <div class="kpi"><div class="label">Avg $/GWh</div><div class="value dollar">${t.avg_dollar_per_gwh?fmt$(t.avg_dollar_per_gwh):'<span style=\"color:var(--muted);font-size:14px\">Set capacities in Settings</span>'}</div></div>
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
        x:configured.map(l=>l.line),y:configured.map(l=>l.dollar_per_gwh||0),
        type:'bar',marker:{color:C.green},
        text:configured.map(l=>l.dollar_per_gwh?fmt$(l.dollar_per_gwh):''),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{x}<br>%{y:$,.0f}/GWh<extra></extra>'
    }],UL({l:70,r:60,t:20,b:90}),PC);

    // Actual vs Forecasted $/GWh
    const cmpLay=UL({l:70,r:20,t:40,b:90},{barmode:'group',legend:{font:{color:C.muted},x:0,y:1.15,orientation:'h'}});
    Plotly.newPlot('chart-ue-compare',[
        {x:configured.map(l=>l.line),y:configured.map(l=>l.forecast_per_gwh||0),type:'bar',name:'Forecasted',marker:{color:C.surface2}},
        {x:configured.map(l=>l.line),y:configured.map(l=>l.dollar_per_gwh||0),type:'bar',name:'Actual',marker:{color:C.green}},
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

    // Stacked: actual vs remaining forecast per GWh
    const stkLay=UL({l:70,r:20,t:40,b:90},{barmode:'stack',legend:{font:{color:C.muted},x:0,y:1.15,orientation:'h'}});
    Plotly.newPlot('chart-ue-stack',[
        {x:configured.map(l=>l.line),y:configured.map(l=>(l.dollar_per_gwh||0)),type:'bar',name:'Actual $/GWh',marker:{color:C.green}},
        {x:configured.map(l=>l.line),y:configured.map(l=>{const f=l.forecast_per_gwh||0;const a=l.dollar_per_gwh||0;return Math.max(0,f-a);}),type:'bar',name:'Remaining Forecast $/GWh',marker:{color:C.surface2}},
    ],stkLay,PC);

    // Detail table
    let html='<table id="ue-tbl" class="display compact" style="width:100%"><thead><tr><th>Line</th><th>GWh</th><th>Actual Spend</th><th>Forecasted</th><th>$/GWh (Actual)</th><th>$/GWh (Forecast)</th><th>ft&sup2;</th><th>ft&sup2;/GWh</th><th>Stations</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    configured.forEach(l=>{
        html+=`<tr><td style="font-weight:600;color:var(--green)">${l.line}</td><td>${l.gwh.toFixed(1)}</td><td class="dollar">${fmtF$(l.actual_spend)}</td><td class="dollar">${fmtF$(l.forecasted)}</td><td class="dollar">${l.dollar_per_gwh?fmtF$(l.dollar_per_gwh):'--'}</td><td class="dollar">${l.forecast_per_gwh?fmtF$(l.forecast_per_gwh):'--'}</td><td>${l.sqft?(l.sqft).toLocaleString():''}</td><td>${l.sqft_per_gwh?(l.sqft_per_gwh).toLocaleString(undefined,{maximumFractionDigits:0}):''}</td><td>${l.station_count}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('ue-table-wrap').innerHTML=html;
    if(dtI['ue-tbl'])dtI['ue-tbl'].destroy();
    dtI['ue-tbl']=$('#ue-tbl').DataTable(dtOpts({pageLength:25,order:[[4,'desc']],dom:'Bfrtip',buttons:['csv']}));
}

/* ====== REFRESH ====== */
async function refreshPipeline(){
    const btn=document.querySelector('.btn-refresh');
    btn.disabled=true;btn.textContent='Refreshing...';showToast('Running pipeline...');
    try{
        const res=await fetch('/api/refresh',{method:'POST'});const d=await res.json();
        if(d.ok){showToast('Refreshed! Reloading...');setTimeout(()=>location.reload(),1000);}
        else{showToast('Pipeline failed');console.error(d.stderr);}
    }catch(e){showToast('Error: '+e.message);}
    btn.disabled=false;btn.textContent='Refresh Data';
}
function showToast(msg){const t=document.getElementById('toast');t.textContent=msg;t.style.display='block';setTimeout(()=>{t.style.display='none';},3000);}
function initFromHash(){
    const hash=window.location.hash.slice(1);
    if(hash&&['executive','source','stations','vendors','spares','detail','timeline','projects','uniteco','settings'].includes(hash)){
        const navItem=document.querySelector(`.nav-item[onclick*="showPage('${hash}')"]`);
        showPage(hash,navItem);
    }else{
        loadExecutive();
    }
}
window.addEventListener('hashchange',initFromHash);
initLineFilter();
initFromHash();
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
    if week:
        df["_week"] = df["_date"].dt.isocalendar().apply(
            lambda r: f"{int(r['year'])}-W{int(r['week']):02d}" if pd.notna(r["year"]) else "", axis=1
        )
        df = df[df["_week"] == week]

    total = float(df["_sub"].sum())
    count = len(df)

    cols = ["source", "po_number", "date_order", "vendor_name", "product_category",
            "item_description", "station_id", "project_name", "mapping_confidence",
            "price_subtotal", "price_total", "created_by_name"]
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

    overrides = store.read_json("forecast_overrides.json")
    if not isinstance(overrides, dict):
        overrides = {}
    overrides[station_id] = float(new_forecast)
    store.write_json("forecast_overrides.json", overrides)

    by_station = _load_csv("capex_by_station.csv")
    if not by_station.empty and station_id in by_station["station_id"].values:
        idx = by_station.index[by_station["station_id"] == station_id]
        by_station.loc[idx, "forecasted_cost"] = float(new_forecast)
        actual = pd.to_numeric(by_station.loc[idx, "actual_spend"], errors="coerce").fillna(0)
        by_station.loc[idx, "variance"] = actual - float(new_forecast)
        fc = float(new_forecast) if float(new_forecast) != 0 else float("nan")
        by_station.loc[idx, "variance_pct"] = ((actual - float(new_forecast)) / fc * 100).round(1).fillna(0)
        store.write_csv("capex_by_station.csv", by_station)

    return jsonify({"ok": True})


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    """Return all dashboard settings (line capacities, sq ft, etc.)."""
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}
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
    df = _load_csv("capex_clean.csv")
    if by_station.empty:
        return jsonify({"lines": [], "totals": {}})

    by_station["_mod"] = by_station["station_id"].apply(lambda s: _extract_line(s) or "Other")
    mod_agg = by_station.groupby("_mod").agg(
        forecasted=("forecasted_cost", lambda x: pd.to_numeric(x, errors="coerce").sum()),
        actual=("actual_spend", lambda x: pd.to_numeric(x, errors="coerce").sum()),
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
        actual = float(row["actual"])
        forecasted = float(row["forecasted"])
        gwh = float(line_caps.get(mod, 0))
        sqft = float(line_sqft.get(mod, 0))
        entry = {
            "line": mod,
            "actual_spend": actual,
            "forecasted": forecasted,
            "gwh": gwh,
            "sqft": sqft,
            "dollar_per_gwh": actual / gwh if gwh > 0 else None,
            "forecast_per_gwh": forecasted / gwh if gwh > 0 else None,
            "sqft_per_gwh": sqft / gwh if gwh > 0 else None,
            "station_count": int(row["stations"]),
        }
        lines.append(entry)
        total_spend += actual
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
        "capacity_method": "sum(max(sum_mod_gwh, sum_inv_gwh)) by BASE hub",
    }

    return jsonify({"lines": lines, "totals": totals, "hubs": hubs})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Re-run the pipeline (--skip-bq) to pick up override changes."""
    import subprocess
    import sys
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve().parent / "capex_pipeline.py"), "--skip-bq"],
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parent),
    )
    return jsonify({
        "ok": result.returncode == 0,
        "message": "Pipeline refreshed" if result.returncode == 0 else "Pipeline failed",
        "stdout": result.stdout[-2000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    })


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    print("Mfg Budgeting App: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
