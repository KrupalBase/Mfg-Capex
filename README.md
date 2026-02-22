# Mfg Budgeting App

Manufacturing CAPEX spend analytics platform for Base Power Company. Consolidates Purchase Order data from Odoo (via BigQuery), credit card transactions from Ramp, and station planning data into an interactive dashboard and review workflow.

## Architecture

```
bigquery/
├── capex_dashboard.py        # Flask dashboard app (Plotly charts, DataTables)
├── station_review_app.py     # Flask review UI for station mapping corrections
├── capex_pipeline.py         # End-to-end data pipeline (single entry point)
├── po_export_utils.py        # Shared data cleaning, mapping, extraction logic
├── storage_backend.py        # Abstraction layer: local filesystem or GCS
├── auth.py                   # Google OAuth 2.0 (basepowercompany.com only)
├── Dockerfile                # Unified image for both apps (APP_MODE env var)
├── deploy.ps1 / deploy.sh    # Cloud Run deployment scripts
├── requirements.txt          # Python dependencies
├── run_po_creators_7m.py     # BigQuery query runner (all PO creators)
├── run_po_by_number.py       # BigQuery query runner (single PO lookup)
├── po_by_creators_last_7m.sql
├── po_by_number.sql
└── data/                     # Local pipeline output (gitignored)
    ├── capex_clean.csv
    ├── capex_by_station.csv
    ├── spares_catalog.csv
    ├── bf1_stations.json
    ├── station_overrides.json
    └── dashboard_settings.json
```

## Features

- **Executive Summary** -- KPIs, budget vs actual by module, monthly spend trends, spend by employee
- **Odoo vs Ramp** -- Side-by-side comparison of PO and credit card spend
- **Station Drill-Down** -- Per-station BOM, vendor breakdown, order timeline, editable forecasts
- **Vendor Analysis** -- Top vendors, station heatmap, concentration metrics
- **Materials / Spares** -- Searchable catalog with part numbers extracted from descriptions
- **Unit Economics** -- $/GWh and ft²/GWh per production line (hub-capacity method)
- **Other Projects** -- Non-production spend (NPI, Pilot, Facilities, Quality, IT, etc.)
- **Global Line Filter** -- Filter all pages by production line (MOD/INV)
- **Chart Drill-Down** -- Click any chart element to see underlying transactions
- **Station Review UI** -- Agent-assisted mapping with human override workflow
- **Table View** -- Full DataTable with inline editing, column filters, CSV export

## Prerequisites

- Python 3.12+
- Google Cloud SDK (`gcloud`) authenticated with BigQuery access
- GCP project `mfg-eng-19197` (for cloud deployment)

## Local Development

```bash
cd bigquery
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

### Run the pipeline

```bash
# Full run (pulls fresh data from BigQuery)
python capex_pipeline.py

# Skip BigQuery pull (reprocess local data only)
python capex_pipeline.py --skip-bq
```

### Start the apps

```bash
# Dashboard on :5050
python capex_dashboard.py

# Review UI on :5051
python station_review_app.py
```

## Cloud Deployment (Google Cloud Run)

Both apps share a single Docker image. The `APP_MODE` environment variable selects which app runs.

### Environment variables (set on Cloud Run)

| Variable | Purpose |
|---|---|
| `APP_MODE` | `dashboard` or `review` |
| `GCS_BUCKET` | GCS bucket name for data files |
| `GOOGLE_CLIENT_ID` | OAuth 2.0 client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth 2.0 client secret |
| `FLASK_SECRET_KEY` | Session signing key |

### Deploy

```powershell
# PowerShell (builds image, deploys both services, optionally seeds data)
cd bigquery
.\deploy.ps1 -Seed
```

```bash
# Bash
cd bigquery
bash deploy.sh --seed
```

## Data Flow

1. **BigQuery** -- Pull Odoo PO data for specified creators (last 7 months)
2. **Ramp CSV** -- Merge credit card transactions into unified schema
3. **Station Mapping Agent** -- Auto-classify transactions to BF1 production stations
4. **Human Review** -- Override/correct mappings via the review UI
5. **Pipeline Output** -- `capex_clean.csv`, `capex_by_station.csv`, `spares_catalog.csv`
6. **Dashboard** -- Interactive visualization of all processed data

## Authentication

Cloud deployment uses in-app Google OAuth 2.0, restricted to `@basepowercompany.com` accounts. Local development skips auth when `GOOGLE_CLIENT_ID` is not set.
