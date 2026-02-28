# ---------------------------------------------------------------------------
# Deploy CAPEX Dashboard + Review UI to Google Cloud Run
#
# Prerequisites (run once, manually):
#   1. Create the GCP project "mfg-eng" under basepowercompany.com org
#   2. Link a billing account to the project
#   3. Run:
#      gcloud services enable `
#        run.googleapis.com `
#        storage.googleapis.com `
#        iap.googleapis.com `
#        artifactregistry.googleapis.com `
#        cloudbuild.googleapis.com `
#        --project=mfg-eng-19197
#
#      gcloud storage buckets create gs://capex-pipeline-data `
#        --project=mfg-eng-19197 --location=us-central1
#
# Usage:
#   .\deploy.ps1              # build + deploy both services
#   .\deploy.ps1 -Seed        # also upload local data/ to GCS bucket
# ---------------------------------------------------------------------------
param(
    [switch]$Seed
)

$PROJECT  = "mfg-eng-19197"
$REGION   = "us-central1"
$BUCKET   = "capex-pipeline-data"
$IMAGE_TAG = "us-central1-docker.pkg.dev/$PROJECT/capex/capex-app:latest"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

# ---- 1. Create Artifact Registry repo (idempotent) -----------------------
Write-Host "==> Ensuring Artifact Registry repo exists..."
$ErrorActionPreference = "SilentlyContinue"
gcloud artifacts repositories describe capex `
    --project=$PROJECT --location=$REGION --format="value(name)" 2>$null
$ErrorActionPreference = "Stop"
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Repo not found, creating..."
    gcloud artifacts repositories create capex `
        --project=$PROJECT --location=$REGION `
        --repository-format=docker `
        --description="CAPEX dashboard images"
    if ($LASTEXITCODE -ne 0) { throw "Failed to create Artifact Registry repo" }
} else {
    Write-Host "  Repo already exists."
}

# ---- 2. Build container image via Cloud Build -----------------------------
Write-Host "==> Building container image..."
gcloud builds submit $SCRIPT_DIR `
    --project=$PROJECT `
    --tag=$IMAGE_TAG `
    --timeout=600
if ($LASTEXITCODE -ne 0) { throw "Cloud Build failed" }

# ---- OAuth credentials (in-app Google OAuth) ------------------------------
# Public-safe deployment: secrets must come from environment variables.
$CLIENT_ID = $env:GOOGLE_CLIENT_ID
$CLIENT_SECRET = $env:GOOGLE_CLIENT_SECRET
$FLASK_SECRET = $env:FLASK_SECRET_KEY

$missingVars = @()
if (-not $CLIENT_ID) { $missingVars += "GOOGLE_CLIENT_ID" }
if (-not $CLIENT_SECRET) { $missingVars += "GOOGLE_CLIENT_SECRET" }
if (-not $FLASK_SECRET) { $missingVars += "FLASK_SECRET_KEY" }
if ($missingVars.Count -gt 0) {
    throw "Missing required environment variables for deploy: $($missingVars -join ', '). Set them before running deploy.ps1."
}

$ENV_COMMON = "GCS_BUCKET=$BUCKET,GOOGLE_CLIENT_ID=$CLIENT_ID,GOOGLE_CLIENT_SECRET=$CLIENT_SECRET,FLASK_SECRET_KEY=$FLASK_SECRET"

# ---- 3. Deploy dashboard service -----------------------------------------
Write-Host "==> Deploying capex-dashboard..."
gcloud run deploy capex-dashboard `
    --project=$PROJECT `
    --region=$REGION `
    --image=$IMAGE_TAG `
    --platform=managed `
    --set-env-vars="APP_MODE=dashboard,$ENV_COMMON" `
    --memory=512Mi `
    --cpu=1 `
    --min-instances=0 `
    --max-instances=2 `
    --timeout=120 `
    --allow-unauthenticated
if ($LASTEXITCODE -ne 0) { throw "Dashboard deploy failed" }

# ---- 4. Deploy review service ---------------------------------------------
Write-Host "==> Deploying capex-review..."
gcloud run deploy capex-review `
    --project=$PROJECT `
    --region=$REGION `
    --image=$IMAGE_TAG `
    --platform=managed `
    --set-env-vars="APP_MODE=review,$ENV_COMMON" `
    --memory=512Mi `
    --cpu=1 `
    --min-instances=0 `
    --max-instances=2 `
    --timeout=120 `
    --allow-unauthenticated
if ($LASTEXITCODE -ne 0) { throw "Review deploy failed" }

# ---- 5. Seed GCS bucket with local data (optional) -----------------------
if ($Seed) {
    Write-Host "==> Backing up + uploading local data/ to gs://$BUCKET/ ..."
    python "$SCRIPT_DIR\push_clean_to_cloud.py" --gcs-bucket "$BUCKET" --project "$PROJECT" --major-update
    if ($LASTEXITCODE -ne 0) { throw "Seed upload failed" }
    Write-Host "  Done. Files in bucket:"
    gcloud storage ls "gs://$BUCKET/" --project=$PROJECT
}

# ---- 6. Grant Cloud Run SA access to GCS ---------------------------------
$ErrorActionPreference = "SilentlyContinue"
$SA = gcloud run services describe capex-dashboard `
    --project=$PROJECT --region=$REGION `
    --format="value(spec.template.spec.serviceAccountName)" 2>$null
$ErrorActionPreference = "Stop"
if (-not $SA -or $SA -match "ERROR" -or $LASTEXITCODE -ne 0) {
    $projNum = gcloud projects describe $PROJECT --format="value(projectNumber)"
    $SA = "$projNum-compute@developer.gserviceaccount.com"
}

Write-Host "==> Granting service account ($SA) access to bucket..."
$ErrorActionPreference = "SilentlyContinue"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" `
    --member="serviceAccount:$SA" `
    --role="roles/storage.objectAdmin" `
    --project=$PROJECT 2>$null
$ErrorActionPreference = "Stop"

# ---- Done -----------------------------------------------------------------
$dashUrl = gcloud run services describe capex-dashboard --project=$PROJECT --region=$REGION --format="value(status.url)"
$reviewUrl = gcloud run services describe capex-review --project=$PROJECT --region=$REGION --format="value(status.url)"

Write-Host ""
Write-Host "================================================================"
Write-Host "  Deployment complete!"
Write-Host ""
Write-Host "  Dashboard: $dashUrl"
Write-Host "  Review UI: $reviewUrl"
Write-Host ""
Write-Host "  Auth: in-app Google OAuth (basepowercompany.com only)"
Write-Host "================================================================"
