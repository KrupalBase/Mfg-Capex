#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Deploy CAPEX Dashboard + Review UI to Google Cloud Run
#
# Prerequisites (run once, manually):
#   1. Create the GCP project "mfg-eng" under basepowercompany.com org
#   2. Link a billing account to the project
#   3. Run:
#      gcloud services enable \
#        run.googleapis.com \
#        storage.googleapis.com \
#        iap.googleapis.com \
#        artifactregistry.googleapis.com \
#        cloudbuild.googleapis.com \
#        --project=mfg-eng-19197
#
#      gcloud storage buckets create gs://capex-pipeline-data \
#        --project=mfg-eng-19197 --location=us-central1
#
# Usage:
#   ./deploy.sh              # build + deploy both services
#   ./deploy.sh --seed       # also upload local data/ to GCS bucket
# ---------------------------------------------------------------------------
set -euo pipefail

PROJECT="mfg-eng-19197"
REGION="us-central1"
BUCKET="capex-pipeline-data"
IMAGE_TAG="us-central1-docker.pkg.dev/${PROJECT}/capex/capex-app:latest"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---- Parse flags ----------------------------------------------------------
SEED=false
for arg in "$@"; do
  case "$arg" in
    --seed) SEED=true ;;
  esac
done

# ---- 1. Create Artifact Registry repo (idempotent) -----------------------
echo "==> Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe capex \
  --project="$PROJECT" --location="$REGION" --format="value(name)" 2>/dev/null \
|| gcloud artifacts repositories create capex \
  --project="$PROJECT" --location="$REGION" \
  --repository-format=docker \
  --description="CAPEX dashboard images"

# ---- 2. Build container image via Cloud Build -----------------------------
echo "==> Building container image..."
gcloud builds submit "$SCRIPT_DIR" \
  --project="$PROJECT" \
  --tag="$IMAGE_TAG" \
  --timeout=600

# ---- 3. Deploy dashboard service -----------------------------------------
echo "==> Deploying capex-dashboard..."
gcloud run deploy capex-dashboard \
  --project="$PROJECT" \
  --region="$REGION" \
  --image="$IMAGE_TAG" \
  --platform=managed \
  --set-env-vars="APP_MODE=dashboard,GCS_BUCKET=$BUCKET" \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --timeout=120 \
  --no-allow-unauthenticated

# ---- 4. Deploy review service ---------------------------------------------
echo "==> Deploying capex-review..."
gcloud run deploy capex-review \
  --project="$PROJECT" \
  --region="$REGION" \
  --image="$IMAGE_TAG" \
  --platform=managed \
  --set-env-vars="APP_MODE=review,GCS_BUCKET=$BUCKET" \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --timeout=120 \
  --no-allow-unauthenticated

# ---- 5. Seed GCS bucket with local data (optional) -----------------------
if [ "$SEED" = true ]; then
  echo "==> Uploading local data/ to gs://$BUCKET/ ..."
  gcloud storage cp "$SCRIPT_DIR/data/"*.csv "gs://$BUCKET/" --project="$PROJECT"
  gcloud storage cp "$SCRIPT_DIR/data/"*.json "gs://$BUCKET/" --project="$PROJECT"
  echo "  Done. Files in bucket:"
  gcloud storage ls "gs://$BUCKET/" --project="$PROJECT"
fi

# ---- 6. Grant Cloud Run service account access to GCS + BigQuery ----------
SA=$(gcloud run services describe capex-dashboard \
  --project="$PROJECT" --region="$REGION" \
  --format="value(spec.template.spec.serviceAccountName)" 2>/dev/null || true)

if [ -z "$SA" ]; then
  SA="${PROJECT_NUMBER:-$(gcloud projects describe $PROJECT --format='value(projectNumber)')}-compute@developer.gserviceaccount.com"
fi

echo "==> Granting service account ($SA) access..."
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" \
  --role="roles/storage.objectAdmin" \
  --project="$PROJECT" 2>/dev/null || true

echo ""
echo "================================================================"
echo "  Deployment complete!"
echo ""
echo "  Dashboard: $(gcloud run services describe capex-dashboard --project=$PROJECT --region=$REGION --format='value(status.url)')"
echo "  Review UI: $(gcloud run services describe capex-review --project=$PROJECT --region=$REGION --format='value(status.url)')"
echo ""
echo "  Both services require authentication (--no-allow-unauthenticated)."
echo "  Next step: enable IAP to restrict to @basepowercompany.com"
echo "  (see deploy_iap.sh or run manually)"
echo "================================================================"
