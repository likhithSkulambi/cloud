#!/usr/bin/env bash
# =============================================================================
# setup_gcp.sh
# One-time GCP project setup for the Smart Irrigation Advisor.
#
# What this script does:
#   1. Enable required GCP APIs
#   2. Create a service account and grant required roles
#   3. Create the BigQuery dataset and tables
#   4. Create the Pub/Sub topic and subscription
#   5. (Optional) Download a service account key for local development
#
# Usage:
#   chmod +x deployment/setup_gcp.sh
#   ./deployment/setup_gcp.sh
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - PROJECT_ID environment variable set, OR edit the variable below
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration – edit these values or export them before running
# ---------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
REGION="${GCP_REGION:-us-central1}"
DATASET="${BIGQUERY_DATASET:-smart_irrigation}"
BQ_LOCATION="${BIGQUERY_LOCATION:-US}"
TOPIC="${PUBSUB_TOPIC_ID:-irrigation-alerts}"
SUBSCRIPTION="irrigation-alerts-sub"
SA_NAME="smart-irrigation-sa"
SA_DISPLAY="Smart Irrigation Advisor SA"
KEY_FILE="./smart-irrigation-sa-key.json"

# ---------------------------------------------------------------------------
print_step() { echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "► $1"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
# ---------------------------------------------------------------------------

print_step "Setting active project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# ---------------------------------------------------------------------------
# 1. Enable APIs
# ---------------------------------------------------------------------------
print_step "Enabling required GCP APIs"
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  --project="$PROJECT_ID"

echo "✔ APIs enabled"

# ---------------------------------------------------------------------------
# 2. Service Account
# ---------------------------------------------------------------------------
print_step "Creating service account: $SA_NAME"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
  echo "  Service account already exists – skipping creation"
else
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="$SA_DISPLAY" \
    --project="$PROJECT_ID"
  echo "✔ Service account created: $SA_EMAIL"
fi

print_step "Granting IAM roles to $SA_EMAIL"
ROLES=(
  "roles/bigquery.dataEditor"
  "roles/bigquery.jobUser"
  "roles/pubsub.publisher"
  "roles/pubsub.subscriber"
  "roles/cloudfunctions.invoker"
  "roles/run.invoker"
)
for ROLE in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet
  echo "  ✔ Granted $ROLE"
done

# ---------------------------------------------------------------------------
# 3. BigQuery Dataset
# ---------------------------------------------------------------------------
print_step "Creating BigQuery dataset: $DATASET (location: $BQ_LOCATION)"
if bq ls --project_id="$PROJECT_ID" | grep -q "$DATASET"; then
  echo "  Dataset already exists – skipping"
else
  bq --location="$BQ_LOCATION" mk \
    --dataset \
    --description="Smart Irrigation Advisor – weather & recommendations" \
    "${PROJECT_ID}:${DATASET}"
  echo "✔ Dataset created: ${PROJECT_ID}:${DATASET}"
fi

# ---------------------------------------------------------------------------
# 4. Pub/Sub Topic & Subscription
# ---------------------------------------------------------------------------
print_step "Creating Pub/Sub topic: $TOPIC"
if gcloud pubsub topics describe "$TOPIC" --project="$PROJECT_ID" &>/dev/null; then
  echo "  Topic already exists – skipping"
else
  gcloud pubsub topics create "$TOPIC" --project="$PROJECT_ID"
  echo "✔ Topic created: $TOPIC"
fi

print_step "Creating Pub/Sub subscription: $SUBSCRIPTION"
if gcloud pubsub subscriptions describe "$SUBSCRIPTION" --project="$PROJECT_ID" &>/dev/null; then
  echo "  Subscription already exists – skipping"
else
  gcloud pubsub subscriptions create "$SUBSCRIPTION" \
    --topic="$TOPIC" \
    --ack-deadline=60 \
    --message-retention-duration=7d \
    --project="$PROJECT_ID"
  echo "✔ Subscription created: $SUBSCRIPTION"
fi

# ---------------------------------------------------------------------------
# 5. (Optional) Service Account Key for local development
# ---------------------------------------------------------------------------
print_step "Generating service account key for local development"
echo "  Key will be saved to: $KEY_FILE"
echo "  ⚠  Keep this file SECRET – add it to .gitignore"
gcloud iam service-accounts keys create "$KEY_FILE" \
  --iam-account="$SA_EMAIL" \
  --project="$PROJECT_ID"
echo "✔ Key saved to $KEY_FILE"
echo ""
echo "  Export for local use:"
echo "    export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/$KEY_FILE"

# ---------------------------------------------------------------------------
print_step "GCP setup complete! ✅"
echo
echo "Next step: Run ./deployment/deploy.sh to deploy the Cloud Functions."
