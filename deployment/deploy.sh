#!/usr/bin/env bash
# =============================================================================
# deploy.sh
# Deploy all three Cloud Functions and Cloud Scheduler jobs for the
# Smart Irrigation Advisor.
#
# Usage:
#   chmod +x deployment/deploy.sh
#   ./deployment/deploy.sh
#
# Prerequisites:
#   - setup_gcp.sh already executed
#   - gcloud CLI authenticated
#   - Source directory is the project root (one level above deployment/)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
REGION="${GCP_REGION:-us-central1}"
SA_NAME="smart-irrigation-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Cloud Function settings
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="540s"
MIN_INSTANCES=0
MAX_INSTANCES=5
SOURCE_DIR="./src"

# Scheduler settings (cron expressions in UTC)
SCHEDULER_TIMEZONE="UTC"
WEATHER_CRON="0 2 * * *"        # 02:00 UTC daily – fetch NASA data
EVALUATE_CRON="30 2 * * *"      # 02:30 UTC daily – evaluate & alert

# Environment variables injected into all functions
# (edit these or export them before running the script)
ENV_VARS="\
GCP_PROJECT_ID=${PROJECT_ID},\
BIGQUERY_DATASET=${BIGQUERY_DATASET:-smart_irrigation},\
BIGQUERY_LOCATION=${BIGQUERY_LOCATION:-US},\
PUBSUB_TOPIC_ID=${PUBSUB_TOPIC_ID:-irrigation-alerts},\
SENDGRID_API_KEY=${SENDGRID_API_KEY:-},\
ALERT_FROM_EMAIL=${ALERT_FROM_EMAIL:-alerts@example.com},\
ALERT_MIN_URGENCY=${ALERT_MIN_URGENCY:-HIGH},\
WEATHER_LOOKBACK_DAYS=${WEATHER_LOOKBACK_DAYS:-7},\
CORS_ORIGIN=${CORS_ORIGIN:-*}"

# ---------------------------------------------------------------------------
print_step() { echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "► $1"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
# ---------------------------------------------------------------------------

gcloud config set project "$PROJECT_ID"

# ---------------------------------------------------------------------------
# Helper: deploy a single Gen2 Cloud Function
# ---------------------------------------------------------------------------
deploy_function() {
  local NAME="$1"
  local ENTRY_POINT="$2"
  local DESCRIPTION="$3"

  print_step "Deploying Cloud Function: $NAME"

  gcloud functions deploy "$NAME" \
    --gen2 \
    --runtime="$RUNTIME" \
    --region="$REGION" \
    --source="$SOURCE_DIR" \
    --entry-point="$ENTRY_POINT" \
    --trigger-http \
    --allow-unauthenticated \
    --service-account="$SA_EMAIL" \
    --memory="$MEMORY" \
    --timeout="$TIMEOUT" \
    --min-instances="$MIN_INSTANCES" \
    --max-instances="$MAX_INSTANCES" \
    --set-env-vars="$ENV_VARS" \
    --description="$DESCRIPTION" \
    --project="$PROJECT_ID"

  # Capture the deployed URL
  local URL
  URL=$(gcloud functions describe "$NAME" \
    --region="$REGION" \
    --gen2 \
    --format="value(serviceConfig.uri)" \
    --project="$PROJECT_ID")

  echo "✔ $NAME deployed → $URL"
  eval "URL_${NAME//-/_}=$URL"
}

# ---------------------------------------------------------------------------
# Deploy the three Cloud Functions
# ---------------------------------------------------------------------------
deploy_function \
  "fetch-and-store-weather" \
  "fetch_and_store_weather" \
  "Smart Irrigation – Fetch NASA POWER weather data (daily)"

deploy_function \
  "evaluate-and-recommend" \
  "evaluate_and_recommend" \
  "Smart Irrigation – Run FAO-56 rule engine and send alerts (daily)"

deploy_function \
  "get-recommendations" \
  "get_recommendations" \
  "Smart Irrigation – HTTP API for irrigation recommendations"

# ---------------------------------------------------------------------------
# Retrieve URLs for scheduler targets
# ---------------------------------------------------------------------------
WEATHER_URL=$(gcloud functions describe "fetch-and-store-weather" \
  --region="$REGION" --gen2 --format="value(serviceConfig.uri)" --project="$PROJECT_ID")

EVALUATE_URL=$(gcloud functions describe "evaluate-and-recommend" \
  --region="$REGION" --gen2 --format="value(serviceConfig.uri)" --project="$PROJECT_ID")

# ---------------------------------------------------------------------------
# Create / Update Cloud Scheduler jobs
# ---------------------------------------------------------------------------
print_step "Setting up Cloud Scheduler – Weather Ingestion Job"
gcloud scheduler jobs describe "irrigation-fetch-weather" \
  --location="$REGION" --project="$PROJECT_ID" &>/dev/null \
  && WEATHER_JOB_EXISTS=true || WEATHER_JOB_EXISTS=false

if $WEATHER_JOB_EXISTS; then
  gcloud scheduler jobs update http "irrigation-fetch-weather" \
    --location="$REGION" \
    --schedule="$WEATHER_CRON" \
    --time-zone="$SCHEDULER_TIMEZONE" \
    --uri="$WEATHER_URL" \
    --http-method=POST \
    --message-body='{}' \
    --oidc-service-account-email="$SA_EMAIL" \
    --project="$PROJECT_ID"
  echo "✔ Scheduler job updated: irrigation-fetch-weather ($WEATHER_CRON UTC)"
else
  gcloud scheduler jobs create http "irrigation-fetch-weather" \
    --location="$REGION" \
    --schedule="$WEATHER_CRON" \
    --time-zone="$SCHEDULER_TIMEZONE" \
    --uri="$WEATHER_URL" \
    --http-method=POST \
    --message-body='{}' \
    --oidc-service-account-email="$SA_EMAIL" \
    --description="Trigger daily NASA POWER weather fetch for all fields" \
    --project="$PROJECT_ID"
  echo "✔ Scheduler job created: irrigation-fetch-weather ($WEATHER_CRON UTC)"
fi

print_step "Setting up Cloud Scheduler – Evaluate & Recommend Job"
gcloud scheduler jobs describe "irrigation-evaluate" \
  --location="$REGION" --project="$PROJECT_ID" &>/dev/null \
  && EVAL_JOB_EXISTS=true || EVAL_JOB_EXISTS=false

if $EVAL_JOB_EXISTS; then
  gcloud scheduler jobs update http "irrigation-evaluate" \
    --location="$REGION" \
    --schedule="$EVALUATE_CRON" \
    --time-zone="$SCHEDULER_TIMEZONE" \
    --uri="$EVALUATE_URL" \
    --http-method=POST \
    --message-body='{}' \
    --oidc-service-account-email="$SA_EMAIL" \
    --project="$PROJECT_ID"
  echo "✔ Scheduler job updated: irrigation-evaluate ($EVALUATE_CRON UTC)"
else
  gcloud scheduler jobs create http "irrigation-evaluate" \
    --location="$REGION" \
    --schedule="$EVALUATE_CRON" \
    --time-zone="$SCHEDULER_TIMEZONE" \
    --uri="$EVALUATE_URL" \
    --http-method=POST \
    --message-body='{}' \
    --oidc-service-account-email="$SA_EMAIL" \
    --description="Trigger daily irrigation rule evaluation and alert dispatch" \
    --project="$PROJECT_ID"
  echo "✔ Scheduler job created: irrigation-evaluate ($EVALUATE_CRON UTC)"
fi

# ---------------------------------------------------------------------------
print_step "Deployment complete! ✅"
echo
echo "  Cloud Function URLs:"
echo "    fetch-and-store-weather  → $WEATHER_URL"
echo "    evaluate-and-recommend   → $EVALUATE_URL"
RECS_URL=$(gcloud functions describe "get-recommendations" \
  --region="$REGION" --gen2 --format="value(serviceConfig.uri)" --project="$PROJECT_ID")
echo "    get-recommendations      → $RECS_URL"
echo
echo "  Scheduler jobs (UTC):"
echo "    irrigation-fetch-weather → $WEATHER_CRON"
echo "    irrigation-evaluate      → $EVALUATE_CRON"
echo
echo "Tip: Test the API immediately with:"
echo "  curl \"$RECS_URL?summary=true\""
