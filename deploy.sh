#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Shuttles Pipeline — Google Cloud Run Jobs deployment
#
# What this does:
#   1. Enables required GCP APIs
#   2. Stores all secrets in Secret Manager
#   3. Builds and pushes the Docker image to Artifact Registry
#   4. Creates (or updates) a Cloud Run Job
#   5. Creates a single hourly Cloud Scheduler job to trigger it
#      (The pipeline reads the "Pipeline Schedule" sheet tab at runtime and
#       exits early if the current time isn't a scheduled run.)
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (`gcloud auth login`)
#   - Docker installed and running
#   - Your .env file filled in (all variables below)
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh
#
# To force a manual run at any time:
#   gcloud run jobs execute shuttles-pipeline --region $REGION \
#     --update-env-vars FORCE_RUN=1
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── 1. Configuration — fill these in ─────────────────────────────────────────

PROJECT_ID="charterup-revops"          # gcloud config get-value project
REGION="us-central1"                      # Cloud Run region (us-central1 recommended)
JOB_NAME="shuttles-pipeline"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/shuttles/$JOB_NAME"

# Timezone for interpreting the Pipeline Schedule sheet times
SCHEDULE_TIMEZONE="America/New_York"   # adjust to your local timezone

# Service account that the Cloud Run Job runs as.
# This account needs Drive + Sheets access (same as your key file).
# If left empty, the default Compute service account is used.
RUNNER_SA=""   # e.g. "shuttles-runner@your-project.iam.gserviceaccount.com"

# ── 2. Secrets — values to store in Secret Manager ───────────────────────────
# Fill these in; they will be stored as Secret Manager secrets and injected
# into the Cloud Run Job as environment variables at runtime.

REDSHIFT_HOST=production-default.cjhwm8ae6gcu.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=coachrail
REDSHIFT_USER=coefficient
REDSHIFT_PASSWORD=j-wU.7nphyCfnsG-ADYx

SF_USERNAME=vanessa.hamer@charterup.com
SF_PASSWORD=GetOnTheBus1
SF_SECURITY_TOKEN=lj9wTCmgPD8lMTabOiKrTS4j

# Paste the ENTIRE contents of your service account JSON key file here
# (the {"type":"service_account",...} JSON — all on one line or multi-line).
# Tip: GOOGLE_SA_JSON=$(cat /path/to/your-key.json)
GOOGLE_SA_JSON="/Users/vanessahamer/Desktop/revenue-reporting/charterup-revops-d0617afd668d.json"

DRIVE_FOLDER_ID=109GvuD5lRakXg3fm4gVj5nJMe5bpCiWk
SETTINGS_SHEET_ID=FjTBjch7BUxJgrvqhP9JyXyJ00ACGyA7348xgQ

# ─────────────────────────────────────────────────────────────────────────────

echo "▶ Project: $PROJECT_ID  Region: $REGION  Job: $JOB_NAME"
gcloud config set project "$PROJECT_ID"

# ── Enable APIs ───────────────────────────────────────────────────────────────
echo "▶ Enabling APIs…"
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  --quiet

# ── Artifact Registry repo ────────────────────────────────────────────────────
echo "▶ Creating Artifact Registry repo (if needed)…"
gcloud artifacts repositories create shuttles \
  --repository-format=docker \
  --location="$REGION" \
  --quiet 2>/dev/null || true

# ── Store secrets ─────────────────────────────────────────────────────────────
echo "▶ Storing secrets in Secret Manager…"

store_secret() {
  local name="$1" value="$2"
  if gcloud secrets describe "$name" --quiet &>/dev/null; then
    echo "   $name — updating"
    printf '%s' "$value" | gcloud secrets versions add "$name" --data-file=-
  else
    echo "   $name — creating"
    printf '%s' "$value" | gcloud secrets create "$name" --data-file=- --replication-policy=automatic
  fi
}

store_secret "shuttles-redshift-host"          "$REDSHIFT_HOST"
store_secret "shuttles-redshift-port"          "$REDSHIFT_PORT"
store_secret "shuttles-redshift-database"      "$REDSHIFT_DATABASE"
store_secret "shuttles-redshift-user"          "$REDSHIFT_USER"
store_secret "shuttles-redshift-password"      "$REDSHIFT_PASSWORD"
store_secret "shuttles-sf-username"            "$SF_USERNAME"
store_secret "shuttles-sf-password"            "$SF_PASSWORD"
store_secret "shuttles-sf-security-token"      "$SF_SECURITY_TOKEN"
store_secret "shuttles-gcp-sa-json"            "$GOOGLE_SA_JSON"
store_secret "shuttles-drive-folder-id"        "$DRIVE_FOLDER_ID"
store_secret "shuttles-settings-sheet-id"      "$SETTINGS_SHEET_ID"

# ── Build & push image ────────────────────────────────────────────────────────
echo "▶ Building and pushing Docker image…"
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
docker build --platform linux/amd64 -t "$IMAGE:latest" .
docker push "$IMAGE:latest"

# ── Runner service account permissions ───────────────────────────────────────
if [ -n "$RUNNER_SA" ]; then
  echo "▶ Granting runner SA access to secrets…"
  for SECRET in \
    shuttles-redshift-host shuttles-redshift-port shuttles-redshift-database \
    shuttles-redshift-user shuttles-redshift-password \
    shuttles-sf-username shuttles-sf-password shuttles-sf-security-token \
    shuttles-gcp-sa-json shuttles-drive-folder-id shuttles-settings-sheet-id; do
    gcloud secrets add-iam-policy-binding "$SECRET" \
      --member="serviceAccount:$RUNNER_SA" \
      --role="roles/secretmanager.secretAccessor" \
      --quiet
  done
fi

SA_FLAG=""
[ -n "$RUNNER_SA" ] && SA_FLAG="--service-account=$RUNNER_SA"

# ── Create / update Cloud Run Job ─────────────────────────────────────────────
echo "▶ Deploying Cloud Run Job…"

# Secret env-var helper: name=SECRET_NAME:latest
S() { echo "$1=shuttles-$2:latest"; }

gcloud run jobs deploy "$JOB_NAME" \
  --image="$IMAGE:latest" \
  --region="$REGION" \
  --task-timeout=3600 \
  --max-retries=1 \
  --set-env-vars="SCHEDULE_TIMEZONE=$SCHEDULE_TIMEZONE" \
  --set-secrets="$(S REDSHIFT_HOST redshift-host),\
$(S REDSHIFT_PORT redshift-port),\
$(S REDSHIFT_DATABASE redshift-database),\
$(S REDSHIFT_USER redshift-user),\
$(S REDSHIFT_PASSWORD redshift-password),\
$(S SF_USERNAME sf-username),\
$(S SF_PASSWORD sf-password),\
$(S SF_SECURITY_TOKEN sf-security-token),\
GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT=shuttles-gcp-sa-json:latest,\
$(S DRIVE_FOLDER_ID drive-folder-id),\
$(S SETTINGS_SHEET_ID settings-sheet-id)" \
  $SA_FLAG \
  --quiet

# ── Cloud Scheduler — one hourly trigger ──────────────────────────────────────
# The pipeline reads the "Pipeline Schedule" sheet tab at runtime and exits
# early when the current time doesn't match a scheduled slot.
echo "▶ Setting up Cloud Scheduler (hourly trigger)…"

# Service account used by Cloud Scheduler to invoke the job.
# If RUNNER_SA is set, reuse it; otherwise use the Compute default SA.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
INVOKER_SA="${RUNNER_SA:-${PROJECT_NUMBER}-compute@developer.gserviceaccount.com}"

gcloud scheduler jobs describe "shuttles-pipeline-hourly" \
  --location="$REGION" --quiet &>/dev/null \
  && gcloud scheduler jobs delete "shuttles-pipeline-hourly" \
     --location="$REGION" --quiet

gcloud scheduler jobs create http "shuttles-pipeline-hourly" \
  --location="$REGION" \
  --schedule="0 * * * *" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/$JOB_NAME:run" \
  --message-body="{}" \
  --oauth-service-account-email="$INVOKER_SA" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --time-zone="UTC" \
  --quiet

echo ""
echo "✅  Deployment complete."
echo ""
echo "   Cloud Run Job:      $JOB_NAME  ($REGION)"
echo "   Trigger:            hourly at :00 UTC"
echo "   Schedule control:   'Pipeline Schedule' tab in your Settings sheet"
echo ""
echo "   Force a manual run:"
echo "   gcloud run jobs execute $JOB_NAME --region $REGION \\"
echo "     --update-env-vars FORCE_RUN=1"
echo ""
echo "   View logs:"
echo "   gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=$JOB_NAME' \\"
echo "     --project $PROJECT_ID --limit 50 --format 'value(textPayload)'"
