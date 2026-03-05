#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=rag-chatbot-485501 \
#   REGION=us-central1 \
#   SERVICE_NAME=policy-opa-eval \
#   ./examples/policy_poc/scripts/deploy_cloudrun_opa.sh

PROJECT_ID="${PROJECT_ID:-rag-chatbot-485501}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-policy-opa-eval}"
IMAGE="${IMAGE:-gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest}"
MAX_ESTIMATED_STEP_USD="${MAX_ESTIMATED_STEP_USD:-5}"

ESTIMATED_STEP_USD="2"
if awk "BEGIN{exit !(${ESTIMATED_STEP_USD} > ${MAX_ESTIMATED_STEP_USD})}"; then
  echo "Estimated step cost ${ESTIMATED_STEP_USD} exceeds cap ${MAX_ESTIMATED_STEP_USD}."
  echo "Stop here and raise cap explicitly if you want to continue."
  exit 1
fi

echo "Building image ${IMAGE}"
gcloud builds submit \
  examples/policy_poc/cloudrun_opa_service \
  --tag "${IMAGE}" \
  --project "${PROJECT_ID}"

echo "Deploying Cloud Run service ${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --platform managed \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --allow-unauthenticated \
  --min-instances 0 \
  --max-instances 2 \
  --memory 512Mi \
  --cpu 1

echo "Service URL:"
gcloud run services describe "${SERVICE_NAME}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --format='value(status.url)'
