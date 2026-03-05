#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=rag-chatbot-485501 \
#   DATASET_ID=agent_trace \
#   CONNECTION_ID=policy_opa_conn \
#   REMOTE_FUNCTION_NAME=opa_policy_eval \
#   CLOUD_RUN_ENDPOINT=https://policy-opa-eval-xxxx.a.run.app \
#   ./examples/policy_poc/scripts/create_remote_function.sh

PROJECT_ID="${PROJECT_ID:-rag-chatbot-485501}"
DATASET_ID="${DATASET_ID:-agent_trace}"
CONNECTION_ID="${CONNECTION_ID:-policy_opa_conn}"
REMOTE_FUNCTION_NAME="${REMOTE_FUNCTION_NAME:-opa_policy_eval}"
CLOUD_RUN_ENDPOINT="${CLOUD_RUN_ENDPOINT:-}"

if [[ -z "${CLOUD_RUN_ENDPOINT}" ]]; then
  echo "CLOUD_RUN_ENDPOINT is required"
  exit 1
fi

TMP_SQL="$(mktemp)"
sed \
  -e "s#\${PROJECT_ID}#${PROJECT_ID}#g" \
  -e "s#\${DATASET_ID}#${DATASET_ID}#g" \
  -e "s#\${CONNECTION_ID}#${CONNECTION_ID}#g" \
  -e "s#\${REMOTE_FUNCTION_NAME}#${REMOTE_FUNCTION_NAME}#g" \
  -e "s#\${CLOUD_RUN_ENDPOINT}#${CLOUD_RUN_ENDPOINT}#g" \
  examples/policy_poc/sql/create_connection_and_remote_function.sql > "${TMP_SQL}"

echo "Applying remote function SQL in US multi-region"
bq query \
  --project_id="${PROJECT_ID}" \
  --location=US \
  --use_legacy_sql=false \
  < "${TMP_SQL}"

rm -f "${TMP_SQL}"
