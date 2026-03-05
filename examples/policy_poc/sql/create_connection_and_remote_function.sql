-- BigQuery Remote Function bootstrap for OPA policy evaluation.
-- Replace placeholders before executing.
--   ${PROJECT_ID}
--   ${DATASET_ID}
--   ${CONNECTION_ID}
--   ${REMOTE_FUNCTION_NAME}
--   ${CLOUD_RUN_ENDPOINT}

CREATE CONNECTION IF NOT EXISTS `${PROJECT_ID}.US.${CONNECTION_ID}`
OPTIONS (
  connection_type = 'CLOUD_RESOURCE'
);

-- Grant invoker on Cloud Run service to the BigQuery connection service account.
-- 1) Discover service account:
--    bq show --connection --location=US ${PROJECT_ID}.US.${CONNECTION_ID}
-- 2) Grant run.invoker:
--    gcloud run services add-iam-policy-binding <SERVICE_NAME> \
--      --region=<REGION> \
--      --member="serviceAccount:<BQ_CONNECTION_SERVICE_ACCOUNT>" \
--      --role="roles/run.invoker"

CREATE OR REPLACE FUNCTION `${PROJECT_ID}.${DATASET_ID}.${REMOTE_FUNCTION_NAME}`(
  payload STRING
)
RETURNS STRING
REMOTE WITH CONNECTION `${PROJECT_ID}.US.${CONNECTION_ID}`
OPTIONS (
  endpoint = '${CLOUD_RUN_ENDPOINT}',
  max_batching_rows = 50
);

-- Sanity check:
-- SELECT `${PROJECT_ID}.${DATASET_ID}.${REMOTE_FUNCTION_NAME}`('{"event_type":"TOOL_STARTING","tool_name":"http_request"}');
