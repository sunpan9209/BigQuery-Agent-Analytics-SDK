-- One-time policy decisions table materialization via Remote Function.
-- Replace placeholders before executing:
--   ${PROJECT_ID}, ${DATASET_ID}, ${SOURCE_TABLE}, ${REMOTE_FUNCTION_FQN}
--   ${POLICY_ID}, ${POLICY_VERSION}, ${LOOKBACK_HOURS}, ${MAX_EVENTS}

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.policy_decisions_poc` AS
WITH filtered_events AS (
  SELECT
    timestamp AS event_timestamp,
    session_id,
    trace_id,
    span_id,
    parent_span_id,
    event_type,
    agent,
    user_id,
    content,
    attributes,
    JSON_EXTRACT_SCALAR(content, '$.tool') AS tool_name
  FROM `${PROJECT_ID}.${DATASET_ID}.${SOURCE_TABLE}`
  WHERE timestamp > TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), INTERVAL ${LOOKBACK_HOURS} HOUR
  )
  ORDER BY timestamp DESC
  LIMIT ${MAX_EVENTS}
),
policy_inputs AS (
  SELECT
    event_timestamp,
    session_id,
    trace_id,
    span_id,
    parent_span_id,
    event_type,
    agent,
    user_id,
    tool_name,
    TO_JSON_STRING(
      STRUCT(
        event_timestamp AS timestamp,
        session_id,
        trace_id,
        span_id,
        parent_span_id,
        event_type,
        agent,
        user_id,
        tool_name,
        content,
        attributes,
        '${POLICY_ID}' AS policy_id,
        '${POLICY_VERSION}' AS policy_version
      )
    ) AS payload_json
  FROM filtered_events
),
policy_raw AS (
  SELECT
    *,
    `${REMOTE_FUNCTION_FQN}`(payload_json) AS raw_output_json
  FROM policy_inputs
)
SELECT
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(trace_id, ''), ':', COALESCE(span_id, ''), ':',
        '${POLICY_ID}', ':', '${POLICY_VERSION}'
      )
    )
  ) AS decision_id,
  event_timestamp,
  session_id,
  trace_id,
  span_id,
  parent_span_id,
  event_type,
  agent,
  user_id,
  tool_name,
  '${POLICY_ID}' AS policy_id,
  '${POLICY_VERSION}' AS policy_version,
  LOWER(COALESCE(JSON_EXTRACT_SCALAR(raw_output_json, '$.action'), 'allow')) AS decision,
  LOWER(COALESCE(JSON_EXTRACT_SCALAR(raw_output_json, '$.severity'), 'low')) AS severity,
  COALESCE(JSON_EXTRACT_SCALAR(raw_output_json, '$.reason_code'), 'none') AS reason_code,
  COALESCE(JSON_EXTRACT_SCALAR(raw_output_json, '$.reason_text'), '') AS reason_text,
  SAFE_CAST(JSON_EXTRACT_SCALAR(raw_output_json, '$.confidence') AS FLOAT64) AS confidence,
  TO_HEX(MD5(payload_json)) AS input_fingerprint,
  payload_json AS raw_input_json,
  raw_output_json,
  CURRENT_TIMESTAMP() AS processing_ts
FROM policy_raw;
