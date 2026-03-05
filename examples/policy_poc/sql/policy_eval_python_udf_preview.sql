-- Preview prototype: Python UDF policy evaluation in BigQuery.
-- This path is experimental and not guaranteed parity with OPA/Rego.
-- Replace placeholders: ${PROJECT_ID}, ${DATASET_ID}, ${SOURCE_TABLE}

CREATE TEMP FUNCTION policy_eval_py(payload STRING)
RETURNS STRING
LANGUAGE python
OPTIONS(entry_point='evaluate')
AS r"""
import json
import re


def evaluate(payload):
  try:
    obj = json.loads(payload or '{}')
  except Exception:
    obj = {}

  tool = (obj.get('tool_name') or '').lower()
  text = json.dumps(obj, sort_keys=True)
  pii = re.search(r'(?i)([A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}|\\b\\d{3}-\\d{2}-\\d{4}\\b)', text)
  if obj.get('event_type') == 'TOOL_STARTING' and tool in {'http_request', 'webhook_post', 'slack_send'} and pii:
    return json.dumps({
      'action': 'deny',
      'severity': 'high',
      'reason_code': 'pii_egress',
      'reason_text': 'Potential PII egress via external tool',
      'confidence': 0.95
    })

  return json.dumps({
    'action': 'allow',
    'severity': 'low',
    'reason_code': 'no_match',
    'reason_text': 'No policy violation',
    'confidence': 0.8
  })
""";

WITH events AS (
  SELECT
    timestamp,
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
),
policy_inputs AS (
  SELECT
    TO_JSON_STRING(
      STRUCT(
        timestamp,
        session_id,
        trace_id,
        span_id,
        parent_span_id,
        event_type,
        agent,
        user_id,
        tool_name,
        content,
        attributes
      )
    ) AS payload_json
  FROM events
)
SELECT
  policy_eval_py(payload_json) AS policy_decision_json
FROM policy_inputs
LIMIT 100;
