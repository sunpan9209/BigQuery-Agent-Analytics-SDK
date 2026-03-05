# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OPA policy evaluator configuration and SQL templates.

This module provides:
1. ``OPAPolicyEvaluator``: configuration object consumed by ``Client.evaluate``.
2. SQL template builders for remote-function and preview Python UDF modes.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal


_FQN_RE = re.compile(r"^[A-Za-z0-9-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


_REMOTE_FUNCTION_DECISIONS_QUERY = """\
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
  FROM `{project}.{dataset}.{table}`
  WHERE timestamp > TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), INTERVAL @policy_lookback_hours HOUR
  )
    AND ({where})
  ORDER BY timestamp DESC
  LIMIT @policy_max_events
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
        @policy_id AS policy_id,
        @policy_version AS policy_version
      )
    ) AS payload_json
  FROM filtered_events
),
policy_raw AS (
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
    payload_json,
    {decision_expr} AS raw_output_json
  FROM policy_inputs
)
SELECT
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(trace_id, ''),
        ':',
        COALESCE(span_id, ''),
        ':',
        @policy_id,
        ':',
        @policy_version
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
  @policy_id AS policy_id,
  @policy_version AS policy_version,
  LOWER(
    COALESCE(
      JSON_EXTRACT_SCALAR(raw_output_json, '$.action'),
      'allow'
    )
  ) AS decision,
  LOWER(
    COALESCE(
      JSON_EXTRACT_SCALAR(raw_output_json, '$.severity'),
      'low'
    )
  ) AS severity,
  COALESCE(
    JSON_EXTRACT_SCALAR(raw_output_json, '$.reason_code'),
    'none'
  ) AS reason_code,
  COALESCE(
    JSON_EXTRACT_SCALAR(raw_output_json, '$.reason_text'),
    ''
  ) AS reason_text,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(raw_output_json, '$.confidence') AS FLOAT64
  ) AS confidence,
  TO_HEX(MD5(payload_json)) AS input_fingerprint,
  payload_json AS raw_input_json,
  raw_output_json,
  CURRENT_TIMESTAMP() AS processing_ts
FROM policy_raw
"""

_PYTHON_UDF_FUNCTION_PREFIX = """\
CREATE TEMP FUNCTION policy_eval_py(payload STRING)
RETURNS STRING
LANGUAGE python
OPTIONS(entry_point='evaluate')
AS r\"\"\"
import json
import re


def evaluate(payload):
  try:
    obj = json.loads(payload or '{}')
  except Exception:
    obj = {}

  tool = (obj.get('tool_name') or '').lower()
  event_type = obj.get('event_type') or ''
  serialized = json.dumps(obj, sort_keys=True)
  pii_regex = r'(?i)([A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}|\\b\\d{3}-\\d{2}-\\d{4}\\b)'
  risky_tools = {'http_request', 'webhook_post', 'slack_send'}
  has_pii = re.search(pii_regex, serialized) is not None

  if event_type == 'TOOL_STARTING' and tool in risky_tools and has_pii:
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
      'confidence': 0.80
  })
\"\"\";
"""

_CREATE_OR_REPLACE_TABLE_QUERY = """\
CREATE OR REPLACE TABLE `{project}.{dataset}.{table}` AS
{decisions_query}
"""

_SESSION_POLICY_SUMMARY_QUERY = """\
WITH policy_decisions AS (
  {source_query}
)
SELECT
  session_id,
  COUNT(*) AS evaluated_events,
  COUNTIF(decision = 'allow') AS allow_count,
  COUNTIF(decision = 'warn') AS warn_count,
  COUNTIF(decision = 'deny') AS deny_count,
  COUNTIF(severity = 'critical') AS critical_count,
  AVG(
    CASE decision
      WHEN 'allow' THEN 1.0
      WHEN 'warn' THEN 0.5
      ELSE 0.0
    END
  ) AS policy_compliance,
  SAFE_DIVIDE(
    COUNTIF(severity = 'critical'),
    COUNT(*)
  ) AS critical_violation_rate
FROM policy_decisions
GROUP BY session_id
ORDER BY session_id
"""

_POLICY_DECISION_ROWS_QUERY = """\
WITH policy_decisions AS (
  {source_query}
)
SELECT
  decision_id,
  event_timestamp,
  session_id,
  trace_id,
  span_id,
  parent_span_id,
  event_type,
  agent,
  user_id,
  tool_name,
  policy_id,
  policy_version,
  decision,
  severity,
  reason_code,
  reason_text,
  confidence,
  input_fingerprint,
  raw_input_json,
  raw_output_json,
  processing_ts
FROM policy_decisions
ORDER BY event_timestamp DESC
"""

_COUNT_SOURCE_EVENTS_QUERY = """\
SELECT COUNT(1) AS event_count
FROM (
  SELECT 1
  FROM `{project}.{dataset}.{table}`
  WHERE timestamp > TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), INTERVAL @policy_lookback_hours HOUR
  )
    AND ({where})
  LIMIT @policy_max_events
)
"""


@dataclass
class OPAPolicyEvaluator:
  """Configuration for policy evaluation driven by BigQuery SQL."""

  policy_id: str
  policy_version: str = "v1"
  mode: Literal["remote_function", "python_udf_preview"] = "remote_function"
  remote_function_fqn: str | None = None
  persist_table: str | None = "policy_decisions_poc"
  return_decisions: bool = True
  lookback_hours: int = 24
  max_events: int = 2000
  allow_fallback_seed_table: bool = True
  fallback_table_id: str = "agent_events_policy_poc"
  enable_preview_python_udf: bool = False
  max_bytes_billed_gb: int = 50
  name: str = "opa_policy_evaluator"

  def validate(self) -> None:
    """Validates evaluator settings."""
    if not self.policy_id:
      raise ValueError("policy_id is required")
    if self.lookback_hours <= 0:
      raise ValueError("lookback_hours must be > 0")
    if self.max_events <= 0:
      raise ValueError("max_events must be > 0")
    if self.max_bytes_billed_gb <= 0:
      raise ValueError("max_bytes_billed_gb must be > 0")

    if self.mode == "remote_function":
      if not self.remote_function_fqn:
        raise ValueError(
            "remote_function_fqn is required when mode='remote_function'"
        )
      _quote_fqn(self.remote_function_fqn)

    if self.mode == "python_udf_preview" and not self.enable_preview_python_udf:
      raise ValueError(
          "python_udf_preview mode requires enable_preview_python_udf=True"
      )

  @property
  def max_bytes_billed(self) -> int:
    """Returns the max bytes billed cap in bytes."""
    return self.max_bytes_billed_gb * (1024**3)


def _quote_fqn(function_fqn: str) -> str:
  """Returns a safely quoted function FQN."""
  normalized = function_fqn.strip().strip("`")
  if not _FQN_RE.fullmatch(normalized):
    raise ValueError(
        "remote_function_fqn must match project.dataset.function_name"
    )
  return f"`{normalized}`"


def build_policy_decisions_query(
    *,
    project: str,
    dataset: str,
    table: str,
    where: str,
    evaluator: OPAPolicyEvaluator,
) -> str:
  """Builds the SQL query returning policy decision rows."""
  evaluator.validate()
  if evaluator.mode == "remote_function":
    decision_expr = f"{_quote_fqn(evaluator.remote_function_fqn or '')}(payload_json)"
    return _REMOTE_FUNCTION_DECISIONS_QUERY.format(
      project=project,
      dataset=dataset,
      table=table,
      where=where,
      decision_expr=decision_expr,
    )

  return _REMOTE_FUNCTION_DECISIONS_QUERY.format(
      project=project,
      dataset=dataset,
      table=table,
      where=where,
      decision_expr="policy_eval_py(payload_json)",
  )


def build_python_udf_prefix() -> str:
  """Returns the preview Python UDF function definition SQL."""
  return _PYTHON_UDF_FUNCTION_PREFIX


def build_script_with_python_udf(query: str) -> str:
  """Prefixes a query with the preview Python UDF definition."""
  return f"{_PYTHON_UDF_FUNCTION_PREFIX}\n{query}"


def build_create_or_replace_table_query(
    *,
    project: str,
    dataset: str,
    table: str,
    decisions_query: str,
    with_python_udf_prefix: bool = False,
) -> str:
  """Builds CREATE OR REPLACE TABLE AS SELECT for decision rows."""
  create_stmt = _CREATE_OR_REPLACE_TABLE_QUERY.format(
      project=project,
      dataset=dataset,
      table=table,
      decisions_query=decisions_query,
  )
  if with_python_udf_prefix:
    return build_script_with_python_udf(create_stmt)
  return create_stmt


def build_session_summary_query(source_query: str) -> str:
  """Builds a per-session summary query from policy decision rows."""
  return _SESSION_POLICY_SUMMARY_QUERY.format(source_query=source_query)


def build_decision_rows_query(source_query: str) -> str:
  """Builds a query returning decision rows from policy decision rows."""
  return _POLICY_DECISION_ROWS_QUERY.format(source_query=source_query)


def build_event_count_query(
    *,
    project: str,
    dataset: str,
    table: str,
    where: str,
) -> str:
  """Builds a query counting candidate source events."""
  return _COUNT_SOURCE_EVENTS_QUERY.format(
      project=project,
      dataset=dataset,
      table=table,
      where=where,
  )
