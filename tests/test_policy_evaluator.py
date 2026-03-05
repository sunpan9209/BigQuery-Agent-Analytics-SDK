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

"""Tests for policy evaluator SQL templates and validation."""

import pytest

from bigquery_agent_analytics.policy_evaluator import (
    build_create_or_replace_table_query,
)
from bigquery_agent_analytics.policy_evaluator import build_policy_decisions_query
from bigquery_agent_analytics.policy_evaluator import build_script_with_python_udf
from bigquery_agent_analytics.policy_evaluator import OPAPolicyEvaluator
from bigquery_agent_analytics.policy_evaluator import _quote_fqn


class TestOPAPolicyEvaluator:
  """Validation and template tests."""

  def test_remote_mode_requires_fqn(self):
    ev = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="remote_function",
        remote_function_fqn=None,
    )
    with pytest.raises(ValueError, match="remote_function_fqn"):
      ev.validate()

  def test_python_preview_requires_flag(self):
    ev = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="python_udf_preview",
        enable_preview_python_udf=False,
    )
    with pytest.raises(ValueError, match="enable_preview_python_udf"):
      ev.validate()

  def test_quote_fqn_valid(self):
    quoted = _quote_fqn("proj.dataset.policy_eval")
    assert quoted == "`proj.dataset.policy_eval`"

  def test_quote_fqn_valid_hyphenated_project(self):
    quoted = _quote_fqn("rag-chatbot-485501.dataset.policy_eval")
    assert quoted == "`rag-chatbot-485501.dataset.policy_eval`"

  def test_quote_fqn_invalid(self):
    with pytest.raises(ValueError, match="project.dataset.function_name"):
      _quote_fqn("bad-fqn")

  def test_build_remote_query_contains_remote_function(self):
    ev = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="remote_function",
        remote_function_fqn="proj.dataset.policy_eval",
    )
    query = build_policy_decisions_query(
        project="proj",
        dataset="dataset",
        table="events",
        where="TRUE",
        evaluator=ev,
    )
    assert "`proj.dataset.policy_eval`(" in query
    assert "payload_json" in query
    assert "JSON_EXTRACT_SCALAR(raw_output_json, '$.action')" in query
    assert "LIMIT @policy_max_events" in query

  def test_build_python_udf_query_contains_language_python(self):
    ev = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="python_udf_preview",
        enable_preview_python_udf=True,
    )
    query = build_policy_decisions_query(
        project="proj",
        dataset="dataset",
        table="events",
        where="TRUE",
        evaluator=ev,
    )
    assert "policy_eval_py(payload_json)" in query
    script = build_script_with_python_udf(query)
    assert "LANGUAGE python" in script
    assert "CREATE TEMP FUNCTION policy_eval_py" in script

  def test_build_create_table_query(self):
    sql = build_create_or_replace_table_query(
        project="proj",
        dataset="ds",
        table="policy_decisions_poc",
        decisions_query="SELECT 1 AS x",
    )
    assert "CREATE OR REPLACE TABLE `proj.ds.policy_decisions_poc`" in sql
    assert "SELECT 1 AS x" in sql
