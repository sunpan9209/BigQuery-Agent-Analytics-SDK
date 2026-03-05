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

"""Tests for OPAPolicyEvaluator integration in Client."""

from unittest.mock import MagicMock

import pytest

from bigquery_agent_analytics.client import Client
from bigquery_agent_analytics.evaluators import EvaluationReport
from bigquery_agent_analytics.policy_evaluator import OPAPolicyEvaluator


def _mock_bq_client():
  return MagicMock()


def _make_mock_row(data):
  mock = MagicMock()
  mock.__iter__ = MagicMock(return_value=iter(data.items()))
  mock.get = data.get
  mock.keys = data.keys
  mock.values = data.values
  mock.items = data.items
  mock.__getitem__ = lambda self, k: data[k]
  return mock


def _make_job(rows):
  job = MagicMock()
  job.result.return_value = rows
  return job


class TestPolicyClientEvaluate:
  """Policy evaluator dispatch and report contract."""

  def test_evaluate_policy_remote_persist_and_return(self):
    mock_bq = _mock_bq_client()
    mock_bq.query.side_effect = [
        _make_job([_make_mock_row({"event_count": 3})]),
        _make_job([]),
        _make_job(
            [
                _make_mock_row(
                    {
                        "session_id": "s1",
                        "evaluated_events": 2,
                        "allow_count": 2,
                        "warn_count": 0,
                        "deny_count": 0,
                        "critical_count": 0,
                        "policy_compliance": 1.0,
                        "critical_violation_rate": 0.0,
                    }
                )
            ]
        ),
        _make_job(
            [
                _make_mock_row(
                    {
                        "decision_id": "abc",
                        "session_id": "s1",
                        "decision": "allow",
                        "reason_code": "no_match",
                    }
                )
            ]
        ),
    ]

    client = Client(
        project_id="proj",
        dataset_id="ds",
        verify_schema=False,
        bq_client=mock_bq,
    )
    evaluator = OPAPolicyEvaluator(
      policy_id="pii_egress_v1",
      remote_function_fqn="proj.ds.policy_eval",
    )
    report = client.evaluate(
        evaluator=evaluator,
        dataset="events_custom",
    )

    assert isinstance(report, EvaluationReport)
    assert report.total_sessions == 1
    assert report.session_scores[0].passed is True
    assert report.details["policy_id"] == "pii_egress_v1"
    assert report.details["source_table"] == "events_custom"
    assert report.details["persisted_table"] == "proj.ds.policy_decisions_poc"
    assert len(report.details["policy_decisions"]) == 1
    assert mock_bq.query.call_count == 4

    persist_query = mock_bq.query.call_args_list[1][0][0]
    assert "CREATE OR REPLACE TABLE `proj.ds.policy_decisions_poc`" in persist_query
    assert "`proj.ds.policy_eval`(" in persist_query
    assert "payload_json" in persist_query

  def test_evaluate_policy_fallback_table_used(self):
    mock_bq = _mock_bq_client()
    mock_bq.query.side_effect = [
        _make_job([_make_mock_row({"event_count": 0})]),
        _make_job([_make_mock_row({"event_count": 2})]),
        _make_job([]),
        _make_job(
            [
                _make_mock_row(
                    {
                        "session_id": "s-fallback",
                        "evaluated_events": 2,
                        "allow_count": 1,
                        "warn_count": 0,
                        "deny_count": 1,
                        "critical_count": 1,
                        "policy_compliance": 0.5,
                        "critical_violation_rate": 0.5,
                    }
                )
            ]
        ),
    ]

    client = Client(
        project_id="proj",
        dataset_id="ds",
        verify_schema=False,
        bq_client=mock_bq,
    )
    evaluator = OPAPolicyEvaluator(
      policy_id="pii_egress_v1",
      remote_function_fqn="proj.ds.policy_eval",
      fallback_table_id="seed_events",
      return_decisions=False,
    )
    report = client.evaluate(
        evaluator=evaluator,
        dataset="primary_events",
    )

    assert report.details["fallback_used"] is True
    assert report.details["source_table"] == "seed_events"
    assert report.session_scores[0].passed is False
    persist_query = mock_bq.query.call_args_list[2][0][0]
    assert "`proj.ds.seed_events`" in persist_query

  def test_python_udf_preview_requires_flag(self):
    mock_bq = _mock_bq_client()
    client = Client(
        project_id="proj",
        dataset_id="ds",
        verify_schema=False,
        bq_client=mock_bq,
    )
    evaluator = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="python_udf_preview",
        enable_preview_python_udf=False,
    )
    with pytest.raises(ValueError, match="enable_preview_python_udf"):
      client.evaluate(evaluator=evaluator)

  def test_python_udf_preview_query_is_script_prefixed(self):
    mock_bq = _mock_bq_client()
    mock_bq.query.side_effect = [
        _make_job([_make_mock_row({"event_count": 1})]),
        _make_job(
            [
                _make_mock_row(
                    {
                        "session_id": "s1",
                        "evaluated_events": 1,
                        "allow_count": 1,
                        "warn_count": 0,
                        "deny_count": 0,
                        "critical_count": 0,
                        "policy_compliance": 1.0,
                        "critical_violation_rate": 0.0,
                    }
                )
            ]
        ),
    ]

    client = Client(
        project_id="proj",
        dataset_id="ds",
        verify_schema=False,
        bq_client=mock_bq,
    )
    evaluator = OPAPolicyEvaluator(
        policy_id="pii_egress_v1",
        mode="python_udf_preview",
        enable_preview_python_udf=True,
        persist_table=None,
        return_decisions=False,
    )
    report = client.evaluate(evaluator=evaluator, dataset="events_custom")

    assert report.total_sessions == 1
    summary_query = mock_bq.query.call_args_list[1][0][0]
    assert "CREATE TEMP FUNCTION policy_eval_py" in summary_query
    assert "LANGUAGE python" in summary_query
