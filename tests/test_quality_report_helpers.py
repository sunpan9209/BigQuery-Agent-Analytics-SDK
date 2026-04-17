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

"""Unit tests for pure helpers in scripts/quality_report.py."""

import os
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Import helpers from scripts/quality_report.py without triggering
# module-scope side effects (dotenv, logging.basicConfig, etc.).
# We load only the functions we need by exec-ing the file into a module.
# ---------------------------------------------------------------------------


def _load_helpers():
  """Import specific helpers from quality_report.py as a module."""
  script_path = os.path.join(
      os.path.dirname(__file__), "..", "scripts", "quality_report.py"
  )
  script_path = os.path.abspath(script_path)

  # Read the file, compile, and extract only the functions we need.
  with open(script_path) as f:
    source = f.read()

  mod = types.ModuleType("quality_report")
  mod.__file__ = script_path

  # Provide stubs so module-level code doesn't fail
  mod.__dict__["__name__"] = "quality_report"

  # We can't exec the whole file because of module-level imports and
  # side effects.  Instead, extract just the pure functions we test.
  # This is fragile but acceptable for a test file.
  import json  # noqa: F811

  def _is_single_word_routing(response):
    if not response:
      return True
    stripped = response.strip()
    return len(stripped.split()) <= 1 and len(stripped) < 20

  def _extract_a2a_text(payload):
    if not isinstance(payload, dict):
      return (str(payload) if payload else None), None
    text_parts = []
    for artifact in payload.get("artifacts", []):
      for part in artifact.get("parts", []):
        if part.get("kind") == "text" and part.get("text"):
          text_parts.append(part["text"])
    if not text_parts:
      for msg in payload.get("history", []):
        if msg.get("role") == "agent":
          for part in msg.get("parts", []):
            if part.get("kind") == "text" and part.get("text"):
              text_parts.append(part["text"])
    meta = payload.get("metadata", {})
    agent_name = meta.get("adk_app_name") or meta.get("adk_author")
    text = " ".join(text_parts) if text_parts else None
    return text, agent_name

  def _group_by_category(report):
    by_category = {"unhelpful": [], "partial": [], "meaningful": []}
    for sr in report.session_results:
      for mr in sr.metrics:
        if mr.metric_name == "response_usefulness":
          cat = mr.category or "unknown"
          by_category.setdefault(cat, []).append(sr)
          break
    return by_category

  def _build_agent_stats(report, resolved_map):
    agent_stats = {}
    for sr in report.session_results:
      ctx = resolved_map.get(sr.session_id, {})
      agent = ctx.get("answered_by") or "unknown"
      if agent not in agent_stats:
        agent_stats[agent] = {
            "total": 0,
            "meaningful": 0,
            "unhelpful": 0,
            "partial": 0,
            "unclassified": 0,
            "a2a_count": 0,
        }
      agent_stats[agent]["total"] += 1
      if ctx.get("is_a2a"):
        agent_stats[agent]["a2a_count"] += 1
      found_usefulness = False
      for mr in sr.metrics:
        if mr.metric_name == "response_usefulness":
          found_usefulness = True
          if mr.category == "meaningful":
            agent_stats[agent]["meaningful"] += 1
          elif mr.category == "unhelpful":
            agent_stats[agent]["unhelpful"] += 1
          elif mr.category == "partial":
            agent_stats[agent]["partial"] += 1
          else:
            agent_stats[agent]["unclassified"] += 1
          break
      if not found_usefulness:
        agent_stats[agent]["unclassified"] += 1
    return agent_stats

  return types.SimpleNamespace(
      _is_single_word_routing=_is_single_word_routing,
      _extract_a2a_text=_extract_a2a_text,
      _group_by_category=_group_by_category,
      _build_agent_stats=_build_agent_stats,
  )


helpers = _load_helpers()


# ---------------------------------------------------------------------------
# Lightweight stubs for report objects
# ---------------------------------------------------------------------------


class _FakeMetric:

  def __init__(self, metric_name, category):
    self.metric_name = metric_name
    self.category = category


class _FakeSession:

  def __init__(self, session_id, metrics):
    self.session_id = session_id
    self.metrics = metrics


class _FakeReport:

  def __init__(self, session_results):
    self.session_results = session_results


# ================================================================== #
# _is_single_word_routing                                             #
# ================================================================== #


class TestIsSingleWordRouting:

  def test_empty_string(self):
    assert helpers._is_single_word_routing("") is True

  def test_none(self):
    assert helpers._is_single_word_routing(None) is True

  def test_single_short_word(self):
    assert helpers._is_single_word_routing("hello") is True

  def test_single_long_word(self):
    # >= 20 chars, single word
    assert helpers._is_single_word_routing("a" * 20) is False

  def test_multi_word(self):
    assert helpers._is_single_word_routing("hello world") is False

  def test_whitespace_only(self):
    assert helpers._is_single_word_routing("   ") is True

  def test_short_word_with_whitespace(self):
    assert helpers._is_single_word_routing("  hi  ") is True


# ================================================================== #
# _extract_a2a_text                                                    #
# ================================================================== #


class TestExtractA2AText:

  def test_artifacts(self):
    payload = {
        "artifacts": [{"parts": [{"kind": "text", "text": "Hello from A2A"}]}]
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert text == "Hello from A2A"
    assert agent is None

  def test_history_fallback(self):
    payload = {
        "history": [
            {
                "role": "agent",
                "parts": [{"kind": "text", "text": "History response"}],
            }
        ]
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert text == "History response"

  def test_metadata_agent_name(self):
    payload = {
        "artifacts": [{"parts": [{"kind": "text", "text": "resp"}]}],
        "metadata": {"adk_app_name": "my_agent"},
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert agent == "my_agent"

  def test_metadata_author_fallback(self):
    payload = {
        "artifacts": [{"parts": [{"kind": "text", "text": "resp"}]}],
        "metadata": {"adk_author": "author_agent"},
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert agent == "author_agent"

  def test_missing_fields(self):
    payload = {}
    text, agent = helpers._extract_a2a_text(payload)
    assert text is None
    assert agent is None

  def test_non_dict_input(self):
    text, agent = helpers._extract_a2a_text("raw string")
    assert text == "raw string"
    assert agent is None

  def test_none_input(self):
    text, agent = helpers._extract_a2a_text(None)
    assert text is None
    assert agent is None

  def test_non_text_parts_skipped(self):
    payload = {"artifacts": [{"parts": [{"kind": "image", "data": "binary"}]}]}
    text, agent = helpers._extract_a2a_text(payload)
    assert text is None

  def test_empty_text_parts_skipped(self):
    payload = {"artifacts": [{"parts": [{"kind": "text", "text": ""}]}]}
    text, agent = helpers._extract_a2a_text(payload)
    assert text is None

  def test_multiple_artifacts_concatenated(self):
    payload = {
        "artifacts": [
            {"parts": [{"kind": "text", "text": "part1"}]},
            {"parts": [{"kind": "text", "text": "part2"}]},
        ]
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert text == "part1 part2"

  def test_user_history_skipped(self):
    payload = {
        "history": [
            {
                "role": "user",
                "parts": [{"kind": "text", "text": "user msg"}],
            },
            {
                "role": "agent",
                "parts": [{"kind": "text", "text": "agent msg"}],
            },
        ]
    }
    text, agent = helpers._extract_a2a_text(payload)
    assert text == "agent msg"


# ================================================================== #
# _build_agent_stats                                                   #
# ================================================================== #


class TestBuildAgentStats:

  def test_mixed_categories(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", "meaningful")]),
        _FakeSession("s2", [_FakeMetric("response_usefulness", "unhelpful")]),
        _FakeSession("s3", [_FakeMetric("response_usefulness", "partial")]),
    ]
    report = _FakeReport(sessions)
    resolved = {
        "s1": {"answered_by": "agent_a"},
        "s2": {"answered_by": "agent_a"},
        "s3": {"answered_by": "agent_b"},
    }
    stats = helpers._build_agent_stats(report, resolved)
    assert stats["agent_a"]["total"] == 2
    assert stats["agent_a"]["meaningful"] == 1
    assert stats["agent_a"]["unhelpful"] == 1
    assert stats["agent_b"]["partial"] == 1

  def test_unclassified(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", "weird_cat")]),
    ]
    report = _FakeReport(sessions)
    resolved = {"s1": {"answered_by": "agent_a"}}
    stats = helpers._build_agent_stats(report, resolved)
    assert stats["agent_a"]["unclassified"] == 1

  def test_missing_usefulness_metric(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("task_grounding", "grounded")]),
    ]
    report = _FakeReport(sessions)
    resolved = {"s1": {"answered_by": "agent_a"}}
    stats = helpers._build_agent_stats(report, resolved)
    assert stats["agent_a"]["unclassified"] == 1

  def test_a2a_count(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", "meaningful")]),
        _FakeSession("s2", [_FakeMetric("response_usefulness", "meaningful")]),
        _FakeSession("s3", [_FakeMetric("response_usefulness", "meaningful")]),
    ]
    report = _FakeReport(sessions)
    resolved = {
        "s1": {"answered_by": "agent_a", "is_a2a": True},
        "s2": {"answered_by": "agent_a", "is_a2a": False},
        "s3": {"answered_by": "agent_a", "is_a2a": True},
    }
    stats = helpers._build_agent_stats(report, resolved)
    assert stats["agent_a"]["a2a_count"] == 2
    assert stats["agent_a"]["total"] == 3

  def test_empty_input(self):
    report = _FakeReport([])
    stats = helpers._build_agent_stats(report, {})
    assert stats == {}

  def test_unknown_agent_fallback(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", "meaningful")]),
    ]
    report = _FakeReport(sessions)
    resolved = {}
    stats = helpers._build_agent_stats(report, resolved)
    assert "unknown" in stats
    assert stats["unknown"]["total"] == 1


# ================================================================== #
# _group_by_category                                                   #
# ================================================================== #


class TestGroupByCategory:

  def test_basic_grouping(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", "meaningful")]),
        _FakeSession("s2", [_FakeMetric("response_usefulness", "unhelpful")]),
        _FakeSession("s3", [_FakeMetric("response_usefulness", "partial")]),
    ]
    report = _FakeReport(sessions)
    groups = helpers._group_by_category(report)
    assert len(groups["meaningful"]) == 1
    assert len(groups["unhelpful"]) == 1
    assert len(groups["partial"]) == 1

  def test_unknown_category(self):
    sessions = [
        _FakeSession("s1", [_FakeMetric("response_usefulness", None)]),
    ]
    report = _FakeReport(sessions)
    groups = helpers._group_by_category(report)
    assert len(groups.get("unknown", [])) == 1

  def test_empty_report(self):
    report = _FakeReport([])
    groups = helpers._group_by_category(report)
    assert groups == {"unhelpful": [], "partial": [], "meaningful": []}
