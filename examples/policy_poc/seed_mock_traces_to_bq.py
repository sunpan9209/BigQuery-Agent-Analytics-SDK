#!/usr/bin/env python3
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

"""Seeds security-focused mock traces for policy POC fallback table."""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import hashlib

from google.cloud import bigquery


def _build_schema() -> list[bigquery.SchemaField]:
  object_ref = [
      bigquery.SchemaField("uri", "STRING"),
      bigquery.SchemaField("version", "STRING"),
      bigquery.SchemaField("authorizer", "STRING"),
      bigquery.SchemaField("details", "JSON"),
  ]
  content_part = [
      bigquery.SchemaField("mime_type", "STRING"),
      bigquery.SchemaField("uri", "STRING"),
      bigquery.SchemaField("object_ref", "RECORD", fields=object_ref),
      bigquery.SchemaField("text", "STRING"),
      bigquery.SchemaField("part_index", "INT64"),
      bigquery.SchemaField("part_attributes", "STRING"),
      bigquery.SchemaField("storage_mode", "STRING"),
  ]
  return [
      bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
      bigquery.SchemaField("event_type", "STRING"),
      bigquery.SchemaField("agent", "STRING"),
      bigquery.SchemaField("session_id", "STRING"),
      bigquery.SchemaField("invocation_id", "STRING"),
      bigquery.SchemaField("user_id", "STRING"),
      bigquery.SchemaField("trace_id", "STRING"),
      bigquery.SchemaField("span_id", "STRING"),
      bigquery.SchemaField("parent_span_id", "STRING"),
      bigquery.SchemaField("content", "JSON"),
      bigquery.SchemaField("content_parts", "RECORD", mode="REPEATED", fields=content_part),
      bigquery.SchemaField("attributes", "JSON"),
      bigquery.SchemaField("latency_ms", "JSON"),
      bigquery.SchemaField("status", "STRING"),
      bigquery.SchemaField("error_message", "STRING"),
      bigquery.SchemaField("is_truncated", "BOOL"),
  ]


def _stable_id(*parts: str) -> str:
  text = "|".join(parts)
  return hashlib.md5(text.encode("utf-8")).hexdigest()[:16]


def _build_rows(now: datetime) -> list[dict]:
  s1 = "policy-poc-s1"
  t1 = _stable_id(s1, "trace")
  s2 = "policy-poc-s2"
  t2 = _stable_id(s2, "trace")

  return [
      {
          "timestamp": (now - timedelta(minutes=5)).isoformat(),
          "event_type": "USER_MESSAGE_RECEIVED",
          "agent": "security_poc_agent",
          "session_id": s1,
          "invocation_id": _stable_id(s1, "inv"),
          "user_id": "demo_user_1",
          "trace_id": t1,
          "span_id": _stable_id(s1, "span-user"),
          "parent_span_id": None,
          "content": {"text_summary": "Send this customer update"},
          "content_parts": [],
          "attributes": {"labels": {"poc": "policy"}},
          "latency_ms": None,
          "status": "OK",
          "error_message": None,
          "is_truncated": False,
      },
      {
          "timestamp": (now - timedelta(minutes=4)).isoformat(),
          "event_type": "TOOL_STARTING",
          "agent": "security_poc_agent",
          "session_id": s1,
          "invocation_id": _stable_id(s1, "inv"),
          "user_id": "demo_user_1",
          "trace_id": t1,
          "span_id": _stable_id(s1, "span-tool1"),
          "parent_span_id": _stable_id(s1, "span-user"),
          "content": {
              "tool": "http_request",
              "args": {
                  "url": "https://api.example.com/webhook",
                  "body": "email=bob@example.com ssn=123-45-6789",
              },
          },
          "content_parts": [],
          "attributes": {"labels": {"poc": "policy", "risk": "high"}},
          "latency_ms": {"total_ms": 120},
          "status": "OK",
          "error_message": None,
          "is_truncated": False,
      },
      {
          "timestamp": (now - timedelta(minutes=3)).isoformat(),
          "event_type": "TOOL_COMPLETED",
          "agent": "security_poc_agent",
          "session_id": s1,
          "invocation_id": _stable_id(s1, "inv"),
          "user_id": "demo_user_1",
          "trace_id": t1,
          "span_id": _stable_id(s1, "span-tool1"),
          "parent_span_id": _stable_id(s1, "span-user"),
          "content": {"tool": "http_request", "result": {"status": "200"}},
          "content_parts": [],
          "attributes": {"labels": {"poc": "policy"}},
          "latency_ms": {"total_ms": 350},
          "status": "OK",
          "error_message": None,
          "is_truncated": False,
      },
      {
          "timestamp": (now - timedelta(minutes=2)).isoformat(),
          "event_type": "TOOL_STARTING",
          "agent": "security_poc_agent",
          "session_id": s2,
          "invocation_id": _stable_id(s2, "inv"),
          "user_id": "demo_user_2",
          "trace_id": t2,
          "span_id": _stable_id(s2, "span-tool2"),
          "parent_span_id": None,
          "content": {
              "tool": "search_docs",
              "args": {"query": "policy response template"},
          },
          "content_parts": [],
          "attributes": {"labels": {"poc": "policy", "risk": "low"}},
          "latency_ms": {"total_ms": 80},
          "status": "OK",
          "error_message": None,
          "is_truncated": False,
      },
      {
          "timestamp": (now - timedelta(minutes=1)).isoformat(),
          "event_type": "AGENT_COMPLETED",
          "agent": "security_poc_agent",
          "session_id": s2,
          "invocation_id": _stable_id(s2, "inv"),
          "user_id": "demo_user_2",
          "trace_id": t2,
          "span_id": _stable_id(s2, "span-done"),
          "parent_span_id": None,
          "content": {"response": "Completed without external egress"},
          "content_parts": [],
          "attributes": {"labels": {"poc": "policy"}},
          "latency_ms": {"total_ms": 200},
          "status": "OK",
          "error_message": None,
          "is_truncated": False,
      },
  ]


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--project", default="rag-chatbot-485501")
  parser.add_argument("--dataset", default="agent_trace")
  parser.add_argument("--table", default="agent_events_policy_poc")
  parser.add_argument("--location", default="US")
  args = parser.parse_args()

  client = bigquery.Client(project=args.project, location=args.location)
  table_id = f"{args.project}.{args.dataset}.{args.table}"

  table = bigquery.Table(table_id, schema=_build_schema())
  table.time_partitioning = bigquery.TimePartitioning(field="timestamp")
  table.clustering_fields = ["event_type", "agent", "user_id"]
  table = client.create_table(table, exists_ok=True)
  print(f"Table ready: {table.full_table_id}")

  rows = _build_rows(datetime.now(timezone.utc))
  errors = client.insert_rows_json(table_id, rows)
  if errors:
    raise RuntimeError(f"Failed to insert rows: {errors}")
  print(f"Inserted {len(rows)} mock rows into {table_id}")


if __name__ == "__main__":
  main()
