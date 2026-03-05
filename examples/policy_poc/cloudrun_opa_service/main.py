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

"""Cloud Run service for BigQuery Remote Function OPA evaluation.

Request contract (BigQuery remote function):
{
  "calls": [["{...payload_json...}"], ...]
}

Response contract:
{
  "replies": ["{...decision_json...}", ...]
}
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Any

from flask import Flask
from flask import jsonify
from flask import request

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

_POLICY_BUNDLE_PATH = os.getenv("OPA_POLICY_BUNDLE_PATH", "/app/policies")
_POLICY_QUERY = os.getenv("OPA_POLICY_QUERY", "data.sdk.policy.decision")

_DEFAULT_ALLOW = {
    "action": "allow",
    "severity": "low",
    "reason_code": "no_match",
    "reason_text": "No policy match",
    "confidence": 0.8,
}


def _safe_payload(payload: Any) -> str:
  if payload is None:
    return "{}"
  if isinstance(payload, str):
    return payload
  return json.dumps(payload)


def _evaluate_with_opa(payload_json: str) -> dict[str, Any]:
  """Evaluates payload JSON against Rego policy using OPA CLI."""
  cmd = [
      "opa",
      "eval",
      "--format",
      "json",
      "--data",
      _POLICY_BUNDLE_PATH,
      "--stdin-input",
      _POLICY_QUERY,
  ]
  proc = subprocess.run(
      cmd,
      input=payload_json.encode("utf-8"),
      capture_output=True,
      check=False,
  )
  if proc.returncode != 0:
    logger.warning("OPA eval failed: %s", proc.stderr.decode("utf-8"))
    return dict(_DEFAULT_ALLOW)

  try:
    opa_out = json.loads(proc.stdout.decode("utf-8"))
  except json.JSONDecodeError:
    logger.warning("OPA output is not JSON")
    return dict(_DEFAULT_ALLOW)

  result_list = opa_out.get("result") or []
  if not result_list:
    return dict(_DEFAULT_ALLOW)

  expressions = result_list[0].get("expressions") or []
  if not expressions:
    return dict(_DEFAULT_ALLOW)

  value = expressions[0].get("value")
  if not isinstance(value, dict):
    return dict(_DEFAULT_ALLOW)

  merged = dict(_DEFAULT_ALLOW)
  merged.update(value)
  return merged


@app.get("/healthz")
def healthz():
  return jsonify({"status": "ok"})


@app.post("/")
def evaluate():
  """Evaluates batched calls from BigQuery Remote Function."""
  body = request.get_json(silent=True) or {}
  calls = body.get("calls") or []

  replies: list[str] = []
  for call in calls:
    if isinstance(call, list) and call:
      payload = call[0]
    else:
      payload = call
    payload_json = _safe_payload(payload)
    decision = _evaluate_with_opa(payload_json)
    replies.append(json.dumps(decision, separators=(",", ":")))

  return jsonify({"replies": replies})


if __name__ == "__main__":
  app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
