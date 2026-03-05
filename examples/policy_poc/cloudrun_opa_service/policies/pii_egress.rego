package sdk.policy

default decision := {
  "action": "allow",
  "severity": "low",
  "reason_code": "no_match",
  "reason_text": "No policy match",
  "confidence": 0.8,
}

pii_pattern := `(?i)([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|\b\d{3}-\d{2}-\d{4}\b)`
risky_tools := {"http_request", "webhook_post", "slack_send"}

decision := {
  "action": "deny",
  "severity": "high",
  "reason_code": "pii_egress",
  "reason_text": "Potential PII egress via external tool",
  "confidence": 0.95,
} if {
  input.event_type == "TOOL_STARTING"
  risky_tools[input.tool_name]
  regex.match(pii_pattern, json.marshal(input))
}
