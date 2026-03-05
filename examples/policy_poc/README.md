# Policy POC Assets

This folder contains one-time POC assets for `OPAPolicyEvaluator`:

- `cloudrun_opa_service/`: Cloud Run endpoint for BigQuery Remote Function calls.
- `sql/`: SQL templates for remote function bootstrap and policy evaluation.
- `seed_mock_traces_to_bq.py`: Inserts security-focused fallback rows into `agent_events_policy_poc`.
- `policy_remote_function_poc.ipynb`: End-to-end notebook flow.
- `scripts/`: Helper scripts for Cloud Run deploy and remote function creation.

## Budget Guardrails

Default controls in scripts/notebook:

- query cap: `MAX_BYTES_BILLED_GB=50` (about `$0.25` max per query)
- step cap gate: `MAX_STEP_BUDGET_USD=5`
- Cloud Run deploy keeps `min-instances=0` and `max-instances=2`

Adjust only if you explicitly want higher spend.
