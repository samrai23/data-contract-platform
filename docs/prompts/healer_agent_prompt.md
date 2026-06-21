# Healer Agent

## Current implementation status

The Healer is **not an LLM node** in the current implementation. It is a deterministic Python function (`execute_auto_heal` in `services/agent_engine/agents.py`) that runs after the Analyst (Gemini) decides AUTO_HEAL.

There is no prompt — the healer follows a fixed sequence of MCP tool calls.

---

## What the healer does (current — Phase 3/4)

```python
# Node: execute_auto_heal
# File: services/agent_engine/agents.py

def execute_auto_heal(state: DriftAgentState) -> dict:
    # Step 1: Log the drift event to BigQuery with status AUTO_HEALED
    event_id = tools.log_drift_event(
        vendor_id=vendor_id,
        drift_type=...,
        field_name=...,
        severity=...,
        resolution_status="AUTO_HEALED",
        agent_verdict=reasoning,
    )

    # Step 2: Call auto_approve to mark it resolved
    tools.submit_approval(
        event_id=event_id,
        approved=True,
        approved_by="gemini-agent",
        notes=reasoning,
    )
```

The healer does not modify the contract in BigQuery yet — that is Phase 5 work.

---

## Why the healer is deterministic (not LLM)

The Analyst makes the judgment call — AUTO_HEAL or ESCALATE. Once that decision is made, execution is mechanical:

1. Write a record to BigQuery
2. Mark it approved

There is no ambiguity to reason about. Using an LLM for a deterministic two-step operation would add latency and cost with zero benefit. LLMs belong in the decision layer, not the execution layer.

**Rule:** Use an LLM where the task requires judgment. Use deterministic code where the steps are fixed.

---

## Planned Phase 5 — healer with contract update

When the contract writer (see `contract_writer_prompt.md`) is implemented, the healer will expand to:

```
execute_auto_heal:
  1. Call contract_writer LLM → produces updated contract JSON
  2. Validate output through NeMo Guardrails (no DROP/DELETE)
  3. POST updated contract to contract-api → BigQuery (version bump)
  4. Log drift event as AUTO_HEALED
  5. Auto-approve
  6. Publish to heal-actions topic → triggers dbt run
```

Steps 1-2 would involve an LLM. Steps 3-6 remain deterministic.

---

## Escalate path (for reference)

When the Analyst decides ESCALATE, `execute_escalate` runs instead:

```python
def execute_escalate(state: DriftAgentState) -> dict:
    # Log event with PENDING_APPROVAL status
    event_id = tools.log_drift_event(
        ...
        resolution_status="PENDING_APPROVAL",
        agent_verdict=f"ESCALATED: {reasoning}",
    )
    # Returns event_id for the human to use in POST /api/v1/approve/{event_id}
```

No LLM involved. The human engineer receives the event_id via the drift log API and approves via Swagger.

---

## NeMo Guardrails (current config)

Guardrails policy is defined in `guardrails/policies/no_destructive_ops.co`.

```colang
define bot refuse destructive schema change
  "I cannot apply a schema change that drops or truncates data."

define flow block destructive operations
  user request schema change
  if "DROP" in request or "TRUNCATE" in request or "DELETE" in request
    bot refuse destructive schema change
```

Currently these rails are configured but not wired into the healer execution path — that integration is Phase 5.
