# Contract Writer Prompt

## Current implementation status

The Contract Writer agent described in the original architecture (README Phase 4) has not been implemented as a separate LLM call in the current codebase.

In the current implementation, the `decide_action` node (Gemini) makes the AUTO_HEAL / ESCALATE / IGNORE routing decision. The `execute_auto_heal` node then logs the event and calls `auto_approve` — but it does **not** yet generate an updated contract or bump the version.

Contract writing (version 1.0.0 → 1.1.0, updating `contract_json` in BigQuery) is planned for Phase 5.

---

## Planned prompt — Phase 5

When implemented, the contract writer will receive the original contract and the approved drift event, and produce an updated contract JSON.

### System prompt (planned)

```
You are a Data Contract Writer for a data engineering platform.
Your job is to produce an updated data contract after a schema drift has been approved.

Rules:
1. Increment the patch version for backward-compatible changes (1.0.0 → 1.0.1):
   - Adding a nullable column
   - Widening a type (INT → BIGINT, INT → STRING)

2. Increment the minor version for new fields (1.0.0 → 1.1.0):
   - Adding a required column

3. Increment the major version for breaking changes (1.0.0 → 2.0.0):
   - Removing a column
   - Narrowing a type (STRING → INT)
   - Renaming a column

Output a valid JSON object matching this schema exactly:
{
  "vendor_id": "string",
  "version": "MAJOR.MINOR.PATCH",
  "fields": [
    {
      "name": "field_name_in_snake_case",
      "field_type": "STRING | INTEGER | FLOAT | BOOLEAN | TIMESTAMP",
      "nullable": true | false
    }
  ]
}

Do not include any explanation outside the JSON object.
```

### User message template (planned)

```
Current contract:
{current_contract_as_json}

Approved drift event:
{drift_event_as_json}

Agent reasoning for approval:
{reasoning}

Generate the updated contract.
```

---

## Why a separate LLM call for contract writing

The analyst (decide_action) and the contract writer serve different cognitive tasks:

- **Analyst:** binary classification — is this safe? → AUTO_HEAL or ESCALATE
- **Contract writer:** structured generation — produce a valid JSON contract with the correct version bump

Mixing these in one prompt risks the analyst reasoning bleeding into the output format, or the model trying to write a contract when it should be classifying. Two focused prompts are more reliable than one combined prompt.

---

## NeMo Guardrails integration (planned)

The contract writer output will pass through NeMo Guardrails (`guardrails/policies/no_destructive_ops.co`) before being applied:

```colang
define bot refuse destructive contract
  "I can't generate a contract that removes required fields or downgrades types."

define flow
  user ask contract writer
  $output = execute contract_writer_llm
  if "DROP" in $output or "DELETE" in $output
    bot refuse destructive contract
  else
    bot send $output
```

This ensures the LLM cannot produce a contract that silently drops fields — even if the drift event was misclassified.
