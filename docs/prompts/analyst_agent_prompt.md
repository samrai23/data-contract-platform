# Analyst Agent Prompt

The analyst decision is made by the `decide_action` node in `services/agent_engine/agents.py`. It uses Gemini 2.5 Flash via `langchain-google-genai`.

---

## System Prompt

This is the fixed instruction set sent with every request. It defines the agent's role and the exact decision rules.

```
You are a Data Contract Agent for a data engineering platform.
Your job is to assess schema drift events and decide how to handle them.

AUTO_HEAL when ALL of the following are true:
  - severity is LOW or the drift is a safe widening type change (INT→BIGINT, INT→STRING)
  - is_safe_to_auto_heal is true in the drift event
  - The vendor has not repeatedly drifted on this same field (check history)

ESCALATE when ANY of the following are true:
  - severity is HIGH (dropped column, narrowing type change like STRING→INT)
  - is_safe_to_auto_heal is false
  - Column was renamed (business logic downstream likely breaks)

IGNORE only if the drift event is clearly a data quality issue (single bad record)
and the contract is otherwise healthy. Be conservative — prefer ESCALATE over IGNORE.

Always respond with a JSON object:
{
  "decision": "AUTO_HEAL" | "ESCALATE" | "IGNORE",
  "reasoning": "1-2 sentence explanation"
}
```

**File:** `services/agent_engine/agents.py` → `_SYSTEM_PROMPT`

---

## User Message Template

This is the dynamic part — filled with the actual drift event, contract, and history at runtime.

```
Drift event detected:
{drift_event_as_json}

Current contract for vendor '{vendor_id}':
{contract_as_json}

Recent drift history (last 5 events):
{history_as_json}

Based on this information, what should we do?
```

**File:** `services/agent_engine/agents.py` → `decide_action()` → `user_message`

---

## Example — Scenario A (AUTO_HEAL)

**Drift event input:**
```json
{
  "vendor_id": "cars24",
  "drift_type": "type_change",
  "field_name": "user_id",
  "old_type": "INTEGER",
  "new_type": "STRING",
  "severity": "LOW",
  "is_safe_to_auto_heal": true
}
```

**Drift history:** No previous events

**Gemini response:**
```json
{
  "decision": "AUTO_HEAL",
  "reasoning": "The type change from INTEGER to STRING on user_id is a safe widening cast with low severity and is marked safe to auto-heal. No drift history suggests this is an isolated change."
}
```

---

## Example — Scenario B (ESCALATE)

**Drift event input:**
```json
{
  "vendor_id": "cars24",
  "drift_type": "type_change",
  "field_name": "city",
  "old_type": "STRING",
  "new_type": "INTEGER",
  "severity": "HIGH",
  "is_safe_to_auto_heal": false
}
```

**Gemini response:**
```json
{
  "decision": "ESCALATE",
  "reasoning": "STRING to INTEGER is a narrowing type cast that risks data loss and is marked unsafe to auto-heal. High severity drift on a categorical field requires human review."
}
```

---

## Example — Repeated drift (ESCALATE despite LOW severity)

**Drift event:** INT → BIGINT on `user_id`, severity=LOW, is_safe=True  
**Drift history:** Previous event: INT → BIGINT on `user_id` (same field, same drift, 3 days ago)

**Gemini response:**
```json
{
  "decision": "ESCALATE",
  "reasoning": "The drift is a safe widening type change with low severity and is marked safe to auto-heal. However, the vendor has previously drifted on the 'user_id' field (INT to BIGINT), which constitutes a repeated drift on the same field, preventing auto-healing and warranting human review."
}
```

This is the LLM's key advantage over rule-based logic — it reads the history and escalates a nominally safe drift because the pattern is concerning.

---

## LLM configuration

```python
_llm = ChatGoogleGenerativeAI(
    model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
    temperature=0,   # deterministic — same input always produces same decision
)
```

**`temperature=0`:** Routing decisions must be consistent. A non-zero temperature means the same INT→STRING drift could produce AUTO_HEAL on one run and ESCALATE on another — unacceptable for a production pipeline.

**Model selection:** Set via `GEMINI_MODEL` in `.env`. Use `python scripts/list_gemini_models.py` to check which models your API key can actually call — the GCP Quota Console is not a reliable source.

---

## Response parsing and fallback

The LLM sometimes wraps JSON in markdown code fences (` ```json ... ``` `). The code strips these before parsing:

```python
if raw.startswith("```"):
    raw = raw.split("```")[1]
    if raw.startswith("json"):
        raw = raw[4:]
```

**Fallback on parse failure:** If `json.loads()` raises `JSONDecodeError`, the code defaults to `ESCALATE`. Never AUTO_HEAL on an unparseable response — the safe action is always to escalate for human review.
