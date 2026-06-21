"""
services/agent_engine/agents.py
=================================

Node functions for the LangGraph drift-resolution graph.

HOW LANGGRAPH NODES WORK:
--------------------------
Each node is a Python function with signature:
    def node_name(state: DriftAgentState) -> dict:

It receives the FULL current state, does its work, and returns a PARTIAL
dict with only the keys it changed.  LangGraph merges the partial update
into the state before passing it to the next node.

Example:
    def fetch_context(state):
        contract = tools.get_contract(state["vendor_id"])
        return {"contract": contract}   # only the new key, not the full state

THE FOUR NODES IN THIS GRAPH:
------------------------------
1. fetch_context  — pulls the contract + drift history from MCP tools
2. decide_action  — LLM reads context and picks AUTO_HEAL / ESCALATE / IGNORE
3. execute_action — acts on the decision (log event + approve or flag)
4. (routing)      — conditional edge that routes based on state["decision"]

WHY GEMINI (not OpenAI / Claude)?
-----------------------------------
The .env file has GEMINI_API_KEY set — no Anthropic or OpenAI key.
Gemini 2.0 Flash is fast, cheap, and has strong tool-calling support
via LangChain's ChatGoogleGenerativeAI wrapper.
"""

from __future__ import annotations

import json
import os
import structlog

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from state import DriftAgentState
import tools

log = structlog.get_logger(__name__)

# ── LLM setup ─────────────────────────────────────────────────────────────────
# ChatGoogleGenerativeAI reads GOOGLE_API_KEY from environment.
# We rename GEMINI_API_KEY → GOOGLE_API_KEY so LangChain picks it up.
if not os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]

_llm = ChatGoogleGenerativeAI(
    model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
    temperature=0,   # deterministic — we want consistent decisions, not creative ones
)

# ── System prompt ──────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """
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
and the contract is otherwise healthy.  Be conservative — prefer ESCALATE over IGNORE.

Always respond with a JSON object:
{
  "decision": "AUTO_HEAL" | "ESCALATE" | "IGNORE",
  "reasoning": "1-2 sentence explanation"
}
""".strip()


# ── Node 1: fetch_context ──────────────────────────────────────────────────────

def fetch_context(state: DriftAgentState) -> dict:
    """
    Fetch the stored contract and recent drift history for this vendor.

    This runs BEFORE the LLM so the LLM has full context to make a
    well-informed decision.  Doing this in a separate node (not inside
    the decide node) keeps concerns separated and makes each node testable
    independently.
    """
    vendor_id = state["vendor_id"]
    log.info("agent.fetch_context", vendor_id=vendor_id)

    try:
        contract = tools.get_contract(vendor_id)
    except Exception as exc:
        log.warning("agent.fetch_context.contract_not_found", vendor_id=vendor_id, error=str(exc))
        contract = None

    try:
        history = tools.get_drift_history(vendor_id, limit=5)
    except Exception as exc:
        log.warning("agent.fetch_context.history_failed", vendor_id=vendor_id, error=str(exc))
        history = []

    return {"contract": contract, "drift_history": history}


# ── Node 2: decide_action ──────────────────────────────────────────────────────

def decide_action(state: DriftAgentState) -> dict:
    """
    Ask Gemini to assess the drift and decide what to do.

    The LLM receives:
    - The drift event (what changed)
    - The stored contract (what was expected)
    - Recent drift history (has this happened before?)

    It returns a JSON decision: AUTO_HEAL | ESCALATE | IGNORE + reasoning.

    WHY LLM FOR THIS DECISION?
    ---------------------------
    Rule-based logic (if severity == HIGH → escalate) handles clear-cut cases.
    But ambiguous cases — like a column being renamed while retaining its type —
    benefit from semantic reasoning.  The LLM can consider context that rigid
    rules can't, like "this vendor renamed city → city_name which is clearly
    cosmetic and safe".
    """
    drift_event = state["drift_event"]
    contract    = state.get("contract")
    history     = state.get("drift_history", [])

    user_message = f"""
Drift event detected:
{json.dumps(drift_event, indent=2)}

Current contract for vendor '{state["vendor_id"]}':
{json.dumps(contract, indent=2) if contract else "CONTRACT NOT FOUND"}

Recent drift history (last 5 events):
{json.dumps(history, indent=2) if history else "No previous drift events."}

Based on this information, what should we do?
""".strip()

    log.info("agent.decide_action.calling_llm", vendor_id=state["vendor_id"],
             drift_type=drift_event.get("drift_type"), severity=drift_event.get("severity"))

    response = _llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=user_message),
    ])

    # Parse the JSON response from the LLM
    raw = response.content.strip()
    # Strip markdown code fences if Gemini wraps the JSON in ```json ... ```
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
        decision  = parsed.get("decision", "ESCALATE").upper()
        reasoning = parsed.get("reasoning", "No reasoning provided.")
    except json.JSONDecodeError:
        # If the LLM didn't return valid JSON, default to safe escalation
        log.warning("agent.decide_action.parse_failed", raw=raw)
        decision  = "ESCALATE"
        reasoning = f"Failed to parse LLM response — defaulting to ESCALATE. Raw: {raw}"

    log.info("agent.decide_action.decided", vendor_id=state["vendor_id"],
             decision=decision, reasoning=reasoning)

    return {"decision": decision, "reasoning": reasoning}


# ── Node 3a: execute_auto_heal ─────────────────────────────────────────────────

def execute_auto_heal(state: DriftAgentState) -> dict:
    """
    Log the drift event and auto-approve it.

    Auto-heal means: we log the event as AUTO_HEALED and immediately approve
    it.  In a full implementation, this would also trigger a contract version
    bump (1.0.0 → 1.1.0) and update the contract_json in BigQuery.
    For Phase 3, logging + approval is sufficient to demonstrate the flow.
    """
    drift_event = state["drift_event"]
    vendor_id   = state["vendor_id"]
    reasoning   = state.get("reasoning", "")

    log.info("agent.auto_heal", vendor_id=vendor_id)

    # Step 1: log the event to BigQuery (via MCP tool)
    try:
        event_id = tools.log_drift_event(
            vendor_id=vendor_id,
            drift_type=drift_event.get("drift_type", "unknown"),
            field_name=drift_event.get("field_name", "unknown"),
            severity=drift_event.get("severity", "LOW"),
            resolution_status="AUTO_HEALED",
            agent_verdict=reasoning,
            old_type=drift_event.get("old_type", ""),
            new_type=drift_event.get("new_type", ""),
        )
    except Exception as exc:
        log.error("agent.auto_heal.log_failed", error=str(exc))
        return {"action_taken": f"AUTO_HEAL attempted but logging failed: {exc}"}

    # Step 2: approve via the contract-api (via MCP tool)
    try:
        tools.submit_approval(
            event_id=event_id,
            approved=True,
            approved_by="gemini-agent",
            notes=reasoning,
        )
        action = f"AUTO_HEALED: event {event_id} approved by gemini-agent"
    except Exception as exc:
        # Approval endpoint requires billing — log the intent even if it fails
        log.warning("agent.auto_heal.approval_failed", error=str(exc))
        action = f"AUTO_HEAL logged (event {event_id}) — approval skipped: {exc}"

    log.info("agent.auto_heal.complete", vendor_id=vendor_id, event_id=event_id)
    return {"event_id": event_id, "action_taken": action}


# ── Node 3b: execute_escalate ──────────────────────────────────────────────────

def execute_escalate(state: DriftAgentState) -> dict:
    """
    Log the drift event as PENDING_APPROVAL and escalate to a human.

    Escalation means: write the event to BigQuery with status PENDING_APPROVAL.
    A human engineer (or an alert system) would then see the event via
    GET /api/v1/drift-log and call POST /api/v1/approve/{event_id} manually.
    """
    drift_event = state["drift_event"]
    vendor_id   = state["vendor_id"]
    reasoning   = state.get("reasoning", "")

    log.info("agent.escalate", vendor_id=vendor_id)

    try:
        event_id = tools.log_drift_event(
            vendor_id=vendor_id,
            drift_type=drift_event.get("drift_type", "unknown"),
            field_name=drift_event.get("field_name", "unknown"),
            severity=drift_event.get("severity", "HIGH"),
            resolution_status="PENDING_APPROVAL",
            agent_verdict=f"ESCALATED: {reasoning}",
            old_type=drift_event.get("old_type", ""),
            new_type=drift_event.get("new_type", ""),
        )
        action = (
            f"ESCALATED: event {event_id} logged as PENDING_APPROVAL. "
            f"Human review required. Call POST /api/v1/approve/{event_id}"
        )
    except Exception as exc:
        log.error("agent.escalate.log_failed", error=str(exc))
        event_id = None
        action = f"ESCALATE attempted but logging failed: {exc}"

    log.info("agent.escalate.complete", vendor_id=vendor_id, event_id=event_id)
    return {"event_id": event_id, "action_taken": action}


# ── Routing function ───────────────────────────────────────────────────────────

def route_decision(state: DriftAgentState) -> str:
    """
    Conditional edge: routes the graph to the right action node based on
    the LLM's decision stored in state["decision"].

    Returns the name of the next node to execute.
    LangGraph uses this return value to pick the next edge.
    """
    decision = state.get("decision", "ESCALATE")
    if decision == "AUTO_HEAL":
        return "auto_heal"
    elif decision == "IGNORE":
        return "escalate"   # treat IGNORE same as ESCALATE for safety
    else:
        return "escalate"
