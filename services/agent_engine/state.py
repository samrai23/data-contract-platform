"""
services/agent_engine/state.py
================================

LangGraph state definition for the drift-resolution agent.

WHAT IS LANGGRAPH STATE?
-------------------------
LangGraph is a framework for building stateful AI agents as graphs.
A "graph" has nodes (steps) and edges (transitions between steps).
Each node receives the current state, does some work, and returns
an updated state.  The state flows through the graph like water
through pipes.

Think of the state as a shared blackboard that every node can read
and write.  When the agent decides something (e.g. "this is AUTO_HEAL"),
it writes that decision to the state.  The next node reads it and acts.

TypedDict vs dataclass:
  LangGraph requires state to be a TypedDict so it can merge partial
  updates efficiently.  Each node only returns the keys it changed —
  LangGraph merges that partial dict into the full state automatically.
  A dataclass would require returning the complete object every time.
"""

from __future__ import annotations
from typing import TypedDict, Optional


class DriftAgentState(TypedDict):
    """
    The shared state that flows through every node in the drift-resolution graph.

    Fields are populated progressively as the graph executes:
      Step 1 (receive_event)  → fills: vendor_id, drift_event
      Step 2 (fetch_context)  → fills: contract, drift_history
      Step 3 (decide_action)  → fills: decision, reasoning
      Step 4 (execute_action) → fills: event_id, action_taken
    """

    # ── Input (set once at graph entry, never changed) ─────────────────────────
    vendor_id:   str
    drift_event: dict   # Raw dict from Pub/Sub message payload

    # ── Context (fetched from MCP tools in step 2) ─────────────────────────────
    contract:      Optional[dict]       # Full contract from contract-api
    drift_history: Optional[list[dict]] # Recent drift events for this vendor

    # ── Decision (set by the LLM in step 3) ────────────────────────────────────
    decision:  Optional[str]    # "AUTO_HEAL" | "ESCALATE" | "IGNORE"
    reasoning: Optional[str]    # LLM's explanation — logged for auditability

    # ── Action result (set in step 4 after taking action) ──────────────────────
    event_id:     Optional[str]  # UUID assigned by log_drift_event MCP tool
    action_taken: Optional[str]  # What actually happened (for the log)
