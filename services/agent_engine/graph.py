"""
services/agent_engine/graph.py
================================

Builds the LangGraph StateGraph and the Pub/Sub subscriber loop.

WHAT IS A LANGGRAPH STATEGRAPH?
---------------------------------
A StateGraph is a directed graph where:
  - Nodes  = Python functions (the agent's steps)
  - Edges  = transitions between steps (fixed or conditional)
  - State  = a TypedDict that flows through every node

Fixed edge:       A → B  (always go from A to B)
Conditional edge: A → B or C depending on state value

THE GRAPH FOR THIS PROJECT:
----------------------------

  START
    ↓
  fetch_context  (get contract + history from MCP server)
    ↓
  decide_action  (LLM reads context and picks AUTO_HEAL / ESCALATE)
    ↓
  [conditional edge on state["decision"]]
    ├── AUTO_HEAL → auto_heal  (log + approve automatically)
    └── ESCALATE  → escalate   (log as PENDING_APPROVAL)
    ↓
  END

COMPILE ONCE, INVOKE MANY TIMES:
---------------------------------
The graph is compiled once at module load time.  Each drift event from
Pub/Sub creates a fresh state dict and calls graph.invoke(state).
The compiled graph is thread-safe and can process multiple events
concurrently (though we keep it single-threaded for simplicity here).

PUB/SUB SUBSCRIBER LOOP:
-------------------------
At the bottom of this file is run_agent_loop() — the main entry point
for the Docker container.  It:
  1. Subscribes to the drift-events Pub/Sub topic
  2. For each message, extracts the drift event payload
  3. Runs the LangGraph graph with that payload
  4. Acks the message (tells Pub/Sub "I processed this, don't re-deliver")
"""

from __future__ import annotations

import json
import os
import structlog

from langgraph.graph import StateGraph, START, END

from state import DriftAgentState
from agents import (
    fetch_context,
    decide_action,
    execute_auto_heal,
    execute_escalate,
    route_decision,
)

log = structlog.get_logger(__name__)


# ── Build the graph ────────────────────────────────────────────────────────────

def build_graph():
    """
    Construct and compile the drift-resolution StateGraph.

    compile() validates the graph structure (no dangling nodes, no cycles
    without exits) and returns an executable object.  Calling .invoke()
    on the compiled graph runs the full node sequence for one input.
    """
    g = StateGraph(DriftAgentState)

    # Register nodes — each is a function defined in agents.py
    g.add_node("fetch_context", fetch_context)
    g.add_node("decide_action", decide_action)
    g.add_node("auto_heal",     execute_auto_heal)
    g.add_node("escalate",      execute_escalate)

    # Fixed edges — always follow this path
    g.add_edge(START,           "fetch_context")
    g.add_edge("fetch_context", "decide_action")

    # Conditional edge — route_decision() returns the name of the next node
    g.add_conditional_edges(
        "decide_action",
        route_decision,
        {
            "auto_heal": "auto_heal",
            "escalate":  "escalate",
        },
    )

    # Both action nodes lead to END
    g.add_edge("auto_heal", END)
    g.add_edge("escalate",  END)

    return g.compile()


# Compile once at module load time — reused for every drift event
drift_graph = build_graph()


# ── Process one drift event ────────────────────────────────────────────────────

def process_drift_event(message_data: dict) -> dict:
    """
    Run the drift-resolution graph for one Pub/Sub message.

    Parameters
    ----------
    message_data : The decoded JSON payload from the Pub/Sub message.
                   Expected keys: vendor_id, drift_type, field_name,
                   old_type, new_type, severity, is_safe_to_auto_heal.

    Returns the final state after all nodes have run.
    """
    vendor_id = message_data.get("vendor_id", "unknown")

    log.info("graph.process_start", vendor_id=vendor_id,
             drift_type=message_data.get("drift_type"),
             severity=message_data.get("severity"))

    initial_state: DriftAgentState = {
        "vendor_id":    vendor_id,
        "drift_event":  message_data,
        "contract":     None,
        "drift_history": None,
        "decision":     None,
        "reasoning":    None,
        "event_id":     None,
        "action_taken": None,
    }

    final_state = drift_graph.invoke(initial_state)

    log.info(
        "graph.process_complete",
        vendor_id=vendor_id,
        decision=final_state.get("decision"),
        action=final_state.get("action_taken"),
    )

    return final_state


# ── Pub/Sub subscriber loop ────────────────────────────────────────────────────

def run_agent_loop() -> None:
    """
    Subscribe to the drift-events Pub/Sub topic and process each message
    by running it through the LangGraph graph.

    This is a blocking loop — it runs forever until the process is killed.
    Docker sends SIGTERM before SIGKILL, so we handle it gracefully.

    WHY SYNCHRONOUS PULL (not streaming)?
    ---------------------------------------
    Each drift event triggers an LLM call (~1-3 seconds).  Synchronous pull
    processes one message at a time — simple, predictable, no concurrency bugs.
    Streaming async pull would fire callbacks concurrently, requiring thread-safe
    state management with no real throughput benefit at this event rate.
    """
    # Set PUBSUB_EMULATOR_HOST BEFORE importing pubsub_v1
    # (same lesson as setup_pubsub_emulator.py — the channel is built at import time)
    emulator = os.getenv("PUBSUB_EMULATOR_HOST")
    if emulator:
        os.environ["PUBSUB_EMULATOR_HOST"] = emulator

    from google.cloud import pubsub_v1

    project      = os.getenv("GCP_PROJECT_ID", "local-dev")
    subscription = os.getenv("PUBSUB_DRIFT_SUBSCRIPTION", "drift-events-sub")
    sub_path     = f"projects/{project}/subscriptions/{subscription}"

    subscriber = pubsub_v1.SubscriberClient()

    log.info("agent_loop.starting", subscription=sub_path,
             emulator=emulator or "real GCP")

    while True:
        try:
            response = subscriber.pull(
                request={"subscription": sub_path, "max_messages": 1},
                timeout=10.0,
            )
        except Exception as exc:
            log.warning("agent_loop.pull_failed", error=str(exc))
            continue

        for msg in response.received_messages:
            try:
                data = json.loads(msg.message.data.decode("utf-8"))
                process_drift_event(data)
            except Exception:
                log.exception("agent_loop.process_failed",
                              message_id=msg.message.message_id)
            finally:
                # Always ack — even on failure — to prevent infinite re-delivery
                # of malformed messages that would crash the loop forever.
                subscriber.acknowledge(
                    request={"subscription": sub_path,
                             "ack_ids": [msg.ack_id]}
                )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
    )
    run_agent_loop()
