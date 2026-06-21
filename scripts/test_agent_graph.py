"""
scripts/test_agent_graph.py
============================

Tests the LangGraph drift-resolution agent with fake drift events.
No Pub/Sub or Docker needed for this test — just:
  1. MCP server running:    cd services/mcp_server && python server.py
  2. contract-api running:  docker compose up -d contract-api

WHAT THIS TESTS:
  - fetch_context node  : can the agent fetch the cars24 contract from MCP server?
  - decide_action node  : does Gemini read the context and return a valid JSON decision?
  - execute node        : does auto_heal or escalate run and log to BigQuery?

TWO SCENARIOS:
  Scenario A — LOW severity, safe type widening (INT → BIGINT)
    Expected: AUTO_HEAL (no data loss, standard BigQuery widening)

  Scenario B — HIGH severity, narrowing type change (STRING → INT)
    Expected: ESCALATE (existing STRING values that aren't numbers would break)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# ── Load .env before anything else ────────────────────────────────────────────
# agents.py maps GEMINI_API_KEY → GOOGLE_API_KEY, but .env must be loaded first
# so the key is in the environment before agents.py is imported.
_ROOT = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

# Resolve relative credentials path against project root (same as bq_tools.py)
_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
if _creds and not Path(_creds).is_absolute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_ROOT / _creds)

# ── Add agent_engine to import path ───────────────────────────────────────────
# The graph, agents, tools, state modules live in services/agent_engine/.
# Python doesn't know about them unless we add that directory to sys.path.
sys.path.insert(0, str(_ROOT / "services" / "agent_engine"))

# ── Now safe to import agent modules ──────────────────────────────────────────
from graph import process_drift_event


# ── Test runner ───────────────────────────────────────────────────────────────

def run_scenario(label: str, drift_event: dict) -> None:
    print(f"\n{'=' * 60}")
    print(f"SCENARIO: {label}")
    print(f"{'=' * 60}")
    print(f"Input drift event:")
    for k, v in drift_event.items():
        print(f"  {k:25s}: {v}")
    print()

    result = process_drift_event(drift_event)

    print(f"\n--- RESULT ---")
    print(f"  Decision     : {result.get('decision')}")
    print(f"  Reasoning    : {result.get('reasoning')}")
    print(f"  Event ID     : {result.get('event_id') or '(not logged — BQ write may need billing)'}")
    print(f"  Action taken : {result.get('action_taken')}")

    decision = result.get("decision")
    if decision in ("AUTO_HEAL", "ESCALATE", "IGNORE"):
        print(f"\n  ✅  Graph completed successfully — decision: {decision}")
    else:
        print(f"\n  ⚠️   Unexpected decision value: {decision!r}")


def _check_env() -> None:
    """Print a diagnostic so you can verify the right values are loaded."""
    print("\n── Environment check ─────────────────────────────────────")

    # .env file
    env_path = _ROOT / ".env"
    print(f"  .env file exists : {env_path.exists()} ({env_path})")

    # Gemini key — show first 8 and last 4 chars so you can recognise it
    # without exposing the full key
    raw_key = os.getenv("GEMINI_API_KEY", "")
    google_key = os.getenv("GOOGLE_API_KEY", "")
    if raw_key:
        masked = raw_key[:8] + "..." + raw_key[-4:] if len(raw_key) > 12 else "(too short)"
        print(f"  GEMINI_API_KEY   : {masked}  (length={len(raw_key)})")
    else:
        print("  GEMINI_API_KEY   : ❌  NOT FOUND — check .env")

    if google_key:
        masked = google_key[:8] + "..." + google_key[-4:]
        print(f"  GOOGLE_API_KEY   : {masked}")
    else:
        print("  GOOGLE_API_KEY   : (not set — will be mapped from GEMINI_API_KEY in agents.py)")

    print(f"  GCP_PROJECT_ID   : {os.getenv('GCP_PROJECT_ID', '(not set)')}")
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "(not set)")
    print(f"  GCP CREDENTIALS  : {creds}")
    print("──────────────────────────────────────────────────────────\n")

    if not raw_key:
        print("❌  GEMINI_API_KEY is missing. Add it to .env:")
        print("    GEMINI_API_KEY=AIzaSy...your-actual-key")
        sys.exit(1)


def main() -> None:
    print("LangGraph Agent — Local Test")
    print("Prerequisites: MCP server on :8001  |  contract-api on :8000")
    _check_env()

    # ── Scenario A: LOW severity — safe widening type change ──────────────────
    # cars24 contract says user_id is INT.
    # The live data now sends BIGINT (larger integer range).
    # INT → BIGINT is a widening change — every INT value fits in BIGINT.
    # No data loss possible. Expected decision: AUTO_HEAL
    run_scenario(
        label="LOW severity — INT → BIGINT (safe widening, expect AUTO_HEAL)",
        drift_event={
            "vendor_id":           "cars24",
            "drift_type":          "type_change",
            "field_name":          "user_id",
            "old_type":            "INT",
            "new_type":            "BIGINT",
            "severity":            "LOW",
            "is_safe_to_auto_heal": True,
            "detected_at":         "2026-06-14T04:00:00Z",
        },
    )

    # ── Scenario B: HIGH severity — narrowing type change ─────────────────────
    # cars24 used to send city as STRING.
    # The live data now sends it as INT.
    # STRING → INT is narrowing — "Delhi", "Noida" can't be cast to INT.
    # Pipeline would crash trying to load these values. Expected: ESCALATE
    run_scenario(
        label="HIGH severity — STRING → INT (narrowing, expect ESCALATE)",
        drift_event={
            "vendor_id":           "cars24",
            "drift_type":          "type_change",
            "field_name":          "city",
            "old_type":            "STRING",
            "new_type":            "INT",
            "severity":            "HIGH",
            "is_safe_to_auto_heal": False,
            "detected_at":         "2026-06-14T04:01:00Z",
        },
    )

    print(f"\n{'=' * 60}")
    print("Both scenarios complete.")
    print("Check BigQuery drift_events table to see logged events:")
    print("  https://console.cloud.google.com/bigquery?project=data-contract-platform")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
