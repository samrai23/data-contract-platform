"""
services/agent_engine/tools.py
================================

Direct HTTP wrappers that the agent engine uses to call other services.

WHY NOT USE THE MCP PROTOCOL FOR THESE CALLS?
----------------------------------------------
MCP (Model Context Protocol) is designed for LLM tool-calling — the LLM
decides which tool to call and the protocol handles schema validation and
routing.  But some agent steps are deterministic in code:
  "Always log the event before taking action"
  "Always fetch the contract for context"

For those steps we don't need the LLM involved — we just call the
contract-api and BigQuery directly over HTTP/client.  This avoids an
extra MCP round-trip and keeps the code simple.

The MCP server is still useful: the LangGraph agent is registered as an
MCP client so the LLM can call tools by name during its reasoning step.

SERVICE URLS:
  Inside Docker:   CONTRACT_API_URL = http://contract-api:8000
  Locally:         CONTRACT_API_URL = http://localhost:8000 (default)
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import httpx
import structlog
from google.cloud import bigquery

log = structlog.get_logger(__name__)

_API_BASE = os.getenv("CONTRACT_API_URL", "http://localhost:8000") + "/api/v1"
_PROJECT  = os.getenv("GCP_PROJECT_ID", "data-contract-platform")
_DATASET  = os.getenv("BIGQUERY_DATASET", "data_contracts")

# Load .env from project root and resolve credentials path (same logic as bq_tools.py)
_PROJECT_ROOT = Path(__file__).parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass
_creds_val = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
if _creds_val and not Path(_creds_val).is_absolute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_PROJECT_ROOT / _creds_val)

_http = httpx.Client(timeout=30.0)


@lru_cache(maxsize=1)
def _bq() -> bigquery.Client:
    return bigquery.Client(project=_PROJECT)


# ── Contract API calls ─────────────────────────────────────────────────────────

def get_contract(vendor_id: str) -> dict:
    """Fetch the active contract for a vendor from the contract-api."""
    log.info("tools.get_contract", vendor_id=vendor_id)
    resp = _http.get(f"{_API_BASE}/contracts/{vendor_id}")
    if resp.status_code == 404:
        raise ValueError(f"No active contract found for vendor '{vendor_id}'.")
    resp.raise_for_status()
    return resp.json()


def get_drift_history(vendor_id: str, limit: int = 10) -> list[dict]:
    """Fetch recent drift events for a vendor from the drift-log API."""
    log.info("tools.get_drift_history", vendor_id=vendor_id)
    resp = _http.get(f"{_API_BASE}/drift-log", params={"vendor_id": vendor_id, "limit": limit})
    resp.raise_for_status()
    return resp.json()


def heal_contract(
    vendor_id: str,
    drift_type: str,
    field_name: str,
    old_type: str = "",
    new_type: str = "",
) -> dict:
    """
    Apply an auto-healed drift to the stored contract via the contract-api's
    heal endpoint — bumps the semantic version and updates contract_json in
    BigQuery. Called by execute_auto_heal() AFTER logging + approval, so a
    contract-update failure never blocks the audit trail from being written.
    """
    log.info("tools.heal_contract", vendor_id=vendor_id, drift_type=drift_type, field_name=field_name)
    resp = _http.patch(
        f"{_API_BASE}/contracts/{vendor_id}/heal",
        json={
            "drift_type": drift_type,
            "field_name": field_name,
            "old_type": old_type or None,
            "new_type": new_type or None,
        },
    )
    if resp.status_code == 404:
        raise ValueError(f"Cannot heal contract for vendor '{vendor_id}': {resp.json().get('detail', 'not found')}")
    resp.raise_for_status()
    return resp.json()


def submit_approval(event_id: str, approved: bool, approved_by: str, notes: str) -> dict:
    """Submit an approval/rejection for a drift event via the contract-api."""
    log.info("tools.submit_approval", event_id=event_id, approved=approved)
    resp = _http.post(
        f"{_API_BASE}/approve/{event_id}",
        json={"approved": approved, "approved_by": approved_by, "notes": notes},
    )
    if resp.status_code == 404:
        raise ValueError(f"Drift event '{event_id}' not found.")
    resp.raise_for_status()
    return resp.json()


# ── BigQuery direct write ──────────────────────────────────────────────────────

def log_drift_event(
    vendor_id: str,
    drift_type: str,
    field_name: str,
    severity: str,
    resolution_status: str,
    agent_verdict: str,
    old_type: str = "",
    new_type: str = "",
) -> str:
    """
    Write a drift event directly to BigQuery and return the generated event_id.

    Uses batch load (load_table_from_json) — free-tier compatible.
    The contract-api has no write endpoint for drift events because the agent
    IS the writer; keeping this direct avoids a circular dependency.
    """
    event_id = str(uuid.uuid4())
    now      = datetime.now(timezone.utc).isoformat()

    row = {
        "event_id":          event_id,
        "vendor_id":         vendor_id,
        "drift_type":        drift_type,
        "detected_at":       now,
        "field_name":        field_name,
        "severity":          severity,
        "resolution_status": resolution_status,
        "agent_verdict":     agent_verdict,
    }
    if old_type:
        row["old_type"] = old_type
    if new_type:
        row["new_type"] = new_type

    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = _bq().load_table_from_json(
        [row],
        f"{_PROJECT}.{_DATASET}.drift_events",
        job_config=load_config,
    )
    job.result()

    log.info("tools.drift_event_logged", event_id=event_id, vendor_id=vendor_id,
             drift_type=drift_type, resolution_status=resolution_status)
    return event_id
