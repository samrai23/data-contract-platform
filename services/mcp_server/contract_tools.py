"""
services/mcp_server/contract_tools.py
======================================

MCP tools that talk to the contract-api over HTTP.

WHY CALL THE API INSTEAD OF BIGQUERY DIRECTLY?
------------------------------------------------
The contract-api already owns the contract read/write logic — validation,
duplicate checks, version management.  Calling it here means the MCP server
never needs its own BigQuery credentials for contract operations.  It also
means every write goes through the same validation path, whether the caller
is a human hitting the Swagger UI or the AI agent calling an MCP tool.

WHAT IS AN MCP TOOL?
--------------------
An MCP tool is a Python function decorated with @mcp.tool().  FastMCP
auto-generates the JSON schema from the type hints and docstring, so the
LLM (Gemini) receives a fully-typed description of every tool and can
call them by name just like a function.
"""

from __future__ import annotations

import os
import httpx
import structlog

log = structlog.get_logger(__name__)

# Base URL of the contract-api service.
# Inside Docker, docker-compose sets CONTRACT_API_URL=http://contract-api:8000.
# When running locally outside Docker, it falls back to localhost.
_API_BASE = os.getenv("CONTRACT_API_URL", "http://localhost:8000") + "/api/v1"

# Shared HTTP client — reused across tool calls to avoid connection overhead.
# timeout=30 gives BigQuery (behind the API) time to respond.
_http = httpx.Client(timeout=30.0)


def get_contract(vendor_id: str) -> dict:
    """
    Retrieve the active data contract for a vendor.

    Returns a dict with keys:
      vendor_id, vendor_name, version, status, fields (list), description,
      owner_email, created_at, updated_at

    Each field in `fields` has: name, field_type, nullable, description.

    Raises ValueError if the vendor is not found (404).
    """
    log.info("mcp.get_contract", vendor_id=vendor_id)
    resp = _http.get(f"{_API_BASE}/contracts/{vendor_id}")

    if resp.status_code == 404:
        raise ValueError(f"No active contract found for vendor '{vendor_id}'.")
    resp.raise_for_status()

    return resp.json()


def list_vendors() -> list[dict]:
    """
    Return a summary list of all registered vendors.

    Each item has: vendor_id, vendor_name, version, status,
    registered_at, owner_email.
    """
    log.info("mcp.list_vendors")
    resp = _http.get(f"{_API_BASE}/vendors")
    resp.raise_for_status()
    return resp.json()


def submit_approval(
    event_id: str,
    approved: bool,
    approved_by: str,
    notes: str,
) -> dict:
    """
    Submit a human approval or rejection decision for a drift event.

    Parameters
    ----------
    event_id    : UUID of the drift event (from the Pub/Sub message).
    approved    : True = approve the schema change, False = reject it.
    approved_by : Identifier of the approver — e.g. "gemini-agent" for
                  auto-approvals or "oncall-engineer@company.com" for humans.
    notes       : Free-text explanation of the decision.

    Returns the updated drift event record from the API.
    Raises ValueError if the event is not in PENDING_APPROVAL status.
    """
    log.info("mcp.submit_approval", event_id=event_id, approved=approved, approved_by=approved_by)
    resp = _http.post(
        f"{_API_BASE}/approve/{event_id}",
        json={
            "approved":    approved,
            "approved_by": approved_by,
            "notes":       notes,
        },
    )

    if resp.status_code == 404:
        raise ValueError(f"Drift event '{event_id}' not found.")
    if resp.status_code == 409:
        raise ValueError(f"Drift event '{event_id}' is already resolved.")
    resp.raise_for_status()

    return resp.json()
