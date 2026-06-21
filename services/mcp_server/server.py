"""
services/mcp_server/server.py
==============================

FastMCP server — exposes data contract tools to the AI agent.

WHAT IS MCP (MODEL CONTEXT PROTOCOL)?
--------------------------------------
MCP is Anthropic's open protocol for connecting AI models to external tools
and data sources.  Think of it like a standardized API layer between an AI
agent and the real world.

Without MCP:  agent → custom HTTP calls → each service has its own interface
With MCP:     agent → MCP client → MCP server → services
                       (one standard protocol for all tools)

The MCP server defines "tools" — typed functions with descriptions.  FastMCP
reads the Python type hints and docstrings to auto-generate the JSON schema
that the LLM sees.  The agent calls tools by name and receives structured
responses.

WHY FASTMCP?
------------
FastMCP is the official Python library for building MCP servers (Anthropic
endorsed it as the recommended approach).  It turns a regular Python function
into an MCP tool with a single decorator:

    @mcp.tool()
    def my_tool(param: str) -> dict:
        '''Description the LLM reads.'''
        return {"result": param}

HOW THE AGENT USES THIS SERVER:
--------------------------------
1. Agent engine starts → connects to mcp-server via SSE transport on port 8001
2. Agent receives a list of available tools + their schemas
3. LLM decides which tool to call and with what arguments
4. FastMCP validates the args, calls the Python function, returns the result
5. LLM sees the result and decides the next step

TRANSPORT — SSE (Server-Sent Events):
--------------------------------------
SSE is HTTP-based streaming — the agent keeps one long-lived HTTP connection
open to the MCP server and receives tool results as events on that stream.
We use SSE (not stdio) because our server and agent run as separate Docker
containers and need to communicate over the network.
"""

from __future__ import annotations

import os
import structlog
from fastmcp import FastMCP

import contract_tools
import bq_tools

# Configure structured logging early
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer() if os.getenv("ENVIRONMENT", "local") == "local"
        else structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger(__name__)

# ── FastMCP Server setup ───────────────────────────────────────────────────────
# The name appears in the MCP handshake — the agent uses it to identify
# which server it's connected to.
mcp = FastMCP("data-contract-server")


# ── Register tools ─────────────────────────────────────────────────────────────
# Each @mcp.tool() decorated function becomes a tool the agent can call.
# FastMCP generates the JSON schema automatically from the type hints + docstring.

@mcp.tool()
def get_contract(vendor_id: str) -> dict:
    """
    Get the active data contract for a vendor, including all field definitions.

    Use this to understand what schema the vendor is SUPPOSED to send before
    assessing whether a detected drift is significant or expected.

    Returns the full contract with fields list (name, field_type, nullable, description).
    """
    return contract_tools.get_contract(vendor_id)


@mcp.tool()
def list_vendors() -> list:
    """
    List all registered vendors with active data contracts.

    Returns vendor_id, vendor_name, version, status, registered_at, owner_email
    for each vendor.
    """
    return contract_tools.list_vendors()


@mcp.tool()
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
    Write a detected drift event to the audit log in BigQuery.

    Always call this tool BEFORE taking any action (auto-heal or escalate),
    so every drift event is auditable regardless of outcome.

    Parameters
    ----------
    vendor_id         : Vendor that owns the contract.
    drift_type        : type_change | new_column | dropped_column | column_rename
    field_name        : Name of the field that drifted.
    severity          : LOW | MEDIUM | HIGH
    resolution_status : AUTO_HEALED | PENDING_APPROVAL | ESCALATED
    agent_verdict     : Your reasoning for the decision (1-2 sentences).
    old_type          : Data type in the contract (empty string if new_column).
    new_type          : Data type in the live schema (empty string if dropped_column).

    Returns the generated event_id (UUID) for reference in follow-up tool calls.
    """
    return bq_tools.log_drift_event(
        vendor_id=vendor_id,
        drift_type=drift_type,
        field_name=field_name,
        old_type=old_type or None,
        new_type=new_type or None,
        severity=severity,
        resolution_status=resolution_status,
        agent_verdict=agent_verdict,
    )


@mcp.tool()
def get_drift_history(vendor_id: str, limit: int = 10) -> list:
    """
    Get the recent drift event history for a vendor from BigQuery.

    Use this to check whether the vendor has drifted before on the same field.
    Repeated drifts on a field suggest the vendor is actively migrating and
    auto-heal may be appropriate even for MEDIUM severity.

    Returns up to `limit` most recent events, newest first.
    """
    return bq_tools.get_drift_history(vendor_id=vendor_id, limit=limit)


@mcp.tool()
def submit_approval(
    event_id: str,
    approved: bool,
    approved_by: str,
    notes: str,
) -> dict:
    """
    Submit an approval or rejection decision for a drift event.

    Call this tool when:
    - approved=True  → you have assessed the drift as safe and want to auto-heal
    - approved=False → the drift is too risky and needs human review

    Parameters
    ----------
    event_id    : UUID returned by log_drift_event.
    approved    : True to approve the schema change, False to reject.
    approved_by : "gemini-agent" for automated decisions.
    notes       : 1-2 sentence explanation of why you approved or rejected.
    """
    return contract_tools.submit_approval(
        event_id=event_id,
        approved=approved,
        approved_by=approved_by,
        notes=notes,
    )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    host = os.getenv("MCP_SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_SERVER_PORT", "8001"))

    log.info("mcp_server.starting", host=host, port=port, tools=5)

    # SSE transport: client opens GET /sse to establish a persistent stream,
    # then sends JSON-RPC messages via POST /messages/?session_id=...
    # Use 0.0.0.0 so Docker port-forwarding works (same lesson as pubsub-emulator).
    mcp.run(transport="sse", host=host, port=port)
