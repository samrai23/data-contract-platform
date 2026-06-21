"""
services/mcp_server/bq_tools.py
=================================

MCP tools that read/write BigQuery directly — specifically the drift_events
and pipeline_audit tables.

WHY DOES THE MCP SERVER TALK TO BIGQUERY DIRECTLY FOR DRIFT LOGS?
------------------------------------------------------------------
The contract-api owns contract reads/writes (vendor registration, approvals).
But drift events are written by the agent itself — there's no API endpoint for
that because the agent IS the writer.  Giving the MCP server a direct BigQuery
connection for drift_events keeps the separation clean:
  - contract-api  = source of truth for contracts
  - mcp-server    = write path for drift audit logs

BigQuery write here uses load_table_from_json (batch load job) for the same
reason as in routers.py — free tier compatibility.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import structlog
from google.cloud import bigquery

log = structlog.get_logger(__name__)

_PROJECT = os.getenv("GCP_PROJECT_ID", "data-contract-platform")
_DATASET = os.getenv("BIGQUERY_DATASET", "data_contracts")

# ── Load .env and resolve credentials path ────────────────────────────────────
# The project .env file already has GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json
# but that relative path is relative to the PROJECT ROOT, not to wherever this
# file lives (services/mcp_server/).  We find the project root, load .env from
# there, then make the credentials path absolute so Python finds the file
# regardless of which directory the script is launched from.
#
# Inside Docker: env_file in docker-compose.yml loads .env automatically,
#   and the credentials file is mounted at /app/gcp-credentials.json.
#   This block is a no-op in that case (GOOGLE_APPLICATION_CREDENTIALS is
#   already set to an absolute path by Docker).
_PROJECT_ROOT = Path(__file__).parent.parent.parent  # …/data-contract-platform/

try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env", override=False)  # override=False: Docker env takes priority
except ImportError:
    pass  # dotenv not installed — rely on env vars already being set

# If the loaded value is still a relative path (e.g. "./gcp-credentials.json"),
# resolve it against the project root so it works from any working directory.
_creds_val = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
if _creds_val and not Path(_creds_val).is_absolute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_PROJECT_ROOT / _creds_val)


@lru_cache(maxsize=1)
def _bq() -> bigquery.Client:
    return bigquery.Client(project=_PROJECT)


def log_drift_event(
    vendor_id: str,
    drift_type: str,
    field_name: str,
    old_type: str | None,
    new_type: str | None,
    severity: str,
    resolution_status: str,
    agent_verdict: str,
) -> str:
    """
    Write a drift event to the drift_events BigQuery table.

    Called by the agent after it makes a decision (AUTO_HEAL or ESCALATE).
    Returns the generated event_id so downstream steps can reference it.

    Parameters
    ----------
    vendor_id         : Vendor that owns the contract.
    drift_type        : "type_change" | "new_column" | "dropped_column" | "column_rename"
    field_name        : Name of the affected field.
    old_type          : Data type in the stored contract (None for new_column).
    new_type          : Data type in the live schema (None for dropped_column).
    severity          : "LOW" | "MEDIUM" | "HIGH"
    resolution_status : "AUTO_HEALED" | "PENDING_APPROVAL" | "ESCALATED"
    agent_verdict     : Human-readable explanation of the agent's decision.
    """
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    row = {
        "event_id":          event_id,
        "vendor_id":         vendor_id,
        "drift_type":        drift_type,
        "detected_at":       now,
        "field_name":        field_name,
        "old_type":          old_type,
        "new_type":          new_type,
        "severity":          severity,
        "resolution_status": resolution_status,
        "agent_verdict":     agent_verdict,
        "approved_by":       None,
        "approval_notes":    None,
        "healed_at":         None,
    }

    # Remove None values — BigQuery load job treats missing keys as NULL.
    row_clean = {k: v for k, v in row.items() if v is not None}

    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = _bq().load_table_from_json(
        [row_clean],
        f"{_PROJECT}.{_DATASET}.drift_events",
        job_config=load_config,
    )
    job.result()

    log.info("bq_tools.drift_event_logged", event_id=event_id, vendor_id=vendor_id,
             drift_type=drift_type, severity=severity, resolution_status=resolution_status)
    return event_id


def get_drift_history(vendor_id: str, limit: int = 10) -> list[dict]:
    """
    Return the most recent drift events for a vendor, newest first.

    Useful for the agent to understand whether this vendor has a pattern
    of drift (e.g. repeated type changes on the same field suggest the
    vendor is actively migrating their schema and auto-heal is reasonable).

    Parameters
    ----------
    vendor_id : Vendor to filter by.
    limit     : Maximum number of events to return (default 10).
    """
    query = f"""
        SELECT
            event_id, drift_type, field_name, old_type, new_type,
            severity, resolution_status, agent_verdict, detected_at
        FROM `{_PROJECT}.{_DATASET}.drift_events`
        WHERE vendor_id = @vendor_id
        ORDER BY detected_at DESC
        LIMIT {min(limit, 50)}
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id)
        ]
    )
    rows = _bq().query(query, job_config=job_config).result()

    return [
        {
            "event_id":          row.event_id,
            "drift_type":        row.drift_type,
            "field_name":        row.field_name,
            "old_type":          row.old_type,
            "new_type":          row.new_type,
            "severity":          row.severity,
            "resolution_status": row.resolution_status,
            "agent_verdict":     row.agent_verdict,
            "detected_at":       row.detected_at.isoformat() if row.detected_at else None,
        }
        for row in rows
    ]
