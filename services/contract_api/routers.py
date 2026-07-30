"""
services/contract_api/routers.py
==================================

Route handlers for all API endpoints.

This file is the "business logic" layer — it sits between the HTTP interface
(models.py defines request/response shapes) and the database (BigQuery).

ENDPOINTS IN THIS FILE:
  POST   /api/v1/vendors              — register a new vendor + initial contract
  GET    /api/v1/vendors              — list all vendors with active contracts
  GET    /api/v1/contracts/{vendor}   — get the full contract for one vendor
  GET    /api/v1/drift-log            — audit trail of drift events
  POST   /api/v1/approve/{event_id}   — human approval/rejection of risky drifts
  GET    /api/v1/health               — liveness check

BIGQUERY PATTERNS USED HERE:
  1. Parameterised queries  — NEVER string-interpolate user input into SQL.
                              Use bigquery.ScalarQueryParameter() instead.
  2. Batch load insert      — bq.load_table_from_json() for INSERT (free-tier compatible).
                              Streaming inserts AND DML both require billing on free tier.
  3. DML UPDATE             — bq.query("UPDATE ...") for the approval endpoint.
                              (Only used after drift events exist; requires billing when called.)
  4. .result()              — always call .result() to block until the job finishes
                              and to surface any errors as Python exceptions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from google.cloud import bigquery
from pydantic import ValidationError

from dependencies import BQClient, DatasetRef
from models import (
    ApprovalRequest,
    ApprovalResponse,
    ContractFieldResponse,
    ContractHealRequest,
    ContractResponse,
    DriftLogEntry,
    HealthResponse,
    VendorListItem,
    VendorRegistrationRequest,
)

# Domain model (data_contracts/templates/pydantic_contract.py) — importable because
# the contract-api Dockerfile puts data_contracts/templates on PYTHONPATH.
# This is the ONE place in contract-api that touches the domain model directly:
# the heal endpoint is the "contract writer" — it owns version-bump business logic
# (bump_minor/bump_major), which the simpler API-facing models.py deliberately omits.
from pydantic_contract import VendorContract, VendorContractField

log = structlog.get_logger(__name__)
router = APIRouter()


# ── POST /vendors ──────────────────────────────────────────────────────────────

@router.post(
    "/vendors",
    response_model=ContractResponse,
    status_code=201,
    summary="Register a new vendor and their initial data contract",
)
def register_vendor(
    body: VendorRegistrationRequest,
    bq: BQClient,
    dataset: DatasetRef,
) -> ContractResponse:
    """
    Register a new vendor and store their initial data contract in BigQuery.

    HOW THIS WORKS STEP BY STEP:
    1. Check if vendor already has an ACTIVE contract → 409 if yes.
    2. Build the row to insert: assign version="1.0.0", status="ACTIVE".
    3. Serialise the fields list to a JSON string for storage in contract_json.
    4. Insert the row using DML INSERT (free-tier compatible).
    5. Return the newly created contract.

    WHY WE STORE fields AS A JSON STRING (not separate rows):
    BigQuery doesn't have a great pattern for "one contract has many fields" without
    either ARRAY<STRUCT> (complex to query) or a separate fields table (requires JOINs).
    Storing fields as a JSON string in one column keeps reads and writes simple.
    The trade-off: you can't filter by individual field names using standard SQL
    (you'd need BigQuery's JSON_VALUE function).  That's acceptable for our use case
    because we always load the whole contract, not just some fields.

    WHY BATCH LOAD INSTEAD OF insert_rows_json OR DML INSERT:
    BigQuery free tier blocks ALL write operations except batch load jobs:
      - Streaming inserts (insert_rows_json): requires billing enabled
      - DML INSERT/UPDATE/DELETE:             requires billing enabled
      - Batch load (load_table_from_json):    FREE on all tiers
    Trade-off: batch load takes ~5-10 seconds vs milliseconds for streaming,
    but for registering one vendor that latency is acceptable.
    """
    # ── Step 1: check for duplicate ───────────────────────────────────────────
    # @vendor_id is a named query parameter — BigQuery replaces it safely.
    # This prevents SQL injection even if vendor_id contained a quote character.
    check_query = f"""
        SELECT vendor_id
        FROM `{dataset}.vendor_contracts`
        WHERE vendor_id = @vendor_id AND status = 'ACTIVE'
        LIMIT 1
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("vendor_id", "STRING", body.vendor_id)
        ]
    )
    existing = list(bq.query(check_query, job_config=check_config).result())
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Vendor '{body.vendor_id}' already has an active contract. "
                "The Healer agent updates contracts automatically when drift is detected. "
                "Use GET /contracts/{vendor_id} to view the current contract."
            ),
        )

    # ── Step 2: build the row ─────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    version = "1.0.0"

    # ── Step 3: serialise fields ──────────────────────────────────────────────
    # Each field is a dict; we store the whole list as one JSON string.
    # model_dump() converts the Pydantic object to a plain Python dict.
    fields_data = [f.model_dump() for f in body.fields]
    fields_json = json.dumps(fields_data)

    row = {
        "vendor_id":     body.vendor_id,
        "vendor_name":   body.vendor_name,
        "version":       version,
        "status":        "ACTIVE",
        "contract_json": fields_json,
        "description":   body.description,
        "owner_email":   str(body.owner_email),
        "contact_email": str(body.contact_email),
        "created_at":    now.isoformat(),
        "updated_at":    now.isoformat(),
    }

    # ── Step 4: insert (batch load job — free-tier compatible) ───────────────
    # load_table_from_json() submits a batch load job.  It is the only INSERT
    # mechanism that works on BigQuery free tier (no billing required).
    # WRITE_APPEND adds the new row to the existing table (does not overwrite).
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    try:
        job = bq.load_table_from_json(
            [row],
            f"{dataset}.vendor_contracts",
            job_config=load_config,
        )
        job.result()
    except Exception as exc:
        log.error("register_vendor.insert_failed", vendor_id=body.vendor_id, error=str(exc))
        raise HTTPException(status_code=500, detail=f"BigQuery insert failed: {exc}") from exc

    log.info("register_vendor.ok", vendor_id=body.vendor_id, version=version)

    # ── Step 5: return the created contract ───────────────────────────────────
    # We return the data we just inserted rather than querying BQ again.
    # Batch load jobs are strongly consistent — the row IS visible immediately
    # after job.result() returns, but a redundant SELECT round-trip adds latency.
    return ContractResponse(
        vendor_id=body.vendor_id,
        vendor_name=body.vendor_name,
        version=version,
        status="ACTIVE",
        fields=[ContractFieldResponse(**f) for f in fields_data],
        description=body.description,
        owner_email=str(body.owner_email),
        created_at=now,
        updated_at=now,
    )


# ── GET /vendors ───────────────────────────────────────────────────────────────

@router.get(
    "/vendors",
    response_model=list[VendorListItem],
    summary="List all registered vendors",
)
def list_vendors(bq: BQClient, dataset: DatasetRef) -> list[VendorListItem]:
    """
    Return a summary list of all vendors with an ACTIVE contract.

    Notice we only SELECT the columns we need (not SELECT *).
    In BigQuery, which is columnar, selecting fewer columns means less data
    scanned = lower cost and faster response.
    """
    query = f"""
        SELECT vendor_id, vendor_name, version, status, created_at, owner_email
        FROM `{dataset}.vendor_contracts`
        WHERE status = 'ACTIVE'
        ORDER BY created_at DESC
    """
    rows = bq.query(query).result()
    return [
        VendorListItem(
            vendor_id=row.vendor_id,
            vendor_name=row.vendor_name,
            version=row.version,
            status=row.status,
            registered_at=row.created_at,
            owner_email=row.owner_email,
        )
        for row in rows
    ]


# ── GET /contracts/{vendor_id} ─────────────────────────────────────────────────

@router.get(
    "/contracts/{vendor_id}",
    response_model=ContractResponse,
    summary="Get the active contract for a specific vendor",
)
def get_contract(
    vendor_id: str,
    bq: BQClient,
    dataset: DatasetRef,
) -> ContractResponse:
    """
    Retrieve the full active contract for a vendor, including all field definitions.

    The vendor_id comes from the URL path (e.g. GET /contracts/cars24).
    FastAPI extracts it automatically from the {vendor_id} placeholder.

    This is the endpoint the MCP server will call in Phase 3 instead of
    reading local JSON files.
    """
    query = f"""
        SELECT *
        FROM `{dataset}.vendor_contracts`
        WHERE vendor_id = @vendor_id AND status = 'ACTIVE'
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id)
        ]
    )
    rows = list(bq.query(query, job_config=job_config).result())

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No active contract found for vendor '{vendor_id}'. "
                   f"Register it first with POST /api/v1/vendors.",
        )

    row = rows[0]

    # Deserialise the JSON string back into a list of field dicts
    fields = json.loads(row.contract_json)

    return ContractResponse(
        vendor_id=row.vendor_id,
        vendor_name=row.vendor_name,
        version=row.version,
        status=row.status,
        fields=[ContractFieldResponse(**f) for f in fields],
        description=row.description or "",
        owner_email=row.owner_email,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


# ── PATCH /contracts/{vendor_id}/heal ──────────────────────────────────────────

@router.patch(
    "/contracts/{vendor_id}/heal",
    response_model=ContractResponse,
    summary="Apply an auto-healed drift to the stored contract and bump its version",
)
def heal_contract(
    vendor_id: str,
    body: ContractHealRequest,
    bq: BQClient,
    dataset: DatasetRef,
) -> ContractResponse:
    """
    Apply one auto-healed drift event to the stored contract's field list and
    bump the semantic version — the piece execute_auto_heal() was previously
    missing (it only logged the event and approved it, contract_json never
    changed, so the next batch would flag the same field as "drift" again).

    Only called by the agent engine, and only after Gemini returns AUTO_HEAL —
    which schema_comparator.py only ever marks is_safe_to_auto_heal=True for
    type_change (widening) and new_column. dropped_column and column_rename are
    handled here too for completeness (so this endpoint is correct standalone),
    but in the current graph they always route to ESCALATE, never here.

    WHY DML UPDATE (same exception as POST /approve/{event_id}):
    This modifies fields on an EXISTING row (contract_json, version). Batch
    load is append-only and cannot update a row that's already there, so DML
    is the only option. This requires billing enabled on the BigQuery project
    — same requirement the approve endpoint already has. Cost is a few KB
    scanned on a table with a handful of rows, effectively $0.
    """
    # ── Step 1: fetch the current active contract row ─────────────────────────
    query = f"""
        SELECT *
        FROM `{dataset}.vendor_contracts`
        WHERE vendor_id = @vendor_id AND status = 'ACTIVE'
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id)
        ]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No active contract found for vendor '{vendor_id}'. Cannot heal a contract that doesn't exist.",
        )
    row = rows[0]

    # Deserialise the stored fields and validate them through the domain model
    # (the same VendorContractField/VendorContract classes register_vendor's
    # data passed through implicitly via the API models' validators).
    try:
        stored_fields = [VendorContractField(**f) for f in json.loads(row.contract_json)]
    except (ValidationError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Stored contract for '{vendor_id}' failed to parse: {exc}",
        ) from exc

    # ── Step 2: apply the drift to the field list ──────────────────────────────
    updated_fields = list(stored_fields)

    try:
        if body.drift_type == "type_change":
            idx = next((i for i, f in enumerate(updated_fields) if f.name == body.field_name), None)
            if idx is None:
                raise HTTPException(404, f"Field '{body.field_name}' not found — cannot apply type_change heal.")
            if body.new_type is None:
                raise HTTPException(422, "type_change heal requires new_type.")
            old = updated_fields[idx]
            updated_fields[idx] = VendorContractField(
                name=old.name, field_type=body.new_type, nullable=old.nullable, description=old.description,
            )

        elif body.drift_type == "new_column":
            if any(f.name == body.field_name for f in updated_fields):
                raise HTTPException(409, f"Field '{body.field_name}' already exists — cannot add it as new_column.")
            if body.new_type is None:
                raise HTTPException(422, "new_column heal requires new_type.")
            updated_fields.append(VendorContractField(
                name=body.field_name, field_type=body.new_type, nullable=True,
                description="Auto-added by heal (detected as new_column drift).",
            ))

        elif body.drift_type == "dropped_column":
            before = len(updated_fields)
            updated_fields = [f for f in updated_fields if f.name != body.field_name]
            if len(updated_fields) == before:
                raise HTTPException(404, f"Field '{body.field_name}' not found — cannot apply dropped_column heal.")

        elif body.drift_type == "column_rename":
            # schema_comparator.py formats rename field_name as "old_name → new_name"
            parts = body.field_name.split(" → ")
            if len(parts) != 2:
                raise HTTPException(422, f"column_rename field_name must be 'old → new', got '{body.field_name}'.")
            old_name, new_name = parts
            idx = next((i for i, f in enumerate(updated_fields) if f.name == old_name), None)
            if idx is None:
                raise HTTPException(404, f"Field '{old_name}' not found — cannot apply column_rename heal.")
            old = updated_fields[idx]
            updated_fields[idx] = VendorContractField(
                name=new_name, field_type=old.field_type, nullable=old.nullable, description=old.description,
            )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Healed field failed validation: {exc}") from exc

    # ── Step 3: rebuild + validate the full contract, then bump the version ────
    # Constructing (not model_copy) re-runs no_duplicate_field_names — model_copy
    # would skip validators and could silently accept a bad rename/collision.
    try:
        candidate = VendorContract(
            vendor_id=row.vendor_id,
            version=row.version,
            fields=[f.model_dump() for f in updated_fields],
            description=row.description or "",
            owner_email=row.owner_email or "",
        )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Healed contract failed validation: {exc}") from exc

    # Additive, non-breaking changes → minor bump. Structural/breaking → major bump.
    # (In practice only type_change/new_column reach this endpoint via AUTO_HEAL —
    # dropped_column/column_rename are always ESCALATE — but the rule holds either way.)
    if body.drift_type in ("type_change", "new_column"):
        healed = candidate.bump_minor()
    else:
        healed = candidate.bump_major()

    # ── Step 4: persist via DML UPDATE (see docstring for why not batch load) ──
    # mode="json" forces field_type (a str Enum) to serialise as its plain string
    # value rather than relying on Enum-as-str coercion.
    new_contract_json = json.dumps([f.model_dump(mode="json") for f in healed.fields])
    now = datetime.now(timezone.utc)

    update_query = f"""
        UPDATE `{dataset}.vendor_contracts`
        SET
            contract_json = @contract_json,
            version       = @version,
            updated_at    = @updated_at
        WHERE vendor_id = @vendor_id AND status = 'ACTIVE'
    """
    update_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("contract_json", "STRING", new_contract_json),
            bigquery.ScalarQueryParameter("version", "STRING", healed.version),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now.isoformat()),
            bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id),
        ]
    )
    bq.query(update_query, job_config=update_config).result()

    log.info(
        "heal_contract.ok",
        vendor_id=vendor_id,
        drift_type=body.drift_type,
        field_name=body.field_name,
        old_version=row.version,
        new_version=healed.version,
        healed_by=body.healed_by,
    )

    return ContractResponse(
        vendor_id=row.vendor_id,
        vendor_name=row.vendor_name,
        version=healed.version,
        status="ACTIVE",
        fields=[ContractFieldResponse(**f.model_dump(mode="json")) for f in healed.fields],
        description=row.description or "",
        owner_email=row.owner_email or "",
        created_at=row.created_at,
        updated_at=now,
    )


# ── GET /drift-log ─────────────────────────────────────────────────────────────

@router.get(
    "/drift-log",
    response_model=list[DriftLogEntry],
    summary="Audit trail of all detected drift events",
)
def get_drift_log(
    bq: BQClient,
    dataset: DatasetRef,
    vendor_id: Optional[str] = Query(None, description="Filter to one vendor"),
    severity: Optional[str] = Query(None, description="LOW | MEDIUM | HIGH | CRITICAL"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
) -> list[DriftLogEntry]:
    """
    Return the audit log of schema drift events, newest first.

    Supports optional filtering by vendor_id and severity.
    Will return an empty list until the agent engine (Phase 3) starts writing events.

    QUERY PARAMETER SECURITY NOTE:
    vendor_id and severity use bigquery.ScalarQueryParameter (safe).
    limit is an integer validated by FastAPI (ge=1, le=500) so it is safe
    to interpolate into the LIMIT clause — BigQuery doesn't support
    parameterised LIMIT values.
    """
    where_parts: list[str] = []
    params: list[bigquery.ScalarQueryParameter] = []

    if vendor_id:
        where_parts.append("vendor_id = @vendor_id")
        params.append(bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id))
    if severity:
        where_parts.append("severity = @severity")
        params.append(bigquery.ScalarQueryParameter("severity", "STRING", severity.upper()))

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    query = f"""
        SELECT
            event_id, vendor_id, drift_type, detected_at,
            field_name, old_type, new_type, severity,
            resolution_status, agent_verdict
        FROM `{dataset}.drift_events`
        {where_sql}
        ORDER BY detected_at DESC
        LIMIT {limit}
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    rows = bq.query(query, job_config=job_config).result() if job_config else bq.query(query).result()

    return [
        DriftLogEntry(
            event_id=row.event_id,
            vendor_id=row.vendor_id,
            drift_type=row.drift_type,
            detected_at=row.detected_at,
            field_name=row.field_name,
            old_type=row.old_type,
            new_type=row.new_type,
            severity=row.severity,
            resolution_status=row.resolution_status,
            agent_verdict=row.agent_verdict,
        )
        for row in rows
    ]


# ── POST /approve/{event_id} ───────────────────────────────────────────────────

@router.post(
    "/approve/{event_id}",
    response_model=ApprovalResponse,
    summary="Human approval or rejection of a risky drift event",
)
def approve_drift_event(
    event_id: str,
    body: ApprovalRequest,
    bq: BQClient,
    dataset: DatasetRef,
) -> ApprovalResponse:
    """
    Approve or reject a drift event that was flagged as requiring human review.

    HOW THIS FITS INTO THE BIGGER PICTURE:
    Phase 3 Healer agent detects a risky drift (e.g. STRING → INT).
    Instead of auto-healing, it calls this endpoint's URL (set in HUMAN_APPROVAL_WEBHOOK).
    A human engineer sees the alert, reviews the drift, and POSTs to this endpoint.
    The Healer agent polls the event's resolution_status and proceeds once it sees
    APPROVED or REJECTED.

    This endpoint:
    1. Verifies the event exists and is in PENDING_APPROVAL status.
    2. Updates the row in drift_events with the decision.
    3. Returns the updated status so the caller can confirm.

    WHY DML UPDATE (not streaming insert):
    We're modifying an existing row (updating resolution_status on a specific event_id).
    Streaming inserts only INSERT new rows — they can't update existing ones.
    BigQuery DML (UPDATE/DELETE) runs as a full query job and costs slot time, but
    for a single-row update triggered by a human action, that's perfectly acceptable.
    """
    # ── Step 1: verify event exists and is awaiting approval ──────────────────
    check_query = f"""
        SELECT event_id, resolution_status
        FROM `{dataset}.drift_events`
        WHERE event_id = @event_id
        LIMIT 1
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
        ]
    )
    rows = list(bq.query(check_query, job_config=check_config).result())

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Drift event '{event_id}' not found.",
        )

    current_status = rows[0].resolution_status
    if current_status != "PENDING_APPROVAL":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Event '{event_id}' is in status '{current_status}' — "
                "cannot re-approve an already-resolved event."
            ),
        )

    # ── Step 2: update the row ────────────────────────────────────────────────
    new_status = "APPROVED" if body.approved else "REJECTED"
    now = datetime.now(timezone.utc)

    update_query = f"""
        UPDATE `{dataset}.drift_events`
        SET
            resolution_status = @status,
            agent_verdict     = @verdict,
            approved_by       = @approved_by,
            approval_notes    = @notes,
            healed_at         = @healed_at
        WHERE event_id = @event_id
    """
    update_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status",      "STRING",    new_status),
            bigquery.ScalarQueryParameter("verdict",     "STRING",    f"Human decision: {new_status}"),
            bigquery.ScalarQueryParameter("approved_by", "STRING",    str(body.approved_by)),
            bigquery.ScalarQueryParameter("notes",       "STRING",    body.notes),
            bigquery.ScalarQueryParameter("healed_at",   "TIMESTAMP", now.isoformat()),
            bigquery.ScalarQueryParameter("event_id",    "STRING",    event_id),
        ]
    )
    bq.query(update_query, job_config=update_config).result()

    log.info(
        "approve_drift.resolved",
        event_id=event_id,
        status=new_status,
        approved_by=str(body.approved_by),
    )

    return ApprovalResponse(
        event_id=event_id,
        approved=body.approved,
        approved_by=str(body.approved_by),
        processed_at=now,
        message=f"Drift event {new_status.lower()} by {body.approved_by}.",
    )


# ── GET /health ────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness and readiness check",
)
def health_check(bq: BQClient, dataset: DatasetRef) -> HealthResponse:
    """
    Returns healthy if FastAPI is up and BigQuery is reachable.

    Runs a trivial SELECT 1 query against BigQuery.  If the query succeeds,
    the BQ connection (credentials, network, project permissions) is working.
    If it fails, the response body still returns 200 but with status="degraded"
    — this lets load balancers and dashboards distinguish "API is down" from
    "API is up but its dependency is unreachable".
    """
    bq_ok = False
    try:
        list(bq.query("SELECT 1 AS ok").result())
        bq_ok = True
    except Exception:
        log.warning("health_check.bigquery_unreachable")

    return HealthResponse(
        status="healthy" if bq_ok else "degraded",
        bigquery_connected=bq_ok,
        timestamp=datetime.now(timezone.utc),
    )
