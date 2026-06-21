"""
services/contract_api/models.py
================================

Pydantic v2 models for all HTTP request and response shapes.

WHY THIS FILE EXISTS — AND WHY IT'S SEPARATE FROM pydantic_contract.py
-----------------------------------------------------------------------
data_contracts/templates/pydantic_contract.py is the DOMAIN model — it
represents the full, validated, versioned contract as stored on disk or
in BigQuery.  It has business logic (bump_minor, bump_major), strict
validators, and is used internally by the drift detector and agent engine.

This file defines the API CONTRACT — what clients send over HTTP and what
they receive back.  The shapes are simpler (no version bumping logic, no
semver validation on input since version is always assigned by the server,
no is_active flags the client shouldn't set).

Keeping them separate means:
  • The HTTP interface can evolve independently of the internal domain model.
  • Clients don't need to know internal details (e.g. status field logic).
  • FastAPI auto-generates clean OpenAPI docs from these simpler shapes.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, EmailStr, field_validator


# ── Shared type alias ──────────────────────────────────────────────────────────
# These are the only field types the platform supports.
# Using Literal means FastAPI's OpenAPI docs show a dropdown with valid values,
# and Pydantic rejects anything not in this list at request parse time.
FieldType = Literal[
    "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING",
    "BOOLEAN", "TIMESTAMP", "DATE", "BYTES", "NUMERIC",
]


# ── Request models (what clients SEND to us) ───────────────────────────────────

class ContractFieldInput(BaseModel):
    """One field in a vendor's data contract, as submitted by the client."""

    name: str
    field_type: FieldType
    nullable: bool = True
    description: str = ""

    @field_validator("name")
    @classmethod
    def must_be_snake_case(cls, v: str) -> str:
        """
        Field names must be lowercase snake_case.

        This mirrors BigQuery column naming rules and ensures our dbt models
        and Great Expectations suites can reference fields without quoting.
        Valid:   user_id, listing_date, fuel_type
        Invalid: userId, Listing-Date, FUEL_TYPE
        """
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                f"'{v}' is not valid snake_case. "
                "Use lowercase letters, digits, and underscores only. "
                "Must start with a letter."
            )
        return v


class VendorRegistrationRequest(BaseModel):
    """
    Request body for POST /api/v1/vendors.

    The client provides the vendor metadata and their initial field list.
    The server assigns version (always starts at 1.0.0) and timestamps.
    """

    vendor_id: str
    vendor_name: str
    contact_email: EmailStr   # vendor's data engineering contact
    owner_email: EmailStr     # our internal engineer who owns this contract
    fields: list[ContractFieldInput]
    description: str = ""

    @field_validator("vendor_id")
    @classmethod
    def must_be_slug(cls, v: str) -> str:
        """
        vendor_id must be a URL-safe slug.

        Used as a path parameter in GET /contracts/{vendor_id} and as the
        partition key in BigQuery queries, so it must be safe to embed in URLs
        and SQL.
        Valid:   cars24, policy-bazaar, make_my_trip
        Invalid: Cars24, policy bazaar, make.my.trip
        """
        if not re.match(r"^[a-z0-9][a-z0-9_-]*$", v):
            raise ValueError(
                f"'{v}' is not a valid vendor_id. "
                "Use lowercase letters, digits, hyphens, and underscores only."
            )
        return v

    @field_validator("fields")
    @classmethod
    def at_least_one_field(cls, v: list[ContractFieldInput]) -> list[ContractFieldInput]:
        if not v:
            raise ValueError("A contract must define at least one field.")
        return v


class ApprovalRequest(BaseModel):
    """
    Request body for POST /api/v1/approve/{event_id}.

    Sent by a human (or an automated approval webhook) to either approve
    a risky schema change or reject it (which keeps the pipeline paused).
    """

    approved: bool
    approved_by: EmailStr
    notes: str = ""


# ── Response models (what we RETURN to clients) ────────────────────────────────

class ContractFieldResponse(BaseModel):
    """One field in a contract, as returned in API responses."""

    name: str
    field_type: str
    nullable: bool
    description: str


class ContractResponse(BaseModel):
    """
    Full contract detail — returned by both POST /vendors (on creation)
    and GET /contracts/{vendor_id}.
    """

    vendor_id: str
    vendor_name: str
    version: str           # semver: "1.0.0", "1.1.0", etc.
    status: str            # ACTIVE | DEPRECATED | DRAFT
    fields: list[ContractFieldResponse]
    description: str
    owner_email: str
    created_at: datetime
    updated_at: datetime


class VendorListItem(BaseModel):
    """
    Summary row returned in GET /vendors.

    Deliberately omits the full fields list — use GET /contracts/{id} for that.
    Keeping list responses small means faster queries (fewer columns scanned
    in BigQuery, smaller JSON payloads over the wire).
    """

    vendor_id: str
    vendor_name: str
    version: str
    status: str
    registered_at: datetime
    owner_email: str


class DriftLogEntry(BaseModel):
    """One row from the drift_events table, returned by GET /drift-log."""

    event_id: str
    vendor_id: str
    drift_type: str        # type_change | new_column | dropped_column | column_rename
    detected_at: datetime
    field_name: str
    old_type: Optional[str] = None   # None for new_column events (field didn't exist before)
    new_type: Optional[str] = None   # None for dropped_column events
    severity: str          # LOW | MEDIUM | HIGH | CRITICAL
    resolution_status: str # PENDING_APPROVAL | AUTO_HEALED | APPROVED | REJECTED
    agent_verdict: Optional[str] = None   # free-text summary from the Analyst agent


class ApprovalResponse(BaseModel):
    """Response body for POST /api/v1/approve/{event_id}."""

    event_id: str
    approved: bool
    approved_by: str
    processed_at: datetime
    message: str


class HealthResponse(BaseModel):
    """Response body for GET /api/v1/health."""

    status: Literal["healthy", "degraded"]
    bigquery_connected: bool
    timestamp: datetime
    version: str = "1.0.0"
