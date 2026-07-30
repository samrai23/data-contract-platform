"""
data_contracts/templates/pydantic_contract.py
=============================================

Pydantic v2 model for a vendor data contract.

This is the canonical, storage-facing representation of a contract — used by:
  - FastAPI (Phase 2) to validate POST /vendors request bodies
  - BigQuery writer (Phase 2) to serialise before writing
  - Local JSON registry (Phase 1) to load from files in data_contracts/registry/

It is intentionally decoupled from schema_comparator.DataContract (which is an
internal dataclass used only inside the drift-detection algorithm).  Conversion
between the two happens at the use site — see ApiContractRegistry.get_contract()
in spark_jobs/streaming/pubsub_drift_job.py.

Supported field types
---------------------
The FieldType enum lists every type the platform's auto-heal rules know about.
Using an enum (instead of a raw string) means a contract with "STRINNG" is
rejected at load time rather than silently producing wrong drift classifications.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class FieldType(str, Enum):
    """
    Canonical set of column types recognised by the platform.

    These map 1:1 to the type strings used in schema_comparator's
    _WIDENING_CHANGES and _NARROWING_CHANGES lookup tables.
    Adding a new type here without updating those tables will cause it to
    fall through to the conservative MEDIUM / not-safe default.
    """

    INT = "INT"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    BYTES = "BYTES"
    NUMERIC = "NUMERIC"


class ContractStatus(str, Enum):
    """Lifecycle state of a contract version."""

    ACTIVE = "active"          # currently enforced
    DEPRECATED = "deprecated"  # superseded by a newer version, still readable
    DRAFT = "draft"            # not yet enforced, under review


# ---------------------------------------------------------------------------
# Field-level model
# ---------------------------------------------------------------------------


class VendorContractField(BaseModel):
    """
    A single column definition inside a VendorContract.

    Attributes
    ----------
    name        : Column name exactly as it appears in the vendor's JSON payload.
    field_type  : Expected data type (must be one of FieldType).
    nullable    : True = NULL values are allowed in this column.
    description : Optional human-readable description for documentation.
    """

    name: str = Field(..., min_length=1, max_length=128)
    field_type: FieldType
    nullable: bool = True
    description: str = Field(default="", max_length=512)

    @field_validator("name")
    @classmethod
    def name_must_be_snake_case(cls, v: str) -> str:
        """
        Enforce strict lowercase snake_case.

        BigQuery column names are case-sensitive and dbt generates SQL from
        them directly. Allowing "userId" or "UserID" would cause mismatches
        between the contract and the actual vendor payload keys.
        Valid:   user_id, listing_date, price2
        Invalid: userId, User_ID, user-id, 2nd_field
        """
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                f"Field name '{v}' must be lowercase snake_case "
                "(lowercase letters, digits, underscores only; must start with a letter)."
            )
        return v


# ---------------------------------------------------------------------------
# Contract-level model
# ---------------------------------------------------------------------------


class VendorContract(BaseModel):
    """
    Complete schema contract for one vendor, one version.

    A new version is created whenever the schema changes — old versions are
    kept as DEPRECATED so the drift audit trail is fully reproducible.

    Attributes
    ----------
    vendor_id   : Unique identifier for the vendor (e.g. "cars24", "paytm").
    version     : Semantic version string "MAJOR.MINOR.PATCH".
    status      : Lifecycle state (active / deprecated / draft).
    fields      : Ordered list of column definitions.
    created_at  : UTC timestamp when this contract version was created.
    updated_at  : UTC timestamp of the last modification.
    description : Human-readable summary of what this vendor provides.
    owner_email : Team or individual responsible for this contract.
    """

    vendor_id: str = Field(..., min_length=1, max_length=64)
    version: str = Field(..., description="Semantic version: MAJOR.MINOR.PATCH")
    status: ContractStatus = ContractStatus.ACTIVE
    fields: list[VendorContractField] = Field(..., min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    description: str = Field(default="", max_length=1024)
    owner_email: str = Field(default="")

    # ── Validators ─────────────────────────────────────────────────────────

    @field_validator("version")
    @classmethod
    def version_must_be_semver(cls, v: str) -> str:
        """Enforce MAJOR.MINOR.PATCH format — rejects "v1", "1.0", "latest"."""
        if not re.match(r"^\d+\.\d+\.\d+$", v):
            raise ValueError(
                f"version '{v}' must follow semantic versioning: MAJOR.MINOR.PATCH "
                "(e.g. '1.0.0', '2.3.1')"
            )
        return v

    @field_validator("vendor_id")
    @classmethod
    def vendor_id_must_be_slug(cls, v: str) -> str:
        """Vendor IDs are used as Pub/Sub message attributes and file names."""
        if not re.match(r"^[a-z0-9][a-z0-9_-]*$", v):
            raise ValueError(
                f"vendor_id '{v}' must be lowercase alphanumeric with hyphens/underscores"
            )
        return v

    @model_validator(mode="after")
    def no_duplicate_field_names(self) -> "VendorContract":
        """Two fields with the same name would cause silent comparison errors."""
        names = [f.name for f in self.fields]
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            raise ValueError(f"Duplicate field names detected: {duplicates}")
        return self

    # ── Helpers ────────────────────────────────────────────────────────────

    def field_names(self) -> list[str]:
        """Ordered list of column names — useful for display."""
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> Optional[VendorContractField]:
        """Return the field definition for a given name, or None."""
        return next((f for f in self.fields if f.name == name), None)

    def bump_minor(self) -> "VendorContract":
        """
        Return a new VendorContract with the minor version incremented.

        Used by the Contract Writer agent when adding safe new columns.
        The original contract is left unchanged (Pydantic models are immutable
        by default — model_copy() returns a new instance).
        """
        major, minor, patch = (int(x) for x in self.version.split("."))
        return self.model_copy(
            update={
                "version": f"{major}.{minor + 1}.0",
                "updated_at": datetime.now(timezone.utc),
                "status": ContractStatus.ACTIVE,
            }
        )

    def bump_major(self) -> "VendorContract":
        """
        Return a new VendorContract with the major version incremented.

        Used by the Contract Writer agent when applying breaking changes
        (dropped columns, type narrowing) that received human approval.
        """
        major, _, __ = (int(x) for x in self.version.split("."))
        return self.model_copy(
            update={
                "version": f"{major + 1}.0.0",
                "updated_at": datetime.now(timezone.utc),
                "status": ContractStatus.ACTIVE,
            }
        )
