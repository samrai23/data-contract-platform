"""
services/drift_detector/schema_comparator.py
============================================

Core drift-detection engine for the Agentic Data Contract & Ingestion Platform.

This module compares an *incoming* JSON schema — inferred from a live Pub/Sub
message batch — against the *stored* vendor DataContract and classifies every
difference into one of four canonical DriftType categories, assigning both a
Severity level and an auto-heal safety flag.

Four drift types
----------------
column_rename   : A field disappeared AND a new field with the *same* type
                  appeared in the incoming schema.  Only declared when the
                  match is unambiguous (1:1 per type).
type_change     : A shared field's data type changed.  Sub-classified into
                  widening (safe), narrowing (unsafe), nullability (unsafe),
                  and other (conservative MEDIUM default).
new_column      : A field is present in the live schema but absent from the
                  stored contract.
dropped_column  : A field is present in the contract but absent from the
                  live schema.

Auto-heal safety rules
----------------------
Safe   (True) : INT→BIGINT, INT→STRING, BIGINT→STRING, FLOAT→DOUBLE, new_column.
Unsafe (False): STRING→INT, STRING→BIGINT, BIGINT→INT, DOUBLE→FLOAT,
                NULLABLE→REQUIRED, dropped_column, column_rename.

Severity rules
--------------
LOW    : new_column; widening type changes (INT→BIGINT, INT→STRING, …).
MEDIUM : column_rename; type changes that are neither cleanly widening
         nor narrowing.
HIGH   : dropped_column; narrowing type changes (STRING→INT, …);
         nullability tightening (NULLABLE→REQUIRED).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DriftType(str, Enum):
    """Four canonical categories of schema drift."""

    COLUMN_RENAME = "column_rename"
    TYPE_CHANGE = "type_change"
    NEW_COLUMN = "new_column"
    DROPPED_COLUMN = "dropped_column"


class Severity(str, Enum):
    """Impact level assigned to each drift event."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ContractField:
    """Single field definition stored in a vendor DataContract."""

    name: str
    field_type: str     # e.g. "INT", "STRING", "BIGINT", "FLOAT", "TIMESTAMP"
    nullable: bool = True  # True ↔ the contract allows NULL for this field


@dataclass
class DataContract:
    """
    Vendor data contract loaded from persistent storage (GCS, BigQuery, etc.).

    Fields are stored as a list — not a dict — so that insertion order is
    preserved for display and the rename-matching algorithm is deterministic.
    """

    vendor_id: str
    fields: list[ContractField]
    version: str = "1.0.0"


@dataclass
class DriftEvent:
    """
    A single detected schema difference between the contract and live schema.

    Attributes
    ----------
    vendor_id           : Owning vendor identifier.
    drift_type          : One of the four canonical DriftType values.
    field_name          : Affected field name.  For column_rename events this
                          is formatted as ``"old_name → new_name"``; for all
                          other drift types it is the bare field name.
    old_type            : Data type recorded in the stored contract.
                          None for new_column events.
    new_type            : Data type observed in the live schema.
                          None for dropped_column events.
    severity            : LOW | MEDIUM | HIGH.
    is_safe_to_auto_heal: True iff the platform may apply the change without
                          human approval (see module docstring for the full
                          decision table).
    """

    vendor_id: str
    drift_type: DriftType
    field_name: str
    old_type: Optional[str]
    new_type: Optional[str]
    severity: Severity
    is_safe_to_auto_heal: bool


@dataclass
class DriftReport:
    """
    Aggregated result of one schema comparison run.

    Attributes
    ----------
    vendor_id    : Vendor that owns the contract being evaluated.
    evaluated_at : UTC timestamp recorded when the comparison ran.
    has_drift    : Convenience flag — True iff ``drift_events`` is non-empty.
    drift_events : Ordered list of every detected DriftEvent in this batch.
    """

    vendor_id: str
    evaluated_at: datetime
    has_drift: bool
    drift_events: list[DriftEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Type widening / narrowing lookup tables
# ---------------------------------------------------------------------------

# Widening conversions: the destination type can represent every value the
# source type can, so no data is ever lost when the change is applied.
_WIDENING_CHANGES: frozenset[tuple[str, str]] = frozenset(
    {
        ("INT", "BIGINT"),    # INT value range ⊂ BIGINT value range
        ("INT", "STRING"),    # Every integer has an exact string representation
        ("BIGINT", "STRING"), # Same logic
        ("FLOAT", "DOUBLE"),  # Double precision is a strict superset of float
    }
)

# Narrowing conversions: the destination type cannot hold every value of the
# source type.  Automatic application risks silent truncation or import errors.
_NARROWING_CHANGES: frozenset[tuple[str, str]] = frozenset(
    {
        ("STRING", "INT"),    # Not all strings are valid integers
        ("STRING", "BIGINT"), # Same concern — parsing can fail at runtime
        ("STRING", "FLOAT"),  # Lossy string-to-float conversion
        ("STRING", "DOUBLE"), # Same
        ("BIGINT", "INT"),    # Values > INT_MAX would overflow silently
        ("DOUBLE", "FLOAT"),  # Precision loss for large double values
    }
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _classify_type_change(
    old_type: str,
    new_type: str,
    vendor_id: str,
    field_name: str,
) -> DriftEvent:
    """
    Build a TYPE_CHANGE DriftEvent for a single field.

    Decision table
    --------------
    Widening pair  (e.g. INT→BIGINT)  → LOW,    safe to auto-heal.
    Narrowing pair (e.g. STRING→INT)  → HIGH,   not safe.
    Nullability    (NULLABLE→REQUIRED)→ HIGH,   not safe — existing NULL rows
                                                would violate the new constraint.
    Anything else                     → MEDIUM, not safe (too ambiguous to judge).

    Parameters
    ----------
    old_type   : Upper-cased type string from the stored contract.
    new_type   : Upper-cased type string from the live schema.
    vendor_id  : Passed through to the DriftEvent for traceability.
    field_name : Passed through to the DriftEvent for traceability.
    """
    # Synthetic nullability types surfaced by compare_schemas when nullability
    # metadata is provided alongside the incoming schema.
    if old_type in ("NULLABLE", "REQUIRED") or new_type in ("NULLABLE", "REQUIRED"):
        # Tightening a NULLABLE field to REQUIRED is always dangerous because
        # existing NULL rows in the sink table would fail validation.
        return DriftEvent(
            vendor_id=vendor_id,
            drift_type=DriftType.TYPE_CHANGE,
            field_name=field_name,
            old_type=old_type,
            new_type=new_type,
            severity=Severity.HIGH,
            is_safe_to_auto_heal=False,
        )

    key = (old_type, new_type)  # both already upper-cased by the caller

    if key in _WIDENING_CHANGES:
        # Value domain only grows — zero risk of data loss.
        return DriftEvent(
            vendor_id=vendor_id,
            drift_type=DriftType.TYPE_CHANGE,
            field_name=field_name,
            old_type=old_type,
            new_type=new_type,
            severity=Severity.LOW,
            is_safe_to_auto_heal=True,
        )

    if key in _NARROWING_CHANGES:
        # Value domain shrinks — existing data may not fit the new type.
        return DriftEvent(
            vendor_id=vendor_id,
            drift_type=DriftType.TYPE_CHANGE,
            field_name=field_name,
            old_type=old_type,
            new_type=new_type,
            severity=Severity.HIGH,
            is_safe_to_auto_heal=False,
        )

    # Unknown / uncommon type pair — default to conservative MEDIUM / not safe.
    # A human reviewer should assess whether the conversion is lossless before
    # approving any schema migration.
    return DriftEvent(
        vendor_id=vendor_id,
        drift_type=DriftType.TYPE_CHANGE,
        field_name=field_name,
        old_type=old_type,
        new_type=new_type,
        severity=Severity.MEDIUM,
        is_safe_to_auto_heal=False,
    )


def _match_renames(
    dropped: dict[str, ContractField],
    added: dict[str, str],
) -> tuple[list[tuple[str, str]], dict[str, ContractField], dict[str, str]]:
    """
    Attempt to pair dropped fields with new fields that share the same type.

    Rename heuristic
    ----------------
    A rename is only inferred when there is *exactly one* dropped field and
    *exactly one* new field that share the same data type.  When multiple
    dropped or multiple new fields share a type (e.g. several STRING columns
    on both sides) the match is ambiguous, so we conservatively treat them
    as independent drops and additions rather than guessing at the pairing.

    Parameters
    ----------
    dropped : Fields present in the contract but absent from the live schema.
    added   : Fields present in the live schema but absent from the contract.

    Returns
    -------
    renames      : List of (old_name, new_name) pairs we are confident about.
    unmatched_d  : Remaining dropped fields after rename matching (true drops).
    unmatched_a  : Remaining added fields after rename matching (true adds).
    """
    # Index dropped fields by their type for O(1) lookup.
    dropped_by_type: dict[str, list[str]] = {}
    for fname, cf in dropped.items():
        dropped_by_type.setdefault(cf.field_type.upper(), []).append(fname)

    # Index added fields by their type.
    added_by_type: dict[str, list[str]] = {}
    for fname, ftype in added.items():
        added_by_type.setdefault(ftype.upper(), []).append(fname)

    renames: list[tuple[str, str]] = []
    matched_dropped: set[str] = set()
    matched_added: set[str] = set()

    for ftype, d_names in dropped_by_type.items():
        a_names = added_by_type.get(ftype, [])
        # 1:1 unambiguous match — safe to call this a rename.
        if len(d_names) == 1 and len(a_names) == 1:
            renames.append((d_names[0], a_names[0]))
            matched_dropped.add(d_names[0])
            matched_added.add(a_names[0])
        # Many-to-many: intentionally left unmatched (ambiguous).

    unmatched_d = {k: v for k, v in dropped.items() if k not in matched_dropped}
    unmatched_a = {k: v for k, v in added.items() if k not in matched_added}

    return renames, unmatched_d, unmatched_a


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Type alias for the incoming schema inferred from a Pub/Sub message batch.
# Maps field name → canonical type string (case-insensitive — normalised
# to upper-case internally).
# Example: {"user_id": "BIGINT", "event_name": "STRING", "ts": "TIMESTAMP"}
IncomingSchema = dict[str, str]


def compare_schemas(
    contract: DataContract,
    incoming: IncomingSchema,
    incoming_nullability: Optional[dict[str, bool]] = None,
) -> DriftReport:
    """
    Compare a Pub/Sub-inferred schema against a stored vendor DataContract.

    Parameters
    ----------
    contract             : The canonical schema on file for this vendor.
    incoming             : Flat dict mapping field names to type strings, as
                           produced by the schema-inference layer.
    incoming_nullability : Optional mapping of field name → is_nullable for
                           the incoming schema.  When provided, the comparator
                           also checks for nullability tightening
                           (NULLABLE contract field that the live schema now
                           treats as REQUIRED).

    Returns
    -------
    DriftReport
        Fully populated report; ``has_drift`` is False and ``drift_events``
        is empty when the schemas are identical.

    Algorithm
    ---------
    1. Normalise all type strings to upper-case.
    2. Partition fields into three groups:
       a. Common fields  → check for type changes and nullability changes.
       b. Only-contract  → candidate dropped columns or rename "old" sides.
       c. Only-incoming  → candidate new columns or rename "new" sides.
    3. Run the rename heuristic (``_match_renames``) on groups (b) and (c).
    4. Unmatched candidates become true drops / true additions.
    5. Collect all DriftEvents, sort for determinism, and return.
    """
    events: list[DriftEvent] = []

    # ------------------------------------------------------------------
    # Step 1 — Build normalised lookup structures.
    # ------------------------------------------------------------------

    # contract_map: field_name → ContractField (type preserved as-is for
    # the dataclass but normalised to upper-case when used in comparisons).
    contract_map: dict[str, ContractField] = {cf.name: cf for cf in contract.fields}

    # incoming_norm: field_name → upper-cased type string
    incoming_norm: dict[str, str] = {
        name: ftype.upper() for name, ftype in incoming.items()
    }

    contract_names = set(contract_map.keys())
    incoming_names = set(incoming_norm.keys())

    # ------------------------------------------------------------------
    # Step 2 — Partition fields.
    # ------------------------------------------------------------------

    common_names  = contract_names & incoming_names
    only_contract = contract_names - incoming_names   # potential drops / rename old side
    only_incoming = incoming_names - contract_names   # potential additions / rename new side

    # ------------------------------------------------------------------
    # Step 2a — Common fields: check type changes and nullability changes.
    # ------------------------------------------------------------------

    for name in sorted(common_names):
        cf = contract_map[name]
        contract_type = cf.field_type.upper()
        live_type = incoming_norm[name]

        # Detect raw type change (e.g. INT → BIGINT).
        if contract_type != live_type:
            events.append(
                _classify_type_change(contract_type, live_type, contract.vendor_id, name)
            )

        # Detect nullability tightening when the caller provided nullability
        # metadata for the incoming schema.
        if incoming_nullability is not None:
            live_nullable = incoming_nullability.get(name, True)
            # Only flag when the contract allowed NULLs but the live schema
            # now requires the field to be NOT NULL — the inverse (loosening)
            # is always backwards-compatible and not flagged.
            if cf.nullable and not live_nullable:
                events.append(
                    _classify_type_change(
                        "NULLABLE", "REQUIRED", contract.vendor_id, name
                    )
                )

    # ------------------------------------------------------------------
    # Step 3 — Rename detection.
    # ------------------------------------------------------------------

    dropped_candidates = {n: contract_map[n] for n in only_contract}
    added_candidates   = {n: incoming_norm[n] for n in only_incoming}

    renames, true_drops, true_adds = _match_renames(dropped_candidates, added_candidates)

    # Rename events — MEDIUM severity, never auto-healable.
    # Downstream consumers and downstream SQL almost certainly reference the
    # old column name, so a rename is a breaking change without a migration.
    for old_name, new_name in sorted(renames):
        shared_type = contract_map[old_name].field_type.upper()
        events.append(
            DriftEvent(
                vendor_id=contract.vendor_id,
                drift_type=DriftType.COLUMN_RENAME,
                field_name=f"{old_name} → {new_name}",
                old_type=shared_type,
                new_type=shared_type,   # type did not change, only the name
                severity=Severity.MEDIUM,
                is_safe_to_auto_heal=False,
            )
        )

    # ------------------------------------------------------------------
    # Step 4a — True dropped columns.
    # ------------------------------------------------------------------

    for name in sorted(true_drops.keys()):
        cf = true_drops[name]
        events.append(
            DriftEvent(
                vendor_id=contract.vendor_id,
                drift_type=DriftType.DROPPED_COLUMN,
                field_name=name,
                old_type=cf.field_type.upper(),
                new_type=None,
                severity=Severity.HIGH,
                # A dropped column means contract-expected data will never
                # arrive; any pipeline reading that column will break silently.
                is_safe_to_auto_heal=False,
            )
        )

    # ------------------------------------------------------------------
    # Step 4b — True new columns.
    # ------------------------------------------------------------------

    for name in sorted(true_adds.keys()):
        events.append(
            DriftEvent(
                vendor_id=contract.vendor_id,
                drift_type=DriftType.NEW_COLUMN,
                field_name=name,
                old_type=None,
                new_type=true_adds[name],
                severity=Severity.LOW,
                # Purely additive: no existing data is touched, and downstream
                # consumers that don't know about the new field are unaffected.
                is_safe_to_auto_heal=True,
            )
        )

    # ------------------------------------------------------------------
    # Step 5 — Assemble and return the DriftReport.
    # ------------------------------------------------------------------

    return DriftReport(
        vendor_id=contract.vendor_id,
        evaluated_at=datetime.now(timezone.utc),
        has_drift=bool(events),
        drift_events=events,
    )


# ---------------------------------------------------------------------------
# Demo entry-point
# ---------------------------------------------------------------------------


def _print_report(report: DriftReport) -> None:
    """Pretty-print a DriftReport to stdout."""
    print(f"\n{'=' * 60}")
    print(f"DriftReport  vendor={report.vendor_id}  has_drift={report.has_drift}")
    print(f"Evaluated at {report.evaluated_at.isoformat()}")
    print(f"{'=' * 60}")
    if not report.drift_events:
        print("  (no drift detected)")
    for evt in report.drift_events:
        print(
            f"  [{evt.severity.value:6}] {evt.drift_type.value:<16} "
            f"field={evt.field_name!r:<30} "
            f"old={evt.old_type!r:<12} new={evt.new_type!r:<12} "
            f"auto_heal={evt.is_safe_to_auto_heal}"
        )
    print()


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Demo: exercise all four drift types against a single base contract.
    # ------------------------------------------------------------------

    BASE_CONTRACT = DataContract(
        vendor_id="vendor_acme",
        version="2.1.0",
        fields=[
            ContractField(name="order_id",       field_type="INT",       nullable=False),
            ContractField(name="customer_name",   field_type="STRING",    nullable=True),
            ContractField(name="amount",          field_type="STRING",    nullable=True),
            ContractField(name="region",          field_type="STRING",    nullable=True),
            ContractField(name="created_at",      field_type="TIMESTAMP", nullable=True),
        ],
    )

    print("Base contract fields:")
    for f in BASE_CONTRACT.fields:
        print(f"  {f.name}: {f.field_type}  nullable={f.nullable}")

    # ---- Demo 1: NEW_COLUMN -------------------------------------------
    # The vendor started sending a new 'discount_pct' field not in the contract.
    print("\n--- Demo 1: NEW_COLUMN (discount_pct added) ---")
    incoming_new_col: IncomingSchema = {
        "order_id":      "INT",
        "customer_name": "STRING",
        "amount":        "STRING",
        "region":        "STRING",
        "created_at":    "TIMESTAMP",
        "discount_pct":  "FLOAT",   # <- new field
    }
    report_new = compare_schemas(BASE_CONTRACT, incoming_new_col)
    _print_report(report_new)

    # ---- Demo 2: DROPPED_COLUMN --------------------------------------
    # The vendor stopped sending 'region', which the contract requires.
    print("--- Demo 2: DROPPED_COLUMN (region missing) ---")
    incoming_drop: IncomingSchema = {
        "order_id":      "INT",
        "customer_name": "STRING",
        "amount":        "STRING",
        "created_at":    "TIMESTAMP",
        # 'region' absent
    }
    report_drop = compare_schemas(BASE_CONTRACT, incoming_drop)
    _print_report(report_drop)

    # ---- Demo 3: TYPE_CHANGE (widening and narrowing) ----------------
    # 'order_id' INT → BIGINT  (safe widening)
    # 'amount'   STRING → FLOAT (narrowing — not all strings are floats)
    print("--- Demo 3: TYPE_CHANGE (INT→BIGINT widening + STRING→FLOAT narrowing) ---")
    incoming_type: IncomingSchema = {
        "order_id":      "BIGINT",  # widening
        "customer_name": "STRING",
        "amount":        "FLOAT",   # narrowing
        "region":        "STRING",
        "created_at":    "TIMESTAMP",
    }
    report_type = compare_schemas(BASE_CONTRACT, incoming_type)
    _print_report(report_type)

    # ---- Demo 4: COLUMN_RENAME ----------------------------------------
    # Vendor renamed 'region' → 'territory' (same type STRING, 1:1 match).
    print("--- Demo 4: COLUMN_RENAME (region → territory) ---")
    incoming_rename: IncomingSchema = {
        "order_id":      "INT",
        "customer_name": "STRING",
        "amount":        "STRING",
        "territory":     "STRING",  # renamed from 'region'
        "created_at":    "TIMESTAMP",
    }
    report_rename = compare_schemas(BASE_CONTRACT, incoming_rename)
    _print_report(report_rename)

    # ---- Demo 5: NULLABILITY CHANGE -----------------------------------
    # 'customer_name' was nullable in the contract but the live schema marks
    # it as NOT NULL — a breaking tightening.
    print("--- Demo 5: NULLABILITY CHANGE (customer_name NULLABLE→REQUIRED) ---")
    incoming_nullability_demo: IncomingSchema = {
        "order_id":      "INT",
        "customer_name": "STRING",
        "amount":        "STRING",
        "region":        "STRING",
        "created_at":    "TIMESTAMP",
    }
    nullability_map = {
        "order_id":      False,   # was already required in contract
        "customer_name": False,   # contract says nullable=True → conflict!
        "amount":        True,
        "region":        True,
        "created_at":    True,
    }
    report_null = compare_schemas(
        BASE_CONTRACT, incoming_nullability_demo, incoming_nullability=nullability_map
    )
    _print_report(report_null)

    # ---- Demo 6: NO DRIFT --------------------------------------------
    print("--- Demo 6: NO DRIFT (schemas identical) ---")
    incoming_identical: IncomingSchema = {
        "order_id":      "INT",
        "customer_name": "STRING",
        "amount":        "STRING",
        "region":        "STRING",
        "created_at":    "TIMESTAMP",
    }
    report_none = compare_schemas(BASE_CONTRACT, incoming_identical)
    _print_report(report_none)

    sys.exit(0)
