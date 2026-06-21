"""
tests/unit/test_schema_comparator.py
=====================================

Unit tests for services/drift_detector/schema_comparator.py.

Coverage targets
----------------
- All four DriftType values (column_rename, type_change, new_column, dropped_column).
- is_safe_to_auto_heal = True paths (INT→BIGINT, INT→STRING, new_column).
- is_safe_to_auto_heal = False paths (STRING→INT, dropped_column, NULLABLE→REQUIRED,
  column_rename).
- Severity assignment for each drift category.
- No-drift scenario (identical schemas).
- Nullability tightening via incoming_nullability parameter.
- Multiple simultaneous drift events in one batch.
- Rename heuristic: ambiguous many-to-many type matches are NOT classified as renames.

Dependencies: pytest only — no external libraries required.
"""

import pytest

from services.drift_detector.schema_comparator import (
    ContractField,
    DataContract,
    DriftEvent,
    DriftReport,
    DriftType,
    Severity,
    compare_schemas,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_contract(*fields: tuple) -> DataContract:
    """
    Build a DataContract from (name, type) or (name, type, nullable) tuples.

    Parameters
    ----------
    *fields : Each element may be (name, type) or (name, type, nullable).
              nullable defaults to True when omitted.
    """
    contract_fields = []
    for f in fields:
        name, ftype = f[0], f[1]
        nullable = f[2] if len(f) > 2 else True
        contract_fields.append(ContractField(name=name, field_type=ftype, nullable=nullable))
    return DataContract(vendor_id="test_vendor", fields=contract_fields)


def _single_event(report: DriftReport) -> DriftEvent:
    """Assert exactly one event in the report and return it."""
    assert len(report.drift_events) == 1, (
        f"Expected 1 drift event, got {len(report.drift_events)}: {report.drift_events}"
    )
    return report.drift_events[0]


# ---------------------------------------------------------------------------
# No drift
# ---------------------------------------------------------------------------


class TestNoDrift:
    def test_identical_schemas_produce_no_events(self):
        contract = _make_contract(
            ("user_id", "INT"),
            ("email", "STRING"),
            ("score", "FLOAT"),
        )
        incoming = {"user_id": "INT", "email": "STRING", "score": "FLOAT"}
        report = compare_schemas(contract, incoming)

        assert report.has_drift is False
        assert report.drift_events == []
        assert report.vendor_id == "test_vendor"

    def test_type_case_insensitive(self):
        """Incoming schema with lower-case types must match contract upper-case types."""
        contract = _make_contract(("amount", "BIGINT"))
        incoming = {"amount": "bigint"}
        report = compare_schemas(contract, incoming)

        assert report.has_drift is False

    def test_empty_schemas(self):
        """Two empty schemas — zero fields on both sides — should produce no drift."""
        contract = DataContract(vendor_id="test_vendor", fields=[])
        report = compare_schemas(contract, {})

        assert report.has_drift is False
        assert report.drift_events == []


# ---------------------------------------------------------------------------
# new_column
# ---------------------------------------------------------------------------


class TestNewColumn:
    def test_new_column_detected(self):
        contract = _make_contract(("id", "INT"), ("name", "STRING"))
        incoming = {"id": "INT", "name": "STRING", "extra": "BOOLEAN"}

        report = compare_schemas(contract, incoming)
        evt = _single_event(report)

        assert report.has_drift is True
        assert evt.drift_type == DriftType.NEW_COLUMN
        assert evt.field_name == "extra"
        assert evt.old_type is None
        assert evt.new_type == "BOOLEAN"

    def test_new_column_severity_is_low(self):
        contract = _make_contract(("id", "INT"))
        incoming = {"id": "INT", "new_field": "STRING"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.severity == Severity.LOW

    def test_new_column_is_safe_to_auto_heal(self):
        """Adding a column is purely additive — no existing data is affected."""
        contract = _make_contract(("id", "INT"))
        incoming = {"id": "INT", "tag": "STRING"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.is_safe_to_auto_heal is True

    def test_multiple_new_columns(self):
        contract = _make_contract(("id", "INT"))
        incoming = {"id": "INT", "col_a": "STRING", "col_b": "FLOAT"}

        report = compare_schemas(contract, incoming)
        assert len(report.drift_events) == 2
        assert all(e.drift_type == DriftType.NEW_COLUMN for e in report.drift_events)
        assert all(e.is_safe_to_auto_heal is True for e in report.drift_events)


# ---------------------------------------------------------------------------
# dropped_column
# ---------------------------------------------------------------------------


class TestDroppedColumn:
    def test_dropped_column_detected(self):
        contract = _make_contract(("id", "INT"), ("legacy_field", "STRING"))
        incoming = {"id": "INT"}  # legacy_field is absent

        report = compare_schemas(contract, incoming)
        evt = _single_event(report)

        assert report.has_drift is True
        assert evt.drift_type == DriftType.DROPPED_COLUMN
        assert evt.field_name == "legacy_field"
        assert evt.old_type == "STRING"
        assert evt.new_type is None

    def test_dropped_column_severity_is_high(self):
        contract = _make_contract(("id", "INT"), ("critical", "BIGINT"))
        incoming = {"id": "INT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.severity == Severity.HIGH

    def test_dropped_column_is_not_safe_to_auto_heal(self):
        """Dropping a column means contracted data will never arrive — not auto-healable."""
        contract = _make_contract(("id", "INT"), ("revenue", "FLOAT"))
        incoming = {"id": "INT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.is_safe_to_auto_heal is False

    def test_multiple_dropped_columns(self):
        contract = _make_contract(("id", "INT"), ("col_a", "STRING"), ("col_b", "BIGINT"))
        incoming = {"id": "INT"}

        report = compare_schemas(contract, incoming)
        assert len(report.drift_events) == 2
        assert all(e.drift_type == DriftType.DROPPED_COLUMN for e in report.drift_events)
        assert all(e.is_safe_to_auto_heal is False for e in report.drift_events)


# ---------------------------------------------------------------------------
# type_change — widening (safe)
# ---------------------------------------------------------------------------


class TestTypeChangeWidening:
    @pytest.mark.parametrize(
        "old_type, new_type",
        [
            ("INT", "BIGINT"),
            ("INT", "STRING"),
            ("BIGINT", "STRING"),
            ("FLOAT", "DOUBLE"),
        ],
    )
    def test_widening_is_low_severity(self, old_type, new_type):
        contract = _make_contract(("value", old_type))
        incoming = {"value": new_type}

        evt = _single_event(compare_schemas(contract, incoming))

        assert evt.drift_type == DriftType.TYPE_CHANGE
        assert evt.severity == Severity.LOW

    @pytest.mark.parametrize(
        "old_type, new_type",
        [
            ("INT", "BIGINT"),
            ("INT", "STRING"),
            ("BIGINT", "STRING"),
            ("FLOAT", "DOUBLE"),
        ],
    )
    def test_widening_is_safe_to_auto_heal(self, old_type, new_type):
        """Widening conversions never lose data — auto-heal is permitted."""
        contract = _make_contract(("value", old_type))
        incoming = {"value": new_type}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.is_safe_to_auto_heal is True

    def test_widening_records_correct_types(self):
        contract = _make_contract(("order_id", "INT"))
        incoming = {"order_id": "BIGINT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.old_type == "INT"
        assert evt.new_type == "BIGINT"


# ---------------------------------------------------------------------------
# type_change — narrowing (unsafe)
# ---------------------------------------------------------------------------


class TestTypeChangeNarrowing:
    @pytest.mark.parametrize(
        "old_type, new_type",
        [
            ("STRING", "INT"),
            ("STRING", "BIGINT"),
            ("STRING", "FLOAT"),
            ("STRING", "DOUBLE"),
            ("BIGINT", "INT"),
            ("DOUBLE", "FLOAT"),
        ],
    )
    def test_narrowing_is_high_severity(self, old_type, new_type):
        contract = _make_contract(("col", old_type))
        incoming = {"col": new_type}

        evt = _single_event(compare_schemas(contract, incoming))

        assert evt.drift_type == DriftType.TYPE_CHANGE
        assert evt.severity == Severity.HIGH

    @pytest.mark.parametrize(
        "old_type, new_type",
        [
            ("STRING", "INT"),
            ("STRING", "BIGINT"),
            ("BIGINT", "INT"),
        ],
    )
    def test_narrowing_is_not_safe_to_auto_heal(self, old_type, new_type):
        """Narrowing risks data loss — must not be auto-healed."""
        contract = _make_contract(("col", old_type))
        incoming = {"col": new_type}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.is_safe_to_auto_heal is False

    def test_string_to_int_records_correct_types(self):
        contract = _make_contract(("amount", "STRING"))
        incoming = {"amount": "INT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.old_type == "STRING"
        assert evt.new_type == "INT"


# ---------------------------------------------------------------------------
# type_change — unknown / other (medium default)
# ---------------------------------------------------------------------------


class TestTypeChangeUnknown:
    def test_uncommon_type_pair_is_medium_severity(self):
        contract = _make_contract(("col", "DATE"))
        incoming = {"col": "TIMESTAMP"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.severity == Severity.MEDIUM

    def test_uncommon_type_pair_is_not_safe_to_auto_heal(self):
        contract = _make_contract(("col", "DATE"))
        incoming = {"col": "TIMESTAMP"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.is_safe_to_auto_heal is False


# ---------------------------------------------------------------------------
# column_rename
# ---------------------------------------------------------------------------


class TestColumnRename:
    def test_rename_detected_on_1to1_type_match(self):
        """
        Contract has 'region: STRING', incoming has 'territory: STRING'.
        No other STRING fields in the delta → unambiguous rename.
        """
        contract = _make_contract(
            ("id", "INT"),
            ("region", "STRING"),
            ("score", "FLOAT"),
        )
        incoming = {
            "id": "INT",
            "territory": "STRING",   # renamed from 'region'
            "score": "FLOAT",
        }
        report = compare_schemas(contract, incoming)
        evt = _single_event(report)

        assert evt.drift_type == DriftType.COLUMN_RENAME
        assert "region" in evt.field_name
        assert "territory" in evt.field_name

    def test_rename_severity_is_medium(self):
        contract = _make_contract(("id", "INT"), ("old_name", "BIGINT"))
        incoming = {"id": "INT", "new_name": "BIGINT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.severity == Severity.MEDIUM

    def test_rename_is_not_safe_to_auto_heal(self):
        """Column renames break downstream consumers that reference the old name."""
        contract = _make_contract(("id", "INT"), ("customer_id", "INT"))
        incoming = {"id": "INT", "client_id": "INT"}

        evt = _single_event(compare_schemas(contract, incoming))

        assert evt.drift_type == DriftType.COLUMN_RENAME
        assert evt.is_safe_to_auto_heal is False

    def test_rename_old_and_new_type_are_equal(self):
        """A rename does not change the field type; both old_type and new_type match."""
        contract = _make_contract(("id", "INT"), ("orig", "FLOAT"))
        incoming = {"id": "INT", "dest": "FLOAT"}

        evt = _single_event(compare_schemas(contract, incoming))
        assert evt.old_type == evt.new_type == "FLOAT"

    def test_ambiguous_rename_not_classified(self):
        """
        Two dropped STRINGs and two new STRINGs — the heuristic cannot determine
        which maps to which, so all four are emitted as separate drop/add events.
        """
        contract = _make_contract(
            ("id", "INT"),
            ("city", "STRING"),
            ("country", "STRING"),
        )
        incoming = {
            "id": "INT",
            "town": "STRING",    # ambiguous — 2 STRING drops, 2 STRING adds
            "nation": "STRING",
        }
        report = compare_schemas(contract, incoming)

        types = {e.drift_type for e in report.drift_events}
        # Must NOT contain a rename; should be drops + adds only.
        assert DriftType.COLUMN_RENAME not in types
        assert DriftType.DROPPED_COLUMN in types
        assert DriftType.NEW_COLUMN in types


# ---------------------------------------------------------------------------
# Nullability changes
# ---------------------------------------------------------------------------


class TestNullabilityChange:
    def test_nullable_to_required_is_high_severity(self):
        """
        Contract marks 'email' as nullable=True.  Live schema now requires it.
        This is a breaking change — existing NULL rows would fail validation.
        id is already non-nullable in both the contract and live schema to keep
        it out of the drift event count.
        """
        contract = _make_contract(("id", "INT", False), ("email", "STRING", True))
        incoming = {"id": "INT", "email": "STRING"}
        nullability = {"id": False, "email": False}  # email now NOT NULL in live schema

        report = compare_schemas(contract, incoming, incoming_nullability=nullability)
        evt = _single_event(report)

        assert evt.drift_type == DriftType.TYPE_CHANGE
        assert evt.old_type == "NULLABLE"
        assert evt.new_type == "REQUIRED"
        assert evt.severity == Severity.HIGH

    def test_nullable_to_required_is_not_safe_to_auto_heal(self):
        contract = _make_contract(("id", "INT", False), ("name", "STRING", True))
        incoming = {"id": "INT", "name": "STRING"}
        nullability = {"id": False, "name": False}

        report = compare_schemas(contract, incoming, incoming_nullability=nullability)
        evt = _single_event(report)
        assert evt.is_safe_to_auto_heal is False

    def test_required_to_nullable_does_not_flag(self):
        """
        Loosening a required field to nullable is always backwards-compatible
        and must NOT produce a drift event.
        """
        contract = _make_contract(("id", "INT", False))   # nullable=False in contract
        incoming = {"id": "INT"}
        nullability = {"id": True}   # now nullable in live schema — loosening

        report = compare_schemas(contract, incoming, incoming_nullability=nullability)
        assert report.has_drift is False

    def test_no_nullability_param_skips_nullability_check(self):
        """Without incoming_nullability, no nullability events should be emitted."""
        contract = _make_contract(("id", "INT", False), ("name", "STRING", True))
        incoming = {"id": "INT", "name": "STRING"}

        report = compare_schemas(contract, incoming)   # no nullability arg
        assert report.has_drift is False


# ---------------------------------------------------------------------------
# Multiple simultaneous drift events
# ---------------------------------------------------------------------------


class TestMultipleDrifts:
    def test_all_four_drift_types_in_one_batch(self):
        """
        Base contract: id (INT), region (STRING), score (FLOAT), legacy (BIGINT)

        Incoming schema:
          - id: BIGINT        → type_change widening (INT→BIGINT)
          - territory: STRING → rename from 'region' (1:1 STRING match)
          - score: STRING     → type_change narrowing ← wait, FLOAT→STRING is widening
          - new_tag: BOOLEAN  → new_column
          - 'legacy' absent   → dropped_column
        """
        contract = _make_contract(
            ("id", "INT"),
            ("region", "STRING"),
            ("amount", "STRING"),
            ("legacy", "BIGINT"),
        )
        incoming = {
            "id": "BIGINT",      # widening INT→BIGINT
            "territory": "STRING",  # rename from 'region' (1:1 STRING match)
            # 'amount' kept as STRING — no drift
            "amount": "INT",     # narrowing STRING→INT
            "new_tag": "BOOLEAN",   # new column
            # 'legacy' dropped
        }
        report = compare_schemas(contract, incoming)

        drift_types = {e.drift_type for e in report.drift_events}
        assert DriftType.TYPE_CHANGE in drift_types       # id INT→BIGINT AND amount STRING→INT
        assert DriftType.COLUMN_RENAME in drift_types     # region → territory
        assert DriftType.NEW_COLUMN in drift_types        # new_tag
        assert DriftType.DROPPED_COLUMN in drift_types    # legacy
        assert report.has_drift is True

    def test_has_drift_false_only_when_no_events(self):
        contract = _make_contract(("id", "INT"))
        report_clean = compare_schemas(contract, {"id": "INT"})
        report_dirty = compare_schemas(contract, {"id": "BIGINT"})

        assert report_clean.has_drift is False
        assert report_dirty.has_drift is True

    def test_vendor_id_propagated_to_all_events(self):
        contract = DataContract(
            vendor_id="vendor_xyz",
            fields=[
                ContractField("a", "INT"),
                ContractField("b", "STRING"),
            ],
        )
        incoming = {"a": "BIGINT", "c": "FLOAT"}  # type change + new + dropped

        report = compare_schemas(contract, incoming)
        assert all(e.vendor_id == "vendor_xyz" for e in report.drift_events)


# ---------------------------------------------------------------------------
# is_safe_to_auto_heal — consolidated truth table
# ---------------------------------------------------------------------------


class TestAutoHealTruthTable:
    """
    Exhaustive checks for every entry in the auto-heal decision table
    documented in the module docstring.
    """

    # ---- Safe = True ----

    def test_int_to_bigint_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "INT")), {"x": "BIGINT"}))
        assert evt.is_safe_to_auto_heal is True

    def test_int_to_string_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "INT")), {"x": "STRING"}))
        assert evt.is_safe_to_auto_heal is True

    def test_bigint_to_string_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "BIGINT")), {"x": "STRING"}))
        assert evt.is_safe_to_auto_heal is True

    def test_float_to_double_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "FLOAT")), {"x": "DOUBLE"}))
        assert evt.is_safe_to_auto_heal is True

    def test_new_column_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "INT")), {"x": "INT", "y": "STRING"}))
        assert evt.is_safe_to_auto_heal is True

    # ---- Safe = False ----

    def test_string_to_int_not_safe(self):
        evt = _single_event(compare_schemas(_make_contract(("x", "STRING")), {"x": "INT"}))
        assert evt.is_safe_to_auto_heal is False

    def test_dropped_column_not_safe(self):
        contract = _make_contract(("x", "INT"), ("y", "STRING"))
        evt = _single_event(compare_schemas(contract, {"x": "INT"}))
        assert evt.is_safe_to_auto_heal is False

    def test_nullable_to_required_not_safe(self):
        contract = _make_contract(("x", "INT", True))
        report = compare_schemas(contract, {"x": "INT"}, incoming_nullability={"x": False})
        evt = _single_event(report)
        assert evt.is_safe_to_auto_heal is False

    def test_column_rename_not_safe(self):
        contract = _make_contract(("old_col", "FLOAT"))
        evt = _single_event(compare_schemas(contract, {"new_col": "FLOAT"}))
        assert evt.is_safe_to_auto_heal is False
