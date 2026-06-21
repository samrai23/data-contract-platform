"""
tests/unit/test_contract_validator.py
=======================================

Unit tests for data_contracts/templates/pydantic_contract.py

No external dependencies — Pydantic raises ValidationError on invalid
input, which is what we're verifying here.
"""

import pytest
from pydantic import ValidationError

from pydantic_contract import (
    ContractStatus,
    FieldType,
    VendorContract,
    VendorContractField,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _field(**kwargs) -> VendorContractField:
    """Build a valid field, overriding any defaults with kwargs."""
    defaults = {"name": "user_id", "field_type": FieldType.INT}
    defaults.update(kwargs)
    return VendorContractField(**defaults)


def _contract(**kwargs) -> VendorContract:
    """Build a valid contract, overriding any defaults with kwargs."""
    defaults = {
        "vendor_id": "cars24",
        "version": "1.0.0",
        "fields": [_field()],
        "owner_email": "de-team@company.com",
    }
    defaults.update(kwargs)
    return VendorContract(**defaults)


# ── Field name validation ──────────────────────────────────────────────────────

class TestFieldNameValidation:
    def test_valid_snake_case_accepted(self):
        f = _field(name="vehicle_id")
        assert f.name == "vehicle_id"

    def test_single_word_accepted(self):
        f = _field(name="price")
        assert f.name == "price"

    def test_name_with_numbers_accepted(self):
        f = _field(name="address2")
        assert f.name == "address2"

    def test_camel_case_rejected(self):
        with pytest.raises(ValidationError, match="snake_case"):
            _field(name="userId")

    def test_uppercase_rejected(self):
        with pytest.raises(ValidationError):
            _field(name="USER_ID")

    def test_hyphen_rejected(self):
        with pytest.raises(ValidationError):
            _field(name="user-id")

    def test_starts_with_digit_rejected(self):
        with pytest.raises(ValidationError):
            _field(name="2nd_field")

    def test_spaces_rejected(self):
        with pytest.raises(ValidationError):
            _field(name="user id")


# ── Field defaults ─────────────────────────────────────────────────────────────

class TestFieldDefaults:
    def test_nullable_defaults_to_true(self):
        f = _field()
        assert f.nullable is True

    def test_description_defaults_to_empty(self):
        f = _field()
        assert f.description == ""

    def test_nullable_can_be_set_false(self):
        f = _field(nullable=False)
        assert f.nullable is False


# ── Contract vendor_id validation ──────────────────────────────────────────────

class TestVendorIdValidation:
    def test_lowercase_slug_accepted(self):
        c = _contract(vendor_id="cars24")
        assert c.vendor_id == "cars24"

    def test_underscore_allowed(self):
        c = _contract(vendor_id="make_my_trip")
        assert c.vendor_id == "make_my_trip"

    def test_hyphen_allowed(self):
        c = _contract(vendor_id="policy-bazaar")
        assert c.vendor_id == "policy-bazaar"

    def test_uppercase_rejected(self):
        with pytest.raises(ValidationError, match="vendor_id"):
            _contract(vendor_id="Cars24")

    def test_spaces_rejected(self):
        with pytest.raises(ValidationError):
            _contract(vendor_id="cars 24")

    def test_dot_rejected(self):
        with pytest.raises(ValidationError):
            _contract(vendor_id="cars.24")


# ── Contract version validation ────────────────────────────────────────────────

class TestVersionValidation:
    def test_valid_semver_accepted(self):
        c = _contract(version="1.0.0")
        assert c.version == "1.0.0"

    def test_semver_with_non_zero_parts_accepted(self):
        c = _contract(version="2.13.7")
        assert c.version == "2.13.7"

    def test_v_prefix_rejected(self):
        with pytest.raises(ValidationError, match="semantic versioning"):
            _contract(version="v1.0.0")

    def test_two_part_version_rejected(self):
        with pytest.raises(ValidationError):
            _contract(version="1.0")

    def test_text_version_rejected(self):
        with pytest.raises(ValidationError):
            _contract(version="latest")


# ── Contract fields validation ─────────────────────────────────────────────────

class TestContractFieldsValidation:
    def test_empty_fields_rejected(self):
        with pytest.raises(ValidationError, match="at least 1"):
            _contract(fields=[])

    def test_duplicate_field_names_rejected(self):
        with pytest.raises(ValidationError, match="Duplicate field names"):
            _contract(fields=[
                _field(name="user_id"),
                _field(name="user_id"),   # duplicate
            ])

    def test_unique_field_names_accepted(self):
        c = _contract(fields=[
            _field(name="user_id"),
            _field(name="price", field_type=FieldType.DOUBLE),
        ])
        assert len(c.fields) == 2


# ── Contract status ────────────────────────────────────────────────────────────

class TestContractStatus:
    def test_status_defaults_to_active(self):
        c = _contract()
        assert c.status == ContractStatus.ACTIVE

    def test_status_can_be_set_to_draft(self):
        c = _contract(status=ContractStatus.DRAFT)
        assert c.status == ContractStatus.DRAFT


# ── Version bumping ────────────────────────────────────────────────────────────

class TestVersionBumping:
    def test_bump_minor_increments_minor(self):
        c = _contract(version="1.0.0")
        bumped = c.bump_minor()
        assert bumped.version == "1.1.0"

    def test_bump_minor_resets_patch(self):
        c = _contract(version="1.2.5")
        bumped = c.bump_minor()
        assert bumped.version == "1.3.0"

    def test_bump_major_increments_major(self):
        c = _contract(version="1.2.3")
        bumped = c.bump_major()
        assert bumped.version == "2.0.0"

    def test_bump_major_resets_minor_and_patch(self):
        c = _contract(version="3.7.12")
        bumped = c.bump_major()
        assert bumped.version == "4.0.0"

    def test_bump_returns_new_instance(self):
        c = _contract(version="1.0.0")
        bumped = c.bump_minor()
        assert c.version == "1.0.0"       # original unchanged
        assert bumped.version == "1.1.0"  # new instance has new version
        assert c is not bumped            # different objects
