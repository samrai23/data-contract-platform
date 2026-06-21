"""
tests/integration/test_api_endpoints.py
=========================================

Integration tests for services/contract_api — the FastAPI layer.

WHY NO REAL BIGQUERY HERE?
---------------------------
BigQuery doesn't have a local emulator. Rather than requiring a GCP
project just to run tests, we use FastAPI's dependency_overrides system
to inject a mock BigQuery client.

This means every test runs fast (no network), works offline, and doesn't
cost money.  The trade-off: we're testing that our code calls BigQuery
correctly, not that BigQuery itself behaves as expected.

HOW FastAPI DEPENDENCY OVERRIDES WORK:
---------------------------------------
Normally FastAPI calls get_bq_client() to get the BigQuery client.
We replace it with a lambda that returns a MagicMock instead:

    app.dependency_overrides[get_bq_client] = lambda: mock_bq_client

FastAPI uses this override for every request in the test.
The reset_overrides fixture clears overrides after each test so they
don't bleed into the next test.

WHAT WE'RE TESTING:
--------------------
- Routing: does each URL dispatch to the right handler?
- Validation: does Pydantic reject bad input with 422?
- Business logic: does duplicate vendor → 409, missing vendor → 404?
- Response shape: does the JSON match our Pydantic response models?
- BigQuery interaction: does the handler call the right BQ methods?
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest
from fastapi.testclient import TestClient

from dependencies import get_bq_client, get_dataset_ref
from main import app

# ── Constants ──────────────────────────────────────────────────────────────────

FAKE_DATASET = "test-project.test_dataset"
FAKE_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ── Mock row helper ────────────────────────────────────────────────────────────

class FakeBQRow:
    """
    Simulates a BigQuery Row object.

    BigQuery rows support attribute access (row.vendor_id), not just dict access
    (row["vendor_id"]).  Our route handlers use attribute access, so MagicMock
    alone won't work — we need this simple class.
    """
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ── Mock client factory ────────────────────────────────────────────────────────

def _mock_bq(
    query_results: list | None = None,
    insert_errors: list | None = None,
) -> MagicMock:
    """
    Returns a MagicMock that behaves like bigquery.Client.

    query_results → what bq.query(...).result() returns (a list of FakeBQRow)
    insert_errors → what bq.insert_rows_json() returns ([] means success)
    """
    mock = MagicMock()
    mock.query.return_value.result.return_value = query_results or []
    mock.insert_rows_json.return_value = insert_errors or []
    return mock


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_overrides():
    """Clear ALL dependency overrides after every test to prevent bleed-through."""
    # Always override dataset so tests never use a real project
    app.dependency_overrides[get_dataset_ref] = lambda: FAKE_DATASET
    yield
    app.dependency_overrides.clear()


# ── POST /vendors ──────────────────────────────────────────────────────────────

class TestRegisterVendor:
    VALID_BODY = {
        "vendor_id": "cars24",
        "vendor_name": "Cars24",
        "contact_email": "data@cars24.com",
        "owner_email": "de@company.com",
        "description": "Vehicle listings",
        "fields": [
            {"name": "vehicle_id", "field_type": "STRING", "nullable": False},
            {"name": "user_id",    "field_type": "INT",    "nullable": True},
            {"name": "price",      "field_type": "DOUBLE", "nullable": True},
        ],
    }

    def test_new_vendor_returns_201(self, client):
        # BQ check: no existing vendor (empty result)
        # BQ insert: success (empty error list)
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[],   # no existing vendor
            insert_errors=[],   # insert succeeds
        )
        resp = client.post("/api/v1/vendors", json=self.VALID_BODY)
        assert resp.status_code == 201

    def test_new_vendor_response_has_correct_shape(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        resp = client.post("/api/v1/vendors", json=self.VALID_BODY)
        body = resp.json()
        assert body["vendor_id"] == "cars24"
        assert body["version"] == "1.0.0"
        assert body["status"] == "ACTIVE"
        assert len(body["fields"]) == 3
        assert body["fields"][0]["name"] == "vehicle_id"

    def test_duplicate_vendor_returns_409(self, client):
        # Simulate BQ returning an existing row (vendor already registered)
        existing_row = FakeBQRow(vendor_id="cars24")
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[existing_row],   # found existing vendor
        )
        resp = client.post("/api/v1/vendors", json=self.VALID_BODY)
        assert resp.status_code == 409
        assert "already has an active contract" in resp.json()["detail"]

    def test_bq_insert_failure_returns_500(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[],
            insert_errors=[{"index": 0, "errors": [{"reason": "timeout"}]}],
        )
        resp = client.post("/api/v1/vendors", json=self.VALID_BODY)
        assert resp.status_code == 500

    def test_invalid_vendor_id_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad = {**self.VALID_BODY, "vendor_id": "Cars 24"}   # spaces + uppercase
        resp = client.post("/api/v1/vendors", json=bad)
        assert resp.status_code == 422

    def test_invalid_field_name_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad_fields = [{**self.VALID_BODY["fields"][0], "name": "vehicleId"}]  # camelCase
        bad = {**self.VALID_BODY, "fields": bad_fields}
        resp = client.post("/api/v1/vendors", json=bad)
        assert resp.status_code == 422

    def test_invalid_field_type_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad_fields = [{**self.VALID_BODY["fields"][0], "field_type": "VARCHAR"}]  # not in our enum
        bad = {**self.VALID_BODY, "fields": bad_fields}
        resp = client.post("/api/v1/vendors", json=bad)
        assert resp.status_code == 422

    def test_invalid_email_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad = {**self.VALID_BODY, "contact_email": "not-an-email"}
        resp = client.post("/api/v1/vendors", json=bad)
        assert resp.status_code == 422

    def test_empty_fields_list_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad = {**self.VALID_BODY, "fields": []}
        resp = client.post("/api/v1/vendors", json=bad)
        assert resp.status_code == 422


# ── GET /vendors ───────────────────────────────────────────────────────────────

class TestListVendors:
    def test_returns_200_with_list(self, client):
        rows = [
            FakeBQRow(
                vendor_id="cars24",
                vendor_name="Cars24",
                version="1.0.0",
                status="ACTIVE",
                created_at=FAKE_NOW,
                owner_email="de@company.com",
            ),
        ]
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=rows)
        resp = client.get("/api/v1/vendors")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["vendor_id"] == "cars24"

    def test_empty_list_when_no_vendors(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.get("/api/v1/vendors")
        assert resp.status_code == 200
        assert resp.json() == []


# ── GET /contracts/{vendor_id} ─────────────────────────────────────────────────

class TestGetContract:
    FIELDS_JSON = json.dumps([
        {"name": "vehicle_id", "field_type": "STRING", "nullable": False, "description": ""},
        {"name": "user_id",    "field_type": "INT",    "nullable": True,  "description": ""},
    ])

    def _make_row(self):
        return FakeBQRow(
            vendor_id="cars24",
            vendor_name="Cars24",
            version="1.0.0",
            status="ACTIVE",
            contract_json=self.FIELDS_JSON,
            description="Vehicle listings",
            owner_email="de@company.com",
            created_at=FAKE_NOW,
            updated_at=FAKE_NOW,
        )

    def test_existing_vendor_returns_200(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[self._make_row()]
        )
        resp = client.get("/api/v1/contracts/cars24")
        assert resp.status_code == 200
        body = resp.json()
        assert body["vendor_id"] == "cars24"
        assert len(body["fields"]) == 2
        assert body["fields"][0]["name"] == "vehicle_id"

    def test_unknown_vendor_returns_404(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.get("/api/v1/contracts/unknown-vendor")
        assert resp.status_code == 404
        assert "No active contract" in resp.json()["detail"]


# ── GET /drift-log ─────────────────────────────────────────────────────────────

class TestDriftLog:
    def test_returns_200_empty_list_before_phase3(self, client):
        # In Phase 2 the drift_events table is empty — expected behaviour
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.get("/api/v1/drift-log")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_limit_param_accepted(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.get("/api/v1/drift-log?limit=10")
        assert resp.status_code == 200

    def test_limit_over_max_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        resp = client.get("/api/v1/drift-log?limit=999")
        assert resp.status_code == 422

    def test_vendor_filter_param_accepted(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.get("/api/v1/drift-log?vendor_id=cars24")
        assert resp.status_code == 200


# ── POST /approve/{event_id} ───────────────────────────────────────────────────

class TestApprove:
    VALID_BODY = {"approved": True, "approved_by": "lead@company.com", "notes": "Looks safe"}

    def test_approve_pending_event(self, client):
        mock_bq = MagicMock()
        # First call (check query) returns a PENDING_APPROVAL row
        pending_row = FakeBQRow(event_id="evt-001", resolution_status="PENDING_APPROVAL")
        mock_bq.query.return_value.result.return_value = [pending_row]

        app.dependency_overrides[get_bq_client] = lambda: mock_bq
        resp = client.post("/api/v1/approve/evt-001", json=self.VALID_BODY)
        assert resp.status_code == 200
        body = resp.json()
        assert body["event_id"] == "evt-001"
        assert body["approved"] is True

    def test_unknown_event_returns_404(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(query_results=[])
        resp = client.post("/api/v1/approve/no-such-event", json=self.VALID_BODY)
        assert resp.status_code == 404

    def test_already_resolved_event_returns_409(self, client):
        already_done = FakeBQRow(event_id="evt-002", resolution_status="AUTO_HEALED")
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[already_done]
        )
        resp = client.post("/api/v1/approve/evt-002", json=self.VALID_BODY)
        assert resp.status_code == 409

    def test_invalid_email_in_approved_by_returns_422(self, client):
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq()
        bad = {**self.VALID_BODY, "approved_by": "not-an-email"}
        resp = client.post("/api/v1/approve/evt-001", json=bad)
        assert resp.status_code == 422


# ── GET /health ────────────────────────────────────────────────────────────────

class TestHealth:
    def test_healthy_when_bq_reachable(self, client):
        # bq.query("SELECT 1").result() returns something truthy
        app.dependency_overrides[get_bq_client] = lambda: _mock_bq(
            query_results=[FakeBQRow(ok=1)]
        )
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["bigquery_connected"] is True

    def test_degraded_when_bq_unreachable(self, client):
        # Simulate BQ being down by raising an exception
        mock_bq = MagicMock()
        mock_bq.query.return_value.result.side_effect = Exception("Connection refused")
        app.dependency_overrides[get_bq_client] = lambda: mock_bq

        resp = client.get("/api/v1/health")
        assert resp.status_code == 200          # API itself is still up
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["bigquery_connected"] is False
