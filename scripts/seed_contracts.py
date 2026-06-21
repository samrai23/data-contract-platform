"""
scripts/seed_contracts.py
==========================

Registers 3 sample vendor contracts by calling the live FastAPI API.

WHY SEED VIA THE API (not directly to BigQuery)?
-------------------------------------------------
We could write rows directly to BigQuery using the Python client.
But seeding through the API tests the full integration stack:
  HTTP request → FastAPI validation → BigQuery insert → response
If something is wrong with the API, validation, or BigQuery schema,
this script will catch it.  Direct BQ writes would bypass all of that.

PREREQUISITES:
  1. The contract-api container must be running:  make up
  2. The BigQuery tables must exist:              bash scripts/setup_bigquery.sh
  3. Your .env must have valid GCP credentials.

USAGE:
  # Seed against the local Docker container (default)
  python scripts/seed_contracts.py

  # Seed against a remote URL
  CONTRACT_API_URL=https://your-cloud-run-url.run.app python scripts/seed_contracts.py

  # Re-run safely — vendors that already exist return 409, which is logged as a
  # warning and does NOT stop the script.  No duplicates are created.
"""

from __future__ import annotations

import os
import sys

import httpx

API_BASE = os.getenv("CONTRACT_API_URL", "http://localhost:8000") + "/api/v1"

# ── Vendor definitions ─────────────────────────────────────────────────────────
# These three vendors represent the most common Delhi NCR data engineering
# interview scenarios.  The field schemas intentionally mirror what
# simulate_drift.py sends, so the end-to-end drift simulation works out of the box.

VENDORS = [
    {
        "vendor_id": "cars24",
        "vendor_name": "Cars24",
        "contact_email": "data-feeds@cars24.com",
        "owner_email": "de-oncall@yourcompany.com",
        "description": "Used vehicle listing feed — daily batch of ~500K records. "
                       "Each record is one vehicle listed for sale.",
        "fields": [
            {
                "name": "vehicle_id",
                "field_type": "STRING",
                "nullable": False,
                "description": "Unique vehicle listing identifier (e.g. DL-01-AB-1234)",
            },
            {
                "name": "user_id",
                "field_type": "INT",
                "nullable": True,
                "description": "Seller's internal user ID.  Drift scenario: vendor changes this to STRING.",
            },
            {
                "name": "price",
                "field_type": "DOUBLE",
                "nullable": True,
                "description": "Listed price in INR",
            },
            {
                "name": "listing_date",
                "field_type": "TIMESTAMP",
                "nullable": True,
                "description": "When the listing was published",
            },
            {
                "name": "city",
                "field_type": "STRING",
                "nullable": True,
                "description": "City where the vehicle is listed",
            },
            {
                "name": "fuel_type",
                "field_type": "STRING",
                "nullable": True,
                "description": "Petrol | Diesel | Electric | CNG",
            },
        ],
    },
    {
        "vendor_id": "paytm",
        "vendor_name": "Paytm",
        "contact_email": "data-engineering@paytm.com",
        "owner_email": "de-oncall@yourcompany.com",
        "description": "Payment transaction events — real-time stream via Pub/Sub. "
                       "High volume: ~2M transactions/day.",
        "fields": [
            {
                "name": "transaction_id",
                "field_type": "STRING",
                "nullable": False,
                "description": "UUID for this transaction",
            },
            {
                "name": "user_id",
                "field_type": "BIGINT",
                "nullable": False,
                "description": "Paytm user ID.  BIGINT because their user base exceeds INT max.",
            },
            {
                "name": "amount",
                "field_type": "DOUBLE",
                "nullable": False,
                "description": "Transaction amount in INR",
            },
            {
                "name": "currency",
                "field_type": "STRING",
                "nullable": True,
                "description": "ISO 4217 currency code (INR, USD, etc.)",
            },
            {
                "name": "created_at",
                "field_type": "TIMESTAMP",
                "nullable": False,
                "description": "Transaction timestamp in UTC",
            },
            {
                "name": "status",
                "field_type": "STRING",
                "nullable": True,
                "description": "SUCCESS | FAILED | PENDING | REFUNDED",
            },
            {
                "name": "payment_method",
                "field_type": "STRING",
                "nullable": True,
                "description": "UPI | CARD | WALLET | NET_BANKING",
            },
        ],
    },
    {
        "vendor_id": "policybazaar",
        "vendor_name": "PolicyBazaar",
        "contact_email": "data-ops@policybazaar.com",
        "owner_email": "de-oncall@yourcompany.com",
        "description": "Insurance policy applications — daily batch. "
                       "Each record is one submitted application.",
        "fields": [
            {
                "name": "policy_id",
                "field_type": "STRING",
                "nullable": False,
                "description": "Unique policy application identifier",
            },
            {
                "name": "applicant_id",
                "field_type": "STRING",
                "nullable": False,
                "description": "Applicant's ID (hashed PAN or Aadhaar token)",
            },
            {
                "name": "policy_type",
                "field_type": "STRING",
                "nullable": False,
                "description": "LIFE | HEALTH | AUTO | TERM | TRAVEL",
            },
            {
                "name": "premium_amount",
                "field_type": "DOUBLE",
                "nullable": True,
                "description": "Annual premium in INR",
            },
            {
                "name": "start_date",
                "field_type": "DATE",
                "nullable": True,
                "description": "Policy coverage start date",
            },
            {
                "name": "end_date",
                "field_type": "DATE",
                "nullable": True,
                "description": "Policy coverage end date",
            },
            {
                "name": "is_active",
                "field_type": "BOOLEAN",
                "nullable": True,
                "description": "Whether the policy is currently active",
            },
        ],
    },
]


# ── Seed function ──────────────────────────────────────────────────────────────

def seed_all() -> None:
    """
    POST each vendor to the API and report the result.

    Uses httpx.Client (synchronous) — no async needed for a one-shot script.
    timeout=30.0 gives BigQuery time to respond on the first cold query.
    """
    print(f"Seeding vendors to: {API_BASE}")
    print("=" * 60)

    success = 0
    skipped = 0
    failed = 0

    with httpx.Client(timeout=30.0) as client:
        for vendor in VENDORS:
            vendor_id = vendor["vendor_id"]
            try:
                response = client.post(f"{API_BASE}/vendors", json=vendor)

                if response.status_code == 201:
                    data = response.json()
                    print(f"✅  {vendor_id:20s}  v{data['version']}  registered")
                    success += 1

                elif response.status_code == 409:
                    # Vendor already exists — not an error, just a no-op
                    print(f"⚠️   {vendor_id:20s}  already registered — skipping")
                    skipped += 1

                else:
                    try:
                        detail = response.json().get('detail', response.text)
                    except Exception:
                        detail = response.text or "(empty response)"
                    print(f"❌  {vendor_id:20s}  HTTP {response.status_code}: {detail}")
                    failed += 1

            except httpx.ConnectError:
                print(
                    f"❌  Cannot connect to API at {API_BASE}.\n"
                    "    Is the contract-api container running?  Try: make up"
                )
                sys.exit(1)

    print("=" * 60)
    print(f"Done.  registered={success}  skipped={skipped}  failed={failed}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    seed_all()
