"""
simulate_drift.py
─────────────────
Sends synthetic vendor feed messages to a Pub/Sub topic.
Injects schema drift after N messages so you can watch the agent react.

Usage:
    python scripts/simulate_drift.py --vendor cars24 --drift-after 10
    python scripts/simulate_drift.py --vendor paytm  --drift-type type_change

Prerequisites:
    - Pub/Sub emulator running locally (make up) OR real GCP project set
    - PUBSUB_EMULATOR_HOST env var set for local dev (auto-set by docker-compose)
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Load .env before reading GCP_PROJECT_ID so the project name is consistent
# with what setup_pubsub_emulator.py created ("data-contract-platform")
_ROOT = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

from google.cloud import pubsub_v1

DRIFT_TYPES = ["rename_column", "type_change", "add_column", "drop_column"]

VENDOR_SCHEMAS = {
    "cars24": {
        "normal": {
            "vehicle_id":   lambda: f"CAR{random.randint(10000, 99999)}",
            "user_id":      lambda: random.randint(1000, 9999),           # INT
            "price":        lambda: round(random.uniform(150000, 1500000), 2),
            "listing_date": lambda: datetime.now(timezone.utc).isoformat(),
            "city":         lambda: random.choice(["Gurgaon", "Delhi", "Noida"]),
            "fuel_type":    lambda: random.choice(["Petrol", "Diesel", "CNG"]),
        },
        # Drift variants — only the changed/remaining fields are defined
        "rename_column": {"vehicleId": None, "userId": None, "price": None, "listing_date": None, "city": None, "fuel_type": None},
        "type_change":   {"vehicle_id": None, "user_id": lambda: str(random.randint(1000, 9999)), "price": None},  # user_id INT → STRING
        "drop_column":   {"vehicle_id": None, "price": None, "listing_date": None},  # city + fuel_type dropped
        "add_column":    {"vehicle_id": None, "user_id": None, "price": None, "listing_date": None, "city": None, "fuel_type": None, "km_driven": lambda: random.randint(5000, 120000)},
    },
    "paytm": {
        "normal": {
            "txn_id":      lambda: f"TXN{random.randint(100000, 999999)}",
            "amount":      lambda: round(random.uniform(10, 50000), 2),
            "merchant_id": lambda: f"MER{random.randint(1000, 9999)}",
            "status":      lambda: random.choice(["SUCCESS", "FAILED", "PENDING"]),
            "created_at":  lambda: datetime.now(timezone.utc).isoformat(),
        },
        "rename_column": {"transaction_id": None, "amount": None, "merchant_id": None, "status": None, "created_at": None},
        "type_change":   {"txn_id": None, "amount": lambda: str(round(random.uniform(10, 50000), 2)), "merchant_id": None, "status": None, "created_at": None},
        "drop_column":   {"txn_id": None, "amount": None, "status": None},
        "add_column":    {"txn_id": None, "amount": None, "merchant_id": None, "status": None, "created_at": None, "upi_ref": lambda: f"UPI{random.randint(100000, 999999)}"},
    },
}


def generate_record(schema_def: dict) -> dict:
    """Evaluate any callable values (lambdas) to produce a single record."""
    return {k: (v() if callable(v) else v) for k, v in schema_def.items() if v is not None}


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate vendor feed with schema drift")
    parser.add_argument("--vendor",          default="cars24", choices=list(VENDOR_SCHEMAS.keys()))
    parser.add_argument("--topic",           default="vendor-feeds")
    parser.add_argument("--project",         default=os.getenv("GCP_PROJECT_ID", "data-contract-platform"))
    parser.add_argument("--total-messages",  type=int,   default=20)
    parser.add_argument("--drift-after",     type=int,   default=10, help="Inject drift after N messages")
    parser.add_argument("--drift-type",      default="type_change", choices=DRIFT_TYPES)
    parser.add_argument("--delay",           type=float, default=0.5, help="Seconds between messages")
    args = parser.parse_args()

    # Point at the local emulator when running outside Docker.
    # Inside Docker, docker-compose already sets PUBSUB_EMULATOR_HOST.
    if not os.getenv("PUBSUB_EMULATOR_HOST"):
        os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    schema = VENDOR_SCHEMAS[args.vendor]

    print(f"\nSending {args.total_messages} messages  vendor={args.vendor}")
    print(f"Topic  : {topic_path}")
    print(f"Drift  : '{args.drift_type}' injected after message {args.drift_after}\n")

    for i in range(1, args.total_messages + 1):
        use_drift = i > args.drift_after
        record = generate_record(
            schema.get(args.drift_type, schema["normal"]) if use_drift else schema["normal"]
        )

        envelope = {
            "vendor_id":      args.vendor,
            "schema_version": "v2-drifted" if use_drift else "v1",
            "sequence":       i,
            "payload":        record,
            "emitted_at":     datetime.now(timezone.utc).isoformat(),
        }

        # Pub/Sub message attributes are key-value string metadata attached
        # alongside the payload — useful for server-side filtering.
        future = publisher.publish(
            topic_path,
            data=json.dumps(envelope).encode("utf-8"),
            vendor_id=args.vendor,
            schema_version=envelope["schema_version"],
        )
        future.result()  # block until the server acknowledges receipt

        status = "DRIFTED" if use_drift else "normal "
        print(f"  [{i:3d}/{args.total_messages}] {status} — {list(record.keys())}")
        time.sleep(args.delay)

    print("\nDone. Check drift-detector logs and agent-engine for the response.\n")


if __name__ == "__main__":
    main()
