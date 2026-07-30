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


# ---------------------------------------------------------------------------
# BUG FIX (2026-07-30): the previous version of this file defined drift
# variants as flat dicts using `None` to mean "keep this field as normal".
# generate_record() actually treated `None` as "omit this key from the
# record" — so e.g. a "type_change" run didn't test ONE isolated type
# change, it also silently dropped every field not explicitly re-listed,
# firing 5 bogus dropped_column HIGH-severity escalations alongside the
# 1 intended type_change event on every single drifted message. Confirmed
# live against the real stack: a "type_change" run produced 1 AUTO_HEAL +
# 6 ESCALATE decisions instead of 1 AUTO_HEAL.
#
# Fixed by making each drift variant an explicit TRANSFORM applied on top
# of a freshly generated normal record, instead of a parallel field list:
#   rename   {old: new}     — rename a key, value unchanged
#   override {name: fn}     — replace a field's value/type (type_change)
#   drop     [name, ...]    — remove fields entirely (drop_column)
#   add      {name: fn}     — add a new field (add_column)
# This guarantees a "type_change" run touches ONLY the overridden field(s)
# and every other field stays exactly as in the normal schema.
# ---------------------------------------------------------------------------

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
        "drift": {
            # Cosmetic rename — same example agents.py's own system prompt
            # docstring uses for "clearly cosmetic and safe".
            "rename_column": {"rename": {"city": "city_name"}},
            # INT → STRING is a safe widening change (schema_comparator's
            # _WIDENING_CHANGES) — expected decision: AUTO_HEAL.
            "type_change":   {"override": {"user_id": lambda: str(random.randint(1000, 9999))}},
            # HIGH severity, never safe to auto-heal — expected: ESCALATE.
            "drop_column":   {"drop": ["fuel_type"]},
            # Purely additive — expected: AUTO_HEAL.
            "add_column":    {"add": {"km_driven": lambda: random.randint(5000, 120000)}},
        },
    },
    "paytm": {
        "normal": {
            "txn_id":      lambda: f"TXN{random.randint(100000, 999999)}",
            "amount":      lambda: round(random.uniform(10, 50000), 2),
            "merchant_id": lambda: f"MER{random.randint(1000, 9999)}",
            "status":      lambda: random.choice(["SUCCESS", "FAILED", "PENDING"]),
            "created_at":  lambda: datetime.now(timezone.utc).isoformat(),
        },
        "drift": {
            "rename_column": {"rename": {"merchant_id": "merchant_ref"}},
            # DOUBLE → STRING is NOT in the widening table — falls to the
            # conservative MEDIUM/not-safe default. Expected: ESCALATE.
            # (Deliberately different outcome from cars24's type_change —
            # gives the simulation a mix of AUTO_HEAL and ESCALATE type changes.)
            "type_change":   {"override": {"amount": lambda: str(round(random.uniform(10, 50000), 2))}},
            "drop_column":   {"drop": ["created_at"]},
            "add_column":    {"add": {"upi_ref": lambda: f"UPI{random.randint(100000, 999999)}"}},
        },
    },
}


def generate_record(normal_schema: dict, drift_spec: dict | None) -> dict:
    """
    Build one record: start from every field in normal_schema (all generators
    evaluated), then apply at most one drift transform on top of it.

    A drift_spec of None returns a clean normal record. Otherwise it's one of
    the "drift" sub-dicts above — see the bug-fix note for the transform keys.
    """
    record = {k: gen() for k, gen in normal_schema.items()}
    if not drift_spec:
        return record

    for old_name, new_name in drift_spec.get("rename", {}).items():
        if old_name in record:
            record[new_name] = record.pop(old_name)
    for name, gen in drift_spec.get("override", {}).items():
        record[name] = gen()
    for name in drift_spec.get("drop", []):
        record.pop(name, None)
    for name, gen in drift_spec.get("add", {}).items():
        record[name] = gen()
    return record


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
        drift_spec = schema["drift"][args.drift_type] if use_drift else None
        record = generate_record(schema["normal"], drift_spec)

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
