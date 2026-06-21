"""
scripts/setup_pubsub_emulator.py
==================================

Creates all Pub/Sub topics and subscriptions in the local emulator
using the Python gRPC client library.

ROOT CAUSE OF EARLIER FAILURES (and why this version fixes them)
-----------------------------------------------------------------
Attempt 1 — Python gRPC, env var set after import:
  The Pub/Sub client library reads PUBSUB_EMULATOR_HOST at IMPORT TIME to
  decide which gRPC endpoint to connect to.  Setting os.environ[...] after
  `from google.cloud import pubsub_v1` is too late — the channel was already
  configured to point at pubsub.googleapis.com (real GCP), so the call hung
  waiting for auth that never arrived.

Attempt 2 — urllib.request (plain HTTP):
  The emulator speaks gRPC (HTTP/2).  urllib sends HTTP/1.1.  Incompatible.
  The emulator closed the connection immediately.

Attempt 3 — docker exec gcloud:
  gcloud CLI always calls the real GCP REST API regardless of
  PUBSUB_EMULATOR_HOST.  That env var only affects the Python/Java/Go
  client libraries, not gcloud itself.

THIS VERSION:
  1. Sets PUBSUB_EMULATOR_HOST BEFORE importing google.cloud.pubsub_v1
     so the channel is created pointing at localhost:8085 (the emulator).
  2. Sets GRPC_DNS_RESOLVER=native for reliable localhost DNS on Windows.
  3. Adds timeout=10.0 to every API call — if something is wrong you get a
     clear TimeoutError in 10 seconds instead of hanging forever.

USAGE
-----
  docker compose up -d pubsub-emulator   (wait ~5 seconds)
  python scripts/setup_pubsub_emulator.py
"""

from __future__ import annotations

import os
import sys
import time

# ── MUST happen before any google.cloud.pubsub_v1 import ──────────────────────
# The Pub/Sub client library reads this env var at import time to choose
# its gRPC endpoint.  Once the channel is built (at import), changing the
# env var has no effect.  This line must be first.
os.environ.setdefault("PUBSUB_EMULATOR_HOST", "localhost:8085")

# On Windows, gRPC's default DNS resolver sometimes fails to resolve
# 'localhost'.  The native resolver uses the OS resolver, which always works.
os.environ.setdefault("GRPC_DNS_RESOLVER", "native")

# NOW it is safe to import the Pub/Sub library
from google.cloud import pubsub_v1                        # noqa: E402
from google.api_core.exceptions import AlreadyExists      # noqa: E402

PROJECT      = "data-contract-platform"
EMULATOR_HOST = os.environ["PUBSUB_EMULATOR_HOST"]

TOPICS_AND_SUBS = [
    ("vendor-feeds",  "vendor-feeds-sub"),   # simulate_drift.py → drift-detector
    ("drift-events",  "drift-events-sub"),   # drift-detector → agent-engine
    ("heal-actions",  "heal-actions-sub"),   # agent-engine → healer
]


# ── Wait for the emulator port to open ────────────────────────────────────────

def wait_for_emulator(max_retries: int = 20, delay: float = 1.0) -> None:
    import socket
    host, port_str = EMULATOR_HOST.split(":")
    port = int(port_str)

    print(f"Waiting for emulator at {EMULATOR_HOST}...")
    for attempt in range(max_retries):
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"  TCP port open ({attempt + 1} attempt(s)).")
                # The gRPC server needs a few more seconds after the TCP port opens
                print("  Waiting 4s for gRPC server to initialise...")
                time.sleep(4)
                print("  Ready.\n")
                return
        except (ConnectionRefusedError, OSError):
            time.sleep(delay)

    print(f"\nERROR: Emulator at {EMULATOR_HOST} did not start.")
    print("Run:  docker compose up -d pubsub-emulator")
    sys.exit(1)


# ── Create topics and subscriptions ───────────────────────────────────────────

def create_resources() -> None:
    publisher  = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    ok    = 0
    total = len(TOPICS_AND_SUBS) * 2

    for topic_name, sub_name in TOPICS_AND_SUBS:
        topic_path = f"projects/{PROJECT}/topics/{topic_name}"
        sub_path   = f"projects/{PROJECT}/subscriptions/{sub_name}"

        # ── Topic ──────────────────────────────────────────────────────────────
        # timeout=10.0: if gRPC hangs (e.g. wrong endpoint), fail in 10s not forever
        try:
            publisher.create_topic(request={"name": topic_path}, timeout=10.0)
            print(f"  ✅  Topic created       : {topic_name}")
            ok += 1
        except AlreadyExists:
            print(f"  ⚠️   Topic already exists : {topic_name}")
            ok += 1
        except Exception as e:
            print(f"  ❌  Topic failed        : {topic_name}")
            print(f"      {type(e).__name__}: {e}\n")

        # ── Subscription ───────────────────────────────────────────────────────
        try:
            subscriber.create_subscription(
                request={"name": sub_path, "topic": topic_path},
                timeout=10.0,
            )
            print(f"       ✅  Subscription created       : {sub_name}")
            ok += 1
        except AlreadyExists:
            print(f"       ⚠️   Subscription already exists : {sub_name}")
            ok += 1
        except Exception as e:
            print(f"       ❌  Subscription failed        : {sub_name}")
            print(f"           {type(e).__name__}: {e}")

        print()

    print("=" * 60)
    if ok == total:
        print(f"✅  All {total} resources ready.\n")
        print("Next steps:")
        print("  1.  docker compose up -d drift-detector")
        print("  2.  python scripts/simulate_drift.py")
        print("  3.  docker compose logs -f drift-detector")
    else:
        print(f"⚠️   Only {ok}/{total} resources created — see errors above.")
        sys.exit(1)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Setting up Pub/Sub emulator  (project={PROJECT}, host={EMULATOR_HOST})")
    print("=" * 60)
    wait_for_emulator()
    create_resources()


if __name__ == "__main__":
    main()
