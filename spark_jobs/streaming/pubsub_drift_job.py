"""
spark_jobs/streaming/pubsub_drift_job.py
=========================================

Main Pub/Sub subscriber and drift-processing loop.

What this file does
-------------------
1. Pulls batches of vendor feed messages from the vendor-feeds Pub/Sub subscription.
2. Groups messages by vendor_id within each batch.
3. Infers the live schema from each vendor's payloads using schema_infer.py.
4. Loads the vendor's stored contract from the local JSON registry.
5. Calls schema_comparator.compare_schemas() to detect drift.
6. If drift is found, publishes DriftEvents via event_publisher.py.
7. Acknowledges all processed messages so they are not re-delivered.

Contract registry (Phase 1 vs Phase 2)
---------------------------------------
Phase 1 (now): Contracts are loaded from local JSON files in
               data_contracts/registry/{vendor_id}.json.
               ContractRegistry handles this with a simple in-memory cache.

Phase 2 (later): The registry lookup will be replaced with a call to the
               MCP server (services/mcp_server/contract_tools.py), which
               queries BigQuery.  The rest of this file stays the same.

Message envelope format
-----------------------
Each Pub/Sub message published by simulate_drift.py wraps the vendor payload:
  {
    "vendor_id":      "cars24",
    "schema_version": "v1",
    "sequence":       42,
    "payload":        { ...actual vendor fields... },
    "emitted_at":     "2024-01-15T10:30:00Z"
  }

Schema inference runs on the "payload" dict only — not the envelope —
because the envelope fields (vendor_id, sequence, emitted_at) are platform
metadata, not vendor data, and should not be compared against the contract.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from typing import Optional

import httpx
import structlog
from google.cloud import pubsub_v1

from config import PubSubOptions
from schema_comparator import (
    ContractField,
    DataContract,
    compare_schemas,
)
from schema_infer import infer_schema_from_batch

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Contract registry — Phase 3 (calls contract-api over HTTP)
# ---------------------------------------------------------------------------


class ApiContractRegistry:
    """
    Fetches vendor contracts from the contract-api over HTTP and caches them.

    WHY HTTP INSTEAD OF LOCAL JSON FILES (Phase 1 → Phase 3 upgrade):
    Phase 1 loaded contracts from local JSON files in data_contracts/registry/.
    That worked for a single machine but breaks in a distributed setup:
      - The drift-detector container has no guarantee the JSON files are in sync
        with what's been registered via the API
      - Vendors registered via POST /api/v1/vendors go into BigQuery (the source
        of truth), not into local JSON files
      - The contract-api already owns the contract read path — callers should
        not duplicate that logic

    This class calls GET /api/v1/contracts/{vendor_id} and converts the JSON
    response into the DataContract dataclass that compare_schemas() expects.
    The interface (get_contract method) is identical to the old ContractRegistry
    so nothing else in this file changes.

    Caching: contracts are cached in memory for the lifetime of the process.
    This is safe because contracts only change when a vendor re-registers or
    an agent auto-heals a drift — both are rare events relative to the
    message-pull frequency.
    """

    def __init__(self, contract_api_url: str) -> None:
        self._base = contract_api_url.rstrip("/") + "/api/v1"
        self._cache: dict[str, DataContract] = {}
        self._http = httpx.Client(timeout=10.0)
        log.info("api_registry.ready", base_url=self._base)

    def get_contract(self, vendor_id: str) -> Optional[DataContract]:
        """
        Return the stored DataContract for vendor_id, or None if not registered.

        Calls GET /api/v1/contracts/{vendor_id}.
        404 → vendor not registered (not an error, just returns None).
        Other errors → logged, returns None (detector skips this vendor's batch).
        """
        if vendor_id in self._cache:
            return self._cache[vendor_id]

        try:
            resp = self._http.get(f"{self._base}/contracts/{vendor_id}")

            if resp.status_code == 404:
                log.warning("api_registry.vendor_not_found", vendor_id=vendor_id)
                return None

            resp.raise_for_status()
            data = resp.json()

            dc = DataContract(
                vendor_id=data["vendor_id"],
                version=data["version"],
                fields=[
                    ContractField(
                        name=f["name"],
                        field_type=f["field_type"],
                        nullable=f.get("nullable", True),
                    )
                    for f in data["fields"]
                ],
            )
            self._cache[vendor_id] = dc
            log.info(
                "api_registry.loaded",
                vendor_id=vendor_id,
                version=data["version"],
                field_count=len(data["fields"]),
            )
            return dc

        except Exception:
            log.exception("api_registry.fetch_failed", vendor_id=vendor_id)
            return None


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------


def process_vendor_batch(
    vendor_id: str,
    payload_strings: list[str],
    registry: ContractRegistry,
    publisher: object,  # DriftEventPublisher — avoids circular import at type-check time
) -> None:
    """
    Run the full drift-detection pipeline for one vendor's messages.

    Parameters
    ----------
    vendor_id       : The vendor this batch belongs to.
    payload_strings : JSON strings of the *payload* field only (not the envelope).
    registry        : ContractRegistry to look up the stored contract.
    publisher       : DriftEventPublisher to send drift events.
    """
    contract = registry.get_contract(vendor_id)
    if contract is None:
        # No contract registered — nothing to compare against.
        # This is not an error; it just means the vendor is unknown.
        return

    # Infer the live schema from this batch of payload messages
    incoming_schema = infer_schema_from_batch(payload_strings)

    if not incoming_schema:
        log.warning("drift_job.empty_schema_inferred", vendor_id=vendor_id)
        return

    # Compare live schema against stored contract
    report = compare_schemas(contract, incoming_schema)

    log.info(
        "drift_job.comparison_complete",
        vendor_id=vendor_id,
        message_count=len(payload_strings),
        has_drift=report.has_drift,
        drift_event_count=len(report.drift_events),
    )

    if report.has_drift:
        publisher.publish_report(report)


# ---------------------------------------------------------------------------
# Main subscriber loop
# ---------------------------------------------------------------------------


def run_subscriber(
    pubsub_cfg: PubSubOptions,
    registry_dir: str = "/app/data_contracts/registry",  # kept for backward compat, unused
    max_messages: int = 1_000,
    idle_sleep_seconds: float = 5.0,
) -> None:
    """
    Pull messages from the vendor-feeds Pub/Sub subscription in a tight loop.

    This function runs forever (until the process receives SIGTERM).
    It is the main blocking call started by detector.py.

    Parameters
    ----------
    pubsub_cfg         : Pub/Sub config from load_config().
    registry_dir       : Path to the local contract registry directory.
                         Overridable for testing; defaults to the container path.
    max_messages       : Maximum messages to pull per iteration.
    idle_sleep_seconds : How long to pause when the subscription is empty,
                         to avoid burning CPU on tight polling.

    Pull vs Push
    ------------
    We use Pull (synchronous) instead of Push (webhook) because:
    - The drift detector is a long-running Python process, not a stateless
      HTTP handler.
    - Pull lets us control batch size and processing rate explicitly.
    - No public HTTPS endpoint needed — simpler networking in Docker.
    """
    # Deferred import so event_publisher.py (which imports schema_comparator)
    # does not create a circular dependency at module load time.
    from event_publisher import DriftEventPublisher

    contract_api_url = os.getenv("CONTRACT_API_URL", "http://localhost:8000")
    registry = ApiContractRegistry(contract_api_url)
    publisher = DriftEventPublisher(pubsub_cfg.drift_topic_path)
    subscriber = pubsub_v1.SubscriberClient()

    log.info(
        "drift_job.subscriber_started",
        subscription=pubsub_cfg.vendor_subscription_path,
        max_messages_per_pull=max_messages,
    )

    while True:
        # Pull a batch of messages
        response = subscriber.pull(
            request={
                "subscription": pubsub_cfg.vendor_subscription_path,
                "max_messages": max_messages,
            },
            timeout=30,  # gRPC deadline — returns empty if no messages arrive
        )

        if not response.received_messages:
            log.debug("drift_job.idle_waiting")
            time.sleep(idle_sleep_seconds)
            continue

        # Group payload strings by vendor_id
        # defaultdict(list) avoids key-not-found checks inside the loop
        vendor_payloads: dict[str, list[str]] = defaultdict(list)
        ack_ids: list[str] = []

        for received in response.received_messages:
            ack_ids.append(received.ack_id)

            try:
                envelope = json.loads(received.message.data.decode("utf-8"))
                vendor_id = envelope.get("vendor_id", "")
                payload = envelope.get("payload", {})

                if not vendor_id or not isinstance(payload, dict):
                    log.warning(
                        "drift_job.malformed_message",
                        data_preview=received.message.data[:100],
                    )
                    continue

                # Store the payload as a JSON string — infer_schema_from_batch
                # expects a list of JSON strings, not dicts
                vendor_payloads[vendor_id].append(json.dumps(payload))

            except (json.JSONDecodeError, UnicodeDecodeError):
                log.warning(
                    "drift_job.unparseable_message",
                    data_preview=received.message.data[:100],
                )

        # Process each vendor's batch
        for vendor_id, payload_strings in vendor_payloads.items():
            process_vendor_batch(vendor_id, payload_strings, registry, publisher)

        # Acknowledge ALL messages — including malformed ones.
        # We do not want unprocessable messages to be re-delivered forever;
        # they have been logged above and can be investigated separately.
        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": pubsub_cfg.vendor_subscription_path,
                    "ack_ids": ack_ids,
                }
            )
            log.info("drift_job.messages_acked", count=len(ack_ids))
