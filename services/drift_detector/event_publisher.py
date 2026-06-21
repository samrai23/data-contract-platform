"""
services/drift_detector/event_publisher.py
==========================================

Publishes detected drift events to the GCP Pub/Sub drift-events topic.

Each DriftEvent in a DriftReport becomes one Pub/Sub message so that:
  - The agent engine can subscribe and process events independently.
  - Consumers can filter server-side using Pub/Sub message attributes
    (e.g. subscribe only to severity=HIGH or is_safe_to_auto_heal=false).
  - Each event has a full audit payload — no joins needed downstream.

Message structure
-----------------
  data      : UTF-8 JSON with all DriftEvent fields + detected_at timestamp.
  attributes: Flat string key-value pairs for server-side filtering.
    vendor_id            : e.g. "cars24"
    drift_type           : e.g. "type_change"
    severity             : "LOW" | "MEDIUM" | "HIGH"
    is_safe_to_auto_heal : "true" | "false"  (Pub/Sub attrs are always strings)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from google.cloud import pubsub_v1

from schema_comparator import DriftEvent, DriftReport

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Published result
# ---------------------------------------------------------------------------


@dataclass
class PublishedResult:
    """Tracks the outcome of publishing a single DriftEvent."""

    message_id: str      # Pub/Sub server-assigned ID, useful for deduplication
    drift_event: DriftEvent


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------


class DriftEventPublisher:
    """
    Wraps the Pub/Sub PublisherClient and handles serialisation of DriftEvents.

    One instance is created per process and reused — creating a new
    PublisherClient for every message is expensive (it starts a background
    thread and opens a gRPC channel each time).

    Parameters
    ----------
    topic_path : Full Pub/Sub topic resource path.
                 Format: "projects/{project_id}/topics/{topic_name}"
                 Use PubSubOptions.drift_topic_path from config.py.
    """

    def __init__(self, topic_path: str) -> None:
        self._topic_path = topic_path
        # PublisherClient reads PUBSUB_EMULATOR_HOST automatically if set.
        # load_config() ensures that env var is set before this is instantiated.
        self._client = pubsub_v1.PublisherClient()
        log.info("event_publisher.ready", topic=topic_path)

    def publish_report(self, report: DriftReport) -> list[PublishedResult]:
        """
        Publish every DriftEvent in a DriftReport as a separate Pub/Sub message.

        Clean reports (has_drift=False) are a no-op — nothing is published.

        Parameters
        ----------
        report : DriftReport returned by schema_comparator.compare_schemas().

        Returns
        -------
        list[PublishedResult]
            One entry per successfully published event, in the same order as
            report.drift_events.  Empty list if has_drift is False.
        """
        if not report.has_drift:
            log.debug(
                "event_publisher.no_drift_skip",
                vendor_id=report.vendor_id,
            )
            return []

        results: list[PublishedResult] = []

        for event in report.drift_events:
            try:
                message_id = self._publish_single(event)
                results.append(PublishedResult(message_id=message_id, drift_event=event))
                log.info(
                    "event_publisher.published",
                    vendor_id=event.vendor_id,
                    drift_type=event.drift_type.value,
                    field=event.field_name,
                    severity=event.severity.value,
                    message_id=message_id,
                )
            except Exception:
                # Log and continue — a failed publish for one event should not
                # block the remaining events in the same report.
                log.exception(
                    "event_publisher.publish_failed",
                    vendor_id=event.vendor_id,
                    field=event.field_name,
                )

        log.info(
            "event_publisher.report_published",
            vendor_id=report.vendor_id,
            total_events=len(report.drift_events),
            published=len(results),
        )
        return results

    def _publish_single(self, event: DriftEvent) -> str:
        """
        Serialise one DriftEvent and publish it.

        Returns the Pub/Sub message ID assigned by the server.

        Payload (data field)
        --------------------
        Contains all DriftEvent fields plus detected_at so downstream
        consumers have a complete picture without needing to join anything.

        Attributes
        ----------
        Pub/Sub message attributes are flat string key-value pairs.
        They enable server-side subscription filters like:
          attributes.severity = "HIGH"
          attributes.is_safe_to_auto_heal = "false"
        The agent engine uses these to route messages to the right handler.
        """
        payload = {
            "vendor_id":            event.vendor_id,
            "drift_type":           event.drift_type.value,
            "field_name":           event.field_name,
            "old_type":             event.old_type,
            "new_type":             event.new_type,
            "severity":             event.severity.value,
            "is_safe_to_auto_heal": event.is_safe_to_auto_heal,
            "detected_at":          datetime.now(timezone.utc).isoformat(),
        }

        future = self._client.publish(
            self._topic_path,
            data=json.dumps(payload).encode("utf-8"),
            # Attributes must all be strings — Pub/Sub does not support booleans
            vendor_id=event.vendor_id,
            drift_type=event.drift_type.value,
            severity=event.severity.value,
            is_safe_to_auto_heal=str(event.is_safe_to_auto_heal).lower(),
        )

        # .result() blocks until the server acknowledges receipt.
        # This gives us the message ID and surfaces any publish errors immediately
        # rather than silently dropping messages.
        return future.result()
