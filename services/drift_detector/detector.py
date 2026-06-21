"""
services/drift_detector/detector.py
=====================================

Entry point for the drift-detector Docker container.

This file's only job is to:
  1. Configure structured logging.
  2. Load all config from environment variables.
  3. Register a SIGTERM handler for graceful Docker shutdown.
  4. Start the blocking Pub/Sub subscriber loop.

Everything else — schema inference, contract comparison, event publishing —
lives in the modules it imports.  This file stays thin by design: it is an
orchestration entry point, not a logic layer.

Running locally (outside Docker)
---------------------------------
    cd services/drift_detector
    PUBSUB_EMULATOR_HOST=localhost:8085 python detector.py

Running inside Docker
---------------------
    docker compose up drift-detector
    (docker-compose sets all env vars automatically)
"""

from __future__ import annotations

import signal
import sys
import os

import structlog

# Add /app/spark_jobs to path so imports in kafka_drift_job.py resolve.
# In the container /app is the working directory and spark_jobs/ is mounted
# there.  When running locally this resolves relative to the project root.
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# spark_jobs/streaming/ is added so schema_comparator, schema_infer, config
# can be imported by kafka_drift_job without relative import gymnastics.
_STREAMING_DIR = os.path.join(_PROJECT_ROOT, "spark_jobs", "streaming")
if _STREAMING_DIR not in sys.path:
    sys.path.insert(0, _STREAMING_DIR)

# Local service directory — schema_comparator.py and event_publisher.py live here
_SERVICE_DIR = os.path.dirname(__file__)
if _SERVICE_DIR not in sys.path:
    sys.path.insert(0, _SERVICE_DIR)


# ---------------------------------------------------------------------------
# Logging setup — must happen before any other imports that use structlog
# ---------------------------------------------------------------------------

def _configure_logging() -> None:
    """
    Configure structlog for JSON output in production, pretty output in local.

    JSON renderer: every log line is a valid JSON object — easy to ingest into
    Cloud Logging or any log aggregator.
    Console renderer: coloured, human-readable output for local development.
    """
    environment = os.getenv("ENVIRONMENT", "local")

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if environment == "local":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(10),  # DEBUG level
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    _configure_logging()
    log = structlog.get_logger(__name__)

    log.info("drift_detector.starting")

    # Load config — this also sets PUBSUB_EMULATOR_HOST if in local mode
    from config import load_config
    _, pubsub_cfg, _ = load_config()

    log.info(
        "drift_detector.config_loaded",
        environment=os.getenv("ENVIRONMENT", "local"),
        emulator=pubsub_cfg.emulator_host or "real GCP",
        vendor_topic=pubsub_cfg.vendor_topic,
        drift_topic=pubsub_cfg.drift_topic,
        subscription=pubsub_cfg.vendor_subscription,
    )

    # Graceful shutdown — Docker sends SIGTERM before SIGKILL.
    # Catching it lets the current pull iteration finish and ack messages
    # before the process exits, preventing re-delivery of processed messages.
    def _handle_sigterm(signum: int, frame: object) -> None:
        log.info("drift_detector.shutdown_requested", signal=signum)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)   # also handle Ctrl+C locally

    # Start the blocking subscriber loop — this never returns under normal operation
    from pubsub_drift_job import run_subscriber

    registry_dir = os.getenv(
        "CONTRACT_REGISTRY_DIR",
        os.path.join(_PROJECT_ROOT, "data_contracts", "registry"),
    )

    log.info("drift_detector.subscriber_loop_starting", registry_dir=registry_dir)

    try:
        run_subscriber(pubsub_cfg, registry_dir=registry_dir)
    except SystemExit:
        log.info("drift_detector.stopped_cleanly")
    except Exception:
        log.exception("drift_detector.fatal_error")
        sys.exit(1)


if __name__ == "__main__":
    main()
