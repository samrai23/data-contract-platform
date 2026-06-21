"""
spark_jobs/streaming/config.py
===============================

Centralised configuration for all Spark, Pub/Sub, and BigQuery settings
used by the drift-detection streaming job.

Two execution environments are supported:

local    — Spark runs in local[N] mode on a dev machine or Docker container.
           Pub/Sub is the local GCP emulator at pubsub-emulator:8085 (no auth).
           Used for `make up` / all local development.

dataproc — Spark runs on Dataproc Serverless in GCP.
           Pub/Sub is the real GCP service (authenticated via service account).
           Used for staging and production.

All values are read from environment variables (via pydantic-settings).
Nothing is hardcoded here — this file is safe to commit.

Usage
-----
    from spark_jobs.streaming.config import load_config

    spark_cfg, pubsub_cfg, bq_cfg = load_config()
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------------------------------------------------------------------------
# Environment enum
# ---------------------------------------------------------------------------


class RunEnvironment(str, Enum):
    """
    Controls Spark master URL, Pub/Sub routing, and shuffle partition count.
    Read from the ENVIRONMENT env var (defaults to "local").
    """

    LOCAL = "local"
    DATAPROC = "dataproc"


# ---------------------------------------------------------------------------
# PlatformSettings — single pydantic-settings model reads ALL env vars
# ---------------------------------------------------------------------------


class PlatformSettings(BaseSettings):
    """
    Reads every configuration value from the environment / .env file.

    pydantic-settings v2 maps field names to env var names automatically
    (case-insensitive).  extra="ignore" silently drops unknown vars —
    safe and necessary in Docker containers that inject many system vars.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Runtime ───────────────────────────────────────────────────────────
    environment: RunEnvironment = RunEnvironment.LOCAL

    # ── Google Cloud ──────────────────────────────────────────────────────
    gcp_project_id: str = Field(default="local-dev")
    gcp_region: str = Field(default="asia-south1")
    google_application_credentials: str = Field(default="./gcp-credentials.json")

    # ── BigQuery ──────────────────────────────────────────────────────────
    bigquery_dataset: str = Field(default="data_contracts")
    bigquery_contracts_table: str = Field(default="vendor_contracts")
    bigquery_drift_table: str = Field(default="drift_events")
    bigquery_audit_table: str = Field(default="pipeline_audit")

    # ── Pub/Sub topics ────────────────────────────────────────────────────
    pubsub_vendor_topic: str = Field(
        default="vendor-feeds",
        description="Vendors publish JSON records here",
    )
    pubsub_drift_topic: str = Field(
        default="drift-events",
        description="Drift detector publishes classified drift events here",
    )
    pubsub_heal_topic: str = Field(
        default="heal-actions",
        description="Agent engine publishes heal decisions here",
    )

    # ── Pub/Sub subscriptions ─────────────────────────────────────────────
    # Each consumer service has its own named subscription on a topic.
    # Multiple subscriptions on the same topic receive ALL messages independently.
    pubsub_vendor_subscription: str = Field(
        default="vendor-feeds-sub",
        description="Drift detector pulls from vendor-feeds via this subscription",
    )
    pubsub_drift_subscription: str = Field(
        default="drift-events-sub",
        description="Agent engine pulls from drift-events via this subscription",
    )

    # ── Pub/Sub emulator ──────────────────────────────────────────────────
    # Empty string = use real GCP Pub/Sub.
    # Non-empty (e.g. "pubsub-emulator:8085") = route to local Docker emulator.
    # The google-cloud-pubsub library reads PUBSUB_EMULATOR_HOST automatically
    # when it is set in the environment — load_config() handles that.
    pubsub_emulator_host: str = Field(default="")

    # ── Spark ─────────────────────────────────────────────────────────────
    spark_app_name: str = Field(default="drift-detector")
    spark_local_cores: int = Field(default=2)
    spark_checkpoint_location: str = Field(
        default="/tmp/spark-checkpoints/drift-detector"
    )
    spark_trigger_interval_seconds: int = Field(default=30)
    spark_max_messages_per_pull: int = Field(
        default=1_000,
        description="Max Pub/Sub messages pulled per processing cycle",
    )

    # ── Downstream services ───────────────────────────────────────────────
    agent_engine_url: str = Field(default="http://localhost:8002")


# ---------------------------------------------------------------------------
# Derived configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PubSubOptions:
    """
    Pub/Sub connection settings used by the drift detector and agent engine.

    Key concepts
    ------------
    Topic        — a named channel. Publishers send messages here.
    Subscription — a named reader handle attached to a topic. Each consumer
                   service creates one subscription; all messages on the topic
                   are delivered to every subscription independently.

    Path format
    -----------
    GCP Pub/Sub identifies resources by full resource paths:
      projects/{project_id}/topics/{topic}
      projects/{project_id}/subscriptions/{subscription}
    The *_path properties build these automatically.

    Emulator
    --------
    When emulator_host is non-empty, load_config() sets the
    PUBSUB_EMULATOR_HOST environment variable.  The google-cloud-pubsub
    client library reads that variable and routes all requests to the
    local Docker emulator instead of real GCP — no code change needed.
    """

    project_id: str

    # Topic names
    vendor_topic: str       # vendors publish JSON records here
    drift_topic: str        # drift detector publishes drift events here
    heal_topic: str         # agent engine publishes heal decisions here

    # Subscription names (one per consumer service)
    vendor_subscription: str    # used by drift-detector to read vendor-feeds
    drift_subscription: str     # used by agent-engine to read drift-events

    # Empty = real GCP, non-empty = local Docker emulator
    emulator_host: str = ""

    @property
    def is_emulator(self) -> bool:
        """True when running against the local Pub/Sub Docker emulator."""
        return bool(self.emulator_host)

    # ── Full resource path helpers ────────────────────────────────────────

    @property
    def vendor_topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.vendor_topic}"

    @property
    def drift_topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.drift_topic}"

    @property
    def heal_topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.heal_topic}"

    @property
    def vendor_subscription_path(self) -> str:
        return f"projects/{self.project_id}/subscriptions/{self.vendor_subscription}"

    @property
    def drift_subscription_path(self) -> str:
        return f"projects/{self.project_id}/subscriptions/{self.drift_subscription}"


@dataclass
class BigQueryOptions:
    """
    BigQuery dataset and table references.

    The full_* properties return three-part identifiers
    (project.dataset.table) required for cross-project queries.
    """

    project_id: str
    dataset: str
    contracts_table: str
    drift_table: str
    audit_table: str
    credentials_path: str

    @property
    def full_contracts_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.contracts_table}"

    @property
    def full_drift_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.drift_table}"

    @property
    def full_audit_table(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.audit_table}"


@dataclass
class SparkSessionConfig:
    """
    Parameters used to construct and configure the SparkSession.

    Only application-level Spark properties live here.
    Memory, executor count, and GC settings belong in the spark-submit
    command or Dataproc cluster config — not in application code.
    """

    app_name: str
    environment: RunEnvironment
    local_cores: int
    checkpoint_location: str
    trigger_interval_seconds: int
    max_messages_per_pull: int
    gcp_project_id: str
    credentials_path: str

    @property
    def master_url(self) -> str:
        """
        Spark master URL — only set in local mode.
        On Dataproc the cluster manager injects this at spark-submit time.
        """
        if self.environment == RunEnvironment.LOCAL:
            return f"local[{self.local_cores}]"
        return ""

    def as_spark_conf(self) -> dict[str, str]:
        """
        Dict of Spark configuration properties for SparkConf / builder.

        shuffle.partitions: 2 is enough for local dev with hundreds of records.
        200 is the Spark default and appropriate for Dataproc at scale.
        """
        conf: dict[str, str] = {
            "spark.app.name": self.app_name,
            "spark.sql.streaming.checkpointLocation": self.checkpoint_location,
            "spark.sql.shuffle.partitions": (
                "2" if self.environment == RunEnvironment.LOCAL else "200"
            ),
        }

        if self.credentials_path and os.path.exists(self.credentials_path):
            conf.update(
                {
                    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": (
                        self.credentials_path
                    ),
                    "spark.hadoop.fs.gs.auth.service.account.json.keyfile": (
                        self.credentials_path
                    ),
                }
            )

        return conf


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def load_config() -> tuple[SparkSessionConfig, PubSubOptions, BigQueryOptions]:
    """
    Load all configuration from the environment and return three typed objects.

    Side effect
    -----------
    If pubsub_cfg.is_emulator is True, this function sets the
    PUBSUB_EMULATOR_HOST environment variable so the google-cloud-pubsub
    client library automatically routes to the local Docker emulator.
    This must happen before any PublisherClient or SubscriberClient is created.

    Example
    -------
        spark_cfg, pubsub_cfg, bq_cfg = load_config()

        # Build Spark session
        builder = SparkSession.builder.appName(spark_cfg.app_name)
        if spark_cfg.master_url:
            builder = builder.master(spark_cfg.master_url)
        for k, v in spark_cfg.as_spark_conf().items():
            builder = builder.config(k, v)
        spark = builder.getOrCreate()

        # Use Pub/Sub paths directly
        publisher = pubsub_v1.PublisherClient()
        publisher.publish(pubsub_cfg.drift_topic_path, data=b"...")
    """
    s = PlatformSettings()

    pubsub_cfg = PubSubOptions(
        project_id=s.gcp_project_id,
        vendor_topic=s.pubsub_vendor_topic,
        drift_topic=s.pubsub_drift_topic,
        heal_topic=s.pubsub_heal_topic,
        vendor_subscription=s.pubsub_vendor_subscription,
        drift_subscription=s.pubsub_drift_subscription,
        emulator_host=s.pubsub_emulator_host,
    )

    # Tell the Pub/Sub client library to use the local emulator.
    # This env var is read once when the first client is instantiated,
    # so it must be set here before any client code runs.
    if pubsub_cfg.is_emulator:
        os.environ["PUBSUB_EMULATOR_HOST"] = pubsub_cfg.emulator_host

    spark_cfg = SparkSessionConfig(
        app_name=s.spark_app_name,
        environment=s.environment,
        local_cores=s.spark_local_cores,
        checkpoint_location=s.spark_checkpoint_location,
        trigger_interval_seconds=s.spark_trigger_interval_seconds,
        max_messages_per_pull=s.spark_max_messages_per_pull,
        gcp_project_id=s.gcp_project_id,
        credentials_path=s.google_application_credentials,
    )

    bq_cfg = BigQueryOptions(
        project_id=s.gcp_project_id,
        dataset=s.bigquery_dataset,
        contracts_table=s.bigquery_contracts_table,
        drift_table=s.bigquery_drift_table,
        audit_table=s.bigquery_audit_table,
        credentials_path=s.google_application_credentials,
    )

    return spark_cfg, pubsub_cfg, bq_cfg
