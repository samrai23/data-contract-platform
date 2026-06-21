"""
scripts/setup_bigquery.py
==========================
Python equivalent of setup_bigquery.sh — creates the BigQuery dataset and tables.
Run this once when setting up a new GCP project.

Usage:
    python scripts/setup_bigquery.py
"""

import os
import sys
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

PROJECT = os.getenv("GCP_PROJECT_ID")
if not PROJECT:
    print("ERROR: GCP_PROJECT_ID environment variable is not set.")
    print("Set it first:  $env:GCP_PROJECT_ID = 'your-project-id'")
    sys.exit(1)

DATASET  = os.getenv("BIGQUERY_DATASET", "data_contracts")
REGION   = os.getenv("GCP_REGION", "asia-south1")
DATASET_REF = f"{PROJECT}.{DATASET}"

client = bigquery.Client(project=PROJECT)

print(f"Setting up BigQuery for project: {PROJECT} (region: {REGION})")
print("=" * 60)

# ── Dataset ────────────────────────────────────────────────────
dataset = bigquery.Dataset(DATASET_REF)
dataset.location = REGION
dataset.description = "Data Contract Platform — contracts, drift events, and audit logs"
try:
    client.create_dataset(dataset)
    print(f"✅  Dataset created  : {DATASET_REF}")
except Conflict:
    print(f"⚠️   Dataset exists   : {DATASET_REF}")

# ── Table helper ───────────────────────────────────────────────
def create_table(table_id, schema, description, partition_field=None):
    ref = f"{DATASET_REF}.{table_id}"
    table = bigquery.Table(ref, schema=schema)
    table.description = description
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    try:
        client.create_table(table)
        print(f"✅  Table created    : {ref}")
    except Conflict:
        print(f"⚠️   Table exists     : {ref}")

# ── vendor_contracts ───────────────────────────────────────────
create_table(
    "vendor_contracts",
    schema=[
        bigquery.SchemaField("vendor_id",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("vendor_name",   "STRING"),
        bigquery.SchemaField("version",       "STRING"),
        bigquery.SchemaField("status",        "STRING"),
        bigquery.SchemaField("contract_json", "STRING"),
        bigquery.SchemaField("description",   "STRING"),
        bigquery.SchemaField("owner_email",   "STRING"),
        bigquery.SchemaField("contact_email", "STRING"),
        bigquery.SchemaField("created_at",    "TIMESTAMP"),
        bigquery.SchemaField("updated_at",    "TIMESTAMP"),
    ],
    description="Versioned data contracts for each vendor",
)

# ── drift_events ───────────────────────────────────────────────
create_table(
    "drift_events",
    schema=[
        bigquery.SchemaField("event_id",          "STRING"),
        bigquery.SchemaField("vendor_id",          "STRING"),
        bigquery.SchemaField("drift_type",         "STRING"),
        bigquery.SchemaField("detected_at",        "TIMESTAMP"),
        bigquery.SchemaField("field_name",         "STRING"),
        bigquery.SchemaField("old_type",           "STRING"),
        bigquery.SchemaField("new_type",           "STRING"),
        bigquery.SchemaField("severity",           "STRING"),
        bigquery.SchemaField("resolution_status",  "STRING"),
        bigquery.SchemaField("agent_verdict",      "STRING"),
        bigquery.SchemaField("approved_by",        "STRING"),
        bigquery.SchemaField("approval_notes",     "STRING"),
        bigquery.SchemaField("healed_at",          "TIMESTAMP"),
    ],
    description="Audit log of all schema drift events — partitioned by day",
    partition_field="detected_at",
)

# ── pipeline_audit ─────────────────────────────────────────────
create_table(
    "pipeline_audit",
    schema=[
        bigquery.SchemaField("run_id",                  "STRING"),
        bigquery.SchemaField("vendor_id",               "STRING"),
        bigquery.SchemaField("run_at",                  "TIMESTAMP"),
        bigquery.SchemaField("records_processed",       "INTEGER"),
        bigquery.SchemaField("records_rejected",        "INTEGER"),
        bigquery.SchemaField("schema_version",          "STRING"),
        bigquery.SchemaField("drift_detected",          "BOOLEAN"),
        bigquery.SchemaField("auto_healed",             "BOOLEAN"),
        bigquery.SchemaField("human_approval_required", "BOOLEAN"),
    ],
    description="Daily pipeline run summaries per vendor",
    partition_field="run_at",
)

print("=" * 60)
print(f"✅  Done. Dataset: {DATASET_REF}")
print("Next: python scripts/seed_contracts.py")
