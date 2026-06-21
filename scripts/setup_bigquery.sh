#!/bin/bash
# ─────────────────────────────────────────────────────────────
# setup_bigquery.sh
#
# Creates all required BigQuery datasets and tables.
# Run this ONCE when setting up a new GCP project.
#
# Usage:
#   GCP_PROJECT_ID=my-project bash scripts/setup_bigquery.sh
#
# What this creates:
#   dataset:  data_contracts
#   tables:   vendor_contracts   ← contracts stored by the FastAPI layer
#             drift_events       ← written by the agent engine (Phase 3)
#             pipeline_audit     ← daily pipeline run summaries
# ─────────────────────────────────────────────────────────────

set -e

PROJECT_ID="${GCP_PROJECT_ID:-your-project-id}"
DATASET="${BIGQUERY_DATASET:-data_contracts}"
REGION="${GCP_REGION:-asia-south1}"

echo "Setting up BigQuery for project: $PROJECT_ID (region: $REGION)"
echo ""

# ── Dataset ────────────────────────────────────────────────────────────────────
# A BigQuery dataset is just a namespace (like a database schema in Postgres).
# All three tables live inside this one dataset.
bq mk \
  --dataset \
  --location="$REGION" \
  --description="Data Contract Platform — contracts, drift events, and audit logs" \
  "$PROJECT_ID:$DATASET" 2>/dev/null || echo "Dataset '$DATASET' already exists — skipping"

echo ""

# ── Table 1: vendor_contracts ──────────────────────────────────────────────────
# One row per active contract version.  When a contract is updated (Phase 3),
# the old row gets status='DEPRECATED' and a new row is inserted at the new version.
# This gives us a full contract version history for free.
#
# Columns:
#   vendor_id      — slug identifier (e.g. "cars24").  Indexed via partition/cluster.
#   vendor_name    — human-readable name for display in the dashboard.
#   version        — semver string ("1.0.0").  Starts at 1.0.0, bumped by the Healer agent.
#   status         — ACTIVE | DEPRECATED | DRAFT.  Only one ACTIVE row per vendor_id.
#   contract_json  — JSON array of field definitions, stored as a STRING.
#                    Example: [{"name":"user_id","field_type":"INT","nullable":true}]
#   description    — free-text description of what this vendor feed contains.
#   owner_email    — the data engineer who owns this contract (internal).
#   contact_email  — the vendor's data contact (external).
#   created_at     — when this contract version was first registered.
#   updated_at     — when this row was last modified.
bq mk \
  --table \
  --description="Versioned data contracts for each vendor" \
  "$PROJECT_ID:$DATASET.vendor_contracts" \
  vendor_id:STRING,vendor_name:STRING,version:STRING,status:STRING,\
contract_json:STRING,description:STRING,owner_email:STRING,\
contact_email:STRING,created_at:TIMESTAMP,updated_at:TIMESTAMP \
  2>/dev/null || echo "Table 'vendor_contracts' already exists — skipping"

echo ""

# ── Table 2: drift_events ──────────────────────────────────────────────────────
# One row per detected drift event.  Written by the agent engine in Phase 3.
# Partitioned by day (detected_at) so queries like "show me today's drifts"
# are cheap — BigQuery only scans that day's partition, not the whole table.
#
# Columns:
#   event_id          — UUID assigned by the agent engine when it processes the event.
#   vendor_id         — which vendor triggered the drift.
#   drift_type        — type_change | new_column | dropped_column | column_rename.
#   detected_at       — when the drift detector first saw it (PARTITION column).
#   field_name        — the specific field that drifted.
#   old_type          — what the contract said the type should be.
#   new_type          — what the incoming data actually had.
#   severity          — LOW | MEDIUM | HIGH | CRITICAL.
#   resolution_status — PENDING_APPROVAL | AUTO_HEALED | APPROVED | REJECTED.
#   agent_verdict     — free-text summary of what the Analyst agent decided.
#   approved_by       — email of the human who approved/rejected (NULL if auto-healed).
#   approval_notes    — optional notes from the human approver.
#   healed_at         — when the fix was applied (NULL until healed).
bq mk \
  --table \
  --time_partitioning_field=detected_at \
  --time_partitioning_type=DAY \
  --description="Audit log of all schema drift events — partitioned by day" \
  "$PROJECT_ID:$DATASET.drift_events" \
  event_id:STRING,vendor_id:STRING,drift_type:STRING,detected_at:TIMESTAMP,\
field_name:STRING,old_type:STRING,new_type:STRING,severity:STRING,\
resolution_status:STRING,agent_verdict:STRING,approved_by:STRING,\
approval_notes:STRING,healed_at:TIMESTAMP \
  2>/dev/null || echo "Table 'drift_events' already exists — skipping"

echo ""

# ── Table 3: pipeline_audit ────────────────────────────────────────────────────
# Daily summary of pipeline runs — one row per vendor per run.
# Used by the dbt mart layer (Phase 5) to build the vendor_health dashboard.
#
# Columns:
#   run_id                   — UUID for this pipeline run.
#   vendor_id                — which vendor was processed.
#   run_at                   — when the run started (PARTITION column).
#   records_processed        — total messages in this batch.
#   records_rejected         — messages that failed schema validation.
#   schema_version           — which contract version was active during this run.
#   drift_detected           — did this run find any drift?
#   auto_healed              — was it auto-healed without human intervention?
#   human_approval_required  — did it get routed to the approval webhook?
bq mk \
  --table \
  --time_partitioning_field=run_at \
  --time_partitioning_type=DAY \
  --description="Daily pipeline run summaries per vendor" \
  "$PROJECT_ID:$DATASET.pipeline_audit" \
  run_id:STRING,vendor_id:STRING,run_at:TIMESTAMP,\
records_processed:INTEGER,records_rejected:INTEGER,\
schema_version:STRING,drift_detected:BOOLEAN,\
auto_healed:BOOLEAN,human_approval_required:BOOLEAN \
  2>/dev/null || echo "Table 'pipeline_audit' already exists — skipping"

echo ""
echo "✅ BigQuery setup complete!"
echo "   Dataset : $PROJECT_ID.$DATASET"
echo "   Tables  : vendor_contracts, drift_events, pipeline_audit"
echo ""
echo "Next step: run 'make seed' to populate 3 sample vendor contracts."
