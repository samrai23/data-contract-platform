# Scripts Reference

All scripts live in `scripts/`. Run from the project root unless noted otherwise.

---

## setup_pubsub_emulator.py

**When to run:** Once after starting the Pub/Sub emulator container, before anything else.  
**Prerequisite:** `docker compose up -d pubsub-emulator` (wait ~5 seconds for the emulator to be ready)

```powershell
python scripts/setup_pubsub_emulator.py
```

**What it does:**
1. Waits for the emulator's TCP port (8085) to open
2. Waits an additional 4 seconds for the gRPC server to initialise
3. Creates 3 topics: `vendor-feeds`, `drift-events`, `heal-actions`
4. Creates 3 subscriptions: one per topic (same name + `-sub` suffix)
5. Prints a summary — all 6 resources must show ✅

**Idempotent:** Re-running when topics already exist prints ⚠️ (AlreadyExists) and exits 0 — safe to re-run any time.

**Critical setup detail:**  
`PUBSUB_EMULATOR_HOST` must be set *before* importing `google.cloud.pubsub_v1` — the library reads it at import time to choose its gRPC endpoint. Setting it after the import has no effect. This script does it correctly:
```python
os.environ.setdefault("PUBSUB_EMULATOR_HOST", "localhost:8085")
from google.cloud import pubsub_v1   # safe to import now
```

**Files:** `scripts/setup_pubsub_emulator.py`

---

## seed_contracts.py

**When to run:** Once after contract-api is healthy, before the first drift simulation.  
**Prerequisite:** `docker compose up -d contract-api` (BigQuery tables must exist)

```powershell
python scripts/seed_contracts.py
```

**What it does:**  
Calls `POST /api/v1/vendors` for each vendor defined in the script. The contract-api validates, deduplicates, and stores to BigQuery.

Currently seeds: `cars24` and `paytm`

**Re-run safety:** Existing vendors return `409 Conflict` — the script treats this as a skip, not a failure. Safe to re-run.

**Why seed through the API (not direct BigQuery writes):**  
Direct writes bypass FastAPI validation, Pydantic models, and the duplicate-check logic. Seeding via API is also an integration test — if any layer is broken, the seed fails and tells you exactly where.

**Files:** `scripts/seed_contracts.py`

---

## simulate_drift.py

**When to run:** After all services are up and contracts are seeded. This is the main test trigger.  
**Prerequisite:** All 5 containers running + `setup_pubsub_emulator.py` run + `seed_contracts.py` run

```powershell
# Default: 20 messages, drift injected after message 10, type_change on user_id
python scripts/simulate_drift.py

# Specific vendor and drift type
python scripts/simulate_drift.py --vendor cars24 --drift-after 10 --drift-type type_change

# All drift types
python scripts/simulate_drift.py --drift-type rename_column
python scripts/simulate_drift.py --drift-type add_column
python scripts/simulate_drift.py --drift-type drop_column
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `--vendor` | `cars24` | Which vendor schema to simulate (`cars24` or `paytm`) |
| `--topic` | `vendor-feeds` | Pub/Sub topic to publish to |
| `--total-messages` | `20` | Total messages to send |
| `--drift-after` | `10` | Inject drift starting at message N+1 |
| `--drift-type` | `type_change` | One of: `type_change`, `rename_column`, `add_column`, `drop_column` |
| `--delay` | `0.5` | Seconds between messages |

**What it does:**  
Messages 1–10: normal `cars24` schema (vehicle_id, user_id as INT, price, city, fuel_type)  
Messages 11–20: drifted schema — `type_change` changes `user_id` from INT to STRING

Each message is wrapped in a standard envelope:
```json
{
  "vendor_id": "cars24",
  "schema_version": "v1",
  "sequence": 11,
  "payload": { ...actual vendor fields... },
  "emitted_at": "2026-06-21T..."
}
```

**Expected output in drift-detector logs:**
```
drift_job.comparison_complete   has_drift=True   drift_event_count=1   vendor_id=cars24
event_publisher.published       drift_type=type_change   field=user_id   severity=LOW
```

**Expected output in agent-engine logs:**
```
graph.process_start        drift_type=type_change   vendor_id=cars24
agent.fetch_context        vendor_id=cars24
agent.decide_action.called_llm
agent.escalate             vendor_id=cars24
graph.process_complete     decision=ESCALATE
```

**Files:** `scripts/simulate_drift.py`

---

## list_gemini_models.py

**When to run:** Any time you get a `404 NOT_FOUND` on a Gemini model name, or when you want to check which models are actually available for your API key.

```powershell
python scripts/list_gemini_models.py
```

**What it does:**  
Calls `GET https://generativelanguage.googleapis.com/v1beta/models?key=<GEMINI_API_KEY>` and prints all models that support `generateContent`. This is the same endpoint that `langchain-google-genai` uses internally — so the output is exactly what your key can call.

**Why this script exists:**  
The GCP Quota Console lists quota entries for deprecated models long after Google removes them from the API. `gemini-2.0-flash` appeared in the quota console but returned `404 model no longer available`. Running this script confirmed `gemini-2.5-flash` as the correct current model.

**Rule:** Never trust the GCP Quota Console for model availability. Run this script to get the ground truth.

**Files:** `scripts/list_gemini_models.py`

---

## setup_bigquery.py

**When to run:** Once, before running any service for the first time. Creates the BigQuery dataset and tables.  
**Prerequisite:** `GOOGLE_APPLICATION_CREDENTIALS` set and pointing at a valid service account JSON.

```powershell
python scripts/setup_bigquery.py
```

**What it creates:**
- Dataset: `contracts` (in region `asia-south1`)
- Table: `vendor_contracts` (vendor_id, version, contract_json, registered_at)
- Table: `drift_events` (event_id, vendor_id, drift_type, field_name, severity, resolution_status, ...)

**Idempotent:** Uses `exists_ok=True` on dataset/table creation — safe to re-run.

---

## generate_vendor_feed.py

**When to run:** Standalone — generates a local JSON file of synthetic vendor records for offline testing or schema exploration.

```powershell
python scripts/generate_vendor_feed.py --vendor cars24 --count 100 --output feed.json
```

Does not publish to Pub/Sub — use `simulate_drift.py` for that. This script is for generating static test fixtures.

---

## test_mcp_server.py

**When to run:** During Phase 3 development to verify all 5 MCP tools work correctly.  
**Prerequisite:** `docker compose up -d contract-api` + `python services/mcp_server/server.py` (or `docker compose up -d mcp-server`)

```powershell
python scripts/test_mcp_server.py
```

**What it tests:**
1. `list_vendors` — returns all registered vendors
2. `get_contract(cars24)` — returns contract fields
3. `get_drift_history(cars24)` — returns recent drift events (empty is valid)
4. `log_drift_event(...)` — writes a test event to BigQuery
5. `auto_approve(event_id)` — marks the test event as AUTO_HEALED

**Important:** MCP is not REST. This script uses the FastMCP async `Client` which handles the SSE handshake and JSON-RPC encoding. `curl` does not work against the MCP server.

---

## test_agent_graph.py

**When to run:** During Phase 3 development to verify the LangGraph graph and Gemini integration before wiring up the full Docker stack.  
**Prerequisite:** `docker compose up -d contract-api mcp-server` + Gemini API key in `.env`

```powershell
python scripts/test_agent_graph.py
```

**What it tests:**

| Scenario | Input | Expected decision |
|---|---|---|
| A — safe widening | INT → BIGINT, severity=LOW, is_safe=True | AUTO_HEAL |
| B — risky cast | STRING → INT, severity=HIGH, is_safe=False | ESCALATE |

Both scenarios invoke the full graph: `fetch_context → decide_action → execute_*`. The test confirms Gemini is reachable, the MCP tools return data, and the conditional routing works correctly.
