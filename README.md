# Agentic Data Contract & Ingestion Platform

> **Detects vendor schema drift from a live Pub/Sub feed, classifies it, and either auto-heals the stored contract or escalates to a human for approval — with a full audit trail either way.**

This README was rewritten on 2026-07-30 to match what's actually in this repository, verified file by file. Where earlier drafts of this README described the original plan rather than what got built, this version says so explicitly instead of leaving it implied.

---

## The Problem This Solves

A vendor sends a JSON feed daily. One day, without warning, a field's data type changes, a column disappears, or a column gets renamed. Every downstream pipeline reading that feed breaks — silently or loudly — and someone finds out hours later when a dashboard looks wrong.

This platform watches vendor feeds in near-real-time via Pub/Sub, detects that kind of schema drift automatically, classifies how risky it is, and either fixes it without a human or routes it to a human for approval, logging every decision either way.

---

## Architecture (what actually runs)

```
Vendor Feeds (JSON)
        │
        ▼
 Pub/Sub Topic (vendor-feeds)
        │
        ▼
 drift-detector (plain Python, no PySpark)
        │  ├─ HTTP GET → contract-api (fetch stored contract)
        │  └─ schema_comparator.py: infer + compare + classify
        ▼
 Pub/Sub Topic (drift-events)   ← only fires if drift was detected
        │
        ▼
 agent-engine — ONE LangGraph StateGraph, 4 nodes
 ┌───────────────────────────────────────────────┐
 │ fetch_context → decide_action (Gemini 2.5)     │
 │        │                                       │
 │   AUTO_HEAL ─────────────┐   ESCALATE/IGNORE ──┼─┐
 └───────────────────────────┼─────────────────────┼─┘
                              ▼                     ▼
                  log + approve + PATCH        log as
                  /contracts/{id}/heal        PENDING_APPROVAL
                  (bumps contract version)          │
                              │                      ▼
                              │              Human via Swagger UI
                              │              POST /approve/{event_id}
                              ▼
                         BigQuery (vendor_contracts, drift_events)
```

Runs as **4 Docker containers** (contract-api, mcp-server, agent-engine, drift-detector) plus a local Pub/Sub emulator — verified by `docker-compose.yml`, which is the ground truth for what's wired together. There is no Kafka container anywhere in this stack.

### The one correction worth stating up front

The original plan for this project (an earlier draft of this README) described **three separate agents** — Analyst, Contract Writer, Healer. What's actually built is **one LangGraph `StateGraph` with 4 nodes** (`fetch_context`, `decide_action`, `execute_auto_heal`, `execute_escalate`). This was a deliberate simplification, not a shortfall — see `docs/architecture/system_design.md` for the full reasoning (short version: all three "agents" need to share the same drift event / contract / decision at every step, so a single shared-state graph does the job without an artificial message-passing layer between agent processes).

---

## What's Actually Built (Done)

- **Pub/Sub ingestion** — real, working. `docker-compose.yml` runs a local Pub/Sub emulator; `pubsub_drift_job.py` and `event_publisher.py` use `google-cloud-pubsub` directly. No Kafka.
- **Schema drift detection** — pure Python, zero external dependencies (`schema_comparator.py`, `schema_infer.py`), unit-tested (558 lines of tests). Detects 4 drift types: `type_change`, `new_column`, `dropped_column`, `column_rename`, each classified LOW/MEDIUM/HIGH with a widening-vs-narrowing type table deciding auto-heal safety.
- **Rename-detection heuristic** — only declares a rename when exactly one dropped field and one new field share a type; ambiguous many-to-many matches are deliberately left as separate drops/adds rather than guessed.
- **LangGraph agent** — 1 StateGraph, 4 nodes, Gemini 2.5 Flash (`temperature=0`) makes the AUTO_HEAL / ESCALATE / IGNORE call. IGNORE is routed to the same handler as ESCALATE (never silently drops an event). A malformed/unparseable LLM response defaults to ESCALATE, never AUTO_HEAL.
- **MCP server** — 1 server, 5 tools (`get_contract`, `list_vendors`, `log_drift_event`, `get_drift_history`, `submit_approval`), FastMCP over SSE transport.
- **Contract API** — FastAPI, 7 endpoints (see API Reference below), BigQuery-backed, parameterized queries throughout (no SQL injection surface).
- **Contract-writer / auto-heal now actually updates the contract** *(added 2026-07-30)* — `PATCH /api/v1/contracts/{vendor_id}/heal` applies the healed field change to the stored contract and calls `bump_minor()`/`bump_major()` on the domain model, then persists it via a parameterized `UPDATE`. Previously `execute_auto_heal()` only logged the event and approved it without ever touching `contract_json` — meaning the same field would drift again on the next batch. That gap is closed now.
- **Unit test coverage** — real and substantial: `test_schema_comparator.py` (558 lines), `test_contract_validator.py` (209 lines), `test_api_endpoints.py` integration (347 lines).
- **Simulation harness** — `scripts/simulate_drift.py` sends synthetic drift for 2 vendors × 4 drift types. Each drift type is now isolated to exactly the intended field(s) via explicit `rename`/`override`/`drop`/`add` transforms *(fixed 2026-07-30 — a prior version's field-omission bug meant every run silently polluted results with extra bogus events)*.

---

## Explicitly NOT Built (and why that's fine to say out loud)

| Component | Status | Reality |
|---|---|---|
| **Kafka** | Not used | `infra/docker/kafka.yml` and `zookeeper.yml` exist but are never referenced by `docker-compose.yml` — leftover from the original design, before the pivot to Pub/Sub. Pub/Sub is fully managed, needs no ZooKeeper/broker ops, and has a free local emulator with an API identical to production. |
| **PySpark / Spark Structured Streaming** | Not implemented | `schema_infer.py`'s own docstring says "deliberately dependency-free — no PySpark." The whole detection path is plain Python using `google-cloud-pubsub`'s synchronous pull API. `spark_jobs/streaming/config.py` has a fully-written `SparkSessionConfig` dataclass for a *future* Dataproc migration, but nothing ever instantiates it. |
| **dbt** | Not needed, not implemented | No `dbt_project/` directory exists. `dbt-bigquery` is listed in `pyproject.toml` but unused. Decided we don't need it for this project's scope — contract validation already happens in Pydantic, not a transformation layer. |
| **Great Expectations** | Not implemented | Listed in `pyproject.toml`, never wired in. No GE suite/checkpoint files exist. |
| **NeMo Guardrails** | Not implemented | `guardrails/nemo_config/{config.yml,actions.py}` and `guardrails/policies/*.co` are all 0 bytes. Destructive-op prevention is **structural instead**: every BigQuery write in this system is either `load_table_from_json()` (INSERT-only, can't DROP/DELETE/TRUNCATE) or one fixed, hand-written, parameterized `UPDATE` query — there is no code path anywhere that builds arbitrary SQL from agent or user input. |
| **Batch jobs** (`audit_aggregator.py`, `contract_sync.py`) | Scaffolded, empty | Both 0 bytes. `pipeline_audit` BigQuery table exists (created by `setup_bigquery.py`) but nothing writes to it yet. |
| **E2E / some integration tests** | Scaffolded, empty | `tests/unit/test_agent_graph.py`, `tests/integration/test_drift_pipeline.py`, `tests/e2e/test_full_heal_loop.py` are all 0 bytes. |

---

## Results

No `%` auto-healed or mean-time-to-heal numbers are published here yet. Earlier notes for this project stated "84% auto-healed... under 90 seconds" — those were placeholder targets from the original planning doc, never actually measured, and have been removed rather than repeated. This section will be filled in with real `make simulate-drift` results once they've been properly measured.

---

## Future Scope (not yet built — priority order)

1. **Wrap the Gemini call in `decide_action()` in a try/except** — right now a network failure or API error during the LLM call itself (not just a malformed response) raises an unhandled exception for that one event. The outer subscriber loop catches it and doesn't crash, but that event's decision is lost rather than defaulting to ESCALATE.
2. **Write the empty test files** — `test_agent_graph.py`, `test_drift_pipeline.py`, `test_full_heal_loop.py` — especially the E2E test, since it's the only thing that would catch a regression across the full simulate → detect → decide → heal loop.
3. **Measure and record real performance numbers** — run `make simulate-drift` across all 4 drift types × both vendors repeatedly and compute a real AUTO_HEAL/ESCALATE percentage from `drift_events`, instead of leaving the Results section blank.
4. **Implement the batch jobs** (`audit_aggregator.py`, `contract_sync.py`) — `pipeline_audit` table exists with no writer.
5. **NeMo Guardrails as defense-in-depth** — not because the system is currently unsafe (see the "Explicitly NOT Built" table above for why), but as an additional inspection layer if the agent ever generates more open-ended remediation actions than AUTO_HEAL/ESCALATE.
6. **Multi-consumer-aware severity** — right now `is_safe_to_auto_heal` is one global verdict; a genuinely bigger feature would let different downstream consumers register which fields they actually read, so a dropped column nobody reads could be LOW severity instead of always HIGH.
7. **Planned-migration awareness** — let a vendor/engineer pre-register an expected schema change (e.g. "user_id becomes STRING on Tuesday") so the agent can distinguish a communicated migration from a genuine surprise, instead of routing both through the identical ESCALATE path.
8. **Horizontal scaling for agent-engine** — currently processes one Pub/Sub message at a time, synchronously, by design (each LLM call is 1-3s, no thread-safety complexity needed at current volume). If drift-event frequency ever exceeds that, the fix is more agent-engine replicas on the same subscription, not a threading rewrite.

---

## Tech Stack (what's actually used)

| Layer | Technology | Why |
|---|---|---|
| Message broker | GCP Pub/Sub (local emulator for dev) | Fully managed, no ZooKeeper/broker ops, free tier, local emulator with an API identical to production |
| Drift detection | Pure Python (no PySpark) | Zero infra dependency for unit tests; transport-layer decoupling means the comparison logic doesn't care whether messages came from Pub/Sub or a file |
| Data warehouse | Google BigQuery | SQL-queryable audit trail; free tier allows read/DDL/batch-load; DML (contract heal + human approval) requires a linked billing account |
| Agent orchestration | LangGraph (`StateGraph`, 4 nodes) | Shared `TypedDict` state across nodes without a message-passing layer; conditional edges for AUTO_HEAL vs ESCALATE routing |
| LLM | Gemini 2.5 Flash via `langchain-google-genai` | Free tier (1,500 req/day), `temperature=0` for deterministic decisions |
| Tool protocol | MCP (FastMCP, SSE transport) | Typed tool schemas for the LLM; SSE keeps one connection open across a rapid sequence of tool calls |
| API layer | FastAPI + Pydantic v2 | Auto-generated Swagger docs at `/docs`, async-native, `Depends()` injection for a singleton BigQuery client |
| Containerization | Docker + Docker Compose | One container per service, mirrors a 1:1 Cloud Run mapping for production |
| CI/CD | GitHub Actions (`ci.yml` + `cd.yml`) | Lint + test on PR; deploy to Cloud Run on merge to main |

---

## Project Structure

```
data-contract-platform/
├── services/
│   ├── contract_api/       # FastAPI — 7 endpoints, BigQuery-backed
│   ├── drift_detector/     # Pub/Sub subscriber + schema_comparator.py (the core algorithm)
│   ├── agent_engine/       # LangGraph — 4 nodes, Gemini 2.5 Flash
│   └── mcp_server/         # FastMCP — 5 tools, SSE transport
│
├── spark_jobs/
│   ├── streaming/          # Real code (misleadingly named) — pubsub_drift_job.py, schema_infer.py, config.py
│   └── batch/               # Empty scaffolding — audit_aggregator.py, contract_sync.py (0 bytes)
│
├── guardrails/              # NeMo Guardrails scaffolding — ALL FILES EMPTY (0 bytes), not implemented
│
├── data_contracts/
│   ├── templates/           # pydantic_contract.py — the domain model (VendorContract, bump_minor/bump_major)
│   ├── registry/            # Phase 1 static contract files, superseded by the live contract-api
│   └── schemas/
│
├── infra/
│   ├── docker/               # kafka.yml, zookeeper.yml — NOT referenced by docker-compose.yml, unused leftovers
│   ├── cloud_run/
│   └── github_actions/
│
├── tests/
│   ├── unit/                 # test_schema_comparator.py (558 lines), test_contract_validator.py (209 lines) — real
│   ├── integration/          # test_api_endpoints.py (347 lines, real); test_drift_pipeline.py (0 bytes, empty)
│   └── e2e/                  # test_full_heal_loop.py — 0 bytes, empty
│
├── docs/
│   ├── architecture/         # system_design.md — every architecture decision + its rejected alternative
│   ├── prompts/
│   └── runbooks/
│
├── scripts/
│   ├── simulate_drift.py     # Main testing/demo tool — 2 vendors × 4 drift types
│   ├── seed_contracts.py     # Seeds via the live API (not direct BQ writes) — doubles as an integration test
│   ├── setup_bigquery.py     # One-time BQ dataset/table creation (Python version — bash version also exists)
│   ├── setup_pubsub_emulator.py
│   └── list_gemini_models.py # Diagnostic: checks which Gemini models your key can ACTUALLY call
│
├── docker-compose.yml         # Ground truth for what actually runs — 4 services + Pub/Sub emulator, no Kafka
├── Makefile
├── pyproject.toml             # Includes some unused deps (dbt-bigquery, great-expectations, pyspark, nemoguardrails)
└── .env.example
```

---

## Local Setup

### Prerequisites

```bash
python --version   # 3.11.x or 3.12.x
docker --version
docker compose version
git --version
```

### Step 1 — Clone and configure

```bash
git clone https://github.com/samrai23/data-contract-platform.git
cd data-contract-platform
make env   # copies .env.example to .env — fill in your keys
```

### Step 2 — Free-tier API keys

**Google AI Studio (Gemini):**
1. Go to https://aistudio.google.com → "Get API Key" → Create API Key
2. Add to `.env`: `GEMINI_API_KEY=your-key`, `GEMINI_MODEL=gemini-2.5-flash`
3. If you get a `404` or `PERMISSION_DENIED` on any model, run `python scripts/list_gemini_models.py` — it hits the live `v1beta/models` endpoint directly, which is the actual source of truth (the GCP Quota Console lags behind and can show stale info in both directions).

**GCP / BigQuery:**
1. Create a GCP project, enable the BigQuery and Pub/Sub APIs
2. IAM → Service Accounts → Create → download JSON key → save as `gcp-credentials.json` in the project root (already gitignored)
3. Run `python scripts/setup_bigquery.py`
4. **If you need `POST /approve/{event_id}` or `PATCH /contracts/{vendor_id}/heal` to actually persist**, link a billing account to this project (Console → Billing). Batch-load writes (vendor registration, drift logging) work without it; these two DML-based endpoints don't.

No Kafka account or setup needed — Pub/Sub is the only message broker this project uses.

### Step 3 — Start the local stack

```bash
make up   # docker compose up -d --build, then creates Pub/Sub topics + subscriptions automatically
```

The emulator's topics/subscriptions don't persist across a fresh container start, so `make up` always (re)creates them — this is idempotent, safe to run repeatedly.

```
FastAPI docs:   http://localhost:8000/docs
Pub/Sub emu:    http://localhost:8085
MCP Server:     http://localhost:8001
```

### Step 4 — Run a drift simulation

```bash
make seed              # seed sample vendor contracts
make simulate-drift    # sends drift after message N — see scripts/simulate_drift.py --help for options
make logs-agent        # watch the agent react
```

Realistic example of what you'll actually see in `agent-engine` logs for a clean widening type change:
```
agent.decide_action.decided  decision=AUTO_HEAL  reasoning="The drift is a safe widening type
  change from INT to STRING for 'user_id', which has a LOW severity and is marked as safe to
  auto-heal. There is no history of repeated drifts on this specific field, meeting all criteria
  for auto-healing."
```
And for a dropped column:
```
agent.decide_action.decided  decision=ESCALATE  reasoning="The drift event is a 'dropped_column'
  for 'fuel_type', which has a HIGH severity and is explicitly marked as not safe to auto-heal.
  This critical breaking change requires immediate human review."
```

---

## API Reference

```
POST   /api/v1/vendors                     Register a new vendor + their contract (batch load, free tier)
GET    /api/v1/vendors                     List all registered vendors
GET    /api/v1/contracts/{vendor_id}       Get the active contract for a vendor
PATCH  /api/v1/contracts/{vendor_id}/heal  Apply an auto-healed drift + bump version (DML — needs billing)
GET    /api/v1/drift-log                   Audit trail of all detected drift events
POST   /api/v1/approve/{event_id}          Human approval/rejection of a risky drift (DML — needs billing)
GET    /api/v1/health                      Liveness + BigQuery connectivity check
```

Full interactive docs at `http://localhost:8000/docs` when running locally.

---

## Destructive-Operation Safety (the real mechanism, not NeMo Guardrails)

The system never issues a raw `DROP`/`DELETE`/`TRUNCATE` in the first place, so there's nothing to block at runtime:

| Operation | Mechanism | Requires billing? |
|---|---|---|
| Register vendor / log drift event | `load_table_from_json()` — INSERT-only batch load | No |
| Auto-heal contract update | One fixed, parameterized `UPDATE` on `vendor_contracts` | Yes |
| Human approval/rejection | One fixed, parameterized `UPDATE` on `drift_events` | Yes |

There is no code path anywhere that builds a SQL string from agent output or user input and executes it. Safety comes from constraining *what operations are structurally possible*, not from a runtime guardrails layer inspecting output after the fact.

---

## Author

**Sudhanshu Raina** | Data Engineer | Delhi NCR
[LinkedIn](https://www.linkedin.com/in/sudhanshu-raina-39a939189/) · [GitHub](https://github.com/samrai23)
