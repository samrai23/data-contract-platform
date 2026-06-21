# Agentic Data Contract & Ingestion Platform

> **Auto-detects vendor schema drift in real-time, classifies it, and either heals the pipeline autonomously or escalates to a human — with guardrails preventing any destructive operation.**

---

## The Problem This Solves

A vendor sends 500K JSON records daily. Today: `user_id` is an `INTEGER`. Tomorrow, without warning: `user_id` is a `STRING`. Your PySpark job crashes. BigQuery table goes stale. Analyst escalates at 9 AM. You spend 2 hours debugging.

This platform detects that drift in **under 90 seconds**, classifies it, and either auto-heals or routes to human approval — all with guardrails that prevent any destructive operation.

---

## Architecture

```
Vendor Feeds (JSON/CSV)
        │
        ▼
 Pub/Sub Topic (vendor-feeds)
        │
        ▼
Python Subscriber + PySpark ────► Schema Drift Detected?
        │                               │
        │ No drift                      │ Yes
        ▼                               ▼
 BigQuery (raw tables)        Drift Event → Kafka (drift-events)
                                         │
                                         ▼
                              LangGraph Agent Engine
                              ┌─────────────────────┐
                              │  Agent 1: Analyst   │ ← calls MCP Server
                              │  Agent 2: Writer    │ ← generates contract
                              │  Agent 3: Healer    │ ← applies fix or escalates
                              └─────────────────────┘
                                         │
                              ┌──────────┴──────────┐
                              │                     │
                         Safe drift            Risky drift
                              │                     │
                              ▼                     ▼
                      Auto-heal pipeline    Human approval
                      Update dbt schema     via FastAPI webhook
                      Run GE validation
                              │
                              ▼
                   BigQuery (healed data + audit trail)
                   dbt mart layer → Looker Studio dashboard
```

### Tech Stack (all free tier for portfolio)

| Layer | Technology | Why |
|---|---|---|
| Streaming ingest | GCP Pub/Sub (free tier — 10 GB/month) | Fully managed, no cluster ops, native GCP |
| Batch processing | PySpark on Dataproc Serverless | Handles scale, your core skill |
| Data warehouse | Google BigQuery (1TB/month free) | Delhi NCR market standard |
| Agent orchestration | LangGraph 1.0 | Production-grade, 400+ companies |
| Tool protocol | MCP (Model Context Protocol) | Secure, standardised agent tools |
| LLM | Gemini 2.0 Flash (1,500 req/day free) | No billing needed |
| Guardrails | NeMo Guardrails (Apache 2.0, free) | Prevents destructive operations |
| Transformation | dbt + BigQuery | Contract-as-code enforcement |
| Data quality | Great Expectations (open source) | Post-heal validation |
| API layer | FastAPI + Pydantic v2 | Type-safe, auto-documented |
| Containerisation | Docker + Docker Compose | Local dev parity |
| CI/CD | GitHub Actions → Cloud Run | Free tier deploy |

---

## Project Structure

```
data-contract-platform/
│
├── services/
│   ├── contract_api/          # FastAPI — human-facing API
│   │   ├── main.py            # App entry point, middleware
│   │   ├── routers.py         # All route definitions
│   │   ├── models.py          # Pydantic request/response models
│   │   ├── dependencies.py    # BigQuery client, auth injection
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── drift_detector/        # PySpark Structured Streaming job
│   │   ├── detector.py        # Main Spark job entry point
│   │   ├── schema_comparator.py  # Core drift detection logic
│   │   ├── event_publisher.py    # Publishes drift events to Kafka
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── agent_engine/          # LangGraph multi-agent system
│   │   ├── graph.py           # LangGraph state graph definition
│   │   ├── agents.py          # Agent implementations (Analyst, Writer, Healer)
│   │   ├── state.py           # Shared state schema (TypedDict)
│   │   ├── tools.py           # Tool wrappers for MCP calls
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── mcp_server/            # MCP tool server (secure BQ + contract access)
│       ├── server.py          # MCP server entry point
│       ├── bq_tools.py        # BigQuery tools (get_schema, run_query)
│       ├── contract_tools.py  # Contract registry tools
│       ├── Dockerfile
│       └── requirements.txt
│
├── spark_jobs/
│   ├── streaming/
│   │   ├── pubsub_drift_job.py # Main Pub/Sub subscriber + drift processing loop
│   │   ├── schema_infer.py     # Schema inference from JSON message batches
│   │   └── config.py           # Spark + Pub/Sub + BigQuery config
│   └── batch/
│       ├── audit_aggregator.py # Daily audit rollup to BigQuery
│       └── contract_sync.py    # Syncs contracts from registry to BQ
│
├── dbt_project/               # dbt transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml.example   # Copy to ~/.dbt/profiles.yml
│   └── models/
│       ├── staging/           # Raw vendor data + drift events
│       ├── intermediate/      # Contract version history
│       └── mart/              # vendor_health, drift_summary dashboards
│
├── guardrails/
│   ├── nemo_config/
│   │   ├── config.yml         # NeMo Guardrails main config
│   │   └── actions.py         # Custom Python actions (dry-run BQ)
│   └── policies/
│       ├── no_destructive_ops.co   # Colang: block DROP/DELETE/TRUNCATE
│       └── safe_schema_evolution.co # Colang: safe vs risky cast rules
│
├── data_contracts/
│   ├── schemas/               # Versioned contract JSON schemas
│   ├── registry/              # Active contract registry (git-versioned)
│   └── templates/
│       ├── base_contract.json      # Base contract template
│       └── pydantic_contract.py   # Pydantic model for contracts
│
├── infra/
│   ├── docker/                # Kafka + Zookeeper compose fragments
│   ├── cloud_run/             # Cloud Run service definitions
│   └── github_actions/
│       ├── ci.yml             # Lint + test on every PR
│       └── cd.yml             # Build + deploy to Cloud Run on main
│
├── tests/
│   ├── unit/                  # Fast, no external deps
│   ├── integration/           # Requires local Kafka + mock BQ
│   └── e2e/                   # Full drift → heal loop
│
├── docs/
│   ├── architecture/          # System design docs
│   ├── prompts/               # All LLM prompts with version history
│   └── runbooks/              # Local setup, drift simulation, deploy
│
├── scripts/
│   ├── simulate_drift.py      # 🔑 Send drift events to local Kafka
│   ├── seed_contracts.py      # Seed sample vendor contracts to BQ
│   ├── generate_vendor_feed.py # Generate test data
│   └── setup_bigquery.sh      # One-time BQ table creation
│
├── notebooks/                 # Jupyter notebooks for exploration
├── docker-compose.yml         # Full local stack
├── Makefile                   # All dev commands
├── pyproject.toml             # Single source of all Python deps
├── .env.example               # All required env vars documented
└── .gitignore                 # Includes GCP credentials exclusion
```

---

## Build Phases (follow this order)

### Phase 1 — Local Foundation (Week 1)
**Goal:** Get Kafka running locally, write and test the schema comparator.

- [ ] Set up local environment (see Local Setup below)
- [ ] Start Kafka + Kafka UI via `make up`
- [ ] Write `services/drift_detector/schema_comparator.py`
- [ ] Write unit tests for comparator — test all 4 drift types
- [ ] Run `make simulate-drift` and verify events appear in Kafka UI
- [ ] Write `data_contracts/templates/pydantic_contract.py`

**Commit message convention:** `feat(detector): add schema comparator with type drift detection`

### Phase 2 — BigQuery + FastAPI (Week 1–2)
**Goal:** Contracts stored in BQ, API to register vendors.

- [ ] Run `scripts/setup_bigquery.sh` against your GCP project
- [ ] Write `services/contract_api/models.py` (Pydantic v2 contracts)
- [ ] Write `services/contract_api/routers.py` (POST /vendors, GET /contracts)
- [ ] Test API at `http://localhost:8000/docs`
- [ ] Write `scripts/seed_contracts.py` and seed 3 sample vendors
- [ ] Integration test: register vendor → store in BQ → retrieve via API

### Phase 3 — MCP Server (Week 2)
**Goal:** Agents can query BQ and contracts without raw credential access.

- [ ] Write `services/mcp_server/bq_tools.py` (get_schema, get_table_stats)
- [ ] Write `services/mcp_server/contract_tools.py` (get_contract, get_drift_history)
- [ ] Test MCP server locally with a simple Python client
- [ ] Document each tool's input/output schema in `docs/`

### Phase 4 — LangGraph Agents (Week 2–3)
**Goal:** All 3 agents working in a graph, consuming real drift events.

- [ ] Write `services/agent_engine/state.py` — shared TypedDict state
- [ ] Write Agent 1 (Analyst) — reads drift event, classifies severity
- [ ] Write Agent 2 (Contract Writer) — generates new contract + dbt YAML
- [ ] Write Agent 3 (Healer) — applies fix or routes to human approval
- [ ] Wire graph in `services/agent_engine/graph.py` with conditional edges
- [ ] Add NeMo Guardrails to Agent 2 output
- [ ] Test full loop: simulate drift → agent → see heal or escalation

### Phase 5 — dbt + Great Expectations (Week 3)
**Goal:** Data quality enforced, marts available for dashboard.

- [ ] Write dbt staging models for vendor_feeds and drift_events
- [ ] Write dbt mart models for vendor_health dashboard
- [ ] Add GE validation suite on healed data before BQ write
- [ ] Run `dbt run` and `dbt test` — all green

### Phase 6 — CI/CD + Polish (Week 3–4)
**Goal:** Push to GitHub, CI passes, deploy to Cloud Run.

- [ ] Copy `infra/github_actions/ci.yml` → `.github/workflows/ci.yml`
- [ ] Push to GitHub — CI should pass on first try (if tests are green locally)
- [ ] Deploy contract-api to Cloud Run via CD workflow
- [ ] Write your `docs/prompts/` files — document each prompt iteration
- [ ] Write the README benchmark section with your actual test results

---

## Local Setup — Step by Step

### Prerequisites
Install these before anything else:

```bash
# 1. Python 3.11+
python --version  # should be 3.11.x or 3.12.x

# 2. Docker Desktop
docker --version
docker compose version

# 3. Git
git --version

# 4. VS Code extensions to install:
#    - Python (Microsoft)
#    - Docker (Microsoft)
#    - dbt Power User
#    - REST Client (for testing API without Postman)
```

### Step 1 — Clone and configure

```bash
# Clone your repo (after creating it on GitHub)
git clone https://github.com/YOUR_USERNAME/data-contract-platform.git
cd data-contract-platform

# Create your .env file
make env
# Now open .env and fill in your API keys (see Free Tier Setup below)
```

### Step 2 — Free Tier API Keys

Get these — all free, no credit card needed for dev:

**Google AI Studio (Gemini 2.0 Flash):**
1. Go to https://aistudio.google.com
2. Click "Get API Key" → Create API Key
3. Add to `.env`: `GEMINI_API_KEY=your-key`

**GCP / BigQuery (free $300 trial):**
1. Go to https://console.cloud.google.com
2. Create new project, enable billing (trial — no charge)
3. Enable BigQuery API, Pub/Sub API
4. IAM → Service Accounts → Create → Download JSON key
5. Save as `gcp-credentials.json` in project root (already in .gitignore)
6. Run: `bash scripts/setup_bigquery.sh`

**Confluent Kafka (free tier — 1 cluster, 5GB/month):**
1. Go to https://confluent.cloud → Sign up free
2. Create cluster → Basic → GCP → asia-south1
3. Create API Key → copy to `.env`
4. Create topics: `vendor-feeds`, `drift-events`, `heal-actions`
5. For local dev, use the local Kafka in docker-compose instead

### Step 3 — Start the local stack

```bash
# Install Python dependencies
make setup

# Start Kafka + all services
make up

# Verify everything is running
docker ps
# You should see: zookeeper, kafka, kafka-ui, contract-api, mcp-server, agent-engine, drift-detector

# Open Kafka UI
open http://localhost:8080

# Open FastAPI docs
open http://localhost:8000/docs
```

### Step 4 — Run your first drift simulation

```bash
# Seed a sample vendor contract
make seed

# Start sending vendor feed messages (with drift after message 10)
make simulate-drift

# Watch the agent-engine logs react to the drift
make logs-agent
```

You should see:
```
[drift-detector] ⚠️  Schema drift detected for vendor: cars24
[drift-detector] Drift type: type_change on field: user_id (INT → STRING)
[agent-engine]   Agent 1 (Analyst): severity=MEDIUM, classification=safe_cast
[agent-engine]   Agent 2 (Writer): generating updated contract v2...
[agent-engine]   NeMo Guardrails: output validated — no destructive ops
[agent-engine]   Agent 3 (Healer): applying schema fix to BigQuery...
[contract-api]   Contract updated: cars24 v1 → v2
```

### Connecting VS Code to GitHub

```bash
# 1. In VS Code, install the "GitHub Pull Requests" extension

# 2. Sign in to GitHub via VS Code
#    Ctrl+Shift+P → "GitHub: Sign In"

# 3. Create repo on GitHub first (github.com → New Repository)
#    Name: data-contract-platform
#    Private repo (keep it private until it's ready to show)

# 4. Link your local folder to GitHub
git init
git remote add origin https://github.com/YOUR_USERNAME/data-contract-platform.git
git branch -M main

# 5. First commit
git add .
git commit -m "chore: initial project structure"
git push -u origin main

# 6. From now on, VS Code Source Control panel handles commits
#    Or use the terminal — your call
```

### GitHub Actions Setup (for CI to work)

After pushing to GitHub:
1. Go to your repo → Settings → Secrets and variables → Actions
2. Add these secrets:
   - `GCP_PROJECT_ID` — your GCP project ID
   - `GEMINI_API_KEY` — your Google AI Studio key
   - `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` — for Cloud Run deploy (Phase 6 only)

---

## API Reference

```
POST /api/v1/vendors              Register a new vendor + their contract
GET  /api/v1/vendors              List all registered vendors
GET  /api/v1/contracts/{vendor}   Get active contract for a vendor
GET  /api/v1/drift-log            Audit trail of all drift events
POST /api/v1/approve/{event_id}   Human approval for risky schema changes
GET  /api/v1/health               Health check
```

Full interactive docs at `http://localhost:8000/docs` when running locally.

---

## Guardrails Policy Summary

The NeMo Guardrails Colang policies enforce:

| Operation | Allowed | Reason |
|---|---|---|
| INT → BIGINT | ✅ Auto-heal | Backward compatible widening |
| INT → STRING | ✅ Auto-heal | Safe cast, no data loss |
| STRING → INT | ⚠️ Human approval | Potential data loss |
| Add new column | ✅ Auto-heal | Additive, non-breaking |
| Remove column | ⚠️ Human approval | Breaking change |
| NULLABLE → REQUIRED | ⚠️ Human approval | Would fail existing records |
| DROP TABLE | ❌ Hard block | Forbidden by Colang rail |
| DELETE / TRUNCATE | ❌ Hard block | Forbidden by Colang rail |

---

## Results (fill in after building)

| Metric | Result |
|---|---|
| Drift events tested | ___ |
| Auto-healed (no human needed) | ___% |
| Mean time to detect | ___ seconds |
| Mean time to heal | ___ seconds |
| False positive rate | ___% |
| GE test pass rate post-heal | ___% |

---

## Author

**Sudhanshu Raina** | Data Engineer | Delhi NCR
[LinkedIn](https://www.linkedin.com/in/sudhanshu-raina-39a939189/) · [GitHub](https://github.com/samrai23)
