# Data Flow Diagram — Agentic Data Contract Platform

## Level 0 — Context Diagram

The system sits between vendor data sources and the data warehouse. It intercepts every vendor feed, validates it against a stored contract, and either lets it through or triggers an automated resolution pipeline.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        AGENTIC DATA CONTRACT PLATFORM                            │
│                                                                                  │
│   Vendor Feed ──────────────────────────────────────────────────► BigQuery       │
│   (JSON messages)      [detect drift → classify → heal or escalate]  (warehouse) │
│                                                                         ▲        │
│                                              Human Approval ────────────┘        │
│                                              (Swagger UI)                        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**External actors:**
- **Vendor** — sends raw JSON feed messages (Cars24, Paytm, etc.)
- **Data Engineer** — approves risky drift changes via Swagger UI
- **BigQuery** — destination data warehouse (vendor tables + audit trail)

---

## Level 1 — Component DFD

```
┌─────────────────┐
│  simulate_drift │  (vendor feed simulator)
│  .py            │
└────────┬────────┘
         │ JSON envelope
         │ {vendor_id, schema_version, payload}
         ▼
┌─────────────────────────────────────┐
│   Pub/Sub Topic: vendor-feeds       │  (Google Cloud Pub/Sub)
└────────────────────┬────────────────┘
                     │ pull (max 1000 msgs / batch)
                     ▼
         ┌───────────────────────┐
         │    drift-detector     │──────────────────────────────────────────────┐
         │                       │  GET /api/v1/contracts/{vendor_id}           │
         │  1. Parse envelope    │──────────────────────────────────────────────▼
         │  2. Infer live schema │                                  ┌──────────────────┐
         │  3. Load contract     │◄─────────────────────────────────│  contract-api    │
         │  4. Compare schemas   │  DataContract (fields + version) │  (FastAPI :8000) │
         │  5. Publish if drift  │                                  └──────┬───────────┘
         └───────────┬───────────┘                                         │ READ/WRITE
                     │ DriftEvent                                           │
                     │ {vendor_id, field, old_type,                        ▼
                     │  new_type, severity, is_safe}           ┌───────────────────────┐
                     ▼                                         │       BigQuery        │
         ┌──────────────────────────────┐                     │                       │
         │  Pub/Sub Topic: drift-events │                     │  dataset: contracts   │
         └──────────────┬───────────────┘                     │  ├── vendor_contracts │
                        │ subscribe                            │  └── drift_events     │
                        ▼                                      └───────────┬───────────┘
         ┌──────────────────────────────────────────┐                     │
         │           agent-engine                   │                     │
         │           (LangGraph :8002)              │                     │
         │                                          │                     │
         │  ┌─────────────────────────────────┐     │                     │
         │  │  Node 1: fetch_context          │─────┼── GET contract ─────┘
         │  │  GET contract via MCP           │     │
         │  │  GET drift history via MCP      │     │
         │  └──────────────┬──────────────────┘     │
         │                 │                         │
         │  ┌──────────────▼──────────────────┐     │
         │  │  Node 2: decide_action          │     │
         │  │  Gemini 2.5 Flash LLM           │     │
         │  │  → AUTO_HEAL / ESCALATE         │     │
         │  └────┬─────────────────┬──────────┘     │
         │       │                 │                 │
         │  ┌────▼──────┐   ┌──────▼──────┐         │
         │  │ Node 3a:  │   │  Node 3b:   │         │
         │  │ auto_heal │   │  escalate   │         │
         │  │           │   │             │         │
         │  │ Log event │   │ Log event   │         │
         │  │ AUTO_HEAL │   │ PENDING_    │         │
         │  │ via MCP   │   │ APPROVAL    │         │
         │  └─────┬─────┘   └──────┬──────┘         │
         │        │                │                 │
         └────────┼────────────────┼─────────────────┘
                  │                │
                  │  via MCP server│
                  ▼                ▼
         ┌────────────────────────────────────────┐
         │           mcp-server                   │
         │           (FastMCP :8001)              │
         │                                        │
         │  Tools:                                │
         │  ├── get_contract(vendor_id)           │───► contract-api GET
         │  ├── get_drift_history(vendor_id)      │───► BigQuery SELECT
         │  ├── log_drift_event(event)            │───► BigQuery INSERT
         │  ├── auto_approve(event_id)            │───► BigQuery INSERT
         │  └── list_vendors()                    │───► contract-api GET
         └────────────────────────────────────────┘

         When ESCALATE:
         ┌────────────────────────────────────────┐
         │  Data Engineer                         │
         │                                        │
         │  Swagger UI: http://localhost:8000/docs│
         │  POST /api/v1/approve/{event_id}       │───► contract-api ───► BigQuery UPDATE
         └────────────────────────────────────────┘
```

---

## Level 2 — BigQuery Data Model

```
dataset: contracts
│
├── vendor_contracts
│   ├── vendor_id        STRING    (PK: "cars24", "paytm")
│   ├── version          STRING    ("1.0.0", "2.0.0")
│   ├── contract_json    STRING    (JSON: list of {name, field_type, nullable})
│   └── registered_at   TIMESTAMP
│
└── drift_events
    ├── event_id              STRING    (UUID — PK)
    ├── vendor_id             STRING
    ├── drift_type            STRING    ("type_change", "rename_column", etc.)
    ├── field_name            STRING    ("user_id")
    ├── expected_type         STRING    ("INTEGER")
    ├── actual_type           STRING    ("STRING")
    ├── severity              STRING    ("LOW", "MEDIUM", "HIGH")
    ├── is_safe               BOOL
    ├── agent_decision        STRING    ("AUTO_HEAL", "ESCALATE")
    ├── agent_reasoning       STRING    (LLM explanation)
    ├── resolution_status     STRING    ("AUTO_HEALED", "PENDING_APPROVAL", "APPROVED")
    ├── approved_by           STRING    (email, null if auto-healed)
    └── detected_at           TIMESTAMP
```

---

## Level 2 — Pub/Sub Topics and Subscriptions

```
Topic: vendor-feeds
  └── Subscription: vendor-feeds-sub
        └── Consumer: drift-detector (pull, max 1000/batch)

Topic: drift-events
  └── Subscription: drift-events-sub
        └── Consumer: agent-engine (pull, 1 message triggers full graph)

Topic: heal-actions
  └── Subscription: heal-actions-sub
        └── Consumer: (Phase 5 — dbt trigger, not yet implemented)
```

---

## Local Dev vs Production Topology

| Component | Local (this setup) | Production |
|---|---|---|
| Pub/Sub | Emulator at `localhost:8085` | Google Cloud Pub/Sub |
| contract-api | Docker container, port 8000 | Cloud Run |
| mcp-server | Docker container, port 8001 | Cloud Run |
| agent-engine | Docker container, port 8002 | Cloud Run |
| drift-detector | Docker container | Cloud Run |
| BigQuery | Real GCP (free tier) | Real GCP |
| Credentials | `gcp-credentials.json` volume-mounted | Workload Identity |

---

## Startup Sequence (local dev)

```
1. docker compose up -d pubsub-emulator
2. python scripts/setup_pubsub_emulator.py    ← creates topics + subscriptions
3. docker compose up -d contract-api mcp-server agent-engine drift-detector
4. python scripts/seed_contracts.py           ← loads vendor contracts into BigQuery
5. python scripts/simulate_drift.py           ← fires test messages
6. watch Docker Desktop logs (drift-detector → agent-engine)
7. POST /api/v1/approve/{event_id} via Swagger if ESCALATE
```
