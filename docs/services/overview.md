# Services Overview

Four Docker containers, each with a single responsibility. They communicate only through defined interfaces — HTTP, gRPC (Pub/Sub), and MCP over SSE.

---

## contract-api

**Port:** 8000  
**Tech:** FastAPI + Pydantic v2 + BigQuery  
**Purpose:** Single source of truth for vendor contracts. All other services read contracts through this API — nothing reads BigQuery directly for contract data.

### Key files

| File | Responsibility |
|---|---|
| `main.py` | App entry point, lifespan handler, CORS middleware |
| `routers.py` | All route handlers — vendor registration, contract lookup, drift approval |
| `models.py` | Pydantic request/response models with validation |
| `dependencies.py` | BigQuery client singleton via `lru_cache` + `Depends()` |

### Endpoints

```
POST /api/v1/vendors              Register vendor + contract → BigQuery batch load
GET  /api/v1/vendors              List all vendors
GET  /api/v1/contracts/{vendor}   Get active contract for a vendor
GET  /api/v1/drift-log            Audit trail of all drift events
POST /api/v1/approve/{event_id}   Human approval → BigQuery DML UPDATE
GET  /api/v1/health               Health check
```

### BigQuery write strategy

Vendor registration uses `load_table_from_json()` (batch load) — the only write API available on BigQuery free tier without billing. The approve endpoint uses `bq.query("UPDATE ...")` (DML) which requires billing enabled on the project.

### Why all reads go through this API (not direct BigQuery)

drift-detector, agent-engine, and mcp-server all call `GET /api/v1/contracts/{vendor_id}` instead of querying BigQuery directly. This keeps contract validation logic, response schema, and caching in one place — one service, one source of truth.

---

## drift-detector

**Port:** none (no inbound HTTP — pull-only subscriber)  
**Tech:** Python + google-cloud-pubsub + httpx  
**Purpose:** Continuously pulls vendor feed messages from Pub/Sub, infers the live schema from each batch, compares against the stored contract, and publishes a drift event if schemas diverge.

### Key files

| File | Responsibility |
|---|---|
| `detector.py` | Entry point — logging config, signal handlers, starts subscriber loop |
| `spark_jobs/streaming/pubsub_drift_job.py` | Main loop — pull → group by vendor → compare → publish |
| `spark_jobs/streaming/schema_infer.py` | Infers field types from a batch of JSON payloads |
| `services/drift_detector/schema_comparator.py` | Compares live schema against stored contract, returns DriftReport |
| `services/drift_detector/event_publisher.py` | Publishes DriftEvent to `drift-events` Pub/Sub topic |
| `spark_jobs/streaming/config.py` | Loads Pub/Sub + BigQuery config from env vars |

### Processing loop

```
pull(max_messages=1000, timeout=30s)
  → group messages by vendor_id
  → for each vendor:
      1. GET contract from contract-api (cached in memory)
      2. infer_schema_from_batch(payload_strings)
      3. compare_schemas(contract, live_schema)
      4. if drift: publish DriftEvent to drift-events topic
  → acknowledge all messages
  → sleep 5s if idle
```

### Schema inference approach

Rather than comparing one message at a time (unreliable — a single `user_id=123` could be INT or STRING), the detector collects a full batch of up to 1000 messages, infers types across all of them, and does one comparison. This is why synchronous pull is used instead of async streaming callbacks.

### Phase 1 → Phase 3 upgrade: ApiContractRegistry

Phase 1 loaded contracts from local JSON files. Phase 3 replaced this with `ApiContractRegistry` — an HTTP client that calls `GET /api/v1/contracts/{vendor_id}` on contract-api. This ensures the detector always uses the same contract data that was registered via the API, not a stale local file.

---

## mcp-server

**Port:** 8001  
**Tech:** FastMCP + httpx + BigQuery  
**Purpose:** Secure tool layer between the agent-engine and data stores. The agent never calls BigQuery or contract-api directly — it calls named tools exposed by the MCP server. This enforces the principle that agents should not have raw database access.

### Key files

| File | Responsibility |
|---|---|
| `server.py` | FastMCP app — registers tools, starts SSE server |
| `contract_tools.py` | Tools that call contract-api over HTTP |
| `bq_tools.py` | Tools that read/write BigQuery directly |

### Tools exposed

| Tool | Backend | What it does |
|---|---|---|
| `get_contract(vendor_id)` | contract-api HTTP | Returns stored contract fields and version |
| `get_drift_history(vendor_id)` | BigQuery SELECT | Last N drift events for this vendor |
| `log_drift_event(event)` | BigQuery INSERT | Writes drift event with PENDING_APPROVAL status |
| `auto_approve(event_id)` | BigQuery INSERT | Writes AUTO_HEALED resolution |
| `list_vendors()` | contract-api HTTP | Lists all registered vendors |

### Why MCP instead of direct function calls

MCP (Model Context Protocol) adds a protocol boundary between the agent and its tools:
- Tools are declared with input/output schemas — the LLM sees a typed interface
- The agent cannot call arbitrary code — only the declared tools
- NeMo Guardrails can inspect tool calls before they execute
- The server can run independently and be shared across multiple agents

### Protocol: not REST

MCP over SSE is not a REST API. It uses JSON-RPC over a persistent SSE connection:
- `GET /sse` — opens the connection
- `POST /messages/?session_id=...` — sends JSON-RPC method calls on that connection

`curl` and `Invoke-RestMethod` do not work. Use the FastMCP async `Client` or `scripts/test_mcp_server.py`.

---

## agent-engine

**Port:** 8002  
**Tech:** LangGraph + langchain-google-genai + httpx  
**Purpose:** Subscribes to drift events, runs them through a LangGraph state graph, and either auto-heals or escalates for human approval. This is where Gemini makes the routing decision.

### Key files

| File | Responsibility |
|---|---|
| `graph.py` | Defines and compiles the LangGraph StateGraph; subscriber loop |
| `agents.py` | Node implementations — fetch_context, decide_action, auto_heal, escalate |
| `state.py` | TypedDict shared state schema |
| `tools.py` | MCP client wrappers — calls mcp-server tools |

### Graph structure

```
START
  ↓
fetch_context      ← calls get_contract + get_drift_history via MCP
  ↓
decide_action      ← Gemini 2.5 Flash reads context, returns AUTO_HEAL or ESCALATE
  ↓ (conditional edge)
  ├── AUTO_HEAL  → execute_auto_heal  ← calls auto_approve via MCP → BigQuery
  └── ESCALATE   → execute_escalate  ← calls log_drift_event via MCP → BigQuery
  ↓
END
```

### Why LLM routing (not rule-based)

A rule like `if severity == HIGH: escalate` handles clear cases but misses nuance. The LLM reads the full context — contract version, drift type, severity, is_safe flag, and drift history — and makes a judgment call. Example: INT → BIGINT on a field that has drifted before is escalated even if severity=LOW, because repeated drift on the same field signals an unstable vendor schema.

### Gemini integration

`langchain-google-genai` reads `GOOGLE_API_KEY` from the environment. Since the `.env` file uses `GEMINI_API_KEY`, the agents module renames it at startup:
```python
if not os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
```

Model is set via `GEMINI_MODEL=gemini-2.5-flash` in `.env`. `temperature=0` for deterministic routing decisions.

### GCP credentials inside Docker

The volume mount puts `gcp-credentials.json` at `/app/gcp-credentials.json`. The `docker-compose.yml` sets `GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-credentials.json` explicitly in the `environment:` block to override the relative path in `.env` — the relative path resolution fails inside Docker because `Path(__file__).parent.parent.parent` resolves to filesystem root (`/`), not the project root.
