# System Design — Decisions and Trade-offs

Every architecture decision in this platform had alternatives. This document records what was chosen, what was rejected, and why.

---

## Message broker — Pub/Sub over Kafka

**Chosen:** Google Cloud Pub/Sub (emulated locally)  
**Rejected:** Apache Kafka (Confluent Cloud or self-hosted)

**Why Pub/Sub:**
- Fully managed — no ZooKeeper, no broker config, no partition rebalancing
- Native GCP service — integrates directly with Dataflow, Cloud Run, BigQuery subscriptions
- Free tier: 10 GB/month ingress + 10 GB/month egress
- Local emulator available via Google Cloud SDK — identical API to production

**Why not Kafka:**
- Self-hosted Kafka requires ZooKeeper + broker management
- Confluent Cloud free tier is limited and requires credit card
- Kafka's strength (log compaction, replay, consumer group offsets) is overkill for this use case
- Adds operational complexity without adding capability at this scale

**Trade-off accepted:** Pub/Sub does not support message replay (unless you use BigQuery subscriptions or archive to GCS). If the drift-detector crashes mid-batch, unacknowledged messages are re-delivered — which is the correct behaviour here.

---

## Pull vs push subscription

**Chosen:** Synchronous pull (`subscriber.pull(max_messages=1000)`)  
**Rejected:** Async streaming pull / push webhooks

**Why synchronous pull:**

Schema drift detection is a statistical inference problem. A single message saying `user_id=123` is ambiguous — it could be INT or STRING. You need a batch of N messages to reliably determine the live schema.

Synchronous pull collects up to 1000 messages per call, infers schema across the entire batch, then does one comparison. This is inherently batch-oriented.

**Why not async streaming:**
- Async streaming fires a callback per message
- You'd need to buffer messages, track when to flush the buffer, and aggregate yourself
- That re-implements what synchronous pull gives for free
- Async streaming is the right choice for low-latency single-message processing — not batch schema inference

---

## Contract storage — BigQuery over PostgreSQL / Firestore

**Chosen:** BigQuery (`vendor_contracts` table, JSON string for fields)  
**Rejected:** PostgreSQL, Firestore, local JSON files

**Why BigQuery:**
- The primary audience for this data is data engineers who already query BigQuery
- The audit trail (`drift_events`) needs to join with `vendor_contracts` — same warehouse = simple SQL
- No additional infrastructure to manage for a GCP-native project
- Free tier: 1 TB/month query processing, 10 GB storage

**Why not PostgreSQL:**
- Adds a managed database dependency (Cloud SQL) with cost
- Separates contract data from the rest of the data warehouse

**Why not Firestore:**
- Not SQL-queryable — poor for ad-hoc drift analysis
- No join capability with BigQuery without a pipeline

**Why contract_fields stored as JSON string (not ARRAY\<STRUCT\>):**

BigQuery ARRAY\<STRUCT\> requires verbose schema definition and complex query syntax (`UNNEST`, `CROSS JOIN`). A separate `contract_fields` table would require a JOIN on every read.

Storing fields as a JSON string in `contract_json` keeps each read/write as one row operation. The entire contract is always read as a unit anyway — we never need to filter by individual field names in SQL.

Trade-off: can't do `WHERE field_name = 'user_id'` in standard SQL (need `JSON_VALUE()`). Acceptable given the access pattern.

---

## BigQuery write API — batch load over streaming insert

**Chosen:** `load_table_from_json()` (batch load)  
**Rejected:** `insert_rows_json()` (streaming insert), `bq.query("INSERT INTO ...")` (DML)

**Why batch load:**

BigQuery free tier (project without billing) blocks:
- Streaming insert → `403 Streaming insert is not allowed in the free tier`
- DML INSERT/UPDATE → `403 DML queries are not allowed in the free tier`

Batch load (`load_table_from_json`) is a load job — the same API that `bq load`, Spark, and Dataflow use. It is free on all tiers.

**Trade-off accepted:** ~5-10 second latency per write (load jobs are not real-time). For vendor registration (rare, one-off per vendor) and drift event logging (infrequent), this is completely acceptable.

**Exception — approve endpoint:** Uses DML UPDATE to change `resolution_status` on an existing row. Batch load can only append — it cannot modify existing rows. This endpoint requires billing enabled on the BigQuery project.

---

## Agent architecture — single graph over multi-agent

**Chosen:** Single LangGraph StateGraph with 4 nodes  
**Rejected:** Multi-agent with separate Analyst, Writer, and Healer agents

**Why single graph:**

The original architecture (README) described three separate agents: Analyst, Contract Writer, Healer. In practice, these three "agents" need to share state (the same drift event, the same contract, the same decision) at every step. Separate agents would require a message-passing layer between them.

A LangGraph StateGraph gives this for free: one TypedDict flows through all nodes, each node reads what it needs and writes what it produces. No message passing required.

**Current nodes:**
1. `fetch_context` — pulls contract + drift history
2. `decide_action` — Gemini LLM classifies drift and decides action
3. `execute_auto_heal` — logs event as AUTO_HEALED, calls approve tool
4. `execute_escalate` — logs event as PENDING_APPROVAL, awaits human

**Phase 5 expansion:** Contract writer logic (bumping the contract version, updating `contract_json` in BigQuery) will be added as a node between `execute_auto_heal` and END. The graph structure makes this additive — no existing nodes change.

---

## Routing logic — LLM over rule-based

**Chosen:** Gemini LLM reads drift event + history, returns AUTO_HEAL / ESCALATE  
**Rejected:** Hard-coded rules (`if severity == HIGH: escalate`)

**Why LLM:**

Rule-based logic handles clear-cut cases but misses nuance:

| Case | Rule-based | LLM |
|---|---|---|
| INT → BIGINT (safe widening) | Needs explicit rule | ✅ recognises as safe |
| STRING → INT (data loss risk) | Needs explicit rule | ✅ recognises as risky |
| city → city_name rename (cosmetic) | Can't distinguish from semantic rename | ✅ judges as cosmetic |
| city → postal_code rename | Can't distinguish from cosmetic rename | ✅ judges as semantic break |
| Repeated drift on same field | Would need stateful counter rule | ✅ reads history, judges accordingly |

The LLM reads the full context — contract version, is_safe flag, drift history — and applies reasoning that would take dozens of rules to replicate, and still miss edge cases.

**`temperature=0`** for deterministic output — the same drift event should always produce the same decision.

**Fallback:** If the LLM returns invalid JSON, the code defaults to ESCALATE (the safe choice). Never AUTO_HEAL on a parse failure.

---

## Tool protocol — MCP over direct function calls

**Chosen:** MCP (Model Context Protocol) over FastMCP SSE  
**Rejected:** Direct Python function calls, REST API

**Why MCP:**

The agent-engine could call BigQuery and contract-api directly. MCP adds a protocol boundary:

- Tools are declared with typed input/output schemas — the LLM receives a structured interface
- The agent cannot call arbitrary code — only declared tools
- NeMo Guardrails can inspect tool calls before they execute
- The MCP server can be upgraded or swapped independently of the agent

**Why SSE (not REST):**

An agent calls multiple tools in rapid sequence during reasoning (get contract → get history → log event → approve). SSE keeps one TCP connection open for all calls — faster than a new HTTP connection per call. SSE also allows the server to push progress updates for long-running tools.

---

## Container isolation — one service per container

**Chosen:** Separate Docker containers for contract-api, mcp-server, agent-engine, drift-detector  
**Rejected:** Monolith, two-container split

**Why separate containers:**

- **Failure isolation:** drift-detector crash doesn't affect contract-api
- **Independent scaling:** agent-engine (CPU-heavy LLM calls) scales independently of contract-api (I/O-bound)
- **Independent deployment:** contract-api can be updated without rebuilding agent-engine
- **Mirrors production:** each container maps 1:1 to a Cloud Run service in production

**Communication between containers:**

Each service exposes a single interface:

```
contract-api  → HTTP (REST)
mcp-server    → SSE + JSON-RPC (MCP protocol)
agent-engine  → gRPC to Pub/Sub emulator (no inbound HTTP for this service)
drift-detector → gRPC to Pub/Sub emulator (pull subscriber — no inbound HTTP)
```

No service calls another's internal functions. All communication is over the network interface.

---

## FastAPI over Flask / Django

**Chosen:** FastAPI with Pydantic v2  
**Rejected:** Flask, Django REST Framework

**Why FastAPI:**
- Native async support (uvicorn + anyio)
- Pydantic v2 integration — request/response validation with detailed error messages
- Auto-generated Swagger UI at `/docs` — no separate API documentation tool needed
- `Depends()` injection system for clean singleton management (BigQuery client)
- Type hints drive both validation and IDE autocomplete

**Pydantic v2 specifically:**
- `model_validator` replaces v1's `@validator` — cleaner lifecycle hooks
- `field_validator` with `mode='before'` for input normalization
- Significantly faster validation than v1 (written in Rust)
