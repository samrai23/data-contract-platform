# Interview Notes — Agentic Data Contract Platform

Running log of problems solved, design decisions, and talking points.
Each section = one story you can tell in an interview.

---

## Phase 1 — Contract Model & Schema Comparator

### Pydantic v2 field name validator bug — camelCase slipping through

**What happened:** The field name regex was `^[a-zA-Z_][a-zA-Z0-9_]*$` — it allowed uppercase letters, so `userId` and `CamelCase` passed validation silently.

**Fix:** Changed to `^[a-z][a-z0-9_]*$` — must start with lowercase letter, only lowercase + digits + underscore.

**Why it matters:** BigQuery column names are case-insensitive, but dbt models and downstream SQL are written in lowercase. A contract with `userId` and another with `user_id` look like different fields to the pipeline even though they represent the same thing. Enforcing snake_case at registration time prevents this class of bug entirely.

**File:** `data_contracts/templates/pydantic_contract.py`

---

### Pytest pythonpath — ModuleNotFoundError across services

**What happened:** Tests for `drift_detector` couldn't import `schema_comparator` because pytest didn't know where the services lived.

**Fix:** Added `pythonpath` to `pyproject.toml`:
```toml
[tool.pytest.ini_options]
pythonpath = [".", "services/contract_api", "services/drift_detector", "spark_jobs/streaming", "data_contracts/templates"]
```

**Why it matters:** In a monorepo with multiple services, each service's root must be on Python's import path. This is a common setup step that gets skipped and causes confusing import errors.

---

### Seeding data through the API, not direct BigQuery writes

**Decision:** `seed_contracts.py` calls `POST /api/v1/vendors` instead of writing rows directly to BigQuery.

**Why:** Direct BQ writes bypass FastAPI validation, Pydantic models, the duplicate-check logic, and the response contract. Seeding via API is also an integration test — if any layer is broken, the seed fails and tells you exactly where.

**Re-run safety:** Existing vendors return 409, which the script treats as a skip — not a failure. Safe to re-run any time.

**File:** `scripts/seed_contracts.py`

---

## Phase 1 — Docker / Pub/Sub Emulator

### Pub/Sub emulator unreachable from outside Docker container

**What happened:** `gcloud beta emulators pubsub start --host-port=0.0.0.0:8085` appeared to work (logs said "listening on 8085") but was completely unreachable from the host.

---

**Background — Host vs Port (understand this first):**

Think of your computer as an apartment building:
- **Host (IP address)** = the building's street address — *which machine* to go to
- **Port** = the apartment number inside — *which service* inside that machine

So `localhost:8085` means: *"the building called localhost, apartment 8085"*

Your machine actually has **multiple network interfaces** — multiple addresses it can listen on:

```
Your machine
├── 127.0.0.1   → loopback — only processes on THIS machine can reach it
├── 192.168.1.5 → WiFi/LAN card — machines on your local network can reach it
└── 0.0.0.0     → wildcard — means ALL of the above at once
```

When a server starts, it binds to one of these interfaces — it decides which "door" to open:

| Bound to | Who can connect |
|---|---|
| `127.0.0.1` (localhost) | Only processes on the same machine |
| `192.168.1.5` | Only machines on local network |
| `0.0.0.0` | Everyone — same machine, LAN, internet |

---

**Why this specifically broke inside Docker:**

Docker containers are **separate mini-machines**, each with their own network interfaces:

```
Windows machine                    Container (pubsub-emulator)
───────────────────                ──────────────────────────
127.0.0.1 (its loopback)           127.0.0.1 (its OWN loopback) ← DIFFERENT
0.0.0.0                            0.0.0.0
port 8085 exposed ──────────────→  port 8085
```

The emulator was bound to the **container's own** `127.0.0.1:8085`. When Docker tried to forward traffic from Windows into the container, it had to cross the network boundary — but the emulator was only listening on the container's internal loopback. It refused all external traffic.

```
You → localhost:8085 → Docker port forward → container:8085
                                                    ↓
                                        emulator bound to 127.0.0.1
                                        not accepting from outside
                                              ❌ connection refused
```

After fixing to `0.0.0.0`:

```
You → localhost:8085 → Docker port forward → container:8085
                                                    ↓
                                        emulator bound to 0.0.0.0
                                        accepts from any interface
                                              ✅ connected
```

---

**The gcloud twist — why the fix wasn't obvious:**

You'd expect `--host-port=0.0.0.0:8085` to pass `--host=0.0.0.0` down to the Java emulator binary. It doesn't. gcloud **hardcodes** `--host=localhost` internally when starting the Java process — your value is silently ignored. This is a bug/limitation in gcloud itself.

So the emulator always ended up on `127.0.0.1` inside the container no matter what you told gcloud. The only fix was to bypass gcloud entirely and call the Java binary directly:

```yaml
# docker-compose.yml
command: >
  /usr/lib/google-cloud-sdk/platform/pubsub-emulator/bin/cloud-pubsub-emulator
  --host=0.0.0.0
  --port=8085
```

**Lesson:** When a service "appears" to be running but is unreachable, always check which interface it's actually bound to. Inside Docker, anything bound to `127.0.0.1` is invisible to the outside world — it must be `0.0.0.0` to accept traffic through Docker's port forwarding.

---

### gRPC env var must be set before importing the library

**What happened:** Setup script connected to real GCP instead of the local emulator, hanging forever waiting for auth.

**Root cause:** `google.cloud.pubsub_v1` reads `PUBSUB_EMULATOR_HOST` **at import time** to choose its gRPC endpoint. Setting `os.environ[...]` after the import is too late — the channel was already pointed at `pubsub.googleapis.com`.

**Fix:**
```python
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"  # MUST be before import
os.environ["GRPC_DNS_RESOLVER"] = "native"             # Windows: use OS DNS resolver
from google.cloud import pubsub_v1                      # NOW safe
```

**Lesson:** Some libraries capture configuration at import time, not at call time. Always check the library's docs for environment variables that need to be set before import.

**File:** `scripts/setup_pubsub_emulator.py`

---

### Healthcheck — nc not available in cloud-sdk image

**What happened:** Docker healthcheck using `nc -z localhost 8085` failed because `netcat` isn't installed in the Google Cloud SDK image.

**Fix:** Used bash's built-in TCP check instead:
```yaml
test: ["CMD-SHELL", "timeout 2 bash -c '</dev/tcp/localhost/8085' && exit 0 || exit 1"]
```

**Lesson:** Don't assume common Unix tools are available in minimal Docker images. `/dev/tcp` is a bash builtin — always available if bash is.

---

## Phase 2 — FastAPI + BigQuery

### BigQuery free tier blocks ALL write operations except batch load

**What happened:** Two successive fixes both failed with 403:
1. `insert_rows_json()` → `403 Streaming insert is not allowed in the free tier`
2. `bq.query("INSERT INTO ...")` → `403 DML queries are not allowed in the free tier`

**Root cause:** BigQuery free tier (no billing account) only allows:
- SELECT queries (reads)
- DDL (CREATE TABLE / CREATE DATASET)
- Batch load jobs

**Fix:** Switched to `load_table_from_json()` — the same API Spark/Dataflow/`bq load` CLI use:
```python
load_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
job = bq.load_table_from_json([row], f"{dataset}.vendor_contracts", job_config=load_config)
job.result()
```

**Trade-off:** ~5-10 second latency per insert vs milliseconds for streaming. Acceptable for one-off vendor registration.

**Note:** `approve_drift_event` still uses DML UPDATE — will need billing enabled (or a delete+reload workaround) when drift events start being written in Phase 3.

**File:** `services/contract_api/routers.py`

---

### SQL injection prevention with parameterized queries

**Pattern used throughout `routers.py`:**
```python
# NEVER do this:
query = f"WHERE vendor_id = '{vendor_id}'"

# Always do this:
query = "WHERE vendor_id = @vendor_id"
job_config = bigquery.QueryJobConfig(query_parameters=[
    bigquery.ScalarQueryParameter("vendor_id", "STRING", vendor_id)
])
```

**Exception — LIMIT clause:** BigQuery doesn't support parameterized LIMIT. The safe pattern is to validate the integer range in FastAPI first, then interpolate:
```python
limit: int = Query(default=50, ge=1, le=500)  # FastAPI enforces 1-500
query = f"... LIMIT {limit}"                   # safe — already constrained
```

---

### docker compose restart vs --force-recreate

**What happened:** Updated `.env` but container kept using the old `GCP_PROJECT_ID`.

**Root cause:** `docker compose restart` restarts the process inside the existing container. It does NOT re-read `env_file`. The environment is baked in at container creation time.

**Rule:**
| What changed | Command |
|---|---|
| `.py` file (with `--reload`) | Nothing — uvicorn auto-reloads via volume mount |
| `.env` / environment vars | `docker compose up -d --force-recreate <service>` |
| `requirements.txt` | `docker compose up -d --build <service>` |
| `Dockerfile` | `docker compose up -d --build <service>` |

---

### FastAPI lifespan handler — on_event deprecated

**Old (deprecated):**
```python
@app.on_event("startup")
async def startup(): ...
```

**New (correct):**
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("started")
    yield  # app runs here; teardown code goes after yield

app = FastAPI(..., lifespan=lifespan)
```

**Why better:** Single function handles both startup and shutdown. The `yield` boundary makes the lifecycle explicit. Allows resource cleanup (close connections, flush buffers) on shutdown.

---

### BigQuery client singleton via lru_cache + Depends()

**Pattern (`dependencies.py`):**
```python
@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.gcp_project_id)

BQClient = Annotated[bigquery.Client, Depends(get_bq_client)]
```

**Why lru_cache over a module-level global:**
- Module-level globals are created at import time — before settings are loaded from `.env`
- `lru_cache` defers creation to first request — settings are guaranteed loaded by then
- Test mocking is clean: `app.dependency_overrides[get_bq_client] = lambda: FakeClient()`

---

## Architecture Decisions

### Why synchronous Pub/Sub pull (not async streaming)

Schema drift detection is a **statistical inference problem** — you need a batch of N messages to reliably detect a type change (one message saying `user_id=123` could be STRING or INT). A synchronous pull collects N messages, infers schema across all of them, then does one comparison.

Async streaming would fire a callback per message — you'd have to buffer and aggregate yourself, re-implementing what synchronous pull gives for free.

---

### Why separate Docker containers per service

Each service has a single responsibility and communicates only through defined interfaces:

```
contract-api  ──HTTP──→  BigQuery
drift-detector ──HTTP──→  contract-api (GET /contracts/{vendor_id})
              ──gRPC──→  pubsub-emulator (publish drift events)
agent-engine  ──gRPC──→  pubsub-emulator (subscribe)
              ──MCP──→   mcp-server
              ──HTTP──→  contract-api (POST /approve)
```

Benefits: independent scaling, failure isolation, independent deployment, mirrors production Cloud Run / GKE topology.

---

### Why contract_json stored as STRING (not ARRAY\<STRUCT\>)

BigQuery ARRAY\<STRUCT\> is complex to write and query. A separate `contract_fields` table would require JOINs on every contract read. Storing the fields list as a JSON string keeps reads and writes simple — one row = one complete contract. Trade-off: can't filter by individual field names in standard SQL (need `JSON_VALUE()`), which is acceptable because we always load the whole contract.

---

## Phase 3 — MCP Server + Agent Engine

### MCP is not a REST API — curl doesn't work, and here's why

**What happened:** Tried to test the MCP server with `curl http://localhost:8001/tools/list` and got 404. Also tried `Invoke-RestMethod` in PowerShell — same result.

---

**Background — REST vs MCP protocol (understand this first):**

REST (what most APIs use):
```
You send:    GET /vendors HTTP/1.1
Server gets: one request
Server sends: one JSON response
Connection:  closes immediately
```
Every endpoint is a URL. `curl` works because it just sends one HTTP request and reads one response.

MCP over SSE (what the MCP server uses):
```
Step 1 — Client connects: GET /sse
          Server keeps the connection OPEN (this is the SSE stream)

Step 2 — Client sends a message: POST /messages/?session_id=abc
          Body: { "jsonrpc": "2.0", "method": "tools/list", "params": {} }

Step 3 — Server sends the response DOWN THE OPEN SSE STREAM (not as an HTTP response)
          Connection stays open for more messages
```

`tools/list` and `tools/call` are **JSON-RPC method names** — they're like function names you put in the request body. They are NOT URL paths. That's why `GET /tools/list` returns 404 — there's no such URL. The real endpoint is `POST /messages/` with the method name in the body.

**Analogy:**
- REST is like sending a letter: one request → one reply → done.
- MCP/SSE is like a phone call: you dial once, stay connected, and have a back-and-forth conversation. `tools/list` is something you say during the call, not a different phone number to dial.

**Why MCP uses SSE instead of REST:**
An AI agent calls tools in rapid succession during reasoning — get contract, check history, log event, approve. Keeping one SSE connection open for all those calls is much faster than opening a new TCP connection for each one. SSE also lets the server push progress updates back to the agent for long-running tools.

---

**Root cause of the 404:** Hitting `/tools/list` as a URL doesn't exist in the MCP SSE protocol. The server only exposes:
- `GET /sse` — opens the SSE connection
- `POST /messages/?session_id=...` — sends JSON-RPC messages on that connection

**Fix:** Use FastMCP's async `Client` which handles the SSE handshake and JSON-RPC encoding automatically:
```python
from fastmcp import Client
import asyncio

async def test():
    async with Client("http://localhost:8001/sse") as client:
        tools = await client.list_tools()        # internally sends tools/list JSON-RPC
        result = await client.call_tool("list_vendors", {})  # sends tools/call JSON-RPC

asyncio.run(test())
```

**File:** `scripts/test_mcp_server.py`

**Lesson:** When you see "MCP server", don't assume it's REST. It's a stateful JSON-RPC protocol over SSE. Always use the MCP client library to talk to it, not curl.

---

### FastMCP transport — sse vs streamable-http, and why changing it broke the client

**What happened:** Assumed FastMCP 3.x had renamed `sse` to `streamable-http` and changed `server.py`. The server started with `streamable-http` and exposed its endpoint at `/mcp`. But the test script was still connecting to `/sse` → 404.

**Background — the two FastMCP transports:**

| Transport | Server endpoint | Client URL |
|---|---|---|
| `sse` | `GET /sse` + `POST /messages/` | `Client("http://host:port/sse")` |
| `streamable-http` | `POST /mcp` | `Client("http://host:port/mcp")` |

They are NOT interchangeable. Changing the server transport without changing the client URL breaks the connection entirely. The 404 comes from the client trying to open `/sse` when the server is only listening on `/mcp`.

**Fix:** Reverted server.py back to `sse` transport — it was working, there was no reason to change it.

**Rule:** Transport is a contract between server and client. If you change the server's transport, you must change the client URL at the same time. Never change one without the other.

**When to use which:**
- `sse` — simpler, well-supported, works with all MCP clients, good for local dev
- `streamable-http` — newer spec, single endpoint, slightly better for proxies/load balancers

---

### FastMCP 3.x — call_tool() returns a wrapped object, not raw Python

**What happened:** After fixing the test script to use the FastMCP async client, `list_vendors` was called successfully (server logs showed `mcp.list_vendors`) but the test crashed with:
```
'CallToolResult' object is not iterable
```

The code was doing `for item in result` where `result = await client.call_tool(...)`.

---

**Background — why the result is wrapped:**

The MCP protocol is designed to be generic — tools can return text, images, files, or structured data. To support all of these in one unified format, every tool response is wrapped in a **content block** structure:

```
What you'd expect (raw Python):
  [{"vendor_id": "cars24", "version": "1.0.0"}, ...]

What FastMCP 3.x actually returns (CallToolResult):
  CallToolResult(
    content=[
      TextContent(type="text", text='[{"vendor_id": "cars24", "version": "1.0.0"}, ...]')
    ]
  )
```

The actual data is a JSON string buried in `result.content[0].text`. You have to:
1. Access `.content[0].text` to get the JSON string
2. Call `json.loads()` to parse it back into a Python object

This is the same reason HTTP responses have a body instead of just sending raw bytes — the envelope (headers + content-type) lets the receiver know how to interpret what's inside.

**Fix:** A helper function that unpacks `CallToolResult` into a plain Python object:
```python
import json

def extract(call_result):
    """Unpack FastMCP 3.x CallToolResult into a plain Python object."""
    if hasattr(call_result, "content") and call_result.content:
        text = call_result.content[0].text
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text   # return raw string if it's not JSON
    return call_result    # fallback: return as-is

# Usage:
vendors = extract(await client.call_tool("list_vendors", {}))
# Now vendors is a plain Python list, safe to iterate
```

**File:** `scripts/test_mcp_server.py`

**Lesson:** MCP client libraries wrap tool responses in content blocks. Always extract `.content[0].text` and parse it. This applies to any MCP client (Python, TypeScript, etc.) — it's part of the MCP protocol spec, not a FastMCP quirk.

**Follow-up bug — empty results cause a second `CallToolResult` error:**

After fixing the `list_vendors` case, the same `'CallToolResult' object is not iterable` error appeared on `get_drift_history` — even though that tool was called correctly.

**Root cause:** When a tool returns an empty list `[]`, FastMCP encodes it as empty content blocks (`content=[]`). The original `extract()` function checked `if call_result.content:` — which is `False` for an empty list — and fell through to `return call_result`, returning the raw `CallToolResult` object instead of `[]`.

This is subtle: the same `if content:` guard that protects against missing content also masks the empty-result case. Two different situations produce the same falsy `content`:
- Tool hasn't returned yet (truly missing content) → should return `CallToolResult` as-is
- Tool returned `[]` (empty result) → should return `[]`

**Fix:** Separate the two cases explicitly:

```python
def extract(call_result):
    if not hasattr(call_result, "content"):
        return call_result    # not a CallToolResult at all

    if not call_result.content:
        return []             # tool returned empty — NOT the same as "no result"

    text = call_result.content[0].text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text
```

**Lesson:** When writing helper functions that unwrap protocol objects, always test with empty results, not just non-empty ones. Empty collections are a common edge case that `if result:` guards silently swallow.

---

### MCP server local testing — contract-api must be running

**What happened:** MCP server started fine. Test script connected fine. `list_vendors` tool was called — but crashed inside the MCP server with a `ConnectError: connection refused`.

**Root cause — service dependency chain:**

When you run the MCP server locally (outside Docker) and call `list_vendors`, here's what actually happens:

```
test script
    ↓  SSE / JSON-RPC  (over localhost:8001)
MCP server (running locally, port 8001)
    ↓  HTTP GET /api/v1/vendors  (over localhost:8000)
contract-api (running in Docker, port 8000)  ← THIS wasn't running
    ↓  BigQuery client
BigQuery (real GCP)
```

The MCP server's `list_vendors` tool calls `contract_tools.list_vendors()` which makes an HTTP call to `http://localhost:8000/api/v1/vendors`. If the contract-api Docker container isn't up, that HTTP call is refused — and the error propagates back through the MCP server to the test script.

The confusing part: the error says "connection refused to localhost:8000" but the message you see is "Error calling tool 'list_vendors'" — it looks like an MCP error when it's actually a missing dependency.

**Fix:**
```powershell
docker compose up -d contract-api   # start the dependency first
python scripts/test_mcp_server.py   # then test
```

**Rule for local testing order:**
```
1. docker compose up -d contract-api   (Phase 2 — always needed)
2. python services/mcp_server/server.py  (Phase 3 MCP server)
3. python scripts/test_mcp_server.py     (test)
4. python services/agent_engine/graph.py (Phase 3 agent)
```

**Lesson:** When testing microservices locally, always start dependencies first. The error you see is often from the top of the call chain (MCP server), not the bottom (contract-api). Read the innermost error in the traceback — `ConnectError: localhost:8000 refused` tells you exactly which service is missing.

---

### Why the MCP server calls contract-api instead of BigQuery directly (for contracts)

**Decision:** `contract_tools.py` in the MCP server calls `GET /api/v1/contracts/{vendor_id}` on the contract-api, not BigQuery directly.

**Why:**
The contract-api already owns all the contract business logic — validation, duplicate checks, version management, the response schema. If the MCP server called BigQuery directly it would need to:
1. Duplicate the query logic that already exists in routers.py
2. Duplicate the response parsing and Pydantic model logic
3. Have its own BigQuery credentials and dataset config

Calling the API avoids all of that — one source of truth, one place to update when the schema changes.

**Where the MCP server DOES call BigQuery directly:** `bq_tools.py` — for writing drift events and reading drift history. The contract-api has no endpoint for this because the agent itself is the writer of drift events. A circular dependency (agent → contract-api → agent to write drift) would be awkward. Direct BigQuery access for audit writes is the clean separation.

**Interview angle:** "The MCP server has two data access patterns: for contracts it calls the contract-api (to reuse existing business logic and avoid duplication), and for drift events it writes to BigQuery directly (because the agent is the producer — there's no existing API for that)."

---

### Credentials auto-discovery — resolving relative paths in .env against project root

**What happened:** Running `test_agent_graph.py` from the project root caused a `DefaultCredentialsError` even though `.env` had `GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json`. The path `./gcp-credentials.json` resolved against the current working directory (project root), which worked. But the same code running from `services/agent_engine/` would resolve it against that subdirectory — file not found.

**Fix applied in `bq_tools.py`, `tools.py`, and `test_agent_graph.py`:**
```python
_PROJECT_ROOT = Path(__file__).parent.parent.parent   # always finds project root

try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env", override=False)  # Docker env takes priority
except ImportError:
    pass

_creds_val = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
if _creds_val and not Path(_creds_val).is_absolute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_PROJECT_ROOT / _creds_val)
```

**Why `override=False`:** Inside Docker, the container already has the env vars set via `env_file` in `docker-compose.yml`. `override=False` means Docker's values win — the `.env` load is only a fallback for local runs.

**Lesson:** Relative paths in `.env` files are ambiguous — relative to what? Always resolve them to absolute paths at startup using `Path(__file__)` as the anchor, not `os.getcwd()`. CWD changes depending on how you launch the script.

---

### Complete Gemini API setup journey — from zero to working agent

This is the full story of getting the Gemini LLM connected to the agent engine. It took several steps and multiple distinct errors. Document this precisely because every step is an interview talking point about real-world GCP API integration.

---

#### Step 1 — Getting an API key from Google AI Studio

**Where to go:** [aistudio.google.com](https://aistudio.google.com)

1. Click **"Get API key"** in the top-left
2. Click **"Create API key"** → select or create a project
3. Copy the key and add it to `.env`:
   ```
   GEMINI_API_KEY=AQ.Ab8RN6...
   GEMINI_MODEL=gemini-2.0-flash
   ```

**Observation about the key format:**

The key starts with `AQ.` — not the traditional `AIza` prefix seen in older Google API documentation. This is the current format that Google AI Studio issues as of 2026. It is a valid key. The `AIza` prefix was from an older credential generation flow in GCP Console; AI Studio now issues its own format. The key works with the `google-genai` SDK and `langchain-google-genai`.

**How the key is used in code (`agents.py`):**

```python
# ChatGoogleGenerativeAI reads GOOGLE_API_KEY from the environment.
# We rename GEMINI_API_KEY → GOOGLE_API_KEY so LangChain picks it up.
if not os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]

_llm = ChatGoogleGenerativeAI(
    model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
    temperature=0,
)
```

`temperature=0` means deterministic output — we want consistent, reproducible decisions from the agent, not creative variation.

---

#### Step 2 — First error: `429 RESOURCE_EXHAUSTED` with `limit: 0`

**Error received:**
```
429 RESOURCE_EXHAUSTED
Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests
limit: 0, model: gemini-2.0-flash
```

**Why `limit: 0` is confusing:**

The error says "You exceeded your current quota" — but we hadn't made a single successful request. `limit: 0` does NOT mean "you used up your quota". It means **your allowed quota is zero**. No capacity was ever provisioned for this project.

**Root cause — Google's recent policy change:**

Google updated its abuse-prevention policy for the Gemini API. Previously, you could use the free tier with no payment method at all. As of 2025–2026, **if no billing account is linked to the GCP project, the free tier quota defaults to exactly 0**. The project is treated as unverified and receives no capacity. This applies even to brand-new projects and brand-new keys.

**Why this is a policy decision and not a bug:**

Unlimited anonymous API access was being abused for automated spam and scraping. Requiring a billing account (even if you never actually pay) acts as identity verification — Google knows who you are and can recover costs if you exceed free tier.

---

#### Step 3 — Linking a billing account to the AI Studio project (Tier 1 upgrade)

The AI Studio project auto-created for you is usually named something like `gen-lang-client-0926030018`. This is the project your API key belongs to.

**Steps to link billing:**

1. Go to [aistudio.google.com](https://aistudio.google.com)
2. In the project list, find the row for your project → click **"Set up billing"** under the Billing Tier column
3. This redirects to Google Cloud Console → add a payment method (credit/debit card)
4. The project upgrades to **Tier 1 (Pay-as-you-go)**

**What Tier 1 means:**

- You now have access to the same free daily quotas as before (e.g., 1,500 requests/day for Flash models)
- BUT if you exceed those quotas, Google silently charges your card for the overages
- The upgrade itself costs nothing — you only pay if you exceed free limits

**How to stay completely free — hard-cap your quotas:**

Go to GCP Console → **APIs & Services → Enabled APIs → Generative Language API → Quotas tab**:

| Quota | Free tier limit | What we set | Why |
|---|---|---|---|
| Requests per day (RPD) | 1,500 | 1,000 | Stay below free threshold |
| Requests per minute (RPM) | 15 | 10 | Stay below free threshold |
| Input tokens per minute | varies | default | Fine for dev testing |

With these hard caps, the API returns `429` if you hit the limit — it **cannot charge your card** because you never reach the billing threshold. This is the safest setup for a dev/learning project.

---

#### Step 4 — Second error: `404 NOT_FOUND` — `gemini-2.0-flash` is deprecated

After billing was linked and the agent ran again:

```
404 NOT_FOUND
This model models/gemini-2.0-flash is no longer available.
Please update your code to use a newer model.
```

**Why this was confusing:**

The GCP Quota Console still showed `model:gemini-2.0-flash` in the quota list — so it looked like the model was available. But quota entries for deprecated models **persist in the console indefinitely** even after Google removes the model from the API. The quota console and the live API maintain separate lists.

**Dead ends we tried before finding the fix:**

| Model tried | Error | Why |
|---|---|---|
| `gemini-2.0-flash` | `404 model no longer available` | Deprecated and removed |
| `gemini-1.5-flash` | `404 not found for API version v1beta` | 1.5 series not accessible via v1beta for this project type |
| `gemini-2.0-flash-lite` | `429 limit: 0` | Quota for this model also hadn't been set up yet |
| `gemini-3-flash` | `404 not found for API version v1beta` | Quota console name ≠ API model name (`gemini-3-flash-preview` is the real name) |

**The correct fix — list models directly from the API:**

```python
import urllib.request, json, os

api_key = os.getenv("GEMINI_API_KEY")
url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"

with urllib.request.urlopen(url) as resp:
    data = json.loads(resp.read())

for m in data["models"]:
    if "generateContent" in m.get("supportedGenerationMethods", []):
        print(m["name"].replace("models/", ""))
```

This hits the **same `v1beta` endpoint** that `langchain-google-genai` uses internally — so it shows exactly what models your key can call. Script saved as `scripts/list_gemini_models.py`.

**Output (June 2026 — partial):**
```
gemini-2.5-flash       ← this is the correct current model
gemini-2.5-pro
gemini-2.0-flash       ← listed here but 404 in practice (quota console lag)
gemini-2.0-flash-lite
gemini-3.5-flash
...
```

**Fix:** Updated `.env`:
```
GEMINI_MODEL=gemini-2.5-flash
```

Then set quotas for `gemini-2.5-flash` in GCP Console (same Quotas tab, filter by `gemini-2.5-flash`).

**Lesson:** Never trust the GCP Quota Console for model availability. The source of truth is the `v1beta/models` API endpoint. Run `list_gemini_models.py` any time you get a 404 on a model name.

---

#### Step 5 — Third error: `500 Internal Server Error` on the approve endpoint

After the agent graph was working (both AUTO_HEAL and ESCALATE scenarios passing), we tested `POST /api/v1/approve/{event_id}` via Swagger. Got `500 Internal Server Error`.

**Container logs revealed the real error:**
```
google.api_core.exceptions.Forbidden: 403
Billing has not been enabled for this project.
DML queries are not allowed in the free tier.
Location: asia-south1
```

**Root cause — two separate GCP projects, two separate billing accounts:**

| Project | Purpose | Billing needed for |
|---|---|---|
| `gen-lang-client-0926030018` | Gemini API (AI Studio) | Gemini LLM calls |
| `data-contract-platform` | BigQuery (our platform) | DML UPDATE in approve endpoint |

Linking billing to the AI Studio project in Step 3 only fixed Gemini. **BigQuery billing is per-project** — each GCP project has its own billing account linkage. The `data-contract-platform` project still had no billing, so DML was blocked.

**Why the approve endpoint specifically needs DML (not batch load):**

```
BigQuery write operations:

  load_table_from_json()  →  INSERT only (appends new rows)  →  FREE tier ✅
  bq.query("UPDATE ...")  →  modifies existing rows          →  needs billing ❌ (on free tier)
```

The approve endpoint needs to change `resolution_status` from `PENDING_APPROVAL` to `APPROVED` on an existing row. That is an UPDATE — you cannot do it with batch load. Batch load only appends; it cannot modify a row that's already there.

The SELECT check in step 1 of the endpoint worked fine (reads are always free). Only the UPDATE in step 2 was blocked.

**Fix:** Linked a billing account to the `data-contract-platform` project:
1. GCP Console → Billing → Link a billing account → select `data-contract-platform`
2. Same card, same billing account — just applied to a second project

**Cost of the approve endpoint DML UPDATE:**

BigQuery charges $5 per TB of data scanned. A DML UPDATE that finds one row in a table with 10 rows scans a few kilobytes. Cost: `$0.00` (rounds to zero at any reasonable precision). The free tier gives 1 TB/month of query processing — our entire dev usage for this project doesn't touch even 1 GB.

**After linking billing:** Re-hit the Swagger endpoint → `200 OK`:
```json
{
  "event_id": "8140800a-fb05-4d62-8a37-88397211a5aa",
  "approved": true,
  "approved_by": "sudhanshuraina23@gmail.com",
  "processed_at": "2026-06-16T04:19:19.694748Z",
  "message": "Drift event approved by sudhanshuraina23@gmail.com."
}
```

---

#### Interview summary — what to say

> "Setting up the Gemini API integration involved three separate billing/configuration issues that taught me a lot about how GCP projects and quotas work.
>
> First, the Gemini free tier now requires a billing account linked to the project — even if you never spend anything. Google introduced this as an anti-abuse measure. Without it, your quota limit is literally set to zero. You fix it by linking a card and then setting hard quota caps in GCP Console below the free tier thresholds — so the API stops rather than charges you.
>
> Second, I learned not to trust the GCP Quota Console for which model names are actually callable. `gemini-2.0-flash` was deprecated and removed from the API but still showed up in the console. I wrote a diagnostic script that calls the same `v1beta/models` endpoint that LangChain uses internally, which gave me the accurate list. The correct model was `gemini-2.5-flash`.
>
> Third, I discovered that GCP billing is per-project. My AI Studio project (for Gemini) and my data-contract-platform project (for BigQuery) are separate GCP projects with separate billing. Enabling billing for Gemini didn't help BigQuery. The approve endpoint uses a DML UPDATE — which requires billing — and it was failing with 403 until I linked billing to the BigQuery project separately. BigQuery DML on a few-row table costs effectively nothing but billing must be enabled for the operation to be allowed at all."

---

### LangGraph StateGraph — how the drift resolution graph works

**The complete graph flow:**

```
START
  ↓
fetch_context      — HTTP calls: GET contract + GET drift history
  ↓
decide_action      — Gemini LLM: reads context, returns AUTO_HEAL / ESCALATE / IGNORE
  ↓ (conditional edge: route_decision())
  ├── "AUTO_HEAL"  → execute_auto_heal  — logs event as AUTO_HEALED, submits approval
  └── "ESCALATE"   → execute_escalate  — logs event as PENDING_APPROVAL, awaits human
  ↓
END
```

**Key concepts:**

- **Nodes** are plain Python functions: `def node_name(state: DriftAgentState) -> dict`
  - Receive the full current state
  - Return only the keys they changed (LangGraph merges partial updates)
- **State** is a TypedDict flowing through the graph — each node reads what it needs and writes what it produces
- **Conditional edges**: `add_conditional_edges("decide_action", route_decision, {"auto_heal": ..., "escalate": ...})` — the routing function returns a string that LangGraph maps to the next node
- **`compile()` once, `invoke()` per event** — the compiled graph is a singleton; each drift event is one `invoke()` call with fresh initial state

**Why LLM for the routing decision (not just `if severity == HIGH`):**

Rule-based logic handles clear-cut cases. But ambiguous situations — a column renamed from `city` to `city_name` (cosmetic and safe) vs `city` to `postal_code` (semantic change, breaks downstream) — benefit from language model reasoning. The LLM reads the contract, the drift event, and the history together and makes a contextual judgment.

**Tested scenarios (both passing):**

| Scenario | Input | Expected | Result |
|---|---|---|---|
| A | INT → BIGINT on `user_id`, severity=LOW, is_safe=True | AUTO_HEAL | ✅ AUTO_HEAL |
| B | STRING → INT on `city`, severity=HIGH, is_safe=False | ESCALATE | ✅ ESCALATE |
