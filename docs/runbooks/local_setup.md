# Runbook — Local Setup

Complete from-scratch setup guide. Follow in order.

---

## Prerequisites

Install these before anything else:

| Tool | Version | Check |
|---|---|---|
| Python | 3.11+ | `python --version` |
| Docker Desktop | Latest | `docker --version` |
| Git | Any | `git --version` |
| Google Cloud SDK | Latest | `gcloud --version` |

---

## Step 1 — Clone the repo

```powershell
git clone https://github.com/YOUR_USERNAME/data-contract-platform.git
cd data-contract-platform
```

---

## Step 2 — Create the .env file

Copy the example and fill in values:

```powershell
copy .env.example .env
```

Minimum required values:

```env
GCP_PROJECT_ID=data-contract-platform
GEMINI_API_KEY=<your key from aistudio.google.com>
GEMINI_MODEL=gemini-2.5-flash
GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json
ENVIRONMENT=local
CONTRACT_API_URL=http://localhost:8000
MCP_SERVER_URL=http://localhost:8001
```

---

## Step 3 — GCP project setup

### 3a — Create a GCP project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create project → name it `data-contract-platform`
3. Enable APIs:
   - BigQuery API
   - Cloud Pub/Sub API
   - Generative Language API

### 3b — Create a service account

1. IAM & Admin → Service Accounts → Create Service Account
2. Name: `data-contract-sa`
3. Grant roles:
   - BigQuery Data Editor
   - BigQuery Job User
4. Create key → JSON → save as `gcp-credentials.json` in the project root

`gcp-credentials.json` is in `.gitignore` — it will never be committed.

### 3c — Link billing to the project

Required for BigQuery DML (the approve endpoint) and Gemini API access.

1. GCP Console → Billing → Link a billing account
2. Select your `data-contract-platform` project
3. Link your credit/debit card

Cost at dev scale: effectively $0. BigQuery DML on a few-row table costs fractions of a cent.

### 3d — Gemini API key

1. Go to [aistudio.google.com](https://aistudio.google.com)
2. Get API Key → Create API key
3. Add to `.env`: `GEMINI_API_KEY=<key>`
4. Link billing to the AI Studio project (different from your BigQuery project):
   - In AI Studio → project list → Set up billing → link the same card
5. In GCP Console → APIs & Services → Generative Language API → Quotas:
   - Set hard caps below free tier thresholds (e.g., 1000 RPD, 10 RPM) to prevent charges

---

## Step 4 — Create BigQuery tables

```powershell
python scripts/setup_bigquery.py
```

Creates:
- Dataset: `contracts` (region: `asia-south1`)
- Table: `vendor_contracts`
- Table: `drift_events`

---

## Step 5 — Install Python dependencies (for running scripts locally)

```powershell
pip install -r requirements.txt
# Or per-service if testing a specific service locally:
pip install -r services/contract_api/requirements.txt
```

---

## Step 6 — Start the Pub/Sub emulator

```powershell
docker compose up -d pubsub-emulator
```

Wait ~10 seconds, then create topics and subscriptions:

```powershell
python scripts/setup_pubsub_emulator.py
```

Expected output:
```
✅  Topic created       : vendor-feeds
   ✅  Subscription created       : vendor-feeds-sub
✅  Topic created       : drift-events
   ✅  Subscription created       : drift-events-sub
✅  Topic created       : heal-actions
   ✅  Subscription created       : heal-actions-sub

✅  All 6 resources ready.
```

---

## Step 7 — Start all services

```powershell
docker compose up -d contract-api mcp-server agent-engine drift-detector
```

Verify all containers are running:

```powershell
docker ps
```

Expected containers: `pubsub-emulator`, `contract-api`, `mcp-server`, `agent-engine`, `drift-detector`

---

## Step 8 — Seed vendor contracts

```powershell
python scripts/seed_contracts.py
```

---

## Step 9 — Verify everything is working

```powershell
# Check contract-api health
curl http://localhost:8000/api/v1/health

# Check vendors are seeded
curl http://localhost:8000/api/v1/vendors

# Open Swagger UI
start http://localhost:8000/docs
```

---

## Step 10 — Run a drift simulation

```powershell
python scripts/simulate_drift.py --vendor cars24 --drift-after 10 --drift-type type_change
```

Watch Docker Desktop logs for `drift-detector` and `agent-engine`. See [drift_simulation.md](drift_simulation.md) for the full expected output.

---

## Startup order (required)

Services must start in this order due to dependencies:

```
1. pubsub-emulator          (no deps)
2. setup_pubsub_emulator.py (needs emulator running)
3. contract-api             (needs BigQuery tables)
4. mcp-server               (needs contract-api)
5. agent-engine             (needs pubsub-emulator + mcp-server)
6. drift-detector           (needs pubsub-emulator + contract-api)
7. seed_contracts.py        (needs contract-api)
8. simulate_drift.py        (needs all of the above)
```

---

## Environment variable reference

| Variable | Where set | Used by |
|---|---|---|
| `GCP_PROJECT_ID` | `.env` | All services |
| `GEMINI_API_KEY` | `.env` | agent-engine |
| `GEMINI_MODEL` | `.env` | agent-engine |
| `GOOGLE_APPLICATION_CREDENTIALS` | `.env` (relative) / `docker-compose.yml` (absolute) | All services |
| `PUBSUB_EMULATOR_HOST` | `docker-compose.yml` environment block | drift-detector, agent-engine |
| `CONTRACT_API_URL` | `docker-compose.yml` environment block | mcp-server, drift-detector |
| `MCP_SERVER_URL` | `docker-compose.yml` environment block | agent-engine |
| `ENVIRONMENT` | `.env` | drift-detector (controls log format) |

**Note on credentials path inside Docker:** The `.env` file sets `GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json` (relative path). Inside Docker, the agent-engine's path resolution calculates the wrong absolute path. The `docker-compose.yml` overrides it with `GOOGLE_APPLICATION_CREDENTIALS: /app/gcp-credentials.json` in the `environment:` block for agent-engine specifically.
