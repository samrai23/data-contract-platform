# Runbook — Drift Simulation

Step-by-step guide to running a full drift simulation and observing the system respond.

---

## Prerequisites

All 5 containers must be running and healthy:

```powershell
docker ps
# Expected: pubsub-emulator, contract-api, mcp-server, agent-engine, drift-detector
```

If any are missing:
```powershell
docker compose up -d pubsub-emulator contract-api mcp-server agent-engine drift-detector
```

Pub/Sub topics must exist:
```powershell
python scripts/setup_pubsub_emulator.py
# All 6 resources (3 topics + 3 subscriptions) must show ✅
```

Vendor contracts must be seeded:
```powershell
python scripts/seed_contracts.py
# cars24 and paytm should show "registered" or "already exists (skip)"
```

---

## Run the simulation

```powershell
# Default: cars24, 20 messages, type_change drift after message 10
python scripts/simulate_drift.py

# Or with explicit args
python scripts/simulate_drift.py --vendor cars24 --drift-after 10 --drift-type type_change
```

You will see output like:
```
Sending 20 messages  vendor=cars24
Topic  : projects/data-contract-platform/topics/vendor-feeds
Drift  : 'type_change' injected after message 10

  [  1/20] normal  — ['vehicle_id', 'user_id', 'price', 'listing_date', 'city', 'fuel_type']
  [  2/20] normal  — ['vehicle_id', 'user_id', 'price', 'listing_date', 'city', 'fuel_type']
  ...
  [ 11/20] DRIFTED — ['vehicle_id', 'user_id', 'price']
  ...
  [ 20/20] DRIFTED — ['vehicle_id', 'user_id', 'price']
```

---

## What to watch in Docker Desktop

### drift-detector logs

Open Docker Desktop → click `drift-detector` → Logs tab.

**Expected sequence after message 10:**
```
[info] api_registry.loaded         field_count=6  vendor_id=cars24  version=1.0.0
[info] schema_infer.complete        fields_inferred=6  parsed=40  total_messages=40
[info] drift_job.comparison_complete  has_drift=True  drift_event_count=1  vendor_id=cars24
[info] event_publisher.published    drift_type=type_change  field=user_id  severity=LOW  vendor_id=cars24
[info] drift_job.messages_acked     count=40
[debug] drift_job.idle_waiting      (repeats every 5s — normal)
```

**Key fields to note:**
- `has_drift=True` — drift was detected
- `drift_type=type_change` — the kind of drift
- `field=user_id` — which field changed
- `severity=LOW` — Gemini will likely AUTO_HEAL this

### agent-engine logs

Open Docker Desktop → click `agent-engine` → Logs tab.

**Expected sequence (ESCALATE path — most common for type_change on previously-drifted fields):**
```
[info] graph.process_start        drift_type=type_change  severity=LOW  vendor_id=cars24
[info] agent.fetch_context        vendor_id=cars24
[info] tools.get_contract         vendor_id=cars24
[info] tools.get_drift_history    vendor_id=cars24
[info] agent.decide_action.calling_llm  drift_type=type_change  severity=LOW  vendor_id=cars24
[info] agent.decide_action.decided  decision=ESCALATE  reasoning='...'
[info] agent.escalate             vendor_id=cars24
[info] agent.escalate.complete    event_id=<uuid>  vendor_id=cars24
[info] graph.process_complete     decision=ESCALATE  vendor_id=cars24
```

Copy the `event_id` from `agent.escalate.complete` — you need it to approve.

---

## After ESCALATE — human approval

1. Open `http://localhost:8000/docs`
2. Find `POST /api/v1/approve/{event_id}`
3. Click **Try it out**
4. Set `event_id` to the UUID from the logs
5. Request body:
   ```json
   {
     "approved_by": "your@email.com",
     "notes": "Reviewed and approved"
   }
   ```
6. Click **Execute** → expect `200 OK`

---

## After AUTO_HEAL

No action needed. The agent logs the event as AUTO_HEALED and calls auto_approve automatically. You can verify in the drift log:

```
GET http://localhost:8000/api/v1/drift-log
```

---

## Drift types and expected severity

| `--drift-type` | What changes | Typical severity | Typical decision |
|---|---|---|---|
| `type_change` | `user_id`: INT → STRING | LOW | AUTO_HEAL (if no history) / ESCALATE (if repeated) |
| `rename_column` | `vehicle_id` → `vehicleId` | HIGH | ESCALATE |
| `drop_column` | `city` and `fuel_type` removed | HIGH | ESCALATE |
| `add_column` | New `km_driven` field added | LOW | AUTO_HEAL |

---

## Testing both scenarios

**Scenario A — force AUTO_HEAL:**
```powershell
# Fresh vendor with no drift history → first type_change is usually auto-healed
python scripts/seed_contracts.py   # re-seed if needed
python scripts/simulate_drift.py --drift-type add_column
```

**Scenario B — force ESCALATE:**
```powershell
python scripts/simulate_drift.py --drift-type drop_column
# Dropping columns is always HIGH severity → always ESCALATE
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| drift-detector logs show `vendor_not_found` | cars24 contract not seeded | Run `python scripts/seed_contracts.py` |
| agent-engine shows `credentials not found` | `GOOGLE_APPLICATION_CREDENTIALS` wrong path | Check `docker-compose.yml` has `/app/gcp-credentials.json` in agent-engine environment |
| `drift_job.idle_waiting` but no drift detected | Messages sent before drift-detector was ready | Wait 10s and re-run `simulate_drift.py` |
| agent-engine shows `429 RESOURCE_EXHAUSTED` | Gemini quota hit | Wait for quota reset (per-minute limit) or check GCP Console quotas |
| approve endpoint returns `500` | BigQuery DML blocked — billing not linked | Link billing account to `data-contract-platform` project in GCP Console |
