# Runbook — Production Deploy

Deployment guide for Cloud Run (Phase 6). Services run as separate Cloud Run instances — same container images as local Docker, different configuration.

---

## Architecture (production)

```
Local:       Docker Compose  →  Pub/Sub Emulator
Production:  Cloud Run       →  Google Cloud Pub/Sub (real)
```

All services (`contract-api`, `mcp-server`, `agent-engine`, `drift-detector`) deploy as Cloud Run services. BigQuery and Pub/Sub are managed GCP services — no change from dev.

---

## Prerequisites

- GCP project with billing enabled (`data-contract-platform`)
- Cloud Run API enabled
- Artifact Registry API enabled (for container images)
- GitHub repository connected (for CI/CD via GitHub Actions)
- Workload Identity Federation configured (replaces service account key files in production)

---

## Step 1 — Enable required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com
```

---

## Step 2 — Create Artifact Registry repository

```bash
gcloud artifacts repositories create data-contract-platform \
  --repository-format=docker \
  --location=asia-south1 \
  --description="Container images for data-contract-platform"
```

---

## Step 3 — Build and push images

```bash
# Authenticate Docker with Artifact Registry
gcloud auth configure-docker asia-south1-docker.pkg.dev

# Build and push contract-api
docker build -t asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/contract-api:latest \
  services/contract_api/
docker push asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/contract-api:latest

# Repeat for mcp-server, agent-engine, drift-detector
```

---

## Step 4 — Configure Workload Identity (replaces gcp-credentials.json)

In production, Cloud Run services authenticate via the attached service account — no JSON key file needed.

```bash
# Create service account for the platform
gcloud iam service-accounts create data-contract-sa \
  --display-name="Data Contract Platform SA"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding data-contract-platform \
  --member="serviceAccount:data-contract-sa@data-contract-platform.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding data-contract-platform \
  --member="serviceAccount:data-contract-sa@data-contract-platform.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Grant Pub/Sub permissions
gcloud projects add-iam-policy-binding data-contract-platform \
  --member="serviceAccount:data-contract-sa@data-contract-platform.iam.gserviceaccount.com" \
  --role="roles/pubsub.editor"
```

---

## Step 5 — Deploy contract-api to Cloud Run

```bash
gcloud run deploy contract-api \
  --image=asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/contract-api:latest \
  --region=asia-south1 \
  --platform=managed \
  --service-account=data-contract-sa@data-contract-platform.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=data-contract-platform,ENVIRONMENT=production" \
  --allow-unauthenticated \
  --min-instances=0 \
  --max-instances=3
```

Note the deployed URL — it will be `https://contract-api-<hash>-el.a.run.app`. Use this as `CONTRACT_API_URL` for other services.

---

## Step 6 — Deploy mcp-server

```bash
gcloud run deploy mcp-server \
  --image=asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/mcp-server:latest \
  --region=asia-south1 \
  --platform=managed \
  --service-account=data-contract-sa@data-contract-platform.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=data-contract-platform,CONTRACT_API_URL=<contract-api-url>" \
  --no-allow-unauthenticated \
  --min-instances=0 \
  --max-instances=2
```

---

## Step 7 — Deploy agent-engine

```bash
gcloud run deploy agent-engine \
  --image=asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/agent-engine:latest \
  --region=asia-south1 \
  --platform=managed \
  --service-account=data-contract-sa@data-contract-platform.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=data-contract-platform,MCP_SERVER_URL=<mcp-server-url>,CONTRACT_API_URL=<contract-api-url>,GEMINI_MODEL=gemini-2.5-flash" \
  --set-secrets="GEMINI_API_KEY=gemini-api-key:latest" \
  --no-allow-unauthenticated \
  --min-instances=1 \
  --max-instances=5
```

`GEMINI_API_KEY` is stored in Secret Manager (not as a plain env var in production):
```bash
echo -n "your-api-key" | gcloud secrets create gemini-api-key --data-file=-
```

---

## Step 8 — Deploy drift-detector

drift-detector runs as a continuously-polling Cloud Run service (not triggered by HTTP requests). Set `--min-instances=1` so it always has one instance pulling from Pub/Sub.

```bash
gcloud run deploy drift-detector \
  --image=asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform/drift-detector:latest \
  --region=asia-south1 \
  --platform=managed \
  --service-account=data-contract-sa@data-contract-platform.iam.gserviceaccount.com \
  --set-env-vars="GCP_PROJECT_ID=data-contract-platform,CONTRACT_API_URL=<contract-api-url>,ENVIRONMENT=production" \
  --no-allow-unauthenticated \
  --min-instances=1 \
  --max-instances=1
```

---

## Step 9 — GitHub Actions CI/CD (automated)

Copy the workflow files:

```bash
mkdir -p .github/workflows
cp infra/github_actions/ci.yml .github/workflows/ci.yml
cp infra/github_actions/cd.yml .github/workflows/cd.yml
```

Add GitHub secrets (repo → Settings → Secrets and variables → Actions):

| Secret | Value |
|---|---|
| `GCP_PROJECT_ID` | `data-contract-platform` |
| `GEMINI_API_KEY` | your Gemini key |
| `WIF_PROVIDER` | Workload Identity pool provider |
| `WIF_SERVICE_ACCOUNT` | `data-contract-sa@data-contract-platform.iam.gserviceaccount.com` |
| `ARTIFACT_REGISTRY_REPO` | `asia-south1-docker.pkg.dev/data-contract-platform/data-contract-platform` |

After setup: every push to `main` triggers a build + deploy. Every PR triggers lint + tests only.

---

## Production vs local differences

| Concern | Local | Production |
|---|---|---|
| Pub/Sub | Emulator (`localhost:8085`) | Real GCP Pub/Sub |
| Credentials | `gcp-credentials.json` volume mount | Workload Identity (no key file) |
| Secrets | `.env` file | GCP Secret Manager |
| Container registry | Local Docker | Artifact Registry |
| Service discovery | Docker network (`contract-api:8000`) | Cloud Run URLs |
| Log format | Pretty console (structured) | JSON (Cloud Logging ingests automatically) |
| Min instances | 0 (scale to zero) | drift-detector: 1 (must keep polling) |

---

## Smoke test after deploy

```bash
# Health check
curl https://contract-api-<hash>-el.a.run.app/api/v1/health

# List vendors (should match what was seeded)
curl https://contract-api-<hash>-el.a.run.app/api/v1/vendors

# Drift log (should be empty or have test events)
curl https://contract-api-<hash>-el.a.run.app/api/v1/drift-log
```
