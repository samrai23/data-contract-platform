"""
services/contract_api/dependencies.py
=======================================

FastAPI dependency injection for shared resources.

WHAT IS DEPENDENCY INJECTION IN FASTAPI?
-----------------------------------------
FastAPI has a built-in DI system via the Depends() function.  Instead of
creating a BigQuery client inside every route handler, you declare that a
route "depends on" a function that returns the client.  FastAPI calls that
function once and passes the result in.

Example without DI (bad — creates a new client on every request):
    @router.get("/vendors")
    async def list_vendors():
        bq = bigquery.Client(project="my-project")   # expensive!
        ...

Example with DI (good — client is created once and reused):
    @router.get("/vendors")
    async def list_vendors(bq: BQClient):            # injected automatically
        ...

WHERE THE SINGLETON COMES FROM
--------------------------------
_create_bq_client() is decorated with @lru_cache(maxsize=1).
lru_cache is Python's built-in memoization — the first call runs the
function body and stores the result; every subsequent call returns the
cached result without running the body again.

maxsize=1 means the cache holds exactly one entry (the BigQuery client).
This is effectively a singleton: one client for the lifetime of the process.

The BigQuery client internally manages a connection pool, so sharing one
client across all requests is the correct pattern (same as SQLAlchemy engines).

WHY NOT USE async def FOR THE BQ OPERATIONS IN ROUTERS?
---------------------------------------------------------
google-cloud-bigquery is a synchronous library (bq.query().result() blocks).
FastAPI runs sync route handlers in a thread pool automatically, so blocking
calls don't freeze the event loop.  We get the simplicity of sync code with
the scalability of async routing.

THE Annotated TYPE ALIAS TRICK
--------------------------------
    BQClient = Annotated[bigquery.Client, Depends(get_bq_client)]

This lets route handlers use:
    async def my_route(bq: BQClient)
instead of:
    async def my_route(bq: bigquery.Client = Depends(get_bq_client))

Both are identical to FastAPI — the Annotated alias is just cleaner syntax.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from google.cloud import bigquery


# ── BigQuery client singleton ──────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _create_bq_client() -> bigquery.Client:
    """
    Create the BigQuery client exactly once for the lifetime of the process.

    The client reads credentials from GOOGLE_APPLICATION_CREDENTIALS automatically
    — this is a Google Cloud SDK convention, not something we have to implement.
    As long as the env var points to a valid service account JSON key, the client
    authenticates itself.

    In local Docker: the gcp-credentials.json file is bind-mounted into the container
    and GOOGLE_APPLICATION_CREDENTIALS is set to its path in docker-compose.yml.

    In Cloud Run (production): credentials come from the service account attached
    to the Cloud Run service — no JSON key file needed, no env var needed.
    """
    project = os.getenv("GCP_PROJECT_ID", "local-dev")
    return bigquery.Client(project=project)


def get_bq_client() -> bigquery.Client:
    """
    FastAPI dependency: returns the shared BigQuery client.

    We wrap _create_bq_client() in a plain function (not the lru_cached one)
    because FastAPI resolves Depends() by calling the function — the wrapper
    makes dependency override in tests trivial:
        app.dependency_overrides[get_bq_client] = lambda: mock_client
    """
    return _create_bq_client()


# ── Dataset reference ──────────────────────────────────────────────────────────

def get_dataset_ref() -> str:
    """
    FastAPI dependency: returns the fully-qualified BigQuery dataset path.

    BigQuery uses dot-notation to reference tables:
        project_id.dataset_id.table_name

    All our queries use this pattern:
        SELECT * FROM `{dataset}.vendor_contracts`
    where dataset = "my-project.data_contracts"

    Reading from env vars here (not hardcoded) means the same container image
    works in local dev, staging, and production — only the env vars change.
    """
    project = os.getenv("GCP_PROJECT_ID", "local-dev")
    dataset = os.getenv("BIGQUERY_DATASET", "data_contracts")
    return f"{project}.{dataset}"


# ── Type aliases for cleaner route signatures ──────────────────────────────────
# These are the types you'll see in routers.py function signatures.
# They carry the Depends() declaration inside the type hint itself.

BQClient = Annotated[bigquery.Client, Depends(get_bq_client)]
DatasetRef = Annotated[str, Depends(get_dataset_ref)]
