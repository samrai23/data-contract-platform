"""
services/contract_api/main.py
================================

FastAPI application entry point.

This file's only job is to:
  1. Create the FastAPI app instance with metadata for OpenAPI docs.
  2. Add CORS middleware so a browser-based frontend can call the API.
  3. Register the router from routers.py under the /api/v1 prefix.
  4. Log startup information.

Everything else — route logic, models, dependencies — lives in the files
it imports.  This file stays thin by design.

WHAT IS CORS?
-------------
CORS (Cross-Origin Resource Sharing) is a browser security rule that blocks
JavaScript on one domain (e.g. localhost:3000) from calling an API on a
different domain (e.g. localhost:8000) unless the server explicitly allows it.

allow_origins=["*"] means any domain can call the API — fine for a portfolio
project.  In production you'd list specific domains instead.

Running locally:
    cd services/contract_api
    uvicorn main:app --reload --port 8000
    # Then visit http://localhost:8000/docs
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import router

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan handler — runs startup code before yielding, cleanup after.
    FastAPI replaced the old @app.on_event("startup") pattern with this in v0.93.
    """
    log.info(
        "contract_api.started",
        environment=os.getenv("ENVIRONMENT", "local"),
        bq_project=os.getenv("GCP_PROJECT_ID", "local-dev"),
        bq_dataset=os.getenv("BIGQUERY_DATASET", "data_contracts"),
        docs="http://localhost:8000/docs",
    )
    yield  # application runs here


app = FastAPI(
    title="Data Contract Platform API",
    description=(
        "Manages vendor data contracts and tracks schema drift events.\n\n"
        "**Phase 2**: Contract CRUD (register vendor, list vendors, get contract).\n\n"
        "**Phase 3 additions**: agent-triggered contract updates, human approval flow.\n\n"
        "Use the `/docs` endpoint for interactive API exploration."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1", tags=["contracts"])
