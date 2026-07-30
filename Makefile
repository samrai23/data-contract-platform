# ─────────────────────────────────────────────────────────────
# DATA CONTRACT PLATFORM — MAKEFILE
# All common dev tasks in one place.
# Run `make help` to see all commands.
# ─────────────────────────────────────────────────────────────

.PHONY: help setup env up down logs test lint format simulate-drift seed clean

# Default
help:
	@echo ""
	@echo "  Data Contract Platform — Dev Commands"
	@echo ""
	@echo "  Setup"
	@echo "    make setup          → Install all Python deps + pre-commit hooks"
	@echo "    make env            → Copy .env.example to .env (fill in your keys)"
	@echo ""
	@echo "  Docker"
	@echo "    make up             → Start full local stack (Pub/Sub emulator + all services)"
	@echo "    make down           → Stop all containers"
	@echo "    make logs           → Tail all container logs"
	@echo "    make logs-agent     → Tail agent-engine logs only"
	@echo "    make restart        → Rebuild and restart everything"
	@echo ""
	@echo "  Testing"
	@echo "    make test           → Run all unit + integration tests"
	@echo "    make test-unit      → Run unit tests only"
	@echo "    make test-int       → Run integration tests only"
	@echo ""
	@echo "  Dev Tools"
	@echo "    make lint           → Run ruff linter"
	@echo "    make format         → Run black formatter"
	@echo "    make simulate-drift → Send a drift event to Pub/Sub (for testing)"
	@echo "    make seed           → Seed sample vendor contracts to BigQuery"
	@echo "    make clean          → Remove all containers, volumes, cache"
	@echo ""

setup:
	pip install uv
	uv pip install -e ".[dev]"
	pre-commit install
	@echo "✓ Setup complete"

env:
	cp .env.example .env
	@echo "✓ .env created — add your API keys now"

up:
	docker compose up -d --build
	@echo "  Creating Pub/Sub topics + subscriptions (needed after every fresh emulator start)..."
	python scripts/setup_pubsub_emulator.py
	@echo "✓ Stack running"
	@echo "  FastAPI:        http://localhost:8000/docs"
	@echo "  Pub/Sub emu:    http://localhost:8085"
	@echo "  MCP Server:     http://localhost:8001"

down:
	docker compose down

logs:
	docker compose logs -f

logs-agent:
	docker compose logs -f agent-engine

restart:
	docker compose down
	docker compose up -d --build

test:
	pytest tests/ -v --tb=short

test-unit:
	pytest tests/unit/ -v

test-int:
	pytest tests/integration/ -v

lint:
	ruff check .

format:
	black .

simulate-drift:
	python scripts/simulate_drift.py

seed:
	python scripts/seed_contracts.py

clean:
	docker compose down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	@echo "✓ Cleaned"
