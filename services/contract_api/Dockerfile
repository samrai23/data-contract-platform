FROM python:3.11-slim

WORKDIR /app

# Copy and install dependencies first — separate layer so Docker caches it.
# As long as requirements.txt doesn't change, this layer is reused on every
# rebuild, even if your Python files change.  This is the single most
# important Dockerfile optimisation for fast local development iteration.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service source files
COPY . .

# data_contracts/ is bind-mounted by docker-compose at /app/data_contracts.
# Adding it to PYTHONPATH means pydantic_contract.py is importable if needed.
# /app itself must be in PYTHONPATH so "from routers import router" resolves
# (Python looks for modules on PYTHONPATH, not just the current directory).
ENV PYTHONPATH="/app:/app/data_contracts/templates"

# docker-compose overrides this CMD with --reload for live reload during dev.
# In production (Cloud Run), this CMD runs as-is.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
