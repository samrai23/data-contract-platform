FROM python:3.11-slim

WORKDIR /app

# Install dependencies first — separate layer so it is cached by Docker
# and does not re-run on every code change
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service source files
COPY . .

# PYTHONPATH tells Python where to find modules at import time.
# /app          → schema_comparator.py, event_publisher.py, detector.py
# /app/spark_jobs/streaming → config.py, schema_infer.py, pubsub_drift_job.py
#                             (these are bind-mounted at runtime via docker-compose)
# /app/data_contracts       → pydantic_contract.py
#                             (also bind-mounted at runtime)
ENV PYTHONPATH="/app:/app/spark_jobs/streaming:/app/data_contracts/templates"

CMD ["python", "detector.py"]
