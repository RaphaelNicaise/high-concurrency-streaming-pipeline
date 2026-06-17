# ─── Build Stage ──────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install dependencies in an isolated layer for better cache usage
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─── Runtime Stage ────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="TapDrink Engineering"
LABEL description="High-Concurrency Chaos & Telemetry Simulator"

# Non-root user for security
RUN addgroup --system simulator \
 && adduser --system --ingroup simulator --no-create-home simulator

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy the simulator source
COPY chaos_simulator.py .

# Switch to non-root
USER simulator

# ─── Environment Variable Defaults ────────────────────────────────────────────
# These can be overridden in docker-compose.yml or at runtime via -e flags

ENV INGEST_API_URL="http://ingest-api:8000" \
    BASE_WORKERS="50"                        \
    SPIKE_MULTIPLIER="10"                    \
    SPIKE_DURATION_S="30"                    \
    SPIKE_INTERVAL_S="120"                   \
    CHAOS_RATIO="0.05"                       \
    REQUEST_TIMEOUT_S="10"                   \
    REPORT_INTERVAL_S="5"                    \
    MAX_RETRIES="3"                          \
    LOG_LEVEL="INFO"

# Unbuffered output so logs appear immediately in Docker
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-u", "main.py"]
