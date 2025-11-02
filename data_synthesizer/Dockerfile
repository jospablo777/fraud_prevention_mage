# syntax=docker/dockerfile:1

#=============================
# 0) Base with uv installed
#=============================
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (Astral)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /build

#=============================
# 1) Dependencies layer
#=============================
FROM base AS deps
WORKDIR /build
COPY pyproject.toml uv.lock ./
# Produce a fully-pinned requirements.txt from uv.lock
RUN uv export --frozen -o requirements.txt

#=============================
# 2) Synth builder (train model)
#=============================
FROM python:3.12-slim AS synthbuilder
WORKDIR /build
# System deps (same as base, minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*
# Use the pinned requirements exported above
COPY --from=deps /build/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what the training script needs
COPY create_synthetizer.py ./
COPY original_data ./original_data

# Train: writes artifacts into /build/artifacts
RUN mkdir -p artifacts && python create_synthetizer.py

#=============================
# 3) Runtime image
#=============================
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# non-root
RUN useradd -m -u 10001 appuser && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pinned requirements into system site-packages
COPY --from=deps /build/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code (current layout)
COPY main.py ./
COPY api ./api
COPY core ./core
COPY models ./models
COPY services ./services

# Bring the synthesizer artifacts produced at build time
COPY --from=synthbuilder /build/artifacts ./artifacts

# Defaults (override at docker run)
ENV KAFKA_BOOTSTRAP=localhost:9092 \
    KAFKA_TOPIC=creditcard-transactions \
    SYNTH_PATH=/app/artifacts/creditcard_fraud_gc.pkl \
    FRAUD_RATE=0.001727 \
    INTERVAL_SECS=40 \
    BATCH_MIN=3 \
    BATCH_MAX=30 \
    AUTO_START=true

USER appuser
EXPOSE 8000

# Use python -m uvicorn (robust)
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
