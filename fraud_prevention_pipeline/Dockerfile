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
      curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"
WORKDIR /build

#=============================
# 1) Dependencies layer
#=============================
FROM base AS deps
WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv export --frozen -o requirements.txt

#=============================
# 2) Wheels (download binaries where possible)
#=============================
FROM python:3.12-slim AS wheels
WORKDIR /wheels
RUN python -m pip install --upgrade pip
COPY --from=deps /build/requirements.txt ./requirements.txt

# Make a hashless copy for build-time tooling (keeps your repo file intact)
RUN awk '!/--hash=/' requirements.txt > requirements.nohash.txt

# Try to fetch only binary wheels first. If some pins have no wheel,
# this command will fail; we continue and handle the remainder in the next stage.
RUN mkdir -p /wheels/wheelhouse && \
    pip download --only-binary=:all: -r requirements.nohash.txt -d /wheels/wheelhouse || true

#=============================
# 3) Build missing wheels only
#=============================
FROM python:3.12-slim AS build-missing
WORKDIR /wheels
# toolchain + BLAS/Fortran for numpy/scipy/sklearn builds
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential g++ gfortran pkg-config ninja-build \
      libopenblas-dev liblapack-dev \
      libjpeg-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --upgrade pip wheel setuptools
COPY --from=wheels /wheels/requirements.nohash.txt ./requirements.nohash.txt
COPY --from=wheels /wheels/wheelhouse ./wheelhouse
# This will build wheels ONLY for packages not already present in ./wheelhouse
RUN pip wheel -r requirements.nohash.txt -w ./wheelhouse --find-links=./wheelhouse --no-cache-dir

#=============================
# 4) Runtime image (Mage pipeline)
#=============================
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app
RUN useradd -m -u 10001 appuser && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Install from the final wheelhouse (no builds, no network)
COPY --from=deps /build/requirements.txt ./requirements.txt
COPY --from=wheels /wheels/requirements.nohash.txt ./requirements.nohash.txt
COPY --from=build-missing /wheels/wheelhouse /wheels
RUN pip install --no-index --find-links=/wheels -r requirements.nohash.txt

# -----------------------------
# Mage project files
# -----------------------------
COPY metadata.yaml ./metadata.yaml
COPY pipelines ./pipelines
COPY transformers ./transformers
COPY data_loaders ./data_loaders
COPY data_exporters ./data_exporters
COPY ml_artifacts ./ml_artifacts
COPY main.py ./main.py

ENV MAGE_EXPORT_BASE_DIR=/var/lib/mage/data \
    PIPELINE_NAME=fraud_stream_pipeline
RUN mkdir -p /var/lib/mage/data && chown -R appuser:appuser /var/lib/mage
USER appuser

CMD ["bash", "-lc", "mage run . ${PIPELINE_NAME}"]