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
      curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv (Astral)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /build

#=============================
# 1) Dependencies layer (uv -> requirements.txt)
#=============================
FROM base AS deps
WORKDIR /build
# These must be next to the Dockerfile in your viewer project
COPY pyproject.toml uv.lock ./
# Produce a fully-pinned requirements.txt from uv.lock
RUN uv export --frozen -o requirements.txt

#=============================
# 2) Runtime (slim, install pinned deps)
#=============================
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Non-root user + CA certs
RUN useradd -m -u 10001 appuser && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pinned requirements into system site-packages
COPY --from=deps /build/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY app.py ./app.py

# Defaults: point to the volume where Mage exports files
ENV DATA_ROOT=/var/lib/mage/data \
    GRAIN=fraud_high_risk

USER appuser
EXPOSE 8501

# Streamlit server
CMD ["python", "-m", "streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
