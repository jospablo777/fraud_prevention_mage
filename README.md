# A real-time fraud prevention system (end-to-end)

Here we build an end-to-end solution with a microservice architecture to help detect and manage fraud. This project is composed of 4 microservices:

- **A data generation process (FastAPI):** produces realistic transaction events, and simulates a transactional environment that pushes new transactions every 20 seconds.
- **Apache Kafka:** acts as the streaming backbone.
- **Streaming pipeline (Mage):** consumes from Kafka, cleans data, runs an ML model, and writes results to Parquet.
- **Risk viewer (Streamlit):** a lightweight dashboard that reads the Parquet exports and surfaces high-risk transactions.

There is also an additional one (not packed into a container) that serves as an environment for experimentation and ML development, called `ml_and_experimentation`. You can see an outline of the system in the following diagram:

![Fraud prevention system](./img/Fraud%20prevention%20system.png)

## Repository layout

```
fraud_prevention_mage
├─ data_synthesizer/            # FastAPI producer (generates transactions to Kafka)
├─ fraud_prevention_pipeline/   # Mage streaming pipeline (consumer/clean/predict/export)
├─ risk_viewer/                 # Streamlit app (reads Parquet via DuckDB), presents data in a dashboard.
├─ ml_and_experimentation/      # Not containerized: notebooks, scripts, experiments
├─ img/                         # Diagrams, figures
├─ docker-compose.yml           # Orchestration for local dev
└─ README.md
```

## What gets produced, and where?

- The pipeline exports Parquet files to a named Docker volume mounted at: `/var/lib/mage/data` (inside the pipeline container).

- The risk viewer reads those Parquet files (via DuckDB) directly from the same volume.

- Files are written using Hive-style partitions: `…/GRAIN/year=YYYY/month=MM/part-*.parquet` and the viewer projects `transaction_id`, `event_time`, `fraud_prob`.

## Prerequisites

- A macbook, this currently is a major **limitation**. Due to time constrains I've built and tested the system only for for Apple silicon (M4)

- Docker Desktop or Docker Engine 24+ (Compose v2)

- Internet access to pull images

## Quickstart

Pull images if missing and start everything:

```bash
docker compose up -d --pull=missing
```

Services:

- Kafka broker: localhost:9092 (internal listener for containers: broker:29092)

- Kafka UI: http://localhost:8080

- Data generator API (FastAPI): http://localhost:8000

- Risk viewer (Streamlit): http://localhost:8501

Follow logs:

```bash
docker compose logs -f
```

Stop:
```bash
docker compose down
```
(Volumes persist by default.)


## Configuration

All of these are already wired in docker-compose.yml, but you can override with environment: or --env:

### Data generator (data_synthesizer)

`KAFKA_BOOTSTRAP`: `broker:29092` (internal listener)

`KAFKA_TOPIC`: `creditcard-transactions`

`SYNTH_PATH`: `/app/artifacts/creditcard_fraud_gc.pkl`

`FRAUD_RATE`: e.g. `0.001727` (this is the proportion of fraud observed in the original data set)

`INTERVAL_SECS`: event interval

`BATCH_MIN`, `BATCH_MAX`: batch size (size of the batches is stochatic within this range)

`AUTO_START`: `true|false` (autostarts producing)

### Mage pipeline (fraud_stream_pipeline)

`KAFKA_BOOTSTRAP`: `broker:29092`

`KAFKA_TOPIC`: `creditcard-transactions`

`PIPELINE_NAME`: `fraud_stream_pipeline`

`MAGE_EXPORT_BASE_DIR`: `/var/lib/mage/data` (mounted volume)

### Risk viewer (risk_viewer)

`DATA_ROOT`: `/var/lib/mage/data`

`GRAIN`: `fraud_high_risk`

`DEFAULT_LIMIT`: default table rows (optional)

`REFRESH_MS`: autorefresh interval in ms (default `5000`)