# Fintech Data Platform

A public, synthetic-data reference architecture for event-driven fintech analytics and ML feature pipelines.

## Architecture

```mermaid
flowchart LR
    A[Transaction Producers] --> B[Kafka / Event Bus]
    B --> C[Streaming Validation]
    C --> D[Raw Parquet / Object Storage]
    C --> E[PostgreSQL Operational Store]
    D --> F[Airflow Orchestration]
    F --> G[dbt Transformations]
    G --> H[Analytics Warehouse]
    H --> I[Feature Tables]
    I --> J[Risk / Fraud / Forecast Models]
    J --> K[FastAPI Serving]
    K --> L[Prometheus / Grafana]
    C --> M[Dead Letter Queue]
```

## What this repo demonstrates

- event-driven ingestion
- schema validation and data contracts
- idempotent processing
- Airflow DAG orchestration
- dbt-style SQL transformations
- PostgreSQL analytics modeling
- Parquet lake-style storage
- data quality assertions
- feature engineering boundaries
- model-serving integration points
- observability-friendly metrics

No real customer, employer, banking or transaction data is included.
