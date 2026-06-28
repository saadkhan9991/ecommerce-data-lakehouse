# E-commerce Data Lakehouse

A production-grade data lakehouse built on AWS, demonstrating modern data platform architecture with Apache Iceberg, dbt, Dagster, and CDC ingestion.

> Part of a portfolio series targeting a Senior Data Engineer → Data/Solutions Architect transition.

---

## Architecture

```
Source systems (Postgres, flat files, REST API)
        │
        ├── CDC via Debezium ──────────┐
        ├── Batch via Python scripts ──┤
        │                              ▼
        │                     Kinesis / S3 raw
        │                              │
        ▼                              ▼
   S3 — Bronze zone (raw, Iceberg tables)
        │
        ▼
   S3 — Silver zone (cleaned, deduplicated, typed — dbt models)
        │
        ▼
   S3 — Gold zone (business-level aggregates — dbt models)
        │
        ├──► Athena (ad-hoc queries)
        └──► BI layer (Metabase / Streamlit dashboard)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source simulation | PostgreSQL (Docker) |
| CDC ingestion | Debezium + Kafka Connect |
| Batch ingestion | Python scripts |
| Storage format | Apache Iceberg on S3 |
| Orchestration | Dagster |
| Transformation | dbt (dbt-core) |
| Data quality | Great Expectations |
| Compute | Spark on EMR Serverless |
| Query engine | AWS Athena |
| Dashboard | Metabase (Docker) |
| IaC | Terraform |
| CI/CD | GitHub Actions |

---

## Repository Structure

```
ecommerce-data-lakehouse/
├── terraform/              # AWS infrastructure as code
├── docs/
│   └── ADR.md              # Architecture Decision Record
├── dbt/                    # dbt project — silver + gold transforms
├── dagster/                # Dagster asset definitions + jobs
├── ingestion/              # Batch + CDC ingestion scripts
├── scripts/                # Data generators and utilities
├── dashboard/              # Metabase / Streamlit dashboards
├── .github/
│   └── workflows/          # CI/CD pipelines
├── docker-compose.yml      # Full local dev environment
└── README.md
```

---

## Local Setup

```bash
# Clone the repo
git clone https://github.com/saadakhterkhan/ecommerce-data-lakehouse
cd ecommerce-data-lakehouse

# Start the full local environment
docker compose up -d

# Services:
#   Postgres    → localhost:5432
#   Dagster UI  → localhost:3000
#   Metabase    → localhost:3001
```

---

## Design Decisions

See [`docs/ADR.md`](docs/ADR.md) for the full Architecture Decision Record covering:
- Why Apache Iceberg over plain Parquet
- Why Dagster over Airflow
- Why dbt for transformations
- Why Great Expectations for data quality
- CDC vs full-load trade-offs

---

## Project Status

| Phase | Status |
|-------|--------|
| Repo scaffold + docker-compose | ✅ Done |
| Synthetic data generator | 🚧 In progress |
| Bronze layer (batch ingestion) | 🔜 |
| Bronze layer (CDC / Debezium) | 🔜 |
| Silver layer (dbt models) | 🔜 |
| Gold layer (dbt aggregates) | 🔜 |
| AWS deployment (Terraform) | 🔜 |
| CI/CD (GitHub Actions) | 🔜 |
| Dashboard | 🔜 |
