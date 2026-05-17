# Architecture Decision Record — E-commerce Data Lakehouse

**Author:** Saad Akhter Khan  
**Date:** 2026-05  
**Status:** Draft

---

## 1. Problem Statement

This project simulates a production data platform for an online retailer. The goal is to build a scalable, maintainable lakehouse that handles both batch and CDC (change data capture) ingestion, supports historical querying, and exposes clean business-level aggregates to a BI layer.

Key requirements:
- Handle late-arriving data and schema evolution over time
- Support incremental ingestion (not full reloads every run)
- Enable ad-hoc SQL queries without a running warehouse
- Maintain data lineage and documentation automatically
- Enforce data quality checks at every zone boundary

---

## 2. Decisions

### 2.1 Storage format: Apache Iceberg over plain Parquet

**Options considered:** Plain Parquet, Delta Lake, Apache Hudi, Apache Iceberg

**Decision:** Apache Iceberg

**Rationale:**
- Native support for time travel and snapshot isolation — critical for auditing and debugging
- Schema evolution without rewriting entire datasets
- ACID transactions across multiple files — enables safe concurrent writes
- First-class support in AWS Athena, EMR, and Spark
- Open standard with no vendor lock-in (unlike Delta Lake's Databricks origins)

**Trade-offs accepted:**
- Higher initial setup complexity vs plain Parquet
- Requires a catalog (Glue or local) to manage table metadata

---

### 2.2 Orchestration: Dagster over Airflow

**Options considered:** Apache Airflow, Prefect, Dagster

**Decision:** Dagster

**Rationale:**
- Asset-based model maps naturally to data engineering (Bronze → Silver → Gold)
- Built-in observability: data freshness, upstream/downstream lineage visible in UI
- Python-native — no XML DAG definitions
- Strong dbt integration out of the box
- More modern developer experience for a portfolio that targets 2025+ tooling

**Trade-offs accepted:**
- Smaller community than Airflow
- Less enterprise adoption (some interviewers may not know it) — mitigated by documenting the comparison in this ADR

---

### 2.3 Transformation: dbt

**Options considered:** Custom Python scripts, Spark transformations, dbt

**Decision:** dbt (dbt-core)

**Rationale:**
- Auto-generated data lineage and documentation
- Built-in testing framework (not null, unique, referential integrity)
- SQL-based — widely understood and auditable
- Strong industry adoption — signals production readiness
- Integrates natively with Dagster and Great Expectations

**Trade-offs accepted:**
- SQL-only (no procedural logic) — handled by pushing complex logic to upstream Python or Spark
- Requires a query engine (Athena in this case) to execute

---

### 2.4 Data quality: Great Expectations

**Options considered:** dbt tests only, custom Python assertions, Great Expectations, Soda

**Decision:** Great Expectations

**Rationale:**
- Runs expectations at every zone boundary (Bronze → Silver, Silver → Gold)
- Generates human-readable data docs automatically
- Catches structural issues (schema drift, null rates, value ranges) that dbt tests miss
- Can be triggered from Dagster as a first-class asset check

**Trade-offs accepted:**
- Verbose configuration (expectation suites require upfront investment)
- Heavier than dbt tests alone — justified by the data quality narrative for architect-level interviews

---

### 2.5 CDC ingestion: Debezium + Kafka Connect

**Options considered:** Full table reload on schedule, AWS DMS, Debezium + Kafka Connect

**Decision:** Debezium + Kafka Connect

**Rationale:**
- Row-level change capture — only processes what changed, not full table scans
- Industry-standard pattern for operational database replication
- Works with Postgres out of the box via logical replication slots
- Events are durable (Kafka) and replayable — supports reprocessing

**Trade-offs accepted:**
- Operationally complex (Kafka, Kafka Connect, connectors, schema registry)
- Overkill for small datasets — justified here as a skill demonstration

---

## 3. Cost Model

| Resource | Usage | Monthly estimate |
|----------|-------|-----------------|
| S3 | ~50GB data | ~$1.15 |
| Athena | ~100GB scanned | ~$5.00 |
| EMR Serverless | Occasional Spark jobs | ~$3–5 |
| Glue catalog | Table metadata | ~$1 |
| **Total** | | **~$10–12** |

All costs assume tear-down when not actively testing.

---

## 4. Open Questions

- [ ] Glue Data Catalog vs local Iceberg REST catalog for development?
- [ ] Use MSK Serverless for Kafka or local Kafka in Docker?
- [ ] EMR Serverless vs local PySpark for Iceberg writes during development?
