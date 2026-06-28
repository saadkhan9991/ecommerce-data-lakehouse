"""
Bronze Layer Ingestion — Customers
====================================
Reads the `customers` table from Postgres and writes it to MinIO as an
Apache Iceberg table in the Bronze zone.

This is the first ingestion. Once this works, the pattern repeats for
every other source table.

Usage:
    python ingestion/bronze_customers.py
"""
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
# In production, all of these would come from environment variables or a
# secrets manager. Hardcoded here for clarity during the first build.

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "ecommerce"
POSTGRES_USER = "ecommerce"
POSTGRES_PASSWORD = "ecommerce"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "lakehouse"

# Local SQLite-backed Iceberg catalog. The catalog tracks which tables exist
# and where their files live. In AWS this would be Glue; locally, a tiny
# SQLite file is plenty.
CATALOG_NAME = "local"
WAREHOUSE_PATH = f"s3a://{MINIO_BUCKET}/warehouse"

# Iceberg + Hadoop-AWS JARs. Spark downloads these from Maven Central
# the first time it runs, then caches them.
ICEBERG_JARS = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.20.18",
    "org.postgresql:postgresql:42.7.3",
])


# ─────────────────────────────────────────────────────────────────────────────
# Build the Spark session
# ─────────────────────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    """Configure Spark with Iceberg catalog and S3 (MinIO) settings."""
    return (
        SparkSession.builder
        .appName("bronze_customers")
        .config("spark.jars.packages", ICEBERG_JARS)
        # Register the Iceberg catalog
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.type",
            "hadoop"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            WAREHOUSE_PATH
        )
        # S3 / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# The ingestion
# ─────────────────────────────────────────────────────────────────────────────
def ingest_customers(spark: SparkSession) -> None:
    """Read customers from Postgres, add Bronze metadata, write to Iceberg."""

    print("📥 Reading customers from Postgres...")
    df = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        .option("dbtable", "customers")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    row_count = df.count()
    print(f"   ✅ Read {row_count} rows from source")

    # Bronze convention: add lineage columns so downstream layers always
    # know when and where this data came from.
    df_with_meta = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("postgres.ecommerce.customers"))
    )

    print("💾 Writing to Iceberg table bronze.customers...")
    (
        df_with_meta.writeTo(f"{CATALOG_NAME}.bronze.customers")
        .using("iceberg")
        .createOrReplace()
    )

    print("   ✅ Write complete")


# ─────────────────────────────────────────────────────────────────────────────
# Quick verification: read the Iceberg table back
# ─────────────────────────────────────────────────────────────────────────────
def verify(spark: SparkSession) -> None:
    """Read the Iceberg table back and show a sample."""
    print("\n🔍 Verifying the Bronze table...")
    result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG_NAME}.bronze.customers")
    result.show()

    print("Sample rows:")
    spark.sql(
        f"SELECT customer_id, first_name, last_name, segment, _ingested_at "
        f"FROM {CATALOG_NAME}.bronze.customers LIMIT 5"
    ).show(truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    started = datetime.now(timezone.utc)
    print(f"🚀 Bronze ingestion started at {started.isoformat()}\n")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # Quiet the firehose of INFO logs

    # Create the Bronze namespace if it doesn't exist
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.bronze")

    ingest_customers(spark)
    verify(spark)

    spark.stop()
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\n🎉 Done in {elapsed:.1f}s")