"""
dbt runner with pre-configured Spark session
=============================================
dbt-spark in session mode calls SparkSession.builder.getOrCreate().
If a SparkSession already exists, getOrCreate() returns it.

This script creates a SparkSession with our Iceberg + MinIO + JAR
configuration, then invokes dbt's CLI. dbt inherits the fully
configured session without knowing anything about the setup.

Usage:
    cd dbt
    python run_dbt.py run          # runs all models
    python run_dbt.py run -s stg_customers   # runs one model
    python run_dbt.py test         # runs all tests
    python run_dbt.py compile      # compiles SQL without executing
"""

import sys
from pyspark.sql import SparkSession


# ── Spark + Iceberg + MinIO configuration ─────────────────────────────────
# Same config as our Dagster SparkResource — single source of truth
# would be even better (e.g. shared .env file), but explicit is fine
# for learning.

ICEBERG_JARS = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.20.18",
])

CATALOG_NAME = "local"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE_PATH = "s3a://lakehouse/warehouse"


def create_spark_session() -> SparkSession:
    """Build the Spark session with Iceberg catalog and S3 credentials."""
    return (
        SparkSession.builder
        .appName("dbt_lakehouse")
        .config("spark.jars.packages", ICEBERG_JARS)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .enableHiveSupport()
        .getOrCreate()
    )


if __name__ == "__main__":
    # Step 1: Create the configured Spark session BEFORE dbt starts
    print("🔧 Creating Spark session with Iceberg + MinIO config...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("   ✅ Spark session ready\n")

    # Step 2: Invoke dbt's CLI — it will reuse the existing session
    from dbt.cli.main import cli

    # Pass through any command-line args (e.g. "run", "test", "-s model_name")
    # Always add --profiles-dir . so dbt finds profiles.yml in the dbt/ folder
    dbt_args = sys.argv[1:] if len(sys.argv) > 1 else ["run"]
    dbt_args.extend(["--profiles-dir", "."])

    print(f"🚀 Running: dbt {' '.join(dbt_args)}\n")
    cli(dbt_args)

    spark.stop()