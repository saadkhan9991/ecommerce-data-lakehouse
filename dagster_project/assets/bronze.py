"""
Bronze layer Dagster assets
============================
Each function decorated with @asset corresponds to one Iceberg table
in the Bronze zone. Dagster uses these to build the pipeline graph,
track runs, and manage lineage.

Right now: just `customers`. We add the other 6 tables next session.
"""

from datetime import datetime, timezone

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp


# ─────────────────────────────────────────────────────────────────────────────
# Configuration (will be promoted to a Dagster "resource" later)
# ─────────────────────────────────────────────────────────────────────────────

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "ecommerce"
POSTGRES_USER = "ecommerce"
POSTGRES_PASSWORD = "ecommerce"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "lakehouse"

CATALOG_NAME = "local"
WAREHOUSE_PATH = f"s3a://{MINIO_BUCKET}/warehouse"

ICEBERG_JARS = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.20.18",
    "org.postgresql:postgresql:42.7.3",
])


def build_spark(app_name: str) -> SparkSession:
    """Build a Spark session configured for Iceberg + MinIO."""
    return (
        SparkSession.builder
        .appName(app_name)
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
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Assets
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="customers",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw customers table ingested from Postgres into Iceberg",
    compute_kind="spark",
)
def bronze_customers(context: AssetExecutionContext) -> MaterializeResult:
    """
    Reads the `customers` table from Postgres and writes it to MinIO
    as an Apache Iceberg table at `local.bronze.customers`.
    """
    context.log.info("📥 Building Spark session...")
    spark = build_spark("bronze_customers")
    spark.sparkContext.setLogLevel("WARN")

    # Ensure the bronze namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.bronze")

    context.log.info("📥 Reading customers from Postgres...")
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
    context.log.info(f"✅ Read {row_count} rows from source")

    # Bronze lineage columns
    df_with_meta = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("postgres.ecommerce.customers"))
    )

    context.log.info("💾 Writing to Iceberg table bronze.customers...")
    (
        df_with_meta.writeTo(f"{CATALOG_NAME}.bronze.customers")
        .using("iceberg")
        .createOrReplace()
    )
    context.log.info("✅ Write complete")

    spark.stop()

    # Return rich metadata so it shows in the Dagster UI for every run
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "source_table": MetadataValue.text("postgres.ecommerce.customers"),
            "target_table": MetadataValue.text(f"{CATALOG_NAME}.bronze.customers"),
            "ingested_at": MetadataValue.text(
                datetime.now(timezone.utc).isoformat()
            ),
        }
    )