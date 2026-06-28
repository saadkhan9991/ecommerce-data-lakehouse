"""
Bronze layer Dagster assets
============================
Each function decorated with @asset corresponds to one Iceberg table
in the Bronze zone.

Refactored to use SparkResource (dependency injection) instead of
building a Spark session inside each asset. This means:
  - One Spark session shared across all Bronze assets per run
  - Spark/MinIO/Postgres config lives in definitions.py, not here
  - Adding a new Bronze table is now ~10 lines of code
"""

from datetime import datetime, timezone

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from pyspark.sql.functions import lit, current_timestamp

from dagster_project.resources.spark_resource import SparkResource


@asset(
    name="customers",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw customers table ingested from Postgres into Iceberg",
    compute_kind="spark",
)
def bronze_customers(
    context: AssetExecutionContext,
    spark: SparkResource,
) -> MaterializeResult:
    """
    Reads the `customers` table from Postgres and writes it to MinIO
    as an Apache Iceberg table at `local.bronze.customers`.
    """
    source_table = "customers"
    target_table = f"{spark.catalog_name}.bronze.customers"

    spark.ensure_namespace("bronze")

    context.log.info(f"📥 Reading {source_table} from Postgres...")
    df = spark.read_postgres_table(source_table)

    row_count = df.count()
    context.log.info(f"   ✅ Read {row_count} rows from source")

    # Bronze lineage columns — every row stamped with origin + ingest time
    df_with_meta = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(f"postgres.ecommerce.{source_table}"))
    )

    context.log.info(f"💾 Writing to Iceberg table {target_table}...")
    (
        df_with_meta.writeTo(target_table)
        .using("iceberg")
        .createOrReplace()
    )
    context.log.info("   ✅ Write complete")

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(row_count),
            "source_table": MetadataValue.text(f"postgres.ecommerce.{source_table}"),
            "target_table": MetadataValue.text(target_table),
            "ingested_at": MetadataValue.text(
                datetime.now(timezone.utc).isoformat()
            ),
        }
    )