"""
Bronze layer Dagster assets
============================
One asset per source Postgres table — 7 in total. All share a single
helper function `_ingest_postgres_table` that handles the common logic:

    Postgres → Spark DataFrame → add lineage columns → write Iceberg table

Each asset is a thin wrapper that just declares the source table name.
This keeps the asset definitions tiny and the ingestion logic in one place.
"""

from datetime import datetime, timezone

from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
)
from pyspark.sql.functions import lit, current_timestamp

from dagster_project.resources.spark_resource import SparkResource


# ─────────────────────────────────────────────────────────────────────────────
# Shared ingestion helper — the heart of the Bronze layer
# ─────────────────────────────────────────────────────────────────────────────

def _ingest_postgres_table(
    context: AssetExecutionContext,
    spark: SparkResource,
    source_table: str,
) -> MaterializeResult:
    """
    Read one Postgres table, stamp it with lineage metadata, write it as
    an Iceberg table under the `bronze` namespace.

    The Spark session, JDBC connection details, and S3 credentials all
    come from the injected SparkResource — so this function knows
    nothing about *where* the data lives, only *what to do with it*.
    """
    target_table = f"{spark.catalog_name}.bronze.{source_table}"

    spark.ensure_namespace("bronze")

    context.log.info(f"📥 Reading {source_table} from Postgres...")
    df = spark.read_postgres_table(source_table)

    row_count = df.count()
    context.log.info(f"   ✅ Read {row_count} rows from source")

    # Bronze lineage columns — every row tagged with origin + ingest time
    df_with_meta = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(f"postgres.ecommerce.{source_table}"))
    )

    context.log.info(f"💾 Writing to {target_table}...")
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


# ─────────────────────────────────────────────────────────────────────────────
# The 7 Bronze assets
# ─────────────────────────────────────────────────────────────────────────────
# Conventions used:
#   - key_prefix=["bronze"]  → asset key becomes bronze/<name>
#   - group_name="bronze"    → grouped together in the UI graph
#   - compute_kind="spark"   → Spark icon shown on the node
#   - deps=[...]             → declares lineage relationships (informational)

# ── Reference data (independent — no upstream Bronze deps) ──────────────────

@asset(
    name="customers",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw customers from Postgres, landed as Iceberg.",
    compute_kind="spark",
)
def bronze_customers(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "customers")


@asset(
    name="suppliers",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw suppliers from Postgres, landed as Iceberg.",
    compute_kind="spark",
)
def bronze_suppliers(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "suppliers")


@asset(
    name="products",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw products from Postgres, landed as Iceberg.",
    compute_kind="spark",
)
def bronze_products(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "products")


# ── Operational data (declares lineage to its source-domain references) ────

@asset(
    name="inventory",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw inventory snapshots from Postgres, landed as Iceberg.",
    compute_kind="spark",
    deps=[AssetKey(["bronze", "products"])],  # inventory refers to products
)
def bronze_inventory(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "inventory")


@asset(
    name="orders",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw orders from Postgres, landed as Iceberg.",
    compute_kind="spark",
    deps=[AssetKey(["bronze", "customers"])],  # orders refer to customers
)
def bronze_orders(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "orders")


@asset(
    name="order_items",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw order line items from Postgres, landed as Iceberg.",
    compute_kind="spark",
    deps=[
        AssetKey(["bronze", "orders"]),
        AssetKey(["bronze", "products"]),
    ],
)
def bronze_order_items(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "order_items")


@asset(
    name="returns",
    key_prefix=["bronze"],
    group_name="bronze",
    description="Raw returns from Postgres, landed as Iceberg.",
    compute_kind="spark",
    deps=[AssetKey(["bronze", "order_items"])],  # returns refer to order_items
)
def bronze_returns(
    context: AssetExecutionContext, spark: SparkResource
) -> MaterializeResult:
    return _ingest_postgres_table(context, spark, "returns")