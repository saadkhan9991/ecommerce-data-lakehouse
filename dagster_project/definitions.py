"""
Dagster Definitions
====================
The entry point Dagster looks for. Registers all assets and resources.

The `resources` dict maps resource names (the keyword used by assets,
e.g. `spark`) to the configured resource instance. When an asset takes
a parameter named `spark`, Dagster injects this instance at runtime.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import bronze
from dagster_project.resources.spark_resource import SparkResource

# Auto-discover every @asset defined in the bronze module
all_assets = load_assets_from_modules([bronze])

# Configure the shared Spark resource. Defaults match the local docker-compose
# environment; in production these would be overridden via environment vars.
spark_resource = SparkResource(
    app_name="lakehouse",
    catalog_name="local",
    postgres_host="localhost",
    postgres_port=5432,
    postgres_db="ecommerce",
    postgres_user="ecommerce",
    postgres_password="ecommerce",
    s3_endpoint="http://localhost:9000",
    s3_access_key="minioadmin",
    s3_secret_key="minioadmin",
    s3_bucket="lakehouse",
)

defs = Definitions(
    assets=all_assets,
    resources={
        "spark": spark_resource,
    },
)