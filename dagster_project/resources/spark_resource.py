"""
SparkResource
==============
A Dagster resource that provides a configured SparkSession to any asset
that declares it as a dependency.

Why this exists:
  - Every Bronze asset needs the same Spark/Iceberg/MinIO config.
  - Without a shared resource, each asset would build its own Spark
    session (slow, repetitive, and the wrong architecture).
  - With this resource, all assets share one Spark session per run,
    and the config lives in exactly one place.

Pattern: Dagster's `ConfigurableResource` — based on Pydantic, so config
fields are typed and validated at startup.
"""

from contextlib import contextmanager
from typing import Iterator

from dagster import ConfigurableResource
from pydantic import PrivateAttr
from pyspark.sql import SparkSession


class SparkResource(ConfigurableResource):
    """Shared Spark session configured for Iceberg on S3-compatible storage."""

    # ── Configurable fields (typed and validated by Pydantic) ──────────────
    app_name: str = "lakehouse"
    catalog_name: str = "local"

    # Source database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ecommerce"
    postgres_user: str = "ecommerce"
    postgres_password: str = "ecommerce"

    # Object storage (MinIO locally, real S3 in production — only URL changes)
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "lakehouse"

    # ── Internal cached Spark session (created lazily) ─────────────────────
    _session: SparkSession = PrivateAttr(default=None)

    # ── JAR coordinates (Maven Central — Spark downloads them on first run)
    @property
    def _jars(self) -> str:
        return ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "software.amazon.awssdk:bundle:2.20.18",
            "org.postgresql:postgresql:42.7.3",
        ])

    @property
    def warehouse_path(self) -> str:
        return f"s3a://{self.s3_bucket}/warehouse"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # ── Build (or return cached) Spark session ─────────────────────────────
    @property
    def session(self) -> SparkSession:
        """Return the configured Spark session, building it on first access."""
        if self._session is None:
            self._session = self._build_session()
        return self._session

    def _build_session(self) -> SparkSession:
        catalog = self.catalog_name
        return (
            SparkSession.builder
            .appName(self.app_name)
            .config("spark.jars.packages", self._jars)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config(
                f"spark.sql.catalog.{catalog}",
                "org.apache.iceberg.spark.SparkCatalog"
            )
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", self.warehouse_path)
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

    # ── Convenience helpers used by assets ─────────────────────────────────
    def read_postgres_table(self, table: str):
        """Read a Postgres table into a Spark DataFrame via JDBC."""
        return (
            self.session.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", table)
            .option("user", self.postgres_user)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def ensure_namespace(self, namespace: str) -> None:
        """Create the Iceberg namespace (e.g. 'bronze') if it doesn't exist."""
        self.session.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{namespace}"
        )

    # ── Lifecycle: stop Spark cleanly when the run finishes ────────────────
    @contextmanager
    def yield_for_execution(self, context) -> Iterator["SparkResource"]:
        """
        Dagster lifecycle hook. Yields the resource for assets to use,
        then stops the Spark session after all assets in the run finish.
        """
        try:
            yield self
        finally:
            if self._session is not None:
                self._session.stop()
                self._session = None