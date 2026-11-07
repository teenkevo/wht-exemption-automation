"""Spark session for the WHT taxpayer jobs."""

import os
import sys

from pyspark.sql import SparkSession


def spark_session(
    app_name: str,
    master: str | None = None,
    delta: bool = False,
    configs: dict[str, str] | None = None,
) -> SparkSession:
    python = sys.executable
    os.environ["PYSPARK_PYTHON"] = python
    os.environ["PYSPARK_DRIVER_PYTHON"] = python
    # A hostname that resolves to 127.0.0.1 makes Spark warn and pick another address.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", python)
        .config("spark.pyspark.driver.python", python)
        .config("spark.ui.showConsoleProgress", "false")
    )
    if "spark.log.level" not in (configs or {}):
        builder = builder.config("spark.log.level", "ERROR")
    if delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:
            raise RuntimeError("Install the spark extra so Delta tables can be read and written") from exc
        builder = configure_spark_with_delta_pip(builder)
    for key, value in (configs or {}).items():
        builder = builder.config(key, value)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def job_session(app_name: str, *, delta: bool = False):
    """Session using config/spark_config.yaml."""
    from data_engineering import load_yaml

    raw = load_yaml("spark_config.yaml").get("configs") or {}
    configs = {str(key): str(value) for key, value in raw.items()}
    return spark_session(app_name, delta=delta, configs=configs)
