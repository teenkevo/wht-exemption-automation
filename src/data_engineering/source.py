"""Read the joined taxpayer table from Parquet or Delta."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def read_taxpayers(spark, path: str, source_format: str) -> DataFrame:
    fmt = source_format.strip().lower()
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "delta":
        try:
            frame = spark.read.format("delta").load(path)
            frame.schema
        except Exception as exc:
            raise RuntimeError(
                "The Delta taxpayer table could not be opened. Install the delta extra "
                "so the Delta package is on the Spark classpath."
            ) from exc
        return frame
    raise ValueError(f"source_format must be parquet or delta, got {source_format!r}")


def table_counts(df: DataFrame) -> dict:
    """Row counts only. Taxpayer identifiers stay in the source table."""
    aggregations = [
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("TIN_NO").alias("unique_tins"),
    ]
    if "wht_eligible" in df.columns:
        aggregations.append(F.sum(F.col("wht_eligible").cast("int")).alias("eligible"))
    row = df.agg(*aggregations).first()
    rows = int(row["rows"])
    unique = int(row["unique_tins"])
    eligible = row["eligible"] if "wht_eligible" in df.columns else None
    return {
        "rows": rows,
        "unique_tins": unique,
        "duplicate_rows": rows - unique,
        "eligible": None if eligible is None else int(eligible),
    }
