"""Keep one taxpayer row before the grant rules and the ranker run."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REQUIRED_COLUMNS = [
    "TIN_NO",
    "REG_STATUS",
    "taxpayer_status",
    "total_outstanding_liability",
    "total_returns_count",
    "total_tax_collected",
    "activity_isic4_cd",
    "BUSINESS_START_DT",
]


def prepare_taxpayers(df: DataFrame) -> DataFrame:
    """Drop a null TIN and keep one row per TIN.

    A row that has a business-activity code is preferred. When both rows have
    one, or neither does, the later business start date is kept.
    """
    missing = [name for name in REQUIRED_COLUMNS if name not in df.columns]
    if missing:
        raise ValueError(f"Taxpayer table is missing columns: {', '.join(missing)}")

    window = Window.partitionBy("TIN_NO").orderBy(
        F.col("activity_isic4_cd").isNull().cast("int"),
        F.col("BUSINESS_START_DT").desc(),
    )
    return (
        df.filter(F.col("TIN_NO").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def preprocess(frame, entity_key: str | None = None):
    """Keep one taxpayer row. ``frame`` is a Spark DataFrame keyed by TIN_NO."""
    from data_engineering.config import ProjectConfig

    key = entity_key or ProjectConfig.load().entity_key
    if key not in frame.columns:
        raise ValueError(f"Extract is missing {key}")
    if not hasattr(frame, "withColumn"):
        raise TypeError("Taxpayer prepare expects a Spark DataFrame")
    return prepare_taxpayers(frame)
