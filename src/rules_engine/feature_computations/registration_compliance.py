"""Registration features, from the taxpayer master (``reg``)."""

from pyspark.sql import functions as F

from rules_engine.feature_computations.base import FeatureDefinition, FeatureType

FEATURES = (
    FeatureDefinition(
        name="registration_status",
        description="Registration status code, for example REGD or CNCL",
        dtype=FeatureType.TEXT,
        source_table="reg",
        columns=("REG_STATUS",),
        compute=lambda: F.col("REG_STATUS").cast("string"),
        missing="null, sent to review",
    ),
    FeatureDefinition(
        name="taxpayer_status",
        description="Taxpayer lifecycle status, for example Existing Taxpayer",
        dtype=FeatureType.TEXT,
        source_table="reg",
        columns=("taxpayer_status",),
        compute=lambda: F.col("taxpayer_status").cast("string"),
        missing="null, sent to review",
    ),
    FeatureDefinition(
        name="business_age_years",
        description="Years since the business start date",
        dtype=FeatureType.NUMBER,
        source_table="reg",
        columns=("BUSINESS_START_DT",),
        compute=lambda: F.greatest(
            F.months_between(F.current_date(), F.col("BUSINESS_START_DT").cast("date")) / F.lit(12.0),
            F.lit(0.0),
        ),
        missing="null, sent to review",
    ),
)
