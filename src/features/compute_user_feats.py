"""Leakage-safe features for the WHT similarity ranker.

Every column a checklist rule reads, and the eligibility flag, are kept out of
the model inputs. The score can only use what remains.
"""

from __future__ import annotations

import re
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

LEAK_COLUMNS = {
    "TIN_NO",
    "wht_eligible",
    "label",
    "REG_STATUS",
    "taxpayer_status",
    "total_outstanding_liability",
    "total_tax_collected",
    "avg_payment_amount",
    "total_returns_count",
    "is_registered",
    "is_existing_taxpayer",
    "no_outstanding_liability",
    "has_filed_returns",
}
STRICT_COLUMNS = {
    "max_single_payment",
    "number_of_payments",
    "total_declared_tax",
    "liability_record_count",
    "total_waiver_received",
}
LEAK_PATTERN = re.compile(r"flag|rule|pass|eligib", re.IGNORECASE)
NUMERIC_TYPES = (IntegerType, LongType, ShortType, DoubleType, FloatType, DecimalType)
GROUP_COLUMNS = ("REGION_NAME", "TAX_PAYER_TYPE")


def rule_input_columns() -> set[str]:
    """Joined-table columns read by any rule version in the catalog, past or present."""
    from rules_engine.catalog import default_registry

    registry = default_registry()
    features = {rule.feature_name for code in registry.codes() for rule in registry.history(code)}
    return registry.features.source_columns(features) | features


def with_label(
    df: DataFrame,
    *,
    assessment_date: date | None = None,
    rule_codes=None,
    engine=None,
) -> tuple[DataFrame, str]:
    """Add ``label``: the stored ``wht_eligible`` when present, otherwise a checklist PASS."""
    if "wht_eligible" in df.columns:
        return df.withColumn("label", F.col("wht_eligible").cast("int")), "wht_eligible"
    from rules_engine.catalog import default_engine
    from rules_engine.cycles import DEFAULT_RULES, run_date

    engine = engine or default_engine()
    judged = engine.evaluate_frame(df, assessment_date or run_date(), tuple(rule_codes or DEFAULT_RULES))
    passed = (F.col("checklist_status") == F.lit("PASS")).cast("int")
    return judged.select(*[F.col(name) for name in df.columns], passed.alias("label")), "rules"


def discover_features(
    df: DataFrame,
    *,
    strict: bool,
    max_cardinality: int,
) -> dict:
    excluded_names = set(LEAK_COLUMNS) | rule_input_columns()
    if strict:
        excluded_names |= STRICT_COLUMNS
    excluded = [name for name in df.columns if name in excluded_names or LEAK_PATTERN.search(name)]
    excluded_set = set(excluded)
    candidates = [field for field in df.schema.fields if field.name not in excluded_set]
    num_cols = [field.name for field in candidates if isinstance(field.dataType, NUMERIC_TYPES)]
    string_cols = [field.name for field in candidates if isinstance(field.dataType, StringType)]
    if string_cols:
        card_row = df.agg(*[F.approx_count_distinct(name).alias(name) for name in string_cols]).first()
        cards = {name: int(card_row[name]) for name in string_cols}
    else:
        cards = {}
    cat_cols = [name for name in string_cols if cards[name] <= max_cardinality]
    dropped = [name for name in string_cols if cards[name] > max_cardinality]
    isic_division = "activity_isic4_cd" in df.columns and "activity_isic4_cd" not in cat_cols
    business_age = "BUSINESS_START_DT" in df.columns
    num_feats = [f"log_{name}" for name in num_cols] + [f"has_{name}" for name in num_cols]
    if business_age:
        num_feats.append("business_age_years")
    cat_feats = list(cat_cols)
    if isic_division:
        cat_feats.append("isic_division")
    passthrough = [name for name in GROUP_COLUMNS if name in df.columns and name not in cat_feats and name not in num_feats]
    if not num_feats and not cat_feats:
        raise ValueError("No model inputs remain after the leakage guard")
    return {
        "excluded": excluded,
        "dropped_high_cardinality": dropped,
        "num_cols": num_cols,
        "cat_cols": cat_cols,
        "num_feats": num_feats,
        "cat_feats": cat_feats,
        "passthrough": passthrough,
        "isic_division": isic_division,
        "business_age": business_age,
        "strict": strict,
        "max_categorical_cardinality": max_cardinality,
    }


def apply_features(
    df: DataFrame,
    plan: dict,
    *,
    assessment_date: date | None = None,
    rule_codes=None,
) -> DataFrame:
    if "label" not in df.columns:
        df, _source = with_label(df, assessment_date=assessment_date, rule_codes=rule_codes)
    columns = [
        F.col("TIN_NO").cast("string").alias("TIN_NO"),
        F.col("label").cast("int").alias("label"),
    ]
    for name in plan["num_cols"]:
        value = F.coalesce(F.col(name).cast("double"), F.lit(0.0))
        columns.append(F.log1p(F.greatest(value, F.lit(0.0))).alias(f"log_{name}"))
        columns.append((value > F.lit(0.0)).cast("int").alias(f"has_{name}"))
    for name in plan["cat_cols"]:
        columns.append(F.coalesce(F.col(name).cast("string"), F.lit("UNK")).alias(name))
    if plan["isic_division"]:
        columns.append(
            F.coalesce(F.substring(F.col("activity_isic4_cd").cast("string"), 1, 2), F.lit("UNK")).alias(
                "isic_division"
            )
        )
    if plan["business_age"]:
        age = F.months_between(F.current_date(), F.col("BUSINESS_START_DT").cast("date")) / F.lit(12.0)
        columns.append(F.coalesce(F.greatest(age, F.lit(0.0)), F.lit(0.0)).alias("business_age_years"))
    for name in plan["passthrough"]:
        columns.append(F.col(name).cast("string").alias(name))
    return df.select(*columns).filter(F.col("label").isNotNull())


def compute_user_features(frame, entity_key: str | None = None):
    """Build the ranker inputs for one taxpayer frame."""
    from data_engineering.config import ProjectConfig

    config = ProjectConfig.load()
    key = entity_key or config.entity_key
    if key not in frame.columns:
        raise ValueError(f"Feature input is missing {key}")
    plan = discover_features(
        frame,
        strict=config.strict_features,
        max_cardinality=config.max_categorical_cardinality,
    )
    return apply_features(
        frame,
        plan,
        assessment_date=config.assessment_date,
        rule_codes=config.cycle.rule_codes,
    )
