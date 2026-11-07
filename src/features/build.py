"""Write the ranker feature snapshot and register it in Feast.

Materialization into the online store is the next step, ``make materialize``.
"""

from __future__ import annotations

import json

from pyspark.sql import functions as F

from data_engineering.config import ProjectConfig
from data_engineering.preprocess import prepare_taxpayers
from data_engineering.source import read_taxpayers
from data_engineering.spark import job_session
from features.compute_user_feats import apply_features, discover_features
from features.registry import register


def build(config: ProjectConfig | None = None, spark=None) -> dict:
    config = config or ProjectConfig.load()
    if not config.taxpayers.exists():
        raise FileNotFoundError(f"Taxpayer table not found at {config.taxpayers}")
    own_spark = spark is None
    session = spark or job_session("wht-features", delta=config.source_format == "delta")
    try:
        prepared = prepare_taxpayers(read_taxpayers(session, str(config.taxpayers), config.source_format))
        plan = discover_features(
            prepared,
            strict=config.strict_features,
            max_cardinality=config.max_categorical_cardinality,
        )
        featured = apply_features(
            prepared,
            plan,
            assessment_date=config.assessment_date,
            rule_codes=config.cycle.rule_codes,
        ).withColumn("event_timestamp", F.current_timestamp())
        config.features.parent.mkdir(parents=True, exist_ok=True)
        featured.write.mode("overwrite").parquet(str(config.features))
        config.feature_plan.parent.mkdir(parents=True, exist_ok=True)
        config.feature_plan.write_text(json.dumps(plan, indent=2) + "\n")
        rows = featured.count()
    finally:
        if own_spark:
            session.stop()
    registered = register(config)
    return {
        "rows": rows,
        "features": str(config.features),
        "plan": str(config.feature_plan),
        **registered,
    }


def main() -> None:
    from termstyle import fields, heading, note

    summary = build()
    heading("Features")
    fields(
        [
            ("Rows", f"{summary['rows']:,}", ""),
            ("Registered", str(len(summary["registered_features"])), "green"),
            ("Snapshot", summary["features"], "cyan"),
            ("Plan", summary["plan"], ""),
            ("Registry", summary["registry"], ""),
            ("Online store", summary["online_store"], ""),
        ]
    )
    print()
    note("Next: make materialize")


if __name__ == "__main__":
    main()
