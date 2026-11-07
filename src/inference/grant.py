"""Apply the cycle's checklist rules to every TIN and summarize without taxpayer ids."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rules_engine.engine import RulesEngine, outcome_column_name

CONTEXT_COLUMNS = ("activity_isic4_cd", "BUSINESS_START_DT", "REGION_NAME", "TAX_PAYER_TYPE")
CHECKLIST_COLUMNS = (
    "failed_rules",
    "review_rules",
    "rules_failed_count",
    "checklist_status",
    "decision",
    "cycle",
    "assessment_date",
    "rules_applied",
)


def apply_grant(
    df: DataFrame,
    engine: RulesEngine,
    *,
    cycle: str,
    assessment_date: date,
    rule_codes,
) -> DataFrame:
    """One row per TIN: context, the feature behind each rule, each rule's outcome, and the decision."""
    rules = engine.rules_for(assessment_date, rule_codes)
    judged = engine.evaluate_frame(df, assessment_date, rule_codes).withColumn("cycle", F.lit(cycle))
    evidence = list(dict.fromkeys(rule.feature_name for rule in rules))
    outcomes = [outcome_column_name(rule.rule_code) for rule in rules]
    context = [
        (F.col(name) if name in df.columns else F.lit(None)).cast("string").alias(name)
        for name in CONTEXT_COLUMNS
    ]
    return judged.select(
        F.col("TIN_NO").cast("string").alias("TIN_NO"),
        *context,
        *evidence,
        *outcomes,
        *CHECKLIST_COLUMNS,
    )


def summarize_grant(df: DataFrame, rule_codes) -> dict:
    """Counts only. The summary is safe to log and to commit to a dashboard."""
    header = df.select("cycle", "assessment_date", "rules_applied").limit(1).collect()
    if not header:
        raise ValueError("Grant output is empty")
    aggregations = [
        F.count(F.lit(1)).alias("taxpayer_count"),
        F.sum((F.col("checklist_status") == "PASS").cast("int")).alias("eligible"),
        F.sum((F.col("checklist_status") == "REVIEW").cast("int")).alias("needs_review"),
    ]
    for code in rule_codes:
        outcome = F.col(outcome_column_name(code))
        aggregations.append(F.sum((outcome == "FAIL").cast("int")).alias(f"fail_{code}"))
        aggregations.append(F.sum((outcome == "REVIEW").cast("int")).alias(f"review_{code}"))
        only = (F.col("rules_failed_count") == 1) & (outcome == "FAIL")
        aggregations.append(F.sum(only.cast("int")).alias(f"only_{code}"))
    for count in (1, 2, 3):
        aggregations.append(F.sum((F.col("rules_failed_count") == count).cast("int")).alias(f"nfail_{count}"))
    aggregations.append(F.sum((F.col("rules_failed_count") >= 4).cast("int")).alias("nfail_4plus"))
    row = df.agg(*aggregations).collect()[0]

    def whole(name: str) -> int:
        value = row[name]
        return 0 if value is None else int(value)

    taxpayers = whole("taxpayer_count")
    eligible = whole("eligible")
    review = whole("needs_review")
    return {
        "cycle": str(header[0]["cycle"]),
        "assessment_date": str(header[0]["assessment_date"]),
        "rules_applied": str(header[0]["rules_applied"]).split(","),
        "taxpayer_count": taxpayers,
        "eligible": eligible,
        "needs_review": review,
        "not_eligible": taxpayers - eligible - review,
        "fail_any": {code: whole(f"fail_{code}") for code in rule_codes},
        "review_any": {code: whole(f"review_{code}") for code in rule_codes},
        "fail_only": {code: whole(f"only_{code}") for code in rule_codes},
        "fail_count": {
            "1": whole("nfail_1"),
            "2": whole("nfail_2"),
            "3": whole("nfail_3"),
            "4_or_more": whole("nfail_4plus"),
        },
    }


def write_summary(summary: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2) + "\n")


def _actor() -> str:
    import getpass
    import os

    return os.getenv("WHT_ACTOR") or getpass.getuser()


def _one_row_per_tin(df: DataFrame) -> int:
    summary = df.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("TIN_NO").alias("tins")).first()
    rows = int(summary["rows"])
    tins = int(summary["tins"])
    if rows == 0:
        raise ValueError("No taxpayers to grant")
    if rows != tins:
        raise ValueError("Duplicate TINs remain after prepare")
    return rows


def run(config=None, spark=None, engine: RulesEngine | None = None) -> dict:
    """Prepare one row per TIN, apply the cycle's rules, and write the grant table."""
    from data_engineering.config import ProjectConfig
    from data_engineering.preprocess import prepare_taxpayers
    from data_engineering.source import read_taxpayers
    from data_engineering.spark import job_session
    from rules_engine.catalog import default_engine

    config = config or ProjectConfig.load()
    engine = engine or default_engine()
    if not config.taxpayers.exists():
        raise FileNotFoundError(f"Taxpayer table not found at {config.taxpayers}")
    own_spark = spark is None
    session = spark or job_session("wht-grant", delta=config.source_format == "delta")
    try:
        prepared = prepare_taxpayers(read_taxpayers(session, str(config.taxpayers), config.source_format))
        _one_row_per_tin(prepared)
        granted = apply_grant(
            prepared,
            engine,
            cycle=config.cycle.id,
            assessment_date=config.assessment_date,
            rule_codes=config.cycle.rule_codes,
        )
        config.grants.parent.mkdir(parents=True, exist_ok=True)
        granted.write.mode("overwrite").parquet(str(config.grants))
        saved = session.read.parquet(str(config.grants))
        summary = {
            **summarize_grant(saved, config.cycle.rule_codes),
            "cycle_start": config.cycle.start.isoformat(),
            "cycle_end": config.cycle.end.isoformat(),
        }
        write_summary(summary, config.grant_summary)
        if hasattr(engine.registry, "record_run"):
            summary["run_id"] = engine.registry.record_run(summary, actor=_actor())
        return summary
    finally:
        if own_spark:
            session.stop()


def main() -> None:
    summary = run()
    print(
        f"Grant {summary['cycle']} ({summary['cycle_start']} to {summary['cycle_end']}), "
        f"assessed {summary['assessment_date']} with {', '.join(summary['rules_applied'])}"
    )
    print(
        f"{summary['taxpayer_count']:,} taxpayers: {summary['eligible']:,} eligible, "
        f"{summary['not_eligible']:,} not eligible, {summary['needs_review']:,} need review"
    )
    if "run_id" in summary:
        print(f"Recorded as grant run #{summary['run_id']} in the rules database")


if __name__ == "__main__":
    main()
