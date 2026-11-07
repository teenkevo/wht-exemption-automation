"""The Spark batch checklist agrees with the single-taxpayer checklist, row by row."""

from datetime import date

import pytest

from rules_engine import RuleDefinition, RuleOperator, RulesEngine
from rules_engine.catalog import CHANGES, CATALOG_START, build_registry, release
from rules_engine.engine import outcome_column_name

DAY = date(2026, 11, 7)

EXTRA = (
    release(
        RuleDefinition(
            rule_code="KNOWN_STATUS",
            title="Known registration status",
            description="Registration status is one of the listed codes",
            feature_name="registration_status",
            operator=RuleOperator.IN,
            expected_value=["REGD", "SUSP"],
            mandatory=False,
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="NOT_CANCELLED",
            title="Not cancelled",
            description="Registration status is not CNCL",
            feature_name="registration_status",
            operator=RuleOperator.NE,
            expected_value="CNCL",
            mandatory=False,
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="NO_RED_LANE",
            title="No customs red-lane declarations",
            description="No customs declaration was routed to physical inspection",
            feature_name="customs_red_lane_count",
            operator=RuleOperator.LT,
            expected_value=1,
        ),
        effective_from=CATALOG_START,
    ),
)

RAW_COLUMNS = (
    "TIN_NO",
    "REG_STATUS",
    "taxpayer_status",
    "total_outstanding_liability",
    "total_returns_count",
    "total_tax_collected",
    "red_lane_count",
)
ROWS = [
    ("T1", "REGD", "Existing Taxpayer", 0.0, 3.0, 150_000_000.0, 0.0),
    ("T2", "CNCL", "Existing Taxpayer", 500.0, None, 99_999_999.0, 2.0),
    ("T3", None, "Existing Taxpayer", None, 1.0, 100_000_000.0, None),
    ("T4", "SUSP", None, -10.0, 0.0, None, 0.0),
    ("T5", "REGD", "New Taxpayer", 0.0, 2.0, float("nan"), 1.0),
]


@pytest.mark.parametrize("with_extra", [False, True])
def test_batch_and_single_taxpayer_checklists_agree(spark, with_extra):
    engine = RulesEngine(build_registry(CHANGES + EXTRA) if with_extra else build_registry())
    rules = engine.rules_for(DAY)
    frame = spark.createDataFrame(ROWS, schema=", ".join(f"{name} {'string' if i < 3 else 'double'}" for i, name in enumerate(RAW_COLUMNS)))
    judged = {row["TIN_NO"]: row.asDict() for row in engine.evaluate_frame(frame, DAY).collect()}
    assert len(judged) == len(ROWS)
    for tin, row in judged.items():
        single = engine.evaluate(tin, {rule.feature_name: row[rule.feature_name] for rule in rules}, DAY)
        assert row["checklist_status"] == single.checklist_status.value, tin
        assert row["decision"] == single.decision, tin
        assert row["failed_rules"] == ",".join(single.failed_rules), tin
        assert row["review_rules"] == ",".join(single.review_rules), tin
        for item in single.results:
            assert row[outcome_column_name(item.rule_code)] == item.outcome.value, (tin, item.rule_code)


def test_missing_source_rows_count_as_zero_and_missing_status_goes_to_review(spark):
    engine = RulesEngine(build_registry())
    frame = spark.createDataFrame(ROWS, schema=", ".join(f"{name} {'string' if i < 3 else 'double'}" for i, name in enumerate(RAW_COLUMNS)))
    judged = {row["TIN_NO"]: row for row in engine.evaluate_frame(frame, DAY).collect()}
    assert judged["T1"]["decision"] == "ELIGIBLE"
    assert judged["T2"]["returns_filed_count"] == 0.0
    assert judged["T2"]["failed_rules"] == "REGISTERED,NO_OUTSTANDING_LIABILITY,RETURNS_FILED,MIN_TAX_COLLECTED"
    assert judged["T3"]["outstanding_liability"] == 0.0
    assert judged["T3"]["checklist_status"] == "REVIEW"
    assert judged["T3"]["review_rules"] == "REGISTERED"
    assert judged["T5"]["rule_min_tax_collected"] == "REVIEW"


def test_a_missing_source_column_names_its_lake_table(spark):
    frame = spark.createDataFrame([("T1", "REGD")], schema="TIN_NO string, REG_STATUS string")
    with pytest.raises(ValueError, match=r"total_tax_collected \(from payments\)"):
        RulesEngine(build_registry()).evaluate_frame(frame, DAY)
