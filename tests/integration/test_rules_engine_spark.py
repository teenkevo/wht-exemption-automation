"""The Spark batch checklist agrees with the single-taxpayer checklist, row by row."""

from datetime import date

import pytest

from rules_engine import RuleDefinition, RuleOperator, RulesEngine
from rules_engine.catalog import CATALOG_START, build_registry, release
from rules_engine.engine import outcome_column_name

DAY = date(2026, 11, 7)

# A fixed checklist owned by this test. catalog.py stays empty.
CHECKLIST = (
    release(
        RuleDefinition(
            rule_code="REGISTERED",
            title="Registered",
            description="Registration status is REGD",
            feature_name="registration_status",
            operator=RuleOperator.EQ,
            expected_value="REGD",
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="EXISTING_TAXPAYER",
            title="Existing taxpayer",
            description="Taxpayer status is Existing Taxpayer",
            feature_name="taxpayer_status",
            operator=RuleOperator.EQ,
            expected_value="Existing Taxpayer",
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="NO_OUTSTANDING_LIABILITY",
            title="No outstanding liability",
            description="Outstanding liability across ledger entries is at most zero",
            feature_name="outstanding_liability",
            operator=RuleOperator.LTE,
            expected_value=0.0,
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="RETURNS_FILED",
            title="Has filed returns",
            description="At least one income tax return is on record",
            feature_name="returns_filed_count",
            operator=RuleOperator.GT,
            expected_value=0,
        ),
        effective_from=CATALOG_START,
    ),
    release(
        RuleDefinition(
            rule_code="MIN_TAX_COLLECTED",
            title="Tax collected at least 100M",
            description="Total tax paid is at least UGX 100,000,000",
            feature_name="tax_collected",
            operator=RuleOperator.GTE,
            expected_value=100_000_000,
        ),
        effective_from=CATALOG_START,
    ),
)

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
    rules = CHECKLIST + EXTRA if with_extra else CHECKLIST
    engine = RulesEngine(build_registry(rules))
    rules = engine.rules_for(DAY)
    frame = spark.createDataFrame(ROWS, schema=", ".join(f"{name} {'string' if i < 3 else 'double'}" for i, name in enumerate(RAW_COLUMNS)))
    _register_sources(spark, ROWS)
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
    engine = RulesEngine(build_registry(CHECKLIST))
    frame = spark.createDataFrame(ROWS, schema=", ".join(f"{name} {'string' if i < 3 else 'double'}" for i, name in enumerate(RAW_COLUMNS)))
    _register_sources(spark, ROWS)
    judged = {row["TIN_NO"]: row for row in engine.evaluate_frame(frame, DAY).collect()}
    assert judged["T1"]["decision"] == "ELIGIBLE"
    assert judged["T2"]["returns_filed_count"] == 0.0
    assert judged["T2"]["failed_rules"] == "REGISTERED,NO_OUTSTANDING_LIABILITY,RETURNS_FILED,MIN_TAX_COLLECTED"
    assert judged["T3"]["outstanding_liability"] == 0.0
    assert judged["T3"]["checklist_status"] == "REVIEW"
    assert judged["T3"]["review_rules"] == "REGISTERED"
    assert judged["T5"]["rule_min_tax_collected"] == "REVIEW"
    assert judged["T1"]["features_applied"] == (
        "registration_status:v1,taxpayer_status:v1,outstanding_liability:v1,"
        "returns_filed_count:v1,tax_collected:v1"
    )


def test_an_empty_checklist_marks_every_row_not_configured(spark):
    frame = spark.createDataFrame([("T1",)], schema="TIN_NO string")
    row = RulesEngine(build_registry()).evaluate_frame(frame, DAY).collect()[0]
    assert row["checklist_status"] == "NOT_CONFIGURED"
    assert row["decision"] == "NOT CONFIGURED"
    assert row["rules_applied"] == ""
    assert row["features_applied"] == ""


def test_grant_uses_the_latest_feature_migration(spark, tmp_path):
    from rules_engine.feature_migrations import load_feature_registry

    paid = """SELECT
    TIN_NO,
    cast(sum(TOT_PAYMENT_AMT) as double) AS tax_collected
FROM lakehouse.payments
GROUP BY TIN_NO"""
    _feature_sql(tmp_path / "V001__baseline.sql", paid)
    _feature_sql(tmp_path / "V002__tax_collected_plus_one.sql", paid.replace(
        "cast(sum(TOT_PAYMENT_AMT) as double)",
        "cast(sum(TOT_PAYMENT_AMT) as double) + 1.0",
    ))
    features = load_feature_registry(tmp_path)
    assert features.require("tax_collected").version == 2
    assert features.history("tax_collected")[0].status.value == "DEPRECATED"
    engine = RulesEngine(
        build_registry(
            (
                release(
                    RuleDefinition(
                        rule_code="MIN_TAX_COLLECTED",
                        title="Tax collected",
                        description="Tax collected",
                        feature_name="tax_collected",
                        operator=RuleOperator.GTE,
                        expected_value=0,
                    ),
                    effective_from=CATALOG_START,
                ),
            ),
            features,
        )
    )
    payments = spark.createDataFrame([("T1", 10.0)], schema="TIN_NO string, TOT_PAYMENT_AMT double")
    features.register_sources(spark, {"payments": payments})
    frame = spark.createDataFrame([("T1",)], schema="TIN_NO string")
    row = engine.evaluate_frame(frame, DAY).collect()[0]
    assert row["tax_collected"] == 11.0
    assert row["features_applied"] == "tax_collected:v2"


def _feature_sql(path, expression: str) -> None:
    path.write_text(
        "\n".join(
            [
                "-- feature: tax_collected",
                "-- description: Total tax paid",
                "-- dtype: number",
                "-- source: payments",
                "-- missing: 0, no payments on record",
                expression,
                "",
            ]
        )
    )


def test_a_missing_source_table_names_the_lake_view(spark):
    for name in ("reg", "ledger", "payments", "returns", "customs", "efris", "sector"):
        spark.catalog.dropTempView(f"lakehouse_{name}")
    frame = spark.createDataFrame([("T1", "REGD")], schema="TIN_NO string, REG_STATUS string")
    with pytest.raises(ValueError, match="lakehouse.payments"):
        RulesEngine(build_registry(CHECKLIST)).evaluate_frame(frame, DAY)


def _register_sources(spark, rows):
    from rules_engine.feature_computations import FEATURES

    def frame(data, schema):
        return spark.createDataFrame(data, schema=schema)

    reg = [(tin, status, life) for tin, status, life, *_rest in rows]
    payments = [(tin, tax) for tin, _status, _life, _owed, _filed, tax, _red in rows if tax is not None]
    ledger = [(tin, owed) for tin, _status, _life, owed, _filed, _tax, _red in rows if owed is not None]
    returns = [(tin,) for tin, _status, _life, _owed, filed, _tax, _red in rows if filed]
    customs = [(tin, red) for tin, _status, _life, _owed, _filed, _tax, red in rows if red is not None]
    FEATURES.register_sources(
        spark,
        {
            "reg": frame(reg, "TIN_NO string, REG_STATUS string, taxpayer_status string"),
            "payments": frame(payments, "TIN_NO string, TOT_PAYMENT_AMT double"),
            "ledger": frame(ledger, "TPIN string, OUTSTANDING_LIABILITY double"),
            "returns": frame(returns, "TAXPAYER_TIN string"),
            "customs": frame(customs, "taxpayer_tin string, is_red_flg double"),
        },
    )
