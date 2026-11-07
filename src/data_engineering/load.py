"""Build the WHT taxpayer table from the seven lakehouse sources.

This is the Sparkflows ``WHT_Data_Load`` workflow: registration is the spine,
six aggregates are left-joined on TIN, and the checklist rules in force become
``wht_eligible``. The production load keeps one sector row per TIN. Duplicate
registration rows are left for the grant prepare step.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_engineering.config import SOURCE_NAMES

# Registration columns the grant and the ranker are allowed to see.
# Names, contacts, birth dates, and other identifiers are dropped here.
REG_COLS = [
    "TIN_NO",
    "TAX_PAYER_TYPE",
    "REG_STATUS",
    "ORG_SUB_TYPE",
    "IS_AGENT",
    "IS_VAT_EXEMPTED",
    "IS_RESIDENT_TAXPAYER",
    "IS_SUBSIDARY_COMP",
    "BUSINESS_START_DT",
    "taxpayer_status",
    "IS_INCOME_SRC_BSNS",
    "IS_INCOME_SRC_EMP",
    "IS_INCOME_SRC_PRPTY",
    "IS_INCOME_SRC_RENTAL",
    "STATION_NAME",
    "REGION_NAME",
    "BUSINESS_FISCAL_YEAR",
    "ORG_TYPE",
]

SUM_COLUMNS = (
    "total_tax_collected",
    "total_outstanding_liability",
    "total_efris_sales",
    "total_customs_value",
    "total_declared_tax",
)

COVERAGE_FIELDS = (
    "have_payments",
    "have_ledger",
    "have_returns",
    "have_sector",
    "have_customs",
    "have_efris",
)

_SOURCE_COLUMNS = {
    "reg": tuple(REG_COLS),
    "ledger": ("TPIN", "OUTSTANDING_LIABILITY", "TOTAL_WAIVER"),
    "payments": ("TIN_NO", "TOT_PAYMENT_AMT"),
    "efris": ("sellertin", "sales_amount", "vat_charged"),
    "sector": ("tin_no", "activity_isic4_cd", "updated_dt"),
    "customs": ("taxpayer_tin", "cif_amt_ugx", "is_red_flg"),
    "returns": ("TAXPAYER_TIN", "net_tax"),
}

Reader = Callable[[SparkSession, str], DataFrame]


def _require(df: DataFrame, columns: tuple[str, ...], source: str) -> None:
    missing = [name for name in columns if name not in df.columns]
    if missing:
        raise ValueError(f"{source} is missing columns: {', '.join(missing)}")


def _whole(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def read_table(spark: SparkSession, path: str, source_format: str) -> DataFrame:
    fmt = source_format.strip().lower()
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    raise ValueError(f"source_format must be parquet or delta, got {source_format!r}")


def read_sources(
    spark: SparkSession,
    paths: Mapping[str, str],
    reader: Reader,
) -> dict[str, DataFrame]:
    """Read the seven sources. ``reader`` is injectable so tests can use Parquet."""
    missing = [name for name in SOURCE_NAMES if name not in paths]
    if missing:
        raise KeyError(f"Missing sources: {', '.join(missing)}")
    return {name: reader(spark, str(paths[name])) for name in SOURCE_NAMES}


def prep_reg(df: DataFrame) -> DataFrame:
    """Keep the registration columns used downstream and drop the rest."""
    _require(df, _SOURCE_COLUMNS["reg"], "registration")
    return df.select(*REG_COLS)


def agg_liabilities(df: DataFrame) -> DataFrame:
    """Ledger rows summed to one row per TIN. Active flag and period are not filtered."""
    _require(df, _SOURCE_COLUMNS["ledger"], "ledger")
    return (
        df.groupBy("TPIN")
        .agg(
            F.sum("OUTSTANDING_LIABILITY").alias("total_outstanding_liability"),
            F.sum("TOTAL_WAIVER").alias("total_waiver_received"),
            F.count("TPIN").alias("liability_record_count"),
        )
        .withColumnRenamed("TPIN", "TIN_NO")
    )


def agg_payments(df: DataFrame) -> DataFrame:
    """Payments summed to one row per TIN. Payment date is not filtered."""
    _require(df, _SOURCE_COLUMNS["payments"], "payments")
    return df.groupBy("TIN_NO").agg(
        F.sum("TOT_PAYMENT_AMT").alias("total_tax_collected"),
        F.count("TIN_NO").alias("number_of_payments"),
        F.avg("TOT_PAYMENT_AMT").alias("avg_payment_amount"),
        F.max("TOT_PAYMENT_AMT").alias("max_single_payment"),
    )


def agg_efris(df: DataFrame) -> DataFrame:
    """EFRIS invoices summed to one row per seller TIN."""
    _require(df, _SOURCE_COLUMNS["efris"], "efris")
    return (
        df.groupBy("sellertin")
        .agg(
            F.sum("sales_amount").alias("total_efris_sales"),
            F.count("sellertin").alias("vat_invoice_count"),
            F.sum("vat_charged").alias("total_vat_charged"),
        )
        .withColumnRenamed("sellertin", "TIN_NO")
    )


def prep_sector(df: DataFrame, dedupe: bool = True) -> DataFrame:
    """Map a TIN to an activity code.

    Several sector rows would repeat that TIN after the join. The production
    load keeps a non-null activity code, then the latest update, then the
    code itself. ``dedupe=False`` keeps every sector row, which is how the
    original workflow built the reference table.
    """
    _require(df, _SOURCE_COLUMNS["sector"], "sector")
    out = df.select(F.col("tin_no").alias("TIN_NO"), "activity_isic4_cd", "updated_dt")
    if dedupe:
        window = Window.partitionBy("TIN_NO").orderBy(
            F.col("activity_isic4_cd").isNull().asc(),
            F.col("updated_dt").desc_nulls_last(),
            F.col("activity_isic4_cd").asc_nulls_last(),
        )
        out = (
            out.where(F.col("TIN_NO").isNotNull())
            .withColumn("_rn", F.row_number().over(window))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )
    return out.drop("updated_dt")


def agg_customs(df: DataFrame) -> DataFrame:
    """Customs declaration headers summed to one row per importer TIN."""
    _require(df, _SOURCE_COLUMNS["customs"], "customs")
    return (
        df.groupBy("taxpayer_tin")
        .agg(
            F.sum("cif_amt_ugx").alias("total_customs_value"),
            F.count("taxpayer_tin").alias("customs_entry_count"),
            F.sum("is_red_flg").alias("red_lane_count"),
        )
        .withColumnRenamed("taxpayer_tin", "TIN_NO")
    )


def agg_returns(df: DataFrame) -> DataFrame:
    """Filed returns summed to one row per TIN."""
    _require(df, _SOURCE_COLUMNS["returns"], "returns")
    return (
        df.groupBy("TAXPAYER_TIN")
        .agg(
            F.count("TAXPAYER_TIN").alias("total_returns_count"),
            F.sum("net_tax").alias("total_declared_tax"),
        )
        .withColumnRenamed("TAXPAYER_TIN", "TIN_NO")
    )


def build_base(src: Mapping[str, DataFrame], dedupe_sector: bool = True) -> DataFrame:
    """Left-join the six aggregates onto registration, in workflow order.

    Registration is the spine, so a TIN that exists only in a fact table is
    not added. Duplicate registration rows are kept.
    """
    base = prep_reg(src["reg"])
    parts = (
        agg_liabilities(src["ledger"]),
        agg_payments(src["payments"]),
        agg_efris(src["efris"]),
        prep_sector(src["sector"], dedupe_sector),
        agg_customs(src["customs"]),
        agg_returns(src["returns"]),
    )
    for part in parts:
        base = base.join(part, on="TIN_NO", how="left")
    return base


# Zeppelin's audit flags, each the outcome of one checklist rule.
AUDIT_FLAGS = {
    "is_registered": "REGISTERED",
    "is_existing_taxpayer": "EXISTING_TAXPAYER",
    "no_outstanding_liability": "NO_OUTSTANDING_LIABILITY",
    "has_filed_returns": "RETURNS_FILED",
}


def add_label(
    df: DataFrame,
    *,
    engine=None,
    assessment_date: date | None = None,
    rule_codes=None,
) -> DataFrame:
    """Checklist eligibility flag and the audit flags, from the rules engine.

    ``wht_eligible`` is 1 only when the checklist passes. A taxpayer sent to
    review is not eligible yet. An audit flag is written when its rule is in the cycle.
    """
    from rules_engine.catalog import default_engine
    from rules_engine.cycles import DEFAULT_RULES, run_date
    from rules_engine.engine import outcome_column_name

    engine = engine or default_engine()
    day = assessment_date or run_date()
    codes = tuple(rule_codes or DEFAULT_RULES)
    judged = engine.evaluate_frame(df, day, codes)
    flags = {
        flag: F.when(F.col(outcome_column_name(code)) == "PASS", F.lit(1)).otherwise(F.lit(0))
        for flag, code in AUDIT_FLAGS.items()
        if code in codes
    }
    eligible = F.when(F.col("checklist_status") == "PASS", F.lit(1.0)).otherwise(F.lit(0.0))
    return judged.select(
        *[F.col(name) for name in df.columns],
        eligible.alias("wht_eligible"),
        *[column.alias(flag) for flag, column in flags.items()],
    )


def assemble(
    src: Mapping[str, DataFrame],
    *,
    dedupe_sector: bool = True,
    engine=None,
    assessment_date: date | None = None,
    rule_codes=None,
) -> DataFrame:
    """Join the seven sources and attach the checklist label."""
    return add_label(
        build_base(src, dedupe_sector=dedupe_sector),
        engine=engine,
        assessment_date=assessment_date,
        rule_codes=rule_codes,
    )


def qa_counts(df: DataFrame) -> dict:
    """Row counts and source coverage. Taxpayer identifiers are not included."""
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("TIN_NO").alias("unique_tins"),
        F.sum("wht_eligible").alias("eligible_count"),
        F.count("total_tax_collected").alias("have_payments"),
        F.count("total_outstanding_liability").alias("have_ledger"),
        F.count("total_returns_count").alias("have_returns"),
        F.count("activity_isic4_cd").alias("have_sector"),
        F.count("total_customs_value").alias("have_customs"),
        F.count("total_efris_sales").alias("have_efris"),
    ).first()
    counts = {name: _whole(row[name]) for name in ("total_rows", "unique_tins", "eligible_count", *COVERAGE_FIELDS)}
    counts["not_eligible_count"] = counts["total_rows"] - counts["eligible_count"]
    counts["duplicate_rows"] = counts["total_rows"] - counts["unique_tins"]
    return counts


def duplicate_report(src: Mapping[str, DataFrame]) -> dict[str, int]:
    """Count TINs that occur more than once. The TIN values are not returned."""
    counts = {}
    for name, frame, key in (("reg", src["reg"], "TIN_NO"), ("sector", src["sector"], "tin_no")):
        counts[name] = (
            frame.where(F.col(key).isNotNull())
            .groupBy(key)
            .count()
            .where(F.col("count") > 1)
            .count()
        )
    return counts


def _side(value: object) -> float | bool | None:
    if isinstance(value, bool) or value is None:
        return value
    return float(value)


def compare_with_reference(new: DataFrame, ref: DataFrame) -> list[dict]:
    """Compare two taxpayer tables by counts and amount sums. No TIN values are returned."""
    needed = ("TIN_NO", "wht_eligible", *SUM_COLUMNS)

    def stats(frame: DataFrame) -> dict:
        missing = [name for name in needed if name not in frame.columns]
        if missing:
            raise ValueError(f"Taxpayer table is missing columns: {', '.join(missing)}")
        row = frame.agg(
            F.count("*").alias("rows"),
            F.countDistinct("TIN_NO").alias("unique_tins"),
            F.sum("wht_eligible").alias("eligible"),
            *[F.sum(name).alias(name) for name in SUM_COLUMNS],
        ).first()
        values = row.asDict()
        values["columns"] = len(frame.columns)
        return values

    built, reference = stats(new), stats(ref)
    rows = []
    for key in ("rows", "unique_tins", "eligible", "columns", *SUM_COLUMNS):
        left, right = _side(built[key]), _side(reference[key])
        rows.append(
            {
                "check": key,
                "pipeline": left,
                "reference": right,
                "result": "OK" if left == right else "DIFF",
            }
        )
    same_names = sorted(new.columns) == sorted(ref.columns)
    rows.append(
        {
            "check": "same column names",
            "pipeline": same_names,
            "reference": True,
            "result": "OK" if same_names else "DIFF",
        }
    )
    return rows


def write_base(df: DataFrame, path: str, source_format: str, mode: str = "overwrite") -> None:
    """Save the taxpayer table."""
    fmt = source_format.strip().lower()
    if fmt == "parquet":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write.mode(mode).parquet(path)
        return
    if fmt == "delta":
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
        return
    raise ValueError(f"source_format must be parquet or delta, got {source_format!r}")
