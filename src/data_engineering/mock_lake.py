"""Disposable Delta lake for the seven remote sources.

The tables use the columns the load and the feature queries read, and they
are written under the same relative paths as ``remote_sources`` in
``config.yaml``. ``make mock-lake`` writes ``mock_lake/``. ``WHT_LAKE_ROOT``
points remote mode at that directory. ``make mock-lake-rm`` deletes it.

Registration TINs:

- ``1000000001`` passes the checklist at the 100M and 150M tax thresholds
- ``1000000002`` fails registration, liability, returns, and tax
- ``1000000003`` has no registration status, so the checklist goes to review
- ``1000000004`` is a new taxpayer
- ``1000000005`` has two registration rows
- ``1000000006`` has registration and no fact rows
- ``1000000007`` passes the 100M tax check and fails the 150M check

``9999999999`` is only in a fact table. Registration is the spine, so the
join leaves it out.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from data_engineering import load_yaml
from data_engineering.config import SOURCE_NAMES, lake_relative
from data_engineering.load import REG_COLS

ELIGIBLE = "1000000001"
FAILING = "1000000002"
REVIEW = "1000000003"
NEW = "1000000004"
DUPLICATE = "1000000005"
NO_FACTS = "1000000006"
H1_ONLY = "1000000007"
GHOST = "9999999999"

# Passes both shipped thresholds. 110M passes the July 2026 check only.
_TAX_BOTH = 160_000_000.0
_TAX_H1 = 110_000_000.0
_TAX_LOW = 40_000_000.0

_REG_TYPES = {"BUSINESS_START_DT": DateType()}
_REG_OPTIONAL = {"REG_STATUS", "taxpayer_status", "BUSINESS_START_DT"}

SCHEMAS: dict[str, StructType] = {
    "reg": StructType(
        [StructField(name, _REG_TYPES.get(name, StringType()), name in _REG_OPTIONAL) for name in REG_COLS]
    ),
    "ledger": StructType(
        [
            StructField("TPIN", StringType(), False),
            StructField("OUTSTANDING_LIABILITY", DoubleType(), True),
            StructField("TOTAL_WAIVER", DoubleType(), True),
        ]
    ),
    "payments": StructType(
        [
            StructField("TIN_NO", StringType(), False),
            StructField("TOT_PAYMENT_AMT", DoubleType(), True),
        ]
    ),
    "efris": StructType(
        [
            StructField("sellertin", StringType(), False),
            StructField("sales_amount", DoubleType(), True),
            StructField("vat_charged", DoubleType(), True),
        ]
    ),
    "sector": StructType(
        [
            StructField("tin_no", StringType(), True),
            StructField("activity_isic4_cd", StringType(), True),
            StructField("updated_dt", DateType(), True),
        ]
    ),
    "customs": StructType(
        [
            StructField("taxpayer_tin", StringType(), True),
            StructField("cif_amt_ugx", DoubleType(), True),
            StructField("is_red_flg", IntegerType(), True),
        ]
    ),
    "returns": StructType(
        [
            StructField("TAXPAYER_TIN", StringType(), False),
            StructField("net_tax", DoubleType(), True),
        ]
    ),
}


def _reg(
    tin: str,
    status: str | None,
    life: str | None,
    started: date = date(2015, 3, 1),
) -> tuple:
    values = {name: "N" for name in REG_COLS}
    values.update(
        {
            "TIN_NO": tin,
            "TAX_PAYER_TYPE": "COMPANY",
            "REG_STATUS": status,
            "ORG_SUB_TYPE": "PRIVATE",
            "taxpayer_status": life,
            "BUSINESS_START_DT": started,
            "STATION_NAME": "KAMPALA",
            "REGION_NAME": "CENTRAL",
            "BUSINESS_FISCAL_YEAR": "JUL-JUN",
            "ORG_TYPE": "COMPANY",
        }
    )
    return tuple(values[name] for name in REG_COLS)


ROWS: dict[str, list[tuple]] = {
    "reg": [
        _reg(ELIGIBLE, "REGD", "Existing Taxpayer"),
        _reg(FAILING, "CNCL", "Existing Taxpayer", date(2020, 1, 1)),
        _reg(REVIEW, None, "Existing Taxpayer"),
        _reg(NEW, "REGD", "New Taxpayer"),
        _reg(DUPLICATE, "REGD", "Existing Taxpayer"),
        _reg(DUPLICATE, "REGD", "Existing Taxpayer"),
        _reg(NO_FACTS, "REGD", "Existing Taxpayer"),
        _reg(H1_ONLY, "REGD", "Existing Taxpayer"),
    ],
    "ledger": [
        (ELIGIBLE, 100.0, 5.0),
        (ELIGIBLE, -100.0, 5.0),
        (FAILING, 250.0, 0.0),
        (REVIEW, 0.0, 0.0),
        (NEW, 0.0, 0.0),
        (DUPLICATE, 0.0, 0.0),
        (H1_ONLY, 0.0, 0.0),
    ],
    "payments": [
        (ELIGIBLE, 100_000_000.0),
        (ELIGIBLE, 60_000_000.0),
        (FAILING, _TAX_LOW),
        (REVIEW, _TAX_BOTH),
        (NEW, _TAX_BOTH),
        (DUPLICATE, _TAX_BOTH),
        (H1_ONLY, 60_000_000.0),
        (H1_ONLY, 50_000_000.0),
        (GHOST, 1.0),
    ],
    "efris": [
        (ELIGIBLE, 1_000.0, 180.0),
        (ELIGIBLE, 250.0, 45.0),
        (H1_ONLY, 10.0, 1.8),
    ],
    "sector": [
        (ELIGIBLE, "0000", date(2020, 1, 1)),
        (ELIGIBLE, "4610", date(2024, 6, 1)),
        (ELIGIBLE, None, date(2025, 12, 1)),
        (FAILING, "6201", date(2023, 1, 1)),
        (REVIEW, "4690", date(2022, 1, 1)),
        (NEW, "4711", date(2024, 1, 1)),
        (DUPLICATE, "5610", date(2021, 5, 1)),
        (H1_ONLY, "1010", date(2019, 8, 1)),
    ],
    "customs": [
        (ELIGIBLE, 5_000.0, 0),
        (FAILING, 800.0, 1),
        (FAILING, 200.0, 1),
        (GHOST, 5.0, 0),
    ],
    "returns": [
        (ELIGIBLE, 1_000.0),
        (ELIGIBLE, 500.0),
        (REVIEW, 20.0),
        (NEW, 20.0),
        (DUPLICATE, 20.0),
        (H1_ONLY, 20.0),
    ],
}


def remote_source_paths() -> dict[str, str]:
    """The seven paths from ``config.yaml``, in source order."""
    raw = load_yaml("config.yaml").get("remote_sources") or {}
    if not isinstance(raw, dict):
        raise ValueError("remote_sources must be a mapping of source name to path")
    missing = [name for name in SOURCE_NAMES if not raw.get(name)]
    if missing:
        raise ValueError("remote_sources is missing " + ", ".join(missing))
    return {name: str(raw[name]) for name in SOURCE_NAMES}


def source_frames(spark: SparkSession) -> dict[str, DataFrame]:
    """The seven sources as Spark frames, with the schemas the load requires."""
    return {name: spark.createDataFrame(ROWS[name], schema=SCHEMAS[name]) for name in SOURCE_NAMES}


def write_mock_lake(spark: SparkSession, root: str | Path) -> dict[str, Path]:
    """Write the seven sources as Delta tables under ``root``.

    Each table keeps the relative path from ``remote_sources``. A root of
    ``mock_lake`` stores registration at
    ``mock_lake/dm_modeled/dim_taxpayer_mst``.
    """
    base = Path(root)
    frames = source_frames(spark)
    written: dict[str, Path] = {}
    for name, configured in remote_source_paths().items():
        path = base / lake_relative(configured)
        path.parent.mkdir(parents=True, exist_ok=True)
        frames[name].write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
        written[name] = path
    return written


def main() -> None:
    from data_engineering import REPO_ROOT
    from data_engineering.spark import job_session
    from termstyle import fields, heading, note

    root = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "mock_lake"
    session = job_session("wht-mock-lake", delta=True)
    try:
        written = write_mock_lake(session, root)
    finally:
        session.stop()
    heading("Mock lake")
    fields([(name, str(path), "") for name, path in written.items()])
    print()
    note(f"WHT_MODE=remote WHT_LAKE_ROOT={root} make ingest")
    note("make mock-lake-rm deletes this directory")


if __name__ == "__main__":
    main()
