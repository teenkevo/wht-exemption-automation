"""The mock lake matches the checklist, and remote mode can read it as Delta."""

from dataclasses import replace
from datetime import date

import pytest

from data_engineering.config import ProjectConfig, SOURCE_NAMES
from data_engineering.ingest import run as ingest
from data_engineering.load import build_base
from data_engineering.mock_lake import (
    DUPLICATE,
    ELIGIBLE,
    FAILING,
    GHOST,
    H1_ONLY,
    NEW,
    NO_FACTS,
    REVIEW,
    ROWS,
    source_frames,
    write_mock_lake,
)
from rules_engine import RulesEngine
from rules_engine.catalog import build_registry, release, retire
from rules_engine.cycles import DEFAULT_RULES
from rules_engine.feature_computations import FEATURES
from rules_engine.migrations import load_migrations, migrations_dir

H1 = date(2026, 11, 7)
H2 = date(2027, 2, 1)


def _shipped_engine() -> RulesEngine:
    changes = []
    for migration in load_migrations(migrations_dir()):
        for change in migration.changes:
            if change.retire:
                changes.append(retire(change.rule_code, change.effective_from, change.reason))
            else:
                changes.append(release(change.rule, change.effective_from, change.reason))
    return RulesEngine(build_registry(tuple(changes)))


def _by_tin(frame) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for row in frame.collect():
        grouped.setdefault(row["TIN_NO"], []).append(row)
    return grouped


def _judge(spark, sources, engine, day):
    FEATURES.register_sources(spark, sources)
    return _by_tin(engine.evaluate_frame(build_base(sources), day, DEFAULT_RULES))


def test_mock_rows_follow_the_shipped_checklist(spark):
    engine = _shipped_engine()
    sources = source_frames(spark)
    try:
        h1 = _judge(spark, sources, engine, H1)
        h2 = _judge(spark, sources, engine, H2)
    finally:
        for name in SOURCE_NAMES:
            spark.catalog.dropTempView(f"lakehouse_{name}")

    assert set(h1) == {ELIGIBLE, FAILING, REVIEW, NEW, DUPLICATE, NO_FACTS, H1_ONLY}
    assert GHOST not in h1
    assert len(h1[DUPLICATE]) == 2
    assert {row["decision"] for row in h1[ELIGIBLE]} == {"ELIGIBLE"}
    assert {row["decision"] for row in h1[DUPLICATE]} == {"ELIGIBLE"}
    assert h1[ELIGIBLE][0]["activity_isic4_cd"] == "4610"
    assert h1[ELIGIBLE][0]["total_tax_collected"] == 160_000_000.0
    assert h1[ELIGIBLE][0]["total_outstanding_liability"] == 0.0
    assert h1[ELIGIBLE][0]["total_returns_count"] == 2
    assert h1[ELIGIBLE][0]["red_lane_count"] == 0
    assert h1[FAILING][0]["decision"] == "NOT ELIGIBLE"
    assert h1[FAILING][0]["failed_rules"] == (
        "REGISTERED,NO_OUTSTANDING_LIABILITY,RETURNS_FILED,MIN_TAX_COLLECTED"
    )
    assert h1[REVIEW][0]["decision"] == "NEEDS REVIEW"
    assert h1[REVIEW][0]["review_rules"] == "REGISTERED"
    assert h1[NEW][0]["decision"] == "NOT ELIGIBLE"
    assert h1[NEW][0]["failed_rules"] == "EXISTING_TAXPAYER"
    assert h1[NO_FACTS][0]["decision"] == "NOT ELIGIBLE"
    assert h1[NO_FACTS][0]["tax_collected"] == 0.0
    assert h1[NO_FACTS][0]["returns_filed_count"] == 0.0
    assert h1[NO_FACTS][0]["outstanding_liability"] == 0.0
    assert h1[H1_ONLY][0]["decision"] == "ELIGIBLE"
    assert h1[H1_ONLY][0]["total_tax_collected"] == 110_000_000.0
    assert h2[H1_ONLY][0]["decision"] == "NOT ELIGIBLE"
    assert h2[H1_ONLY][0]["failed_rules"] == "MIN_TAX_COLLECTED"
    assert h2[ELIGIBLE][0]["decision"] == "ELIGIBLE"


def test_remote_mode_reads_the_mock_delta_lake(spark, tmp_path, monkeypatch):
    pytest.importorskip("delta")
    if "DeltaSparkSessionExtension" not in spark.conf.get("spark.sql.extensions", ""):
        pytest.skip("Spark session was started without Delta")

    lake = tmp_path / "lake"
    written = write_mock_lake(spark, lake)
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("WHT_LAKE_ROOT", str(lake))
    config = replace(
        ProjectConfig.load(),
        taxpayers=tmp_path / "wht_base_v1",
        load_summary=tmp_path / "load_summary.json",
    )
    assert config.mode == "remote"
    assert config.lake_format == "delta"
    assert config.source_paths == written
    for path in written.values():
        assert (path / "_delta_log").is_dir()

    loaded = ingest(config, spark=spark)
    assert loaded["total_rows"] == len(ROWS["reg"])
    assert loaded["unique_tins"] == len({row[0] for row in ROWS["reg"]})
    assert loaded["duplicate_source_tins"] == {"reg": 1, "sector": 1}

    table = spark.read.parquet(str(config.taxpayers))
    eligible = table.filter(table.TIN_NO == ELIGIBLE).collect()
    assert len(eligible) == 1
    assert eligible[0]["activity_isic4_cd"] == "4610"
    assert eligible[0]["total_tax_collected"] == 160_000_000.0
    assert table.filter(table.TIN_NO == GHOST).count() == 0
    assert table.filter(table.TIN_NO == DUPLICATE).count() == 2
