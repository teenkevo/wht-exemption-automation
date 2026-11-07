"""Migration files apply in order against PostgreSQL, or roll back as a whole file."""

import os
from datetime import date
from pathlib import Path

import psycopg
import pytest

from rules_engine import RuleDefinition, RuleOperator, RuleStatus
from rules_engine.db import PostgresRuleRegistry
from rules_engine.feature_computations import FEATURES
from rules_engine.migrations import load_migrations, migrations_dir, prepare

ADMIN_URL = os.getenv("TEST_RULES_DB_URL", "postgresql://wht:wht@localhost:5432/postgres")
TEST_DB = "wht_rules_test"
SHIPPED = migrations_dir()
DAY = date(2026, 11, 7)


@pytest.fixture
def registry():
    try:
        admin = psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not running. Start it with: make rules-db-up")
    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {TEST_DB}")
    stored = PostgresRuleRegistry(ADMIN_URL.rsplit("/", 1)[0] + f"/{TEST_DB}", FEATURES)
    stored.init_schema()
    yield stored
    stored.close()


def _rule_file(code: str = "MIN_SALES", feature: str = "efris_sales_total", start: str = "2026-07-01") -> str:
    return f"""
description: {code}
changes:
  - rule_code: {code}
    title: {code}
    description: {code}
    feature_name: {feature}
    operator: gte
    expected_value: 10000000
    effective_from: {start}
    reason: Added {code}
"""


def _apply(registry, directory: Path, baseline: int | None = None):
    files = load_migrations(directory, registry.features)
    plan = prepare(files, registry.applied_migrations(), has_rules=not registry.is_empty(), baseline=baseline)
    for migration in plan.baseline:
        registry.record_baseline(migration, actor="deployer")
    applied = []
    for migration in plan.pending:
        applied.append(registry.apply_migration(migration, actor="deployer"))
    return applied


def test_the_shipped_baseline_loads_the_five_rules(registry):
    _apply(registry, SHIPPED)
    active = registry.get_active_rules(DAY)
    assert {rule.rule_code for rule in active} == {
        "REGISTERED",
        "EXISTING_TAXPAYER",
        "NO_OUTSTANDING_LIABILITY",
        "RETURNS_FILED",
        "MIN_TAX_COLLECTED",
    }
    assert {rule.effective_from for rule in active} == {date(2026, 7, 1)}
    assert registry.current("REGISTERED").created_by == "deployer"
    assert registry.current("REGISTERED").approved_by is None
    assert [row["version"] for row in registry.applied_migrations()] == [1]
    again = prepare(load_migrations(SHIPPED), registry.applied_migrations(), has_rules=True, baseline=None)
    assert again.pending == ()


def test_a_failed_file_does_not_keep_its_draft(registry, tmp_path):
    (tmp_path / "V001__sales.yaml").write_text(_rule_file())
    (tmp_path / "V002__too_early.yaml").write_text(_rule_file(start="2026-07-01"))
    with pytest.raises(ValueError, match="cannot start on"):
        _apply(registry, tmp_path)
    assert [rule.version for rule in registry.history("MIN_SALES")] == [1]
    assert [row["version"] for row in registry.applied_migrations()] == [1]


def test_a_later_file_retires_a_rule(registry, tmp_path):
    (tmp_path / "V001__sales.yaml").write_text(_rule_file())
    (tmp_path / "V002__retire.yaml").write_text(
        """
description: End the sales rule
changes:
  - rule_code: MIN_SALES
    retire: true
    effective_from: 2027-01-01
    reason: Dropped
"""
    )
    _apply(registry, tmp_path)
    retired = registry.history("MIN_SALES")[0]
    assert retired.status == RuleStatus.RETIRED
    assert retired.effective_to == date(2027, 1, 1)
    assert registry.current("MIN_SALES") is None
    assert registry.get_active_rules(date(2026, 12, 1))[0].rule_code == "MIN_SALES"


def test_baseline_does_not_replay_the_file(registry, tmp_path):
    (tmp_path / "V001__collected.yaml").write_text(_rule_file("MIN_TAX_COLLECTED", "tax_collected"))
    draft = registry.register(
        RuleDefinition(
            rule_code="MIN_SALES",
            title="Sales",
            description="Sales",
            feature_name="efris_sales_total",
            operator=RuleOperator.GTE,
            expected_value=1,
        ),
        actor="alice",
    )
    registry.approve("MIN_SALES", draft.version, actor="bob")
    registry.activate("MIN_SALES", draft.version, date(2026, 7, 1), actor="bob")
    _apply(registry, tmp_path, baseline=1)
    assert registry.codes() == ["MIN_SALES"]
    assert [row["version"] for row in registry.applied_migrations()] == [1]
    with pytest.raises(ValueError, match="edited after it was applied"):
        (tmp_path / "V001__collected.yaml").write_text(_rule_file("MIN_TAX_COLLECTED", "tax_collected") + "\n")
        prepare(load_migrations(tmp_path), registry.applied_migrations(), has_rules=True, baseline=None)


def test_existing_rules_without_a_history_are_refused(registry, tmp_path):
    (tmp_path / "V001__sales.yaml").write_text(_rule_file())
    registry.register(
        RuleDefinition(
            rule_code="MIN_SALES",
            title="Sales",
            description="Sales",
            feature_name="efris_sales_total",
            operator=RuleOperator.GTE,
            expected_value=1,
        ),
        actor="alice",
    )
    with pytest.raises(ValueError, match="--baseline"):
        _apply(registry, tmp_path)
    assert registry.applied_migrations() == []
