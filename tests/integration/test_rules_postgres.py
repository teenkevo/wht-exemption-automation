"""The PostgreSQL rule registry, and the guards the database enforces on its own.

Runs against a throwaway database next to the rules database. Skipped when
PostgreSQL is not running (make rules-db-up). TEST_RULES_DB_URL points elsewhere.
"""

import os
from datetime import date

import psycopg
import pytest

from rules_engine.feature_computations import FEATURES
from rules_engine import RuleDefinition, RuleOperator, RulesEngine, RuleStatus
from rules_engine.catalog import seed
from rules_engine.db import PostgresRuleRegistry

ADMIN_URL = os.getenv("TEST_RULES_DB_URL", "postgresql://wht:wht@localhost:5432/postgres")
TEST_DB = "wht_rules_test"
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
    dsn = ADMIN_URL.rsplit("/", 1)[0] + f"/{TEST_DB}"
    registry = PostgresRuleRegistry(dsn, FEATURES)
    registry.init_schema()
    yield registry
    registry.close()


def _sales_rule(**overrides) -> RuleDefinition:
    values = {
        "rule_code": "MIN_SALES",
        "title": "EFRIS sales at least 10M",
        "description": "EFRIS sales of at least UGX 10,000,000",
        "feature_name": "efris_sales_total",
        "operator": RuleOperator.GTE,
        "expected_value": 10_000_000,
    }
    values.update(overrides)
    return RuleDefinition(**values)


def _sql(registry, statement, params=()):
    with psycopg.connect(registry.dsn, autocommit=True) as conn:
        return conn.execute(statement, params)


def test_an_empty_catalog_leaves_the_database_empty(registry):
    assert registry.is_empty()
    seed(registry)
    registry.init_schema()
    assert registry.is_empty()
    assert RulesEngine(registry).rules_for(DAY) == []


def test_register_rejects_an_unknown_feature(registry):
    with pytest.raises(ValueError, match="unknown feature"):
        registry.register(_sales_rule(feature_name="not_a_feature"), actor="alice")
    assert registry.is_empty()


def test_lifecycle_versions_and_audit_trail(registry):
    draft = registry.register(_sales_rule(), actor="alice", reason="new policy memo")
    assert (draft.version, draft.status) == (1, RuleStatus.DRAFT)
    with pytest.raises(ValueError, match="another person must approve"):
        registry.approve("MIN_SALES", 1, actor="alice")
    with pytest.raises(ValueError, match="only an APPROVED"):
        registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="bob")
    registry.approve("MIN_SALES", 1, actor="bob", reason="checked against memo")
    registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="bob")

    registry.register(_sales_rule(expected_value=20_000_000), actor="alice", reason="threshold raised")
    registry.approve("MIN_SALES", 2, actor="carol")
    with pytest.raises(ValueError, match="cannot start on"):
        registry.activate("MIN_SALES", 2, date(2026, 7, 1), actor="carol")
    registry.activate("MIN_SALES", 2, date(2027, 1, 1), actor="carol")

    first, second = registry.history("MIN_SALES")
    assert (first.status, first.effective_to) == (RuleStatus.SUPERSEDED, date(2027, 1, 1))
    assert (second.status, second.effective_from) == (RuleStatus.ACTIVE, date(2027, 1, 1))
    assert [rule.expected_value for rule in registry.get_active_rules(date(2026, 12, 31))] == [10_000_000]
    assert [rule.expected_value for rule in registry.get_active_rules(date(2027, 1, 1))] == [20_000_000]

    trail = [(event.action, event.version, event.actor, event.detail) for event in registry.audit_log("MIN_SALES")]
    assert trail == [
        ("REGISTERED", 1, "alice", "new policy memo"),
        ("APPROVED", 1, "bob", "checked against memo"),
        ("ACTIVATED", 1, "bob", "in force from 2026-07-01"),
        ("REGISTERED", 2, "alice", "threshold raised"),
        ("APPROVED", 2, "carol", ""),
        ("SUPERSEDED", 1, "carol", "in force from 2027-01-01"),
        ("ACTIVATED", 2, "carol", "in force from 2027-01-01"),
    ]
    snapshot = _sql(
        registry,
        "SELECT before->>'status' AS was, after->>'status' AS now FROM rule_audit WHERE action = 'SUPERSEDED'",
    ).fetchone()
    assert snapshot == ("ACTIVE", "SUPERSEDED")


def test_retired_codes_cannot_return(registry):
    registry.register(_sales_rule(), actor="alice")
    registry.approve("MIN_SALES", 1, actor="bob")
    registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="bob")
    registry.retire("MIN_SALES", date(2027, 1, 1), actor="bob", reason="dropped")
    assert registry.get_active_rules(date(2027, 1, 1)) == []
    assert [rule.rule_code for rule in registry.get_active_rules(date(2026, 12, 1))] == ["MIN_SALES"]
    with pytest.raises(ValueError, match="retired"):
        registry.register(_sales_rule(), actor="alice")
    with pytest.raises(psycopg.errors.RaiseException, match="retired"):
        _sql(
            registry,
            "INSERT INTO rule_versions (rule_code, version, title, description, feature_name, operator, created_by) "
            "VALUES ('MIN_SALES', 2, 't', 'd', 'efris_sales_total', 'gte', 'mallory')",
        )


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        ("UPDATE rule_versions SET expected_value = '1' WHERE version = 1", "cannot be edited"),
        ("DELETE FROM rule_versions", "kept forever"),
        ("UPDATE rule_versions SET status = 'DRAFT' WHERE version = 1", "cannot move from ACTIVE to DRAFT"),
        ("UPDATE rule_versions SET effective_from = '2026-08-01' WHERE version = 1", "start date cannot change"),
        ("UPDATE rule_audit SET actor = 'mallory'", "append-only"),
        ("DELETE FROM rule_audit", "append-only"),
    ],
)
def test_database_refuses_tampering(registry, statement, error):
    registry.register(_sales_rule(), actor="alice")
    registry.approve("MIN_SALES", 1, actor="bob")
    registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="bob")
    with pytest.raises(psycopg.errors.RaiseException, match=error):
        _sql(registry, statement)


def test_database_refuses_self_approval_and_overlaps(registry):
    registry.register(_sales_rule(), actor="alice")
    with pytest.raises(psycopg.errors.CheckViolation):
        _sql(registry, "UPDATE rule_versions SET status = 'APPROVED', approved_by = 'alice' WHERE version = 1")
    registry.approve("MIN_SALES", 1, actor="bob")
    registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="bob")
    registry.register(_sales_rule(expected_value=1), actor="alice")
    registry.approve("MIN_SALES", 2, actor="bob")
    with pytest.raises((psycopg.errors.ExclusionViolation, psycopg.errors.UniqueViolation)):
        _sql(registry, "UPDATE rule_versions SET status = 'ACTIVE', effective_from = '2026-09-01' WHERE version = 2")


def test_changes_outside_the_app_are_still_audited(registry):
    registry.register(_sales_rule(), actor="alice")
    registry.approve("MIN_SALES", 1, actor="bob")
    _sql(registry, "UPDATE rule_versions SET status = 'ACTIVE', effective_from = '2026-07-01' WHERE version = 1")
    last = registry.audit_log("MIN_SALES")[-1]
    assert (last.action, last.actor) == ("ACTIVATED", "wht")


def test_grant_runs_are_recorded(registry):
    run_id = registry.record_run(
        {
            "cycle": "wht_2026_27_h1",
            "assessment_date": "2026-11-07",
            "rules_applied": ["REGISTERED:v1"],
            "taxpayer_count": 10,
            "eligible": 2,
            "not_eligible": 7,
            "needs_review": 1,
        },
        actor="scheduler",
    )
    (run,) = registry.runs()
    assert (run["id"], run["actor"], run["eligible"], run["rules_applied"]) == (run_id, "scheduler", 2, ["REGISTERED:v1"])
