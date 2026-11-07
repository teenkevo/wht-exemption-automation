"""The baseline WHT checklist: the rules the rules database starts with.

The rules database (PostgreSQL) is the system of record. ``make rules-db-init``
creates the schema and, when the database is empty, loads this baseline:
each release is registered by ``catalog``, approved by ``catalog-baseline``,
and put in force from its date. After that, rules change through the rules
CLI (``python -m rules_engine.cli``), which records who made each change and why.

Tests and offline runs (``rules_store.type: memory``) replay this list into an
in-memory registry instead.

A requirement that needs data no feature provides starts with a feature in
``rules_engine/feature_computations/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache

from rules_engine.feature_computations import FEATURES
from rules_engine.registry import RuleRegistry
from rules_engine.rule_models import RuleDefinition, RuleOperator

CATALOG_START = date(2026, 7, 1)


@dataclass(frozen=True)
class Release:
    rule: RuleDefinition
    effective_from: date
    reason: str = ""


@dataclass(frozen=True)
class Retirement:
    rule_code: str
    effective_to: date
    reason: str = ""


def release(rule: RuleDefinition, effective_from: date, reason: str = "") -> Release:
    return Release(rule, effective_from, reason)


def retire(rule_code: str, effective_to: date, reason: str = "") -> Retirement:
    return Retirement(rule_code, effective_to, reason)


CHANGES: tuple[Release | Retirement, ...] = (
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
            expected_value=100_000_000.0,
        ),
        effective_from=CATALOG_START,
    ),
)


SEED_AUTHOR = "catalog"
SEED_APPROVER = "catalog-baseline"


def seed(registry, changes: tuple[Release | Retirement, ...] = CHANGES) -> None:
    """Replay the baseline into any registry, memory or PostgreSQL."""
    for change in changes:
        if isinstance(change, Retirement):
            registry.retire(change.rule_code, change.effective_to, actor=SEED_APPROVER, reason=change.reason)
            continue
        draft = registry.register(change.rule, actor=SEED_AUTHOR, reason=change.reason or "baseline catalog")
        registry.approve(draft.rule_code, draft.version, actor=SEED_APPROVER, reason="baseline catalog")
        registry.activate(
            draft.rule_code,
            draft.version,
            change.effective_from,
            actor=SEED_APPROVER,
            reason=change.reason or f"baseline, in force from {change.effective_from}",
        )


def build_registry(changes: tuple[Release | Retirement, ...] = CHANGES, features=FEATURES) -> RuleRegistry:
    """An in-memory registry holding the baseline."""
    registry = RuleRegistry(features)
    seed(registry, changes)
    return registry


def rules_store_settings() -> tuple[str, str]:
    """``rules_store`` from config.yaml. ``RULES_STORE`` and ``RULES_DB_URL`` override it."""
    import os

    from data_engineering import load_yaml

    settings = load_yaml("config.yaml").get("rules_store") or {}
    kind = (os.getenv("RULES_STORE") or str(settings.get("type", "postgres"))).strip().lower()
    if kind not in {"postgres", "memory"}:
        raise ValueError("rules_store.type must be postgres or memory")
    dsn = os.getenv("RULES_DB_URL") or str(settings.get("dsn", ""))
    return kind, dsn


def open_registry():
    """The configured registry. PostgreSQL must already hold the schema."""
    kind, dsn = rules_store_settings()
    if kind == "memory":
        return build_registry()
    from rules_engine.db import PostgresRuleRegistry

    registry = PostgresRuleRegistry(dsn, FEATURES)
    registry.check()
    return registry


@lru_cache(maxsize=1)
def default_registry():
    return open_registry()


def default_engine():
    from rules_engine.engine import RulesEngine

    return RulesEngine(default_registry())
