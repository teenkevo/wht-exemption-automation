"""How a rules database is opened, and an empty checklist to start from.

The rules database (PostgreSQL) is the system of record. ``make rules-db-init``
creates the schema and leaves it empty. Rules are registered with the rules CLI
(``python -m rules_engine.cli``), which records who made each change and why.

``CHANGES`` stays empty. ``release``, ``retire``, and ``seed`` are how a test
or a one-off script installs a checklist of its own.

A requirement that needs data no feature provides starts with a feature in
``rules_engine/feature_computations/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache

from rules_engine.feature_computations import FEATURES
from rules_engine.registry import RuleRegistry
from rules_engine.rule_models import RuleDefinition

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


CHANGES: tuple[Release | Retirement, ...] = ()


SEED_AUTHOR = "catalog"


def seed(registry, changes: tuple[Release | Retirement, ...] = CHANGES) -> None:
    """Replay a list of releases into any registry, memory or PostgreSQL."""
    for change in changes:
        if isinstance(change, Retirement):
            registry.retire(change.rule_code, change.effective_to, actor=SEED_AUTHOR, reason=change.reason)
            continue
        draft = registry.register(change.rule, actor=SEED_AUTHOR, reason=change.reason or "seeded checklist")
        registry.activate(
            draft.rule_code,
            draft.version,
            change.effective_from,
            actor=SEED_AUTHOR,
            reason=change.reason or f"in force from {change.effective_from}",
        )


def build_registry(changes: tuple[Release | Retirement, ...] = CHANGES, features=FEATURES) -> RuleRegistry:
    """An in-memory registry. With no changes, it holds no rules."""
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
