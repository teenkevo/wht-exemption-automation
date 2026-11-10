"""Deterministic WHT checklist: rule definitions, their registry, and the engine that applies them.

- ``feature_computations``: the features a rule can name
- ``feature_migrations``: the SELECT for each feature, latest migration active
- ``rule_models``: what a rule, a result, and an audit event look like
- ``validators``: rule checks before activation, and PASS/FAIL/REVIEW evaluation
- ``registry``: register, version, activate, supersede, and retire rules
- ``db``: the PostgreSQL registry, its schema, and connection errors
- ``engine``: apply the rules in force on a date to one taxpayer or a Spark table
- ``catalog``: the WHT rules, their thresholds, and their history
- ``cycles``: which rules each exemption cycle switches on
"""

from rules_engine.catalog import build_registry, default_engine, default_registry
from rules_engine.engine import RulesEngine
from rules_engine.registry import RuleRegistry
from rules_engine.rule_models import (
    ChecklistResult,
    ChecklistStatus,
    Outcome,
    RuleDefinition,
    RuleOperator,
    RuleResult,
    RuleStatus,
)

__all__ = [
    "ChecklistResult",
    "ChecklistStatus",
    "Outcome",
    "RuleDefinition",
    "RuleOperator",
    "RuleRegistry",
    "RuleResult",
    "RuleStatus",
    "RulesEngine",
    "build_registry",
    "default_engine",
    "default_registry",
]
