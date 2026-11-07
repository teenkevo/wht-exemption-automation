"""Validate rule definitions and evaluate rule conditions.

``evaluate_condition`` judges one taxpayer. ``outcome_column`` is the same
judgement as a Spark expression for the batch grant. Both return PASS, FAIL,
or REVIEW, and the tests hold them to the same answers.
"""

from __future__ import annotations

from decimal import Decimal
from numbers import Real
from typing import Any

from rules_engine.feature_computations import FeatureRegistry, FeatureStatus, FeatureType
from rules_engine.rule_models import (
    FLAG_OPERATORS,
    NUMERIC_OPERATORS,
    SET_OPERATORS,
    Outcome,
    RuleDefinition,
    RuleOperator,
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (Real, Decimal)) and not isinstance(value, bool)


def _value_matches(value: Any, dtype: FeatureType) -> bool:
    if dtype == FeatureType.NUMBER:
        return _is_number(value)
    if dtype == FeatureType.TEXT:
        return isinstance(value, str)
    return isinstance(value, bool)


def validate_shape(rule: RuleDefinition) -> None:
    """Checks that need no feature registry: the operator and its expected value agree."""
    expected = rule.expected_value
    if rule.operator in SET_OPERATORS:
        if not isinstance(expected, (list, tuple, set, frozenset)) or not expected:
            raise ValueError(f"{rule.rule_code}: {rule.operator.value} needs a non-empty list")
    elif rule.operator in NUMERIC_OPERATORS:
        if not _is_number(expected):
            raise ValueError(f"{rule.rule_code}: {rule.operator.value} needs a numeric threshold")
    elif rule.operator in FLAG_OPERATORS:
        if expected is not None:
            raise ValueError(f"{rule.rule_code}: {rule.operator.value} takes no expected value")
    elif expected is None:
        raise ValueError(f"{rule.rule_code}: {rule.operator.value} needs an expected value")
    if rule.effective_from and rule.effective_to and rule.effective_to <= rule.effective_from:
        raise ValueError(f"{rule.rule_code}: effective end must follow effective start")


def validate_rule(rule: RuleDefinition, features: FeatureRegistry) -> None:
    """Full check before activation: the feature exists, is active, and fits the operator."""
    validate_shape(rule)
    feature = features.get(rule.feature_name)
    if feature is None:
        raise ValueError(f"{rule.rule_code}: unknown feature {rule.feature_name}")
    if feature.status != FeatureStatus.ACTIVE:
        raise ValueError(f"{rule.rule_code}: feature {rule.feature_name} is {feature.status.value}")
    if rule.operator in NUMERIC_OPERATORS and feature.dtype != FeatureType.NUMBER:
        raise ValueError(f"{rule.rule_code}: {rule.operator.value} needs a numeric feature")
    if rule.operator in FLAG_OPERATORS and feature.dtype != FeatureType.BOOLEAN:
        raise ValueError(f"{rule.rule_code}: {rule.operator.value} needs a true/false feature")
    if rule.operator in SET_OPERATORS:
        values = list(rule.expected_value)
    elif rule.operator in {RuleOperator.EQ, RuleOperator.NE}:
        values = [rule.expected_value]
    else:
        values = []
    wrong = [value for value in values if not _value_matches(value, feature.dtype)]
    if wrong:
        raise ValueError(
            f"{rule.rule_code}: expected value {wrong[0]!r} does not match "
            f"feature {rule.feature_name} ({feature.dtype.value})"
        )


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _same_kind(left: Any, right: Any) -> bool:
    if _is_number(left) and _is_number(right):
        return True
    return type(left) is type(right)


def evaluate_condition(actual: Any, operator: RuleOperator, expected: Any = None) -> bool:
    """True when the requirement holds. Raises TypeError for a value of the wrong kind."""
    actual = _normalize(actual)
    operator = RuleOperator(operator)
    if operator == RuleOperator.IS_TRUE:
        if not isinstance(actual, bool):
            raise TypeError("A true/false value is required")
        return actual is True
    if operator == RuleOperator.IS_FALSE:
        if not isinstance(actual, bool):
            raise TypeError("A true/false value is required")
        return actual is False
    if operator in {RuleOperator.EQ, RuleOperator.NE}:
        if not _same_kind(actual, expected):
            raise TypeError(f"Cannot compare {type(actual).__name__} with {type(expected).__name__}")
        equal = actual == expected
        return equal if operator == RuleOperator.EQ else not equal
    if operator in SET_OPERATORS:
        members = list(expected)
        if not all(_same_kind(actual, member) for member in members):
            raise TypeError(f"Cannot look up {type(actual).__name__} in the expected list")
        inside = actual in members
        return inside if operator == RuleOperator.IN else not inside
    if operator in NUMERIC_OPERATORS:
        if not _is_number(actual) or not _is_number(expected):
            raise TypeError("Numeric values required")
        if actual != actual:  # NaN
            raise TypeError("Value is not a number")
        if operator == RuleOperator.GT:
            return actual > expected
        if operator == RuleOperator.GTE:
            return actual >= expected
        if operator == RuleOperator.LT:
            return actual < expected
        return actual <= expected
    raise ValueError(f"Unsupported operator: {operator}")


def evaluate_outcome(actual: Any, rule: RuleDefinition) -> tuple[Outcome, str]:
    if actual is None:
        return Outcome.REVIEW, "Feature value unavailable"
    try:
        passed = evaluate_condition(actual, rule.operator, rule.expected_value)
    except (TypeError, ValueError) as exc:
        return Outcome.REVIEW, f"Feature value unusable: {exc}"
    if passed:
        return Outcome.PASS, "Requirement satisfied"
    return Outcome.FAIL, "Requirement not satisfied"


def outcome_column(rule: RuleDefinition, feature):
    """The Spark form of ``evaluate_outcome`` for a column already computed by the feature registry."""
    from pyspark.sql import functions as F

    expected = rule.expected_value
    op = rule.operator
    if op == RuleOperator.IS_TRUE:
        passed = feature == F.lit(True)
    elif op == RuleOperator.IS_FALSE:
        passed = feature == F.lit(False)
    elif op == RuleOperator.EQ:
        passed = feature == F.lit(expected)
    elif op == RuleOperator.NE:
        passed = feature != F.lit(expected)
    elif op == RuleOperator.IN:
        passed = feature.isin(list(expected))
    elif op == RuleOperator.NOT_IN:
        passed = ~feature.isin(list(expected))
    elif op == RuleOperator.GT:
        passed = feature > F.lit(float(expected))
    elif op == RuleOperator.GTE:
        passed = feature >= F.lit(float(expected))
    elif op == RuleOperator.LT:
        passed = feature < F.lit(float(expected))
    else:
        passed = feature <= F.lit(float(expected))
    unusable = feature.isNull()
    if op in NUMERIC_OPERATORS:
        unusable = unusable | F.isnan(feature)
    return (
        F.when(unusable, F.lit(Outcome.REVIEW.value))
        .when(passed, F.lit(Outcome.PASS.value))
        .otherwise(F.lit(Outcome.FAIL.value))
    )
