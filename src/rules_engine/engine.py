"""Apply the rules in force on a date to taxpayer features and produce the checklist.

``evaluate`` judges one taxpayer from a dict of feature values. ``evaluate_frame``
judges a whole Spark table: it computes the features each rule needs, adds one
outcome column per rule, and the overall status and decision. Both use the
same rule versions and the same PASS, FAIL, and REVIEW logic.

A mandatory FAIL makes the checklist FAIL. Otherwise a mandatory REVIEW makes
it REVIEW. Optional rules are reported and do not change the status.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date

from rules_engine.registry import RuleRegistry
from rules_engine.rule_models import (
    DECISIONS,
    ChecklistResult,
    ChecklistStatus,
    Outcome,
    RuleDefinition,
    RuleResult,
)
from rules_engine.validators import evaluate_outcome, outcome_column


def outcome_column_name(rule_code: str) -> str:
    return f"rule_{rule_code.lower()}"


def codes_in_force(registry: RuleRegistry, day: date, rule_codes: Sequence[str]) -> tuple[str, ...]:
    """The cycle codes that have a version in force. Codes not registered yet are left out."""
    available = {rule.rule_code for rule in registry.get_active_rules(day)}
    return tuple(code for code in rule_codes if code in available)


def rules_applied_text(rules: Sequence[RuleDefinition]) -> str:
    return ",".join(rule.key for rule in rules)


def parse_rules_applied(text: str) -> list[tuple[str, int]]:
    pairs = []
    for part in (text or "").split(","):
        if not part:
            continue
        code, _, version = part.partition(":v")
        pairs.append((code, int(version)))
    return pairs


def checklist_status(rules: Sequence[RuleDefinition], outcomes: Sequence[Outcome]) -> ChecklistStatus:
    if not rules:
        return ChecklistStatus.NOT_CONFIGURED
    mandatory = [outcome for rule, outcome in zip(rules, outcomes) if rule.mandatory]
    if Outcome.FAIL in mandatory:
        return ChecklistStatus.FAIL
    if Outcome.REVIEW in mandatory:
        return ChecklistStatus.REVIEW
    return ChecklistStatus.PASS


class RulesEngine:
    def __init__(self, registry: RuleRegistry) -> None:
        self.registry = registry

    def rules_for(self, assessment_date: date, rule_codes: Sequence[str] | None = None) -> list[RuleDefinition]:
        """Rule versions in force on the date. ``rule_codes`` limits and orders them, as a cycle does."""
        in_force = {rule.rule_code: rule for rule in self.registry.get_active_rules(assessment_date)}
        if rule_codes is None:
            return list(in_force.values())
        missing = [code for code in rule_codes if code not in in_force]
        if missing:
            raise ValueError(f"No version in force on {assessment_date} for: {', '.join(missing)}")
        return [in_force[code] for code in rule_codes]

    def evaluate(
        self,
        taxpayer_id: str,
        features: Mapping,
        assessment_date: date,
        rule_codes: Sequence[str] | None = None,
    ) -> ChecklistResult:
        rules = self.rules_for(assessment_date, rule_codes)
        results = []
        for rule in rules:
            actual = features.get(rule.feature_name)
            outcome, reason = evaluate_outcome(actual, rule)
            results.append(self._result(rule, actual, outcome, reason))
        status = checklist_status(rules, [item.outcome for item in results])
        return ChecklistResult(
            taxpayer_id=taxpayer_id,
            assessment_date=assessment_date,
            checklist_status=status,
            decision=DECISIONS[status],
            results=results,
        )

    def explain(self, rule: RuleDefinition, actual, outcome: Outcome) -> RuleResult:
        """Rebuild a stored outcome with its evidence, for a row the batch grant already wrote."""
        reason = {
            Outcome.PASS: "Requirement satisfied",
            Outcome.FAIL: "Requirement not satisfied",
            Outcome.REVIEW: "Feature value unavailable",
        }[outcome]
        return self._result(rule, actual, outcome, reason)

    def _result(self, rule: RuleDefinition, actual, outcome: Outcome, reason: str) -> RuleResult:
        feature = self.registry.features.get(rule.feature_name)
        return RuleResult(
            rule_code=rule.rule_code,
            version=rule.version,
            title=rule.title,
            mandatory=rule.mandatory,
            feature_name=rule.feature_name,
            source_table=None if feature is None else feature.source_table,
            operator=rule.operator,
            actual_value=actual,
            expected_value=rule.expected_value,
            outcome=outcome,
            reason=reason,
            legal_reference=rule.legal_reference,
        )

    def evaluate_frame(self, df, assessment_date: date, rule_codes: Sequence[str] | None = None):
        """Add features, one outcome column per rule, and the checklist columns to a Spark table.

        Added columns: each rule's feature, ``rule_<code>`` outcomes, ``failed_rules``,
        ``review_rules``, ``rules_failed_count``, ``checklist_status``, ``decision``,
        ``assessment_date``, and ``rules_applied`` (``CODE:vN`` for every rule used).
        """
        from pyspark.sql import functions as F

        rules = self.rules_for(assessment_date, rule_codes)
        if not rules:
            return df.withColumns(
                {
                    "failed_rules": F.lit(""),
                    "review_rules": F.lit(""),
                    "rules_failed_count": F.lit(0),
                    "checklist_status": F.lit(ChecklistStatus.NOT_CONFIGURED.value),
                    "assessment_date": F.lit(assessment_date.isoformat()),
                    "rules_applied": F.lit(""),
                    "decision": F.lit(DECISIONS[ChecklistStatus.NOT_CONFIGURED]),
                }
            )
        featured = self.registry.features.compute(df, [rule.feature_name for rule in rules])
        outcomes = {
            outcome_column_name(rule.rule_code): outcome_column(rule, F.col(rule.feature_name))
            for rule in rules
        }
        with_outcomes = featured.withColumns(outcomes)

        def is_(rule: RuleDefinition, outcome: Outcome):
            return F.col(outcome_column_name(rule.rule_code)) == F.lit(outcome.value)

        def listed(outcome: Outcome):
            return F.concat_ws(",", *[F.when(is_(rule, outcome), F.lit(rule.rule_code)) for rule in rules])

        mandatory = [rule for rule in rules if rule.mandatory]
        any_fail = F.lit(False)
        any_review = F.lit(False)
        for rule in mandatory:
            any_fail = any_fail | is_(rule, Outcome.FAIL)
            any_review = any_review | is_(rule, Outcome.REVIEW)
        status = (
            F.when(any_fail, F.lit(ChecklistStatus.FAIL.value))
            .when(any_review, F.lit(ChecklistStatus.REVIEW.value))
            .otherwise(F.lit(ChecklistStatus.PASS.value))
        )
        decision = F.create_map(
            *[part for key, value in DECISIONS.items() for part in (F.lit(key.value), F.lit(value))]
        )[F.col("checklist_status")]
        return (
            with_outcomes.withColumns(
                {
                    "failed_rules": listed(Outcome.FAIL),
                    "review_rules": listed(Outcome.REVIEW),
                    "rules_failed_count": sum(is_(rule, Outcome.FAIL).cast("int") for rule in rules),
                    "checklist_status": status,
                    "assessment_date": F.lit(assessment_date.isoformat()),
                    "rules_applied": F.lit(rules_applied_text(rules)),
                }
            )
            .withColumn("decision", decision)
        )
