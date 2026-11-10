"""Rule registry, validators, and the single-taxpayer engine."""

from datetime import date, datetime, timezone

import pytest

from rules_engine.feature_computations import FEATURES, FeatureDefinition, FeatureRegistry, FeatureStatus, FeatureType
from rules_engine import (
    ChecklistStatus,
    Outcome,
    RuleDefinition,
    RuleOperator,
    RuleRegistry,
    RulesEngine,
    RuleStatus,
    default_registry,
)
from rules_engine.catalog import build_registry, release, retire
from rules_engine.rule_models import AuditAction
from rules_engine.validators import evaluate_condition

DAY = date(2026, 11, 7)
PASSING = {
    "registration_status": "REGD",
    "taxpayer_status": "Existing Taxpayer",
    "outstanding_liability": 0.0,
    "returns_filed_count": 4.0,
    "tax_collected": 150_000_000.0,
}


def _rule(**overrides) -> RuleDefinition:
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


def _registry() -> RuleRegistry:
    return RuleRegistry(FEATURES, clock=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc))


def _in_force(registry, rule: RuleDefinition, start: date):
    draft = registry.register(rule, actor="author")
    registry.approve(draft.rule_code, draft.version, actor="approver")
    return registry.activate(draft.rule_code, draft.version, start, actor="approver")


def _checklist(registry):
    """Two rules installed by the test, not by catalog.py."""
    _in_force(
        registry,
        RuleDefinition(
            rule_code="REGISTERED",
            title="Registered",
            description="Registration status is REGD",
            feature_name="registration_status",
            operator=RuleOperator.EQ,
            expected_value="REGD",
        ),
        date(2026, 7, 1),
    )
    _in_force(
        registry,
        RuleDefinition(
            rule_code="MIN_TAX_COLLECTED",
            title="Tax collected at least 100M",
            description="Total tax paid is at least UGX 100,000,000",
            feature_name="tax_collected",
            operator=RuleOperator.GTE,
            expected_value=100_000_000,
        ),
        date(2026, 7, 1),
    )
    return RulesEngine(registry)


def test_an_empty_catalog_has_no_rules_in_force():
    engine = RulesEngine(default_registry())
    assert engine.rules_for(DAY) == []
    assert engine.evaluate("demo", PASSING, DAY).checklist_status == ChecklistStatus.NOT_CONFIGURED


def test_engine_passes_fails_and_sends_missing_data_to_review():
    engine = _checklist(_registry())
    passed = engine.evaluate("demo", PASSING, DAY)
    assert passed.checklist_status == ChecklistStatus.PASS
    assert passed.decision == "ELIGIBLE"

    failed = engine.evaluate("demo", {**PASSING, "tax_collected": 5.0, "registration_status": None}, DAY)
    assert failed.checklist_status == ChecklistStatus.FAIL
    assert failed.failed_rules == ["MIN_TAX_COLLECTED"]
    assert failed.review_rules == ["REGISTERED"]
    collected = next(item for item in failed.results if item.rule_code == "MIN_TAX_COLLECTED")
    assert collected.actual_value == 5.0
    assert collected.expected_value == 100_000_000
    assert collected.source_table == "payments"


def test_a_cycle_limits_and_orders_the_rules():
    engine = _checklist(_registry())
    result = engine.evaluate("demo", PASSING, DAY, ["MIN_TAX_COLLECTED", "REGISTERED"])
    assert [item.rule_code for item in result.results] == ["MIN_TAX_COLLECTED", "REGISTERED"]
    assert result.checklist_status == ChecklistStatus.PASS
    with pytest.raises(ValueError, match="No version in force"):
        engine.rules_for(DAY, ["NOT_A_RULE"])


def test_optional_rules_are_reported_without_changing_the_status():
    registry = _registry()
    _in_force(registry, _rule(mandatory=False), date(2026, 7, 1))
    result = RulesEngine(registry).evaluate("demo", {"efris_sales_total": 1.0}, DAY)
    assert result.results[0].outcome == Outcome.FAIL
    assert result.checklist_status == ChecklistStatus.PASS


def test_no_rules_in_force_is_not_configured():
    assert RulesEngine(_registry()).evaluate("demo", PASSING, DAY).checklist_status == ChecklistStatus.NOT_CONFIGURED


def test_versions_supersede_and_history_stays_queryable():
    registry = _registry()
    _in_force(registry, _rule(), date(2026, 7, 1))
    second = _in_force(registry, _rule(expected_value=20_000_000), date(2027, 1, 1))
    assert second.version == 2

    assert registry.get("MIN_SALES", 1).status == RuleStatus.SUPERSEDED
    assert registry.get("MIN_SALES", 1).effective_to == date(2027, 1, 1)
    assert [rule.expected_value for rule in registry.get_active_rules(date(2026, 12, 31))] == [10_000_000]
    assert [rule.expected_value for rule in registry.get_active_rules(date(2027, 1, 1))] == [20_000_000]
    actions = [(event.action, event.version) for event in registry.audit_log("MIN_SALES")]
    assert actions == [
        (AuditAction.REGISTERED, 1),
        (AuditAction.APPROVED, 1),
        (AuditAction.ACTIVATED, 1),
        (AuditAction.REGISTERED, 2),
        (AuditAction.APPROVED, 2),
        (AuditAction.SUPERSEDED, 1),
        (AuditAction.ACTIVATED, 2),
    ]


def test_register_deletes_the_yaml_only_after_the_draft_is_stored(tmp_path):
    from types import SimpleNamespace

    from rules_engine.cli import _register

    kept = tmp_path / "missing.yaml"
    kept.write_text("rule_code: MIN_SALES\ntitle: t\ndescription: d\nfeature_name: not_a_feature\noperator: gte\nexpected_value: 1\n")
    registry = _registry()
    with pytest.raises(ValueError, match="unknown feature"):
        _register(registry, SimpleNamespace(file=str(kept), actor="alice", reason="", rm=True))
    assert kept.exists()
    assert registry.codes() == []

    used = tmp_path / "min_sales.yaml"
    used.write_text(
        "rule_code: MIN_SALES\ntitle: t\ndescription: d\nfeature_name: efris_sales_total\noperator: gte\nexpected_value: 10000000\n"
    )
    _register(registry, SimpleNamespace(file=str(used), actor="alice", reason="memo", rm=True))
    assert not used.exists()
    assert registry.get("MIN_SALES", 1).status == RuleStatus.DRAFT


def test_register_rejects_an_unknown_feature():
    registry = _registry()
    with pytest.raises(ValueError, match="unknown feature"):
        registry.register(_rule(feature_name="not_a_feature"), actor="author")
    assert registry.codes() == []


def test_the_author_can_activate_a_draft():
    registry = _registry()
    registry.register(_rule(), actor="author")
    activated = registry.activate("MIN_SALES", 1, date(2026, 7, 1), actor="author")
    assert activated.status == RuleStatus.ACTIVE
    assert activated.approved_by is None
    assert activated.effective_from == date(2026, 7, 1)


def test_activation_cannot_be_backdated_or_repeated():
    registry = _registry()
    _in_force(registry, _rule(), date(2027, 1, 1))
    registry.register(_rule(expected_value=1), actor="author")
    registry.approve("MIN_SALES", 2, actor="approver")
    with pytest.raises(ValueError, match="cannot start on"):
        registry.activate("MIN_SALES", 2, date(2026, 7, 1), actor="approver")
    with pytest.raises(ValueError, match="only a DRAFT"):
        registry.activate("MIN_SALES", 1, date(2027, 6, 1), actor="approver")
    with pytest.raises(ValueError, match="must be DRAFT"):
        registry.register(_rule(status=RuleStatus.ACTIVE))


def test_retired_rules_stop_applying_and_cannot_return():
    registry = build_registry(
        (
            release(_rule(), effective_from=date(2026, 7, 1)),
            retire("MIN_SALES", effective_to=date(2027, 1, 1), reason="dropped by policy"),
        )
    )
    assert [rule.rule_code for rule in registry.get_active_rules(date(2026, 12, 1))] == ["MIN_SALES"]
    assert registry.get_active_rules(date(2027, 1, 1)) == []
    assert registry.get("MIN_SALES", 1).status == RuleStatus.RETIRED
    assert registry.audit_log("MIN_SALES")[-1].detail == "dropped by policy"
    with pytest.raises(ValueError, match="retired"):
        registry.register(_rule(), actor="author")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"feature_name": "not_a_feature"}, "unknown feature"),
        ({"operator": RuleOperator.IN, "expected_value": "REGD"}, "non-empty list"),
        ({"expected_value": "ten"}, "numeric threshold"),
        ({"feature_name": "registration_status", "operator": RuleOperator.GT, "expected_value": 1}, "numeric feature"),
        ({"feature_name": "registration_status", "operator": RuleOperator.EQ, "expected_value": 1}, "does not match"),
        ({"operator": RuleOperator.IS_TRUE, "expected_value": None}, "true/false feature"),
    ],
)
def test_activation_validates_the_rule_against_its_feature(overrides, message):
    registry = _registry()
    with pytest.raises(ValueError, match=message):
        _in_force(registry, _rule(**overrides), date(2026, 7, 1))


def test_inactive_features_cannot_back_a_rule():
    features = FeatureRegistry()
    template = FEATURES.require("efris_sales_total")
    features.register(
        FeatureDefinition(
            name=template.name,
            description=template.description,
            dtype=FeatureType.NUMBER,
            source_table=template.source_table,
            sql=template.sql,
            missing=template.missing,
            status=FeatureStatus.DEPRECATED,
        )
    )
    with pytest.raises(ValueError, match="DEPRECATED"):
        _in_force(RuleRegistry(features), _rule(), date(2026, 7, 1))


def test_conditions_compare_like_with_like():
    assert evaluate_condition(0.0, RuleOperator.EQ, 0) is True
    assert evaluate_condition("REGD", RuleOperator.IN, ["REGD", "ACTV"]) is True
    assert evaluate_condition("REGD", RuleOperator.NOT_IN, ["CNCL"]) is True
    assert evaluate_condition(True, RuleOperator.IS_TRUE) is True
    with pytest.raises(TypeError):
        evaluate_condition("5", RuleOperator.GT, 1)
    with pytest.raises(TypeError):
        evaluate_condition(True, RuleOperator.GTE, 1)
    with pytest.raises(TypeError):
        evaluate_condition(1, RuleOperator.EQ, "1")
