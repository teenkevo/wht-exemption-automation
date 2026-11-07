from datetime import date

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_grant_service
from api.main import create_app
from api.services.grant_service import GrantService, GrantTableInvalid
from rules_engine import RuleDefinition, RuleOperator, RuleRegistry, RulesEngine
from rules_engine.engine import outcome_column_name, rules_applied_text
from rules_engine.feature_computations import FEATURES

DAY = date(2026, 11, 7)


def _engine() -> RulesEngine:
    """The rules this test explains. catalog.py contributes none."""
    registry = RuleRegistry(FEATURES)
    for code, feature, operator, expected in (
        ("REGISTERED", "registration_status", RuleOperator.EQ, "REGD"),
        ("MIN_TAX_COLLECTED", "tax_collected", RuleOperator.GTE, 100_000_000),
    ):
        draft = registry.register(
            RuleDefinition(
                rule_code=code,
                title=code,
                description=code,
                feature_name=feature,
                operator=operator,
                expected_value=expected,
            ),
            actor="author",
        )
        registry.approve(code, draft.version, actor="approver")
        registry.activate(code, draft.version, date(2026, 7, 1), actor="approver")
    return RulesEngine(registry)


def _grant_row(tin: str, features: dict, engine: RulesEngine) -> dict:
    """The row the batch grant writes, built from the same engine."""
    result = engine.evaluate(tin, features, DAY)
    row = {"TIN_NO": tin, **features}
    for item in result.results:
        row[outcome_column_name(item.rule_code)] = item.outcome.value
    row.update(
        failed_rules=",".join(result.failed_rules),
        review_rules=",".join(result.review_rules),
        rules_failed_count=len(result.failed_rules),
        checklist_status=result.checklist_status.value,
        decision=result.decision,
        cycle="wht_2026_27_h1",
        assessment_date=DAY.isoformat(),
        rules_applied=rules_applied_text(engine.rules_for(DAY)),
    )
    return row


PASSING = {
    "registration_status": "REGD",
    "taxpayer_status": "Existing Taxpayer",
    "outstanding_liability": 0.0,
    "returns_filed_count": 4.0,
    "tax_collected": 150_000_000.0,
}


def test_grant_lookup_explains_each_rule(tmp_path):
    engine = _engine()
    path = tmp_path / "grants.parquet"
    rows = [
        _grant_row("DEMO-TIN-0001", PASSING, engine),
        _grant_row("DEMO-TIN-0002", {**PASSING, "tax_collected": 10.0, "registration_status": None}, engine),
    ]
    pd.DataFrame(rows).to_parquet(path)
    service = GrantService.load(path)
    service.engine = engine
    app = create_app()
    app.dependency_overrides[get_grant_service] = lambda: service
    client = TestClient(app)

    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["cycle"] == "wht_2026_27_h1"

    body = client.post("/grant", json={"tin": "DEMO-TIN-0001"}).json()
    assert body["decision"] == "ELIGIBLE"
    assert body["failed_rules"] == []
    assert [check["rule_code"] for check in body["checks"]][0] == "REGISTERED"
    assert body["score"] is None
    assert "does not change the grant decision" in body["score_note"]

    body = client.post("/grant", json={"tin": "DEMO-TIN-0002"}).json()
    assert body["decision"] == "NOT ELIGIBLE"
    assert body["failed_rules"] == ["MIN_TAX_COLLECTED"]
    assert body["review_rules"] == ["REGISTERED"]
    collected = next(check for check in body["checks"] if check["rule_code"] == "MIN_TAX_COLLECTED")
    assert collected["actual_value"] == 10.0
    assert collected["expected_value"] == 100_000_000
    assert collected["operator"] == "gte"
    assert collected["version"] == 1
    assert collected["source_table"] == "payments"

    assert client.post("/grant", json={"tin": "DEMO-TIN-9999"}).status_code == 404


def test_a_tampered_row_is_rejected(tmp_path):
    engine = _engine()
    row = _grant_row("DEMO-TIN-0001", PASSING, engine)
    row[outcome_column_name("MIN_TAX_COLLECTED")] = "FAIL"
    path = tmp_path / "grants.parquet"
    pd.DataFrame([row]).to_parquet(path)
    service = GrantService.load(path)
    service.engine = engine
    with pytest.raises(GrantTableInvalid, match="does not match"):
        service.lookup("DEMO-TIN-0001")


def test_grant_stops_when_a_cycle_code_has_no_version(tmp_path):
    from dataclasses import replace

    from data_engineering.config import ProjectConfig
    from inference.grant import run

    config = replace(ProjectConfig.load(), taxpayers=tmp_path / "taxpayers", grants=tmp_path / "grants")
    with pytest.raises(ValueError, match="No version in force") as caught:
        run(config, engine=RulesEngine(RuleRegistry(FEATURES)))
    assert "REGISTERED" in str(caught.value)
    assert not config.grants.exists()


def test_rules_database_down_answers_503(monkeypatch):
    from rules_engine.db import RulesDatabaseError

    def unreachable(*_args, **_kwargs):
        raise RulesDatabaseError("Rules database is not reachable")

    monkeypatch.setattr(GrantService, "load", unreachable)
    response = TestClient(create_app()).post("/grant", json={"tin": "1000000000"})
    assert response.status_code == 503
    assert "Rules database" in response.json()["detail"]
