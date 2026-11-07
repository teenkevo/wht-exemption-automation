"""The checklist rules API, against a fresh in-memory registry."""

from datetime import date

from fastapi.testclient import TestClient

from api.dependencies import get_rules_registry
from api.main import create_app
from rules_engine.feature_computations import FEATURES
from rules_engine.db import RulesDatabaseError
from rules_engine.registry import RuleRegistry

DAY = "2027-01-01"
DRAFT = {
    "rule_code": "MIN_SALES",
    "title": "EFRIS sales at least 10M",
    "description": "EFRIS sales of at least UGX 10,000,000",
    "feature_name": "efris_sales_total",
    "operator": "gte",
    "expected_value": 10_000_000,
}


def _client(registry):
    app = create_app()
    app.dependency_overrides[get_rules_registry] = lambda: registry
    return TestClient(app)


def test_a_version_moves_from_draft_to_active_and_stays_queryable():
    client = _client(RuleRegistry(FEATURES))
    registered = client.post("/api/v1/checklist/rules", json=DRAFT, headers={"X-Actor": "alice", "X-Reason": "memo"})
    assert registered.status_code == 201
    assert registered.json()["version"] == 1
    assert registered.json()["status"] == "DRAFT"

    refused = client.post(
        "/api/v1/checklist/rules/MIN_SALES/versions/1/approve",
        headers={"X-Actor": "alice"},
    )
    assert refused.status_code == 422
    assert "another person" in refused.json()["detail"]

    approved = client.post(
        "/api/v1/checklist/rules/MIN_SALES/versions/1/approve",
        headers={"X-Actor": "bob", "X-Reason": "checked memo"},
    )
    assert approved.status_code == 200
    assert approved.json()["approved_by"] == "bob"

    activated = client.post(
        "/api/v1/checklist/rules/MIN_SALES/versions/1/activate",
        json={"effective_from": DAY},
        headers={"X-Actor": "bob"},
    )
    assert activated.status_code == 200
    assert activated.json()["status"] == "ACTIVE"

    active = client.get("/api/v1/checklist/rules/active", params={"on": DAY}).json()
    assert [rule["rule_code"] for rule in active] == ["MIN_SALES"]
    assert client.get("/api/v1/checklist/rules/active", params={"on": "2026-12-31"}).json() == []

    history = client.get("/api/v1/checklist/rules/MIN_SALES").json()
    assert [rule["status"] for rule in history] == ["ACTIVE"]
    version = client.get("/api/v1/checklist/rules/MIN_SALES/versions/1").json()
    assert version["expected_value"] == 10_000_000

    trail = client.get("/api/v1/checklist/audit", params={"code": "MIN_SALES"}).json()
    assert [(event["action"], event["actor"]) for event in trail] == [
        ("REGISTERED", "alice"),
        ("APPROVED", "bob"),
        ("ACTIVATED", "bob"),
    ]


def test_retire_ends_the_rule_on_the_given_date():
    registry = RuleRegistry(FEATURES)
    client = _client(registry)
    client.post("/api/v1/checklist/rules", json=DRAFT, headers={"X-Actor": "alice"})
    client.post("/api/v1/checklist/rules/MIN_SALES/versions/1/approve", headers={"X-Actor": "bob"})
    client.post(
        "/api/v1/checklist/rules/MIN_SALES/versions/1/activate",
        json={"effective_from": "2026-07-01"},
        headers={"X-Actor": "bob"},
    )
    retired = client.post(
        "/api/v1/checklist/rules/MIN_SALES/retire",
        json={"effective_from": DAY},
        headers={"X-Actor": "bob", "X-Reason": "dropped"},
    )
    assert retired.status_code == 200
    assert retired.json()["status"] == "RETIRED"
    assert client.get("/api/v1/checklist/rules/active", params={"on": DAY}).json() == []
    assert client.get("/api/v1/checklist/rules/active", params={"on": "2026-12-01"}).json()[0]["status"] == "RETIRED"


def test_unknown_rules_and_a_missing_actor_are_rejected():
    client = _client(RuleRegistry(FEATURES))
    assert client.get("/api/v1/checklist/rules/MIN_SALES").status_code == 404
    assert client.get("/api/v1/checklist/rules/MIN_SALES/versions/1").status_code == 404
    missing_actor = client.post("/api/v1/checklist/rules", json=DRAFT)
    assert missing_actor.status_code == 422


def test_grant_runs_are_listed_newest_first():
    registry = RuleRegistry(FEATURES)
    registry.record_run(
        {
            "cycle": "wht_2026_27_h1",
            "assessment_date": date(2026, 11, 7),
            "rules_applied": ["REGISTERED:v1"],
            "taxpayer_count": 10,
            "eligible": 2,
            "not_eligible": 7,
            "needs_review": 1,
        },
        actor="scheduler",
    )
    body = _client(registry).get("/api/v1/checklist/runs").json()
    assert body[0]["actor"] == "scheduler"
    assert body[0]["eligible"] == 2
    assert body[0]["rules_applied"] == ["REGISTERED:v1"]


def test_rules_database_down_answers_503(monkeypatch):
    def down():
        raise RulesDatabaseError("Rules database is not reachable")

    monkeypatch.setattr("api.dependencies.default_registry", down)
    response = TestClient(create_app()).get("/api/v1/checklist/rules/active")
    assert response.status_code == 503
    assert "Rules database" in response.json()["detail"]
