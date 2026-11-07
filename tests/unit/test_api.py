"""Health and model routes."""

from fastapi.testclient import TestClient

from api.main import create_app

client = TestClient(create_app())


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_models_list_shows_the_serving_model():
    response = client.get("/models")
    assert response.status_code == 200
    assert response.json() == {"models": ["random_forest"], "active": "random_forest"}
