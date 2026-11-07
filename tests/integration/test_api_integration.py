"""POST /predict reads online features, scores them, and maps failures to status codes."""

from pathlib import Path

import pytest
from fastapi import HTTPException

from api.routes.predict import predict
from api.schemas.prediction import PredictRequest
from api.services.feature_service import FeaturesNotReady, TaxpayerNotInStore
from api.services.model_service import ModelNotReady, ModelService


class FakeFeatures:
    def __init__(self, error=None):
        self.error = error

    def get_online(self, entity_id):
        if self.error:
            raise self.error
        return {"log_total_efris_sales": 3.2, "REGION_NAME": "CENTRAL"}


class FakeModel:
    model_name = "random_forest"

    def __init__(self, error=None):
        self.error = error

    def predict(self, entity_id, row):
        if self.error:
            raise self.error
        return {"score": 0.42, "tier": "Medium"}


def test_predict_returns_score_and_tier():
    body = predict(PredictRequest(entity_id=" entity-1 "), FakeFeatures(), FakeModel())
    assert body["entity_id"] == "entity-1"
    assert body["prediction"] == 0.42
    assert body["tier"] == "Medium"
    assert body["model_name"] == "random_forest"


@pytest.mark.parametrize(
    ("features", "model", "status"),
    [
        (FakeFeatures(TaxpayerNotInStore("missing")), FakeModel(), 404),
        (FakeFeatures(FeaturesNotReady("no registry")), FakeModel(), 503),
        (FakeFeatures(), FakeModel(ModelNotReady("no model")), 503),
    ],
)
def test_predict_maps_failures(features, model, status):
    with pytest.raises(HTTPException) as caught:
        predict(PredictRequest(entity_id="entity-1"), features, model)
    assert caught.value.status_code == status


def test_model_without_artifacts_is_not_ready(tmp_path: Path):
    service = ModelService(model_name="random_forest", model_dir=tmp_path)
    assert not service.ready
    with pytest.raises(ModelNotReady, match="make train"):
        service.predict("entity-1", {})
