"""Real-time ranker score from the online feature store and the loaded model."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_feature_service, get_model_service
from api.schemas.prediction import PredictRequest, PredictResponse
from api.services.feature_service import FeatureService, FeaturesNotReady, TaxpayerNotInStore
from api.services.model_service import ModelNotReady, ModelService

router = APIRouter()

SCORE_NOTE = (
    "The score ranks similarity to taxpayers who passed the checklist. "
    "It does not change the grant decision."
)


@router.post("/predict", response_model=PredictResponse)
def predict(
    request: PredictRequest,
    features: FeatureService = Depends(get_feature_service),
    models: ModelService = Depends(get_model_service),
) -> dict:
    entity_id = request.entity_id.strip()
    try:
        row = features.get_online(entity_id)
        result = models.predict(entity_id, row)
    except TaxpayerNotInStore as exc:
        raise HTTPException(status_code=404, detail="Taxpayer is not in the online feature store") from exc
    except (FeaturesNotReady, ModelNotReady) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "entity_id": entity_id,
        "prediction": result["score"],
        "tier": result["tier"],
        "model_name": models.model_name,
        "note": SCORE_NOTE,
    }
