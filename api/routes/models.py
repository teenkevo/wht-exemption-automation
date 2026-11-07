"""Model name configured for serving."""

from fastapi import APIRouter, Depends

from api.dependencies import get_model_service
from api.services.model_service import ModelService

router = APIRouter()


@router.get("/models")
def models(service: ModelService = Depends(get_model_service)) -> dict:
    return {"models": service.list_models(), "active": service.model_name}
