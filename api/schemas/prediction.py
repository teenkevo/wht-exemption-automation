from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    entity_id: str = Field(min_length=1)


class PredictResponse(BaseModel):
    entity_id: str
    prediction: float | None = None
    tier: str | None = None
    model_name: str | None = None
    note: str | None = None
