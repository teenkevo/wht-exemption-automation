"""Shared dependencies for the API routes."""

from functools import lru_cache

from fastapi import HTTPException

from api.services.feature_service import FeatureService
from api.services.grant_service import GrantService
from api.services.model_service import ModelService
from rules_engine.catalog import default_registry
from rules_engine.db import RulesDatabaseError


@lru_cache(maxsize=1)
def get_model_service() -> ModelService:
    return ModelService.load()


@lru_cache(maxsize=1)
def get_feature_service() -> FeatureService:
    return FeatureService.load()


def get_grant_service() -> GrantService:
    try:
        return GrantService.load()
    except RulesDatabaseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def get_rules_registry():
    try:
        return default_registry()
    except RulesDatabaseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
