"""Process liveness and grant-table readiness.

Readiness means the grant table can answer a lookup.
"""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_grant_service
from api.services.grant_service import GrantService

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.get("/ready")
def ready(grants: GrantService = Depends(get_grant_service)) -> dict:
    if not grants.ready:
        raise HTTPException(status_code=503, detail="Grant table is not ready")
    return {"status": "ready", "cycle": grants.cycle}
