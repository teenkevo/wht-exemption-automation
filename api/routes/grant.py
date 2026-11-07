"""Lookup a WHT exemption grant that the batch job has already written."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_grant_service
from api.schemas.grant import GrantRequest, GrantResponse
from api.services.grant_service import (
    GrantNotReady,
    GrantService,
    GrantTableInvalid,
    TaxpayerNotFound,
)
from rules_engine.db import RulesDatabaseError

router = APIRouter()


@router.post("/grant", response_model=GrantResponse)
def grant(request: GrantRequest, service: GrantService = Depends(get_grant_service)) -> dict:
    try:
        return service.lookup(request.tin)
    except TaxpayerNotFound as exc:
        raise HTTPException(status_code=404, detail="Taxpayer is not in the grant table") from exc
    except (GrantNotReady, RulesDatabaseError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except GrantTableInvalid as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
