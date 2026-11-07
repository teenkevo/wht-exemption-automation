"""Read and change checklist rules in the rules registry.

Writes name the person in the ``X-Actor`` header and the reason in ``X-Reason``.
The author of a version cannot approve it. A version is never edited: a correction
is the next version of the same rule code.
"""

from datetime import date
from typing import Annotated

import psycopg
from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query

from api.dependencies import get_rules_registry
from api.schemas.checklist import ChecklistRun, EffectiveFrom, RuleDraft
from rules_engine.db import RulesDatabaseError
from rules_engine.rule_models import AuditEvent, RuleDefinition

router = APIRouter(prefix="/api/v1/checklist", tags=["checklist"])

RuleCode = Annotated[str, Path(pattern=r"^[A-Z][A-Z0-9_]*$")]
Version = Annotated[int, Path(ge=1)]


def get_actor(
    x_actor: Annotated[str, Header(min_length=1)],
    x_reason: Annotated[str, Header()] = "",
) -> tuple[str, str]:
    actor = x_actor.strip()
    if not actor:
        raise HTTPException(status_code=422, detail="X-Actor is required")
    return actor, x_reason


def _call(action):
    try:
        return action()
    except RulesDatabaseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        missing = "has no version" in str(exc) or "has no active version" in str(exc)
        raise HTTPException(status_code=404 if missing else 422, detail=str(exc)) from exc
    except psycopg.Error as exc:
        message = exc.diag.message_primary if getattr(exc, "diag", None) and exc.diag.message_primary else str(exc)
        raise HTTPException(status_code=409, detail=message) from exc


@router.get("/rules/active", response_model=list[RuleDefinition])
def active_rules(
    on: Annotated[date | None, Query(description="Rules in force on this date. Defaults to today.")] = None,
    registry=Depends(get_rules_registry),
) -> list[RuleDefinition]:
    return _call(lambda: registry.get_active_rules(on or date.today()))


@router.get("/rules/{code}", response_model=list[RuleDefinition])
def rule_history(code: RuleCode, registry=Depends(get_rules_registry)) -> list[RuleDefinition]:
    rules = _call(lambda: registry.history(code))
    if not rules:
        raise HTTPException(status_code=404, detail=f"{code} has no versions")
    return rules


@router.get("/rules/{code}/versions/{version}", response_model=RuleDefinition)
def rule_version(code: RuleCode, version: Version, registry=Depends(get_rules_registry)) -> RuleDefinition:
    return _call(lambda: registry.get(code, version))


@router.get("/audit", response_model=list[AuditEvent])
def audit(
    code: Annotated[str | None, Query(pattern=r"^[A-Z][A-Z0-9_]*$")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    registry=Depends(get_rules_registry),
) -> list[AuditEvent]:
    return _call(lambda: registry.audit_log(code, limit=limit))


@router.get("/runs", response_model=list[ChecklistRun])
def runs(
    limit: Annotated[int, Query(ge=1, le=500)] = 10,
    registry=Depends(get_rules_registry),
) -> list[dict]:
    return _call(lambda: registry.runs(limit))


@router.post("/rules", response_model=RuleDefinition, status_code=201)
def register_rule(
    draft: RuleDraft,
    actor: tuple[str, str] = Depends(get_actor),
    registry=Depends(get_rules_registry),
) -> RuleDefinition:
    name, reason = actor
    return _call(lambda: registry.register(RuleDefinition(**draft.model_dump()), actor=name, reason=reason))


@router.post("/rules/{code}/versions/{version}/approve", response_model=RuleDefinition)
def approve_rule(
    code: RuleCode,
    version: Version,
    actor: tuple[str, str] = Depends(get_actor),
    registry=Depends(get_rules_registry),
) -> RuleDefinition:
    name, reason = actor
    return _call(lambda: registry.approve(code, version, actor=name, reason=reason))


@router.post("/rules/{code}/versions/{version}/activate", response_model=RuleDefinition)
def activate_rule(
    code: RuleCode,
    version: Version,
    body: EffectiveFrom,
    actor: tuple[str, str] = Depends(get_actor),
    registry=Depends(get_rules_registry),
) -> RuleDefinition:
    name, reason = actor
    return _call(
        lambda: registry.activate(code, version, body.effective_from, actor=name, reason=reason)
    )


@router.post("/rules/{code}/retire", response_model=RuleDefinition)
def retire_rule(
    code: RuleCode,
    body: EffectiveFrom,
    actor: tuple[str, str] = Depends(get_actor),
    registry=Depends(get_rules_registry),
) -> RuleDefinition:
    name, reason = actor
    return _call(lambda: registry.retire(code, body.effective_from, actor=name, reason=reason))
