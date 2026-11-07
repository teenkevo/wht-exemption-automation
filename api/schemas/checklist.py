"""Request and response bodies for the checklist rules API."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from rules_engine.rule_models import RuleOperator


class RuleDraft(BaseModel):
    """A new version. The registry assigns the version, the status, and the dates."""

    rule_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    feature_name: str = Field(min_length=1)
    operator: RuleOperator
    expected_value: Any = None
    mandatory: bool = True
    legal_reference: str | None = None


class EffectiveFrom(BaseModel):
    """The date a version starts, or the date a rule stops applying."""

    effective_from: date


class ChecklistRun(BaseModel):
    id: int
    run_at: datetime
    actor: str
    cycle: str
    assessment_date: date
    rules_applied: list[str]
    taxpayer_count: int
    eligible: int
    not_eligible: int
    needs_review: int
