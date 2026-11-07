from pydantic import BaseModel, Field

from rules_engine.rule_models import RuleResult


class GrantRequest(BaseModel):
    tin: str = Field(min_length=1)


class GrantResponse(BaseModel):
    tin: str
    decision: str
    checklist_status: str
    cycle: str
    assessment_date: str
    failed_rules: list[str]
    review_rules: list[str]
    checks: list[RuleResult]
    score: float | None = None
    tier: str | None = None
    score_model: str | None = None
    score_note: str
