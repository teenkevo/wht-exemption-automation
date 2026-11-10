"""What a checklist rule, its result, and a registry audit event look like."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RuleStatus(str, Enum):
    DRAFT = "DRAFT"
    APPROVED = "APPROVED"  # signed off by someone other than the author, not yet in force
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"  # replaced by a newer version of the same rule
    RETIRED = "RETIRED"  # the rule itself stopped applying


class RuleOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"


NUMERIC_OPERATORS = frozenset({RuleOperator.GT, RuleOperator.GTE, RuleOperator.LT, RuleOperator.LTE})
SET_OPERATORS = frozenset({RuleOperator.IN, RuleOperator.NOT_IN})
FLAG_OPERATORS = frozenset({RuleOperator.IS_TRUE, RuleOperator.IS_FALSE})


class Outcome(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    REVIEW = "REVIEW"  # the feature value is missing or unusable


class ChecklistStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    REVIEW = "REVIEW"
    NOT_CONFIGURED = "NOT_CONFIGURED"


DECISIONS = {
    ChecklistStatus.PASS: "ELIGIBLE",
    ChecklistStatus.FAIL: "NOT ELIGIBLE",
    ChecklistStatus.REVIEW: "NEEDS REVIEW",
    ChecklistStatus.NOT_CONFIGURED: "NOT CONFIGURED",
}


class RuleDefinition(BaseModel):
    """One version of one checklist rule. Versions are never edited in place."""

    model_config = ConfigDict(frozen=True)

    rule_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    title: str
    description: str

    feature_name: str
    operator: RuleOperator
    expected_value: Any = None

    mandatory: bool = True

    status: RuleStatus = RuleStatus.DRAFT
    version: int = 1

    effective_from: date | None = None
    effective_to: date | None = None  # exclusive

    legal_reference: str | None = None

    created_by: str | None = None
    approved_by: str | None = None

    @property
    def key(self) -> str:
        return f"{self.rule_code}:v{self.version}"

    def applies_on(self, day: date) -> bool:
        if self.status not in {RuleStatus.ACTIVE, RuleStatus.SUPERSEDED, RuleStatus.RETIRED}:
            return False
        if self.effective_from is None or day < self.effective_from:
            return False
        return self.effective_to is None or day < self.effective_to


class RuleResult(BaseModel):
    """One rule applied to one taxpayer, with the evidence behind the outcome."""

    rule_code: str
    version: int
    title: str
    mandatory: bool

    feature_name: str
    feature_version: int | None = None
    source_table: str | None = None
    operator: RuleOperator
    actual_value: Any = None
    expected_value: Any = None

    outcome: Outcome
    reason: str
    legal_reference: str | None = None


class ChecklistResult(BaseModel):
    taxpayer_id: str
    assessment_date: date
    checklist_status: ChecklistStatus
    decision: str
    results: list[RuleResult]

    @property
    def failed_rules(self) -> list[str]:
        return [item.rule_code for item in self.results if item.outcome == Outcome.FAIL]

    @property
    def review_rules(self) -> list[str]:
        return [item.rule_code for item in self.results if item.outcome == Outcome.REVIEW]


class AuditAction(str, Enum):
    REGISTERED = "REGISTERED"
    APPROVED = "APPROVED"
    ACTIVATED = "ACTIVATED"
    SUPERSEDED = "SUPERSEDED"
    RETIRED = "RETIRED"


class AuditEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    at: datetime
    action: AuditAction | str
    rule_code: str
    version: int
    actor: str
    detail: str = ""
