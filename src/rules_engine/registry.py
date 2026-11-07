"""Register, approve, activate, supersede, and retire checklist rules, with an audit trail.

A version moves DRAFT -> APPROVED -> ACTIVE, then SUPERSEDED when a newer
version takes over or RETIRED when the rule ends. The approver must be a
different person from the author. Every version is kept, so a past assessment
can be rerun with the rules that applied then.

Two registries share these methods:

- ``PostgresRuleRegistry`` in ``db/postgres_registry.py`` is the system of record.
  The database enforces the same lifecycle and writes the audit trail.
- ``RuleRegistry`` here keeps everything in memory. Tests and offline runs use
  it, replaying ``catalog.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timezone

from rules_engine.feature_computations import FeatureRegistry
from rules_engine.rule_models import AuditAction, AuditEvent, RuleDefinition, RuleStatus
from rules_engine.validators import validate_rule


def check_approval(rule: RuleDefinition, actor: str) -> None:
    if rule.status != RuleStatus.DRAFT:
        raise ValueError(f"{rule.key} is {rule.status.value}, only a DRAFT can be approved")
    if actor == rule.created_by:
        raise ValueError(f"{rule.key} was written by {actor}; another person must approve it")


def check_activation(target: RuleDefinition, previous: RuleDefinition | None, effective_from: date) -> None:
    if target.status != RuleStatus.APPROVED:
        raise ValueError(f"{target.key} is {target.status.value}, only an APPROVED version can be activated")
    if previous is not None and previous.effective_from is not None and effective_from <= previous.effective_from:
        raise ValueError(
            f"{target.key} cannot start on {effective_from}; "
            f"{previous.key} has been in force since {previous.effective_from}"
        )


def check_retirement(active: RuleDefinition | None, rule_code: str, effective_to: date) -> RuleDefinition:
    if active is None:
        raise ValueError(f"{rule_code} has no active version to retire")
    if active.effective_from is not None and effective_to <= active.effective_from:
        raise ValueError(f"{active.key} cannot end on {effective_to}, before it started")
    return active


class RuleRegistry:
    def __init__(
        self,
        features: FeatureRegistry,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.features = features
        self._clock = clock
        self._rules: dict[str, list[RuleDefinition]] = {}
        self._audit: list[AuditEvent] = []
        self._runs: list[dict] = []

    def _record(self, action: AuditAction, rule: RuleDefinition, actor: str, detail: str = "") -> None:
        self._audit.append(
            AuditEvent(
                at=self._clock(),
                action=action,
                rule_code=rule.rule_code,
                version=rule.version,
                actor=actor,
                detail=detail,
            )
        )

    def _replace(self, rule: RuleDefinition) -> None:
        self._rules[rule.rule_code][rule.version - 1] = rule

    def register(self, rule: RuleDefinition, *, actor: str = "system", reason: str = "") -> RuleDefinition:
        """Add a new DRAFT version. The version number is assigned here.

        The feature must already exist. A draft that could not be approved is refused.
        """
        if rule.status != RuleStatus.DRAFT:
            raise ValueError("New rule versions must be DRAFT")
        validate_rule(rule, self.features)
        versions = self._rules.setdefault(rule.rule_code, [])
        if any(item.status == RuleStatus.RETIRED for item in versions):
            raise ValueError(f"{rule.rule_code} is retired. Register the requirement under a new code")
        draft = rule.model_copy(
            update={
                "version": len(versions) + 1,
                "effective_from": None,
                "effective_to": None,
                "created_by": actor,
                "approved_by": None,
            }
        )
        versions.append(draft)
        self._record(AuditAction.REGISTERED, draft, actor, reason)
        return draft

    def approve(self, rule_code: str, version: int, *, actor: str, reason: str = "") -> RuleDefinition:
        target = self.get(rule_code, version)
        check_approval(target, actor)
        validate_rule(target, self.features)
        approved = target.model_copy(update={"status": RuleStatus.APPROVED, "approved_by": actor})
        self._replace(approved)
        self._record(AuditAction.APPROVED, approved, actor, reason)
        return approved

    def get(self, rule_code: str, version: int) -> RuleDefinition:
        versions = self._rules.get(rule_code, [])
        if not 1 <= version <= len(versions):
            raise ValueError(f"{rule_code} has no version {version}")
        return versions[version - 1]

    def current(self, rule_code: str) -> RuleDefinition | None:
        """The ACTIVE version, if any."""
        return next(
            (item for item in self._rules.get(rule_code, []) if item.status == RuleStatus.ACTIVE),
            None,
        )

    def activate(
        self,
        rule_code: str,
        version: int,
        effective_from: date,
        *,
        actor: str = "system",
        reason: str = "",
    ) -> RuleDefinition:
        """Put an APPROVED version into force. The version it replaces is superseded on that date."""
        target = self.get(rule_code, version)
        previous = self.current(rule_code)
        check_activation(target, previous, effective_from)
        candidate = target.model_copy(update={"effective_from": effective_from})
        validate_rule(candidate, self.features)
        if previous is not None:
            superseded = previous.model_copy(
                update={"status": RuleStatus.SUPERSEDED, "effective_to": effective_from}
            )
            self._replace(superseded)
            self._record(AuditAction.SUPERSEDED, superseded, actor, f"replaced by v{version} from {effective_from}")
        active = candidate.model_copy(update={"status": RuleStatus.ACTIVE})
        self._replace(active)
        self._record(AuditAction.ACTIVATED, active, actor, reason or f"in force from {effective_from}")
        return active

    def retire(self, rule_code: str, effective_to: date, *, actor: str = "system", reason: str = "") -> RuleDefinition:
        """End a rule. It stops applying on ``effective_to`` and its code cannot be reused."""
        active = check_retirement(self.current(rule_code), rule_code, effective_to)
        retired = active.model_copy(update={"status": RuleStatus.RETIRED, "effective_to": effective_to})
        self._replace(retired)
        self._record(AuditAction.RETIRED, retired, actor, reason or f"no longer applies from {effective_to}")
        return retired

    def get_active_rules(self, assessment_date: date) -> list[RuleDefinition]:
        """The version of each rule that applied on a date, including ones later superseded."""
        return [
            item
            for versions in self._rules.values()
            for item in versions
            if item.applies_on(assessment_date)
        ]

    def history(self, rule_code: str) -> list[RuleDefinition]:
        return list(self._rules.get(rule_code, []))

    def codes(self) -> list[str]:
        return list(self._rules)

    def audit_log(self, rule_code: str | None = None, limit: int | None = None) -> list[AuditEvent]:
        events = [event for event in self._audit if rule_code is None or event.rule_code == rule_code]
        return events if limit is None else events[-limit:]

    def record_run(self, summary: dict, *, actor: str) -> int:
        """Keep a grant run next to the rules that decided it. The Postgres registry is the durable copy."""
        run_id = len(self._runs) + 1
        self._runs.append(
            {
                "id": run_id,
                "run_at": self._clock(),
                "actor": actor,
                "cycle": summary["cycle"],
                "assessment_date": summary["assessment_date"],
                "rules_applied": list(summary["rules_applied"]),
                "taxpayer_count": summary["taxpayer_count"],
                "eligible": summary["eligible"],
                "not_eligible": summary["not_eligible"],
                "needs_review": summary["needs_review"],
            }
        )
        return run_id

    def runs(self, limit: int = 10) -> list[dict]:
        return list(reversed(self._runs))[:limit]
