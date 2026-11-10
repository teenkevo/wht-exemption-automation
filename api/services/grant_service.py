"""Read the grant table written by the batch job.

Each row records the outcome of every rule, the rule versions used
(``rules_applied``), and the feature versions those rules ran
(``features_applied``). The lookup rebuilds each check's evidence from those
versions in the catalog. The optional score is the ranker similarity score and
does not change the decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.dataset as ds

from data_engineering.config import ProjectConfig
from rules_engine.catalog import default_engine
from rules_engine.engine import (
    RulesEngine,
    checklist_status,
    outcome_column_name,
    parse_features_applied,
    parse_rules_applied,
)
from rules_engine.rule_models import DECISIONS, Outcome

SCORE_NOTE = (
    "The score ranks similarity to taxpayers who passed the checklist. "
    "It does not change the grant decision."
)
SCORE_COLUMNS = ["TIN_NO", "score", "tier", "model_name"]
REQUIRED_COLUMNS = (
    "TIN_NO",
    "failed_rules",
    "review_rules",
    "checklist_status",
    "decision",
    "cycle",
    "assessment_date",
    "rules_applied",
)


class GrantNotReady(RuntimeError):
    """The grant table has not been built."""


class TaxpayerNotFound(LookupError):
    """The TIN is not in the grant table."""


class GrantTableInvalid(RuntimeError):
    """The grant table does not match the rule record."""


def _split(text: object) -> list[str]:
    return [part for part in str(text or "").split(",") if part]


@dataclass
class GrantService:
    dataset: ds.Dataset | None
    path: Path
    cycle: str | None
    scores: ds.Dataset | None = None
    engine: RulesEngine = field(default_factory=default_engine)

    @property
    def ready(self) -> bool:
        return self.dataset is not None and bool(self.cycle)

    @classmethod
    def load(cls, path: Path | None = None, scores_path: Path | None = None) -> GrantService:
        if path is None:
            config = ProjectConfig.load()
            path = config.grants
            if scores_path is None:
                scores_path = config.scores
        scores = None
        if scores_path is not None and scores_path.exists():
            scores = ds.dataset(str(scores_path), format="parquet", ignore_prefixes=[".", "_"])
        if not path.exists():
            return cls(dataset=None, path=path, cycle=None, scores=scores)
        dataset = ds.dataset(str(path), format="parquet", ignore_prefixes=[".", "_"])
        missing = [name for name in REQUIRED_COLUMNS if name not in dataset.schema.names]
        if missing:
            raise GrantTableInvalid(
                f"Grant table is missing columns: {', '.join(missing)}. Rebuild it with make grant"
            )
        cycle = None
        for batch in dataset.scanner(columns=["cycle"]).to_batches():
            if batch.num_rows:
                cycle = batch.column("cycle")[0].as_py()
                break
        return cls(dataset=dataset, path=path, cycle=None if cycle is None else str(cycle), scores=scores)

    def lookup(self, tin: str) -> dict:
        if not self.ready or self.dataset is None:
            raise GrantNotReady("Build the grant table first")
        cleaned = tin.strip()
        if not cleaned:
            raise TaxpayerNotFound(tin)
        table = self.dataset.to_table(filter=ds.field("TIN_NO") == cleaned)
        if table.num_rows == 0:
            raise TaxpayerNotFound(cleaned)
        if table.num_rows > 1:
            raise GrantTableInvalid("Grant table has more than one row for a TIN")
        payload = self.present(table.to_pylist()[0])
        payload.update(score=None, tier=None, score_model=None, score_note=SCORE_NOTE)
        if self.scores is not None:
            scored = self.scores.to_table(filter=ds.field("TIN_NO") == cleaned, columns=SCORE_COLUMNS)
            if scored.num_rows > 1:
                raise GrantTableInvalid("Score table has more than one row for a TIN")
            if scored.num_rows == 1:
                row = scored.to_pylist()[0]
                payload["score"] = None if row["score"] is None else float(row["score"])
                payload["tier"] = None if row["tier"] is None else str(row["tier"])
                payload["score_model"] = None if row["model_name"] is None else str(row["model_name"])
        return payload

    def present(self, row: dict) -> dict:
        """Rebuild the checklist for one stored row and check it agrees with the stored decision."""
        rules, results = [], []
        feature_versions = dict(parse_features_applied(str(row.get("features_applied") or "")))
        for code, version in parse_rules_applied(row["rules_applied"]):
            try:
                rule = self.engine.registry.get(code, version)
            except ValueError as exc:
                raise GrantTableInvalid(f"{code}:v{version} is not in the rule catalog") from exc
            column = outcome_column_name(code)
            if column not in row:
                raise GrantTableInvalid(f"Grant row has no outcome for {code}")
            outcome = Outcome(row[column])
            rules.append(rule)
            check = self.engine.explain(rule, row.get(rule.feature_name), outcome)
            used = feature_versions.get(rule.feature_name)
            if used is not None:
                check = check.model_copy(update={"feature_version": used})
            results.append(check)
        status = checklist_status(rules, [item.outcome for item in results])
        if status.value != row["checklist_status"] or DECISIONS[status] != row["decision"]:
            raise GrantTableInvalid("Grant row decision does not match its rule outcomes")
        failed = [item.rule_code for item in results if item.outcome == Outcome.FAIL]
        review = [item.rule_code for item in results if item.outcome == Outcome.REVIEW]
        if failed != _split(row["failed_rules"]) or review != _split(row["review_rules"]):
            raise GrantTableInvalid("Grant row rule lists do not match its rule outcomes")
        return {
            "tin": str(row["TIN_NO"]),
            "decision": row["decision"],
            "checklist_status": row["checklist_status"],
            "cycle": str(row["cycle"]),
            "assessment_date": str(row["assessment_date"]),
            "failed_rules": failed,
            "review_rules": review,
            "checks": [item.model_dump(mode="json") for item in results],
        }
