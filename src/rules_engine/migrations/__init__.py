"""Versioned checklist migrations.

Files in this package are named ``V001__short_name.yaml`` and are applied in
order. Each file is one transaction: every change is registered and activated,
or the whole file is rolled back. A checksum is stored so an applied file
cannot be edited. The next change is the next file.

The file number is not a rule version. ``V002`` may create ``REGISTERED:v4``.
The person who runs the command is recorded as the one who registered each rule.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import yaml

from rules_engine.feature_computations import FEATURES, FeatureRegistry
from rules_engine.rule_models import RuleDefinition
from rules_engine.validators import validate_rule

_FILENAME = re.compile(r"^V(\d+)__([a-z0-9_]+)\.yaml$")
_RULE_FIELDS = {
    "rule_code",
    "title",
    "description",
    "feature_name",
    "operator",
    "expected_value",
    "mandatory",
    "legal_reference",
}
_CHANGE_FIELDS = _RULE_FIELDS | {"effective_from", "reason", "retire"}


@dataclass(frozen=True)
class Change:
    rule_code: str
    effective_from: date
    reason: str
    rule: RuleDefinition | None = None

    @property
    def retire(self) -> bool:
        return self.rule is None


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    checksum: str
    path: Path
    changes: tuple[Change, ...]


@dataclass(frozen=True)
class Plan:
    """What ``migrate`` should do. ``baseline`` is recorded without running the files."""

    pending: tuple[Migration, ...]
    baseline: tuple[Migration, ...]


def migrations_dir() -> Path:
    """The folder of ``V001__*.yaml`` files. ``RULES_MIGRATIONS`` overrides it."""
    override = os.getenv("RULES_MIGRATIONS")
    directory = Path(override) if override else Path(__file__).resolve().parent
    if not directory.is_dir():
        raise ValueError(f"No migrations directory at {directory}")
    return directory


def load_migrations(directory: Path, features: FeatureRegistry = FEATURES) -> tuple[Migration, ...]:
    """Read and check every migration file. Versions must be 1, 2, 3, ... with no gaps."""
    paths = sorted(path for path in directory.iterdir() if path.suffix == ".yaml" and not path.name.startswith("."))
    if not paths:
        raise ValueError(f"No migrations in {directory}")
    found: dict[int, Migration] = {}
    for path in paths:
        match = _FILENAME.match(path.name)
        if match is None:
            raise ValueError(
                f"{path.name} must be named V001__short_name.yaml "
                "(V, a version number, two underscores, then lowercase words)"
            )
        version = int(match.group(1))
        if version < 1:
            raise ValueError(f"{path.name} version must start at 1")
        if version in found:
            raise ValueError(f"Version {version} is claimed by {found[version].path.name} and {path.name}")
        found[version] = _read_migration(path, version, features)
    expected = list(range(1, max(found) + 1))
    missing = [version for version in expected if version not in found]
    if missing:
        raise ValueError("Migration versions must be 1, 2, 3, ... with no gaps. Missing " + ", ".join(f"V{v}" for v in missing))
    return tuple(found[version] for version in expected)


def prepare(
    files: tuple[Migration, ...],
    applied: list[dict],
    *,
    has_rules: bool,
    baseline: int | None,
) -> Plan:
    """Decide which files to run, or which to record as already applied."""
    by_version = {migration.version: migration for migration in files}
    recorded = {int(row["version"]): row for row in applied}
    for version, row in recorded.items():
        migration = by_version.get(version)
        if migration is None:
            raise ValueError(f"V{version} is recorded but no file for it is in the migrations directory")
        if migration.checksum != row["checksum"]:
            raise ValueError(f"{migration.path.name} was edited after it was applied. Add a new migration instead.")
    pending = tuple(migration for migration in files if migration.version not in recorded)
    if baseline is None:
        if has_rules and not recorded:
            raise ValueError(
                "Rules already exist and no migration has been recorded. "
                "If this database already matches the files through a version, run: "
                "python -m rules_engine.cli migrate --baseline VERSION"
            )
        return Plan(pending, ())
    if recorded:
        raise ValueError("Migrations are already recorded. Baseline is only for a database that has none.")
    if not has_rules:
        raise ValueError("An empty rules database should run migrate, which applies the files.")
    if baseline not in by_version:
        known = ", ".join(f"V{migration.version}" for migration in files)
        raise ValueError(f"No migration V{baseline}. Known migrations: {known}")
    return Plan((), tuple(migration for migration in files if migration.version <= baseline))


def _read_migration(path: Path, version: int, features: FeatureRegistry) -> Migration:
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{path.name} must hold a description and a changes list")
    description = raw.get("description")
    changes = raw.get("changes")
    if not isinstance(description, str) or not description.strip():
        raise ValueError(f"{path.name} needs a description")
    if not isinstance(changes, list) or not changes:
        raise ValueError(f"{path.name} needs a non-empty changes list")
    extra = sorted(set(raw) - {"description", "changes"})
    if extra:
        raise ValueError(f"{path.name} has unexpected fields: {', '.join(extra)}")
    parsed = tuple(_read_change(path.name, index, item, features) for index, item in enumerate(changes, start=1))
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    return Migration(version, description.strip(), checksum, path, parsed)


def _read_change(filename: str, index: int, raw: object, features: FeatureRegistry) -> Change:
    where = f"{filename} change {index}"
    if not isinstance(raw, dict):
        raise ValueError(f"{where} must be a mapping")
    extra = sorted(str(key) for key in raw if key not in _CHANGE_FIELDS)
    if extra:
        raise ValueError(f"{where} has unexpected fields: {', '.join(extra)}")
    code = raw.get("rule_code")
    if not isinstance(code, str) or not code:
        raise ValueError(f"{where} needs a rule_code")
    effective_from = _as_date(raw.get("effective_from"), f"{where} effective_from")
    reason = raw.get("reason") or ""
    if not isinstance(reason, str):
        raise ValueError(f"{where} reason must be text")
    if raw.get("retire") is True:
        blocked = sorted(str(key) for key in raw if key in _RULE_FIELDS - {"rule_code"})
        if blocked:
            raise ValueError(f"{where} retires {code} and also sets {', '.join(blocked)}")
        return Change(code, effective_from, reason)
    missing = sorted(field for field in ("title", "description", "feature_name", "operator") if not raw.get(field))
    if missing:
        raise ValueError(f"{where} needs {', '.join(missing)}")
    rule = RuleDefinition(
        rule_code=code,
        title=str(raw["title"]),
        description=str(raw["description"]),
        feature_name=str(raw["feature_name"]),
        operator=raw["operator"],
        expected_value=raw.get("expected_value"),
        mandatory=bool(raw.get("mandatory", True)),
        legal_reference=None if raw.get("legal_reference") is None else str(raw["legal_reference"]),
    )
    try:
        validate_rule(rule, features)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}") from exc
    return Change(code, effective_from, reason, rule)


def _as_date(value: object, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{label} must be YYYY-MM-DD") from exc
    raise ValueError(f"{label} must be YYYY-MM-DD")
