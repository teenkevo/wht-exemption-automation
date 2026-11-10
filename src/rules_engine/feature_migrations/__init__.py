"""Versioned feature computations.

Files in this package are named ``V001__short_name.sql`` and are read in order.
Each file holds one or more features. A feature block is a header and one
SELECT over a ``lakehouse.<source>`` table. The query returns ``TIN_NO`` and
the feature. The latest block that names a feature is the query a grant runs.
Earlier blocks for that name stay on record and are not used.

The file number is not the feature version. The second migration that names
``tax_collected`` is ``tax_collected:v2``, whichever file it sits in.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from rules_engine.feature_computations.base import (
    FeatureDefinition,
    FeatureRegistry,
    FeatureStatus,
    FeatureType,
)

_FILENAME = re.compile(r"^V(\d+)__([a-z0-9_]+)\.sql$")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_HEADER = re.compile(r"^--\s+([a-z_]+):\s*(.*)$")
_FIELDS = ("description", "dtype", "source", "missing")


@dataclass(frozen=True)
class _Block:
    name: str
    description: str
    dtype: FeatureType
    source_table: str
    missing: str
    sql: str


def migrations_dir() -> Path:
    """The folder of ``V001__*.sql`` files. ``FEATURE_MIGRATIONS`` overrides it."""
    override = os.getenv("FEATURE_MIGRATIONS")
    directory = Path(override) if override else Path(__file__).resolve().parent
    if not directory.is_dir():
        raise ValueError(f"No feature migrations directory at {directory}")
    return directory


def load_feature_registry(directory: Path | None = None) -> FeatureRegistry:
    """Build the registry. The last migration to name a feature is the active one."""
    directory = directory or migrations_dir()
    registry = FeatureRegistry()
    counts: dict[str, int] = {}
    for block in _load_blocks(directory):
        counts[block.name] = counts.get(block.name, 0) + 1
        version = counts[block.name]
        registry.add_version(
            FeatureDefinition(
                name=block.name,
                description=block.description,
                dtype=block.dtype,
                source_table=block.source_table,
                sql=block.sql,
                missing=block.missing,
                version=version,
                status=FeatureStatus.ACTIVE,
            )
        )
    return registry


def _load_blocks(directory: Path) -> tuple[_Block, ...]:
    paths = sorted(path for path in directory.iterdir() if path.suffix == ".sql" and not path.name.startswith("."))
    if not paths:
        raise ValueError(f"No feature migrations in {directory}")
    found: dict[int, Path] = {}
    for path in paths:
        match = _FILENAME.match(path.name)
        if match is None:
            raise ValueError(
                f"{path.name} must be named V001__short_name.sql "
                "(V, a version number, two underscores, then lowercase words)"
            )
        version = int(match.group(1))
        if version < 1:
            raise ValueError(f"{path.name} version must start at 1")
        if version in found:
            raise ValueError(f"Version {version} is claimed by {found[version].name} and {path.name}")
        found[version] = path
    missing = [version for version in range(1, max(found) + 1) if version not in found]
    if missing:
        raise ValueError(
            "Feature migration versions must be 1, 2, 3, ... with no gaps. Missing "
            + ", ".join(f"V{version}" for version in missing)
        )
    blocks: list[_Block] = []
    for version in range(1, max(found) + 1):
        blocks.extend(_read_file(found[version]))
    return tuple(blocks)


def _read_file(path: Path) -> list[_Block]:
    blocks: list[_Block] = []
    seen: set[str] = set()
    header: dict[str, str] | None = None
    body: list[str] = []

    def flush() -> None:
        nonlocal header, body
        if header is None:
            body = []
            return
        name = header.get("feature", "")
        if name in seen:
            raise ValueError(f"{path.name} defines {name} more than once")
        seen.add(name)
        blocks.append(_block(path.name, header, body))
        header = None
        body = []

    for line in path.read_text().splitlines():
        matched = _HEADER.match(line)
        if matched and matched.group(1) == "feature":
            flush()
            header = {"feature": matched.group(2).strip()}
            continue
        if header is not None and matched and not body:
            key = matched.group(1)
            if key not in _FIELDS:
                raise ValueError(f"{path.name} feature {header.get('feature', '')} has unexpected field {key}")
            if key in header:
                raise ValueError(f"{path.name} feature {header['feature']} repeats {key}")
            header[key] = matched.group(2).strip()
            continue
        if header is not None and (body or line.strip()):
            body.append(line)
    flush()
    if not blocks:
        raise ValueError(f"{path.name} needs a feature block")
    return blocks


def _block(filename: str, header: dict[str, str], body: list[str]) -> _Block:
    name = header.get("feature", "")
    where = f"{filename} feature {name or '(unnamed)'}"
    if not _NAME.match(name):
        raise ValueError(f"{where} needs a feature name such as tax_collected")
    missing_fields = [field for field in _FIELDS if not header.get(field)]
    if missing_fields:
        raise ValueError(f"{where} needs {', '.join(missing_fields)}")
    try:
        dtype = FeatureType(header["dtype"])
    except ValueError as exc:
        raise ValueError(f"{where} dtype must be number, text, or boolean") from exc
    sql = "\n".join(body).strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    if not sql.lower().startswith("select") or ";" in sql:
        raise ValueError(f"{where} needs one SELECT")
    return _Block(name, header["description"], dtype, header["source"], header["missing"], sql)
