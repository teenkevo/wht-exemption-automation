"""Read the WHT taxpayer extract and prepare one row per TIN."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_yaml(name: str) -> dict:
    """Read ``config/<name>``."""
    path = REPO_ROOT / "config" / name
    loaded = yaml.safe_load(path.read_text()) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a mapping")
    return loaded


def resolve_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path
