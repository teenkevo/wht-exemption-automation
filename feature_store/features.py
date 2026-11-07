"""Feast definitions for the WHT ranker snapshot.

The feature job writes ``data/features`` and registers this view. A later
materialize step copies the latest value for each TIN into the online store.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int32, String
from feast.value_type import ValueType

REPO_ROOT = Path(__file__).resolve().parents[1]
ONLINE_TTL = timedelta(days=2)

taxpayer = Entity(name="taxpayer", join_keys=["TIN_NO"], value_type=ValueType.STRING)


def feature_names(plan: dict) -> list[str]:
    """Columns registered for each TIN. The entity key and timestamp stay out."""
    names = ["label"]
    names.extend(plan.get("num_feats", []))
    names.extend(plan.get("cat_feats", []))
    names.extend(plan.get("passthrough", []))
    return names


def feature_view_for_plan(plan: dict, source_path: Path) -> FeatureView:
    """Build the online view from the feature plan written with the snapshot."""
    fields = []
    for name in feature_names(plan):
        if name == "label" or str(name).startswith("has_"):
            dtype = Int32
        elif name in set(plan.get("num_feats", [])):
            dtype = Float64
        else:
            dtype = String
        fields.append(Field(name=name, dtype=dtype))
    source = FileSource(
        name="taxpayer_feature_source",
        path=str(source_path),
        timestamp_field="event_timestamp",
    )
    return FeatureView(
        name="taxpayer_features",
        entities=[taxpayer],
        ttl=ONLINE_TTL,
        schema=fields,
        source=source,
        online=True,
    )


def _published_plan() -> dict:
    path = REPO_ROOT / "artifacts" / "reports" / "feature_plan.json"
    if path.is_file():
        loaded = json.loads(path.read_text())
        if isinstance(loaded, dict) and "num_feats" in loaded:
            return loaded
    return {"num_feats": ["business_age_years"], "cat_feats": [], "passthrough": []}


taxpayer_features = feature_view_for_plan(_published_plan(), REPO_ROOT / "data" / "features")
