"""Compliance features the rules engine evaluates.

Each feature's calculation is a SELECT in ``rules_engine.feature_migrations``.
The query reads ``lakehouse.<source>`` and returns one row per TIN. The latest
migration that names a feature is the active one. An earlier migration for
that name stays on record and is not used.
"""

from rules_engine.feature_computations.base import (
    FeatureDefinition,
    FeatureRegistry,
    FeatureStatus,
    FeatureType,
)
from rules_engine.feature_migrations import load_feature_registry

FEATURES = load_feature_registry()

__all__ = ["FEATURES", "FeatureDefinition", "FeatureRegistry", "FeatureStatus", "FeatureType"]
