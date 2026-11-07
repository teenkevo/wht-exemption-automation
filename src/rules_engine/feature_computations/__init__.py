"""Compliance features the rules engine evaluates.

Each module covers one area and lists its features. A new feature goes in the
module for its area, or in a new module added to ``_MODULES``.
"""

from rules_engine.feature_computations import (
    filing_compliance,
    payment_compliance,
    registration_compliance,
    trade_compliance,
)
from rules_engine.feature_computations.base import (
    FeatureDefinition,
    FeatureRegistry,
    FeatureStatus,
    FeatureType,
)

_MODULES = (registration_compliance, filing_compliance, payment_compliance, trade_compliance)

FEATURES = FeatureRegistry()
for _module in _MODULES:
    for _feature in _module.FEATURES:
        FEATURES.register(_feature)

__all__ = ["FEATURES", "FeatureDefinition", "FeatureRegistry", "FeatureStatus", "FeatureType"]
