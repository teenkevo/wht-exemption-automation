"""Filing features, from filed income tax returns (``returns``)."""

from rules_engine.feature_computations.base import FeatureDefinition, FeatureType, zero_if_absent


FEATURES = (
    FeatureDefinition(
        name="returns_filed_count",
        description="Number of income tax returns filed",
        dtype=FeatureType.NUMBER,
        source_table="returns",
        columns=("total_returns_count",),
        compute=lambda: zero_if_absent("total_returns_count"),
        missing="0, no returns on record",
    ),
    FeatureDefinition(
        name="declared_tax_total",
        description="Net tax declared across filed returns",
        dtype=FeatureType.NUMBER,
        source_table="returns",
        columns=("total_declared_tax",),
        compute=lambda: zero_if_absent("total_declared_tax"),
        missing="0, no returns on record",
    ),
)
