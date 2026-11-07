"""Payment and liability features, from payments (``payments``) and the ledger (``ledger``)."""

from rules_engine.feature_computations.base import FeatureDefinition, FeatureType, zero_if_absent


FEATURES = (
    FeatureDefinition(
        name="tax_collected",
        description="Total tax paid",
        dtype=FeatureType.NUMBER,
        source_table="payments",
        columns=("total_tax_collected",),
        compute=lambda: zero_if_absent("total_tax_collected"),
        missing="0, no payments on record",
    ),
    FeatureDefinition(
        name="payments_count",
        description="Number of payments made",
        dtype=FeatureType.NUMBER,
        source_table="payments",
        columns=("number_of_payments",),
        compute=lambda: zero_if_absent("number_of_payments"),
        missing="0, no payments on record",
    ),
    FeatureDefinition(
        name="outstanding_liability",
        description="Outstanding liability summed across ledger entries",
        dtype=FeatureType.NUMBER,
        source_table="ledger",
        columns=("total_outstanding_liability",),
        compute=lambda: zero_if_absent("total_outstanding_liability"),
        missing="0, no ledger entries on record",
    ),
    FeatureDefinition(
        name="waiver_received",
        description="Total liability waived",
        dtype=FeatureType.NUMBER,
        source_table="ledger",
        columns=("total_waiver_received",),
        compute=lambda: zero_if_absent("total_waiver_received"),
        missing="0, no ledger entries on record",
    ),
)
