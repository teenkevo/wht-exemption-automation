"""Trade features, from EFRIS invoices (``efris``) and customs declarations (``customs``)."""

from rules_engine.feature_computations.base import FeatureDefinition, FeatureType, zero_if_absent


FEATURES = (
    FeatureDefinition(
        name="efris_sales_total",
        description="Sales invoiced through EFRIS",
        dtype=FeatureType.NUMBER,
        source_table="efris",
        columns=("total_efris_sales",),
        compute=lambda: zero_if_absent("total_efris_sales"),
        missing="0, no EFRIS invoices on record",
    ),
    FeatureDefinition(
        name="efris_invoice_count",
        description="Number of EFRIS invoices issued",
        dtype=FeatureType.NUMBER,
        source_table="efris",
        columns=("vat_invoice_count",),
        compute=lambda: zero_if_absent("vat_invoice_count"),
        missing="0, no EFRIS invoices on record",
    ),
    FeatureDefinition(
        name="customs_red_lane_count",
        description="Customs declarations routed to the red lane for physical inspection",
        dtype=FeatureType.NUMBER,
        source_table="customs",
        columns=("red_lane_count",),
        compute=lambda: zero_if_absent("red_lane_count"),
        missing="0, no customs declarations on record",
    ),
)
