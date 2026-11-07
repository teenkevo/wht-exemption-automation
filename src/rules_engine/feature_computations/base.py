"""What a compliance feature is, and the registry that rules validate against.

A feature says where its data comes from (the lake table and the columns on
the joined taxpayer table), how to compute it in Spark, and what a missing
source row means. Rules only name a feature. They never read raw columns.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def zero_if_absent(column: str) -> Column:
    """A left join with no source rows leaves null. For counts and amounts that means zero."""
    return F.coalesce(F.col(column).cast("double"), F.lit(0.0))


class FeatureStatus(str, Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"


class FeatureType(str, Enum):
    NUMBER = "number"
    TEXT = "text"
    BOOLEAN = "boolean"


@dataclass(frozen=True)
class FeatureDefinition:
    """One computed compliance feature.

    ``source_table`` names the lake table in ``remote_sources``. ``columns``
    are the joined-table columns the computation reads. ``missing`` says what
    an absent source row becomes, for example zero returns when the TIN has no
    rows in the returns table. A feature that stays null sends its rules to REVIEW.
    """

    name: str
    description: str
    dtype: FeatureType
    source_table: str
    columns: tuple[str, ...]
    compute: Callable[[], Column]
    missing: str
    status: FeatureStatus = FeatureStatus.ACTIVE


class FeatureRegistry:
    def __init__(self) -> None:
        self._features: dict[str, FeatureDefinition] = {}

    def register(self, feature: FeatureDefinition) -> FeatureDefinition:
        if feature.name in self._features:
            raise ValueError(f"Feature {feature.name} is already registered")
        self._features[feature.name] = feature
        return feature

    def get(self, name: str) -> FeatureDefinition | None:
        return self._features.get(name)

    def require(self, name: str) -> FeatureDefinition:
        feature = self.get(name)
        if feature is None:
            raise ValueError(f"Unknown feature: {name}")
        return feature

    def names(self) -> list[str]:
        return list(self._features)

    def source_columns(self, names: Iterable[str]) -> set[str]:
        return {column for name in names for column in self.require(name).columns}

    def compute(self, df: DataFrame, names: Iterable[str]) -> DataFrame:
        """Add each named feature as a column of the same name."""
        wanted = list(dict.fromkeys(names))
        missing = sorted(
            {
                f"{column} (from {self.require(name).source_table})"
                for name in wanted
                for column in self.require(name).columns
                if column not in df.columns
            }
        )
        if missing:
            raise ValueError("Taxpayer table is missing feature inputs: " + ", ".join(missing))
        return df.withColumns({name: self.require(name).compute() for name in wanted})
