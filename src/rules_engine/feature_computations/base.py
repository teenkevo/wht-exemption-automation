"""What a compliance feature is, and the registry that rules validate against.

A feature names the lake table it reads and the Spark SQL that calculates it.
The query returns one row per TIN. The latest version is the one a grant runs.
Rules only name a feature. They never read raw columns.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    """One version of one computed compliance feature.

    ``source_table`` is the view under ``lakehouse``, for example ``payments``.
    ``sql`` is a SELECT that returns ``TIN_NO`` and this feature. ``missing``
    says what a TIN with no source row becomes. A note that starts with ``0``
    fills zero after the join. Anything else stays null and sends its rules
    to REVIEW.

    A new SQL migration adds the next version and leaves this one unused.
    ``version`` counts that feature's migrations, not the file number.
    """

    name: str
    description: str
    dtype: FeatureType
    source_table: str
    sql: str
    missing: str
    columns: tuple[str, ...] = ()
    version: int = 1
    status: FeatureStatus = FeatureStatus.ACTIVE

    @property
    def key(self) -> str:
        return f"{self.name}:v{self.version}"

    @property
    def fills_absent_with_zero(self) -> bool:
        return self.missing.strip().startswith("0")


class FeatureRegistry:
    def __init__(self) -> None:
        self._features: dict[str, FeatureDefinition] = {}
        self._versions: dict[str, list[FeatureDefinition]] = {}

    def register(self, feature: FeatureDefinition) -> FeatureDefinition:
        """Record version 1 of a feature. A later version is a new SQL migration."""
        if feature.name in self._features:
            raise ValueError(f"Feature {feature.name} is already registered")
        if feature.version != 1:
            raise ValueError(f"Feature {feature.name} must start at version 1")
        self._versions[feature.name] = [feature]
        self._features[feature.name] = feature
        return feature

    def add_version(self, feature: FeatureDefinition) -> FeatureDefinition:
        """Make this the active computation. The previous version stays on record."""
        if feature.status != FeatureStatus.ACTIVE:
            raise ValueError(f"Feature {feature.key} must be registered active")
        history = self._versions.get(feature.name, [])
        expected = len(history) + 1
        if feature.version != expected:
            raise ValueError(f"Feature {feature.name} next version must be {expected}")
        if history:
            history[-1] = replace(history[-1], status=FeatureStatus.DEPRECATED)
        history.append(feature)
        self._versions[feature.name] = history
        self._features[feature.name] = feature
        return feature

    def history(self, name: str) -> tuple[FeatureDefinition, ...]:
        if name not in self._versions:
            raise ValueError(f"Unknown feature: {name}")
        return tuple(self._versions[name])

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

    def register_sources(self, spark, sources: Mapping[str, DataFrame]) -> None:
        """Expose each source so a query can read ``lakehouse.<name>``.

        A Spark temp view cannot live in a database, so ``lakehouse.payments``
        in the migration is read from the temp view ``lakehouse_payments``.
        """
        for name, frame in sources.items():
            frame.createOrReplaceTempView(f"lakehouse_{name}")

    def compute(self, df: DataFrame, names: Iterable[str]) -> DataFrame:
        """Run each feature's SELECT and left-join it onto the taxpayer rows.

        A TIN the query does not return is filled from ``missing``. When the
        lake view is not in this session and the feature column is already on
        ``df``, that stored value is kept.
        """
        wanted = list(dict.fromkeys(names))
        spark = df.sparkSession
        missing_views = sorted(
            {
                f"lakehouse.{feature.source_table}"
                for name in wanted
                if (feature := self.require(name))
                and not spark.catalog.tableExists(f"lakehouse_{feature.source_table}")
                and name not in df.columns
            }
        )
        if missing_views:
            raise ValueError("Feature SQL needs registered tables: " + ", ".join(missing_views))
        for name in wanted:
            feature = self.require(name)
            if not spark.catalog.tableExists(f"lakehouse_{feature.source_table}"):
                continue
            result = spark.sql(_lake_views(feature.sql))
            columns = {column.lower() for column in result.columns}
            if "tin_no" not in columns or feature.name.lower() not in columns:
                raise ValueError(f"{feature.key} SQL must return TIN_NO and {feature.name}")
            counts = result.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("TIN_NO").alias("tins")).first()
            if int(counts["rows"]) != int(counts["tins"]):
                raise ValueError(f"{feature.key} SQL returned more than one row for a TIN")
            value = F.col(feature.name)
            if feature.fills_absent_with_zero:
                value = F.coalesce(value.cast("double"), F.lit(0.0))
            if feature.name in df.columns:
                df = df.drop(feature.name)
            df = df.join(result.select("TIN_NO", value.alias(feature.name)), "TIN_NO", "left")
            if feature.fills_absent_with_zero:
                df = df.withColumn(feature.name, F.coalesce(F.col(feature.name).cast("double"), F.lit(0.0)))
        return df


def _lake_views(sql: str) -> str:
    """Point ``lakehouse.payments`` at the temp view registered for that source."""
    return re.sub(r"\blakehouse\.([A-Za-z_][A-Za-z0-9_]*)", r"lakehouse_\1", sql)
