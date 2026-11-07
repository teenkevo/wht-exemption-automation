"""Aggregate checks on the one-row-per-TIN table. Reports contain no taxpayer identifiers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _whole(value) -> int:
    return 0 if value is None else int(value)


def profile_taxpayers(df: DataFrame) -> dict:
    total = df.count()
    if total == 0:
        raise ValueError("No taxpayers to profile")

    schema = [{"name": field.name, "type": str(field.dataType)} for field in df.schema.fields]
    null_row = df.agg(
        *[F.sum(F.col(name).isNull().cast("int")).alias(name) for name in df.columns]
    ).first()
    missing = [
        {"column": name, "null_pct": round(_whole(null_row[name]) / total * 100, 2)}
        for name in df.columns
    ]
    missing.sort(key=lambda item: (-item["null_pct"], item["column"]))

    label_column = "label" if "label" in df.columns else "wht_eligible" if "wht_eligible" in df.columns else None
    balance: list[dict] = []
    eligible_share = None
    if label_column is not None:
        grouped = df.groupBy(F.col(label_column).cast("int").alias("label")).count().collect()
        balance = [{"label": None if row["label"] is None else int(row["label"]), "count": int(row["count"])} for row in grouped]
        balance.sort(key=lambda item: (item["label"] is None, item["label"] if item["label"] is not None else -1))
        positives = sum(item["count"] for item in balance if item["label"] == 1)
        eligible_share = round(positives / total * 100, 3)

    by_region: list[dict] = []
    if "REGION_NAME" in df.columns and label_column is not None:
        regions = (
            df.groupBy("REGION_NAME")
            .agg(
                F.count(F.lit(1)).alias("taxpayers"),
                F.sum(F.col(label_column).cast("int")).alias("eligible"),
            )
            .orderBy(F.desc("eligible"))
            .collect()
        )
        for row in regions:
            taxpayers = int(row["taxpayers"])
            eligible = _whole(row["eligible"])
            by_region.append(
                {
                    "region": None if row["REGION_NAME"] is None else str(row["REGION_NAME"]),
                    "taxpayers": taxpayers,
                    "eligible": eligible,
                    "eligible_pct": round(eligible / taxpayers * 100, 3) if taxpayers else 0.0,
                }
            )

    return {
        "taxpayers": total,
        "schema": schema,
        "missing": missing,
        "class_balance": balance,
        "eligible_share_pct": eligible_share,
        "by_region": by_region,
    }
