"""Train the similarity ranker and keep its score beside the grant rules.

Random forest and gradient-boosted trees use the unscaled feature vector.
Logistic regression uses the scaled vector. Tiers and the lookup score come
from the configured serving model, which is the random forest.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    GBTClassificationModel,
    GBTClassifier,
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_engineering.config import ProjectConfig
from data_engineering.preprocess import prepare_taxpayers
from data_engineering.source import table_counts
from features.compute_user_feats import apply_features, discover_features, with_label
from features.profile import profile_taxpayers

MODEL_LOADERS = {
    "random_forest": RandomForestClassificationModel,
    "logistic_regression": LogisticRegressionModel,
    "gbt": GBTClassificationModel,
}
SCORE_COLUMNS = ["TIN_NO", "score", "tier", "model_name"]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _split(features: DataFrame, config: ProjectConfig) -> tuple[DataFrame, DataFrame]:
    tagged = features.withColumn("_r", F.rand(config.split_seed))
    train_full = tagged.filter(F.col("_r") < F.lit(config.train_fraction)).drop("_r")
    test = tagged.filter(F.col("_r") >= F.lit(config.train_fraction)).drop("_r")
    positives = train_full.filter(F.col("label") == 1)
    negatives = train_full.filter(F.col("label") == 0)
    n_pos = positives.count()
    if n_pos == 0:
        rescue = test.filter(F.col("label") == 1).limit(1)
        if rescue.limit(1).count() == 0:
            raise ValueError("The ranker needs at least one eligible taxpayer")
        train_full = train_full.unionByName(rescue)
        test = test.join(rescue.select("TIN_NO"), on="TIN_NO", how="left_anti")
        positives = train_full.filter(F.col("label") == 1)
        n_pos = positives.count()
    n_neg = negatives.count()
    if n_pos == 0:
        raise ValueError("The ranker needs at least one eligible taxpayer in the training split")
    if n_neg == 0:
        return positives, test
    fraction = min(1.0, config.negative_ratio * n_pos / n_neg)
    sampled = negatives.sample(False, fraction, config.split_seed)
    if sampled.limit(1).count() == 0:
        sampled = negatives.limit(max(1, int(config.negative_ratio * n_pos)))
    return positives.unionByName(sampled), test


def _fit_preprocessor(train: DataFrame, num_feats: list[str], cat_feats: list[str]) -> PipelineModel:
    stages = []
    encoded = []
    if cat_feats:
        indexed = [f"{name}_idx" for name in cat_feats]
        encoded = [f"{name}_ohe" for name in cat_feats]
        stages.extend(
            StringIndexer(inputCol=name, outputCol=index, handleInvalid="keep")
            for name, index in zip(cat_feats, indexed)
        )
        stages.append(OneHotEncoder(inputCols=indexed, outputCols=encoded, handleInvalid="keep"))
    stages.append(
        VectorAssembler(inputCols=num_feats + encoded, outputCol="features_raw", handleInvalid="keep")
    )
    stages.append(StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True))
    return Pipeline(stages=stages).fit(train)


def _train(prepared: DataFrame, config: ProjectConfig) -> dict:
    forest = RandomForestClassifier(
        featuresCol="features_raw",
        labelCol="label",
        numTrees=config.random_forest_trees,
        maxDepth=config.random_forest_depth,
        seed=config.split_seed,
    ).fit(prepared)
    logistic = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        regParam=config.logistic_reg_param,
        maxIter=config.logistic_max_iter,
    ).fit(prepared)
    gbt = GBTClassifier(
        featuresCol="features_raw",
        labelCol="label",
        maxIter=config.gbt_max_iter,
        maxDepth=config.gbt_max_depth,
        stepSize=config.gbt_step_size,
        seed=config.split_seed,
    ).fit(prepared)
    return {"random_forest": forest, "logistic_regression": logistic, "gbt": gbt}


def _score_column(frame: DataFrame):
    if "probability" in frame.columns:
        return vector_to_array(F.col("probability"))[1]
    return vector_to_array(F.col("rawPrediction"))[1]


def _metrics(scored: DataFrame, name: str, at_k: tuple[int, ...]) -> dict:
    totals = scored.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum((F.col("label") == 1).cast("int")).alias("positives"),
    ).first()
    rows = int(totals["rows"])
    positives = 0 if totals["positives"] is None else int(totals["positives"])
    result: dict = {"model": name, "rows": rows, "positives": positives, "pr_auc": None, "roc_auc": None, "at_k": []}
    if rows == 0 or positives == 0 or positives == rows:
        result["note"] = "The test split does not contain both classes"
        return result
    scored = scored.withColumn("score", F.col("score").cast("double"))
    pr_auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="score", metricName="areaUnderPR")
    roc_auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="score", metricName="areaUnderROC")
    result["pr_auc"] = float(pr_auc.evaluate(scored))
    result["roc_auc"] = float(roc_auc.evaluate(scored))
    prevalence = positives / rows
    usable = [k for k in at_k if 0 < k <= rows]
    if not usable:
        return result
    top = [
        int(row["label"])
        for row in scored.orderBy(F.desc("score")).limit(max(usable)).select("label").collect()
    ]
    for k in usable:
        hits = int(sum(top[:k]))
        precision = hits / k
        result["at_k"].append(
            {
                "k": k,
                "precision": precision,
                "recall": hits / positives,
                "lift": precision / prevalence,
            }
        )
    return result


def _importance(prepared: DataFrame, model, limit: int = 15) -> list[dict]:
    metadata = prepared.schema["features_raw"].metadata
    groups = metadata.get("ml_attr", {}).get("attrs", {})
    names = {item["idx"]: item["name"] for group in groups.values() for item in group}
    weights = model.featureImportances
    ranked = sorted(
        ((names.get(index, str(index)), float(weights[index])) for index in range(len(weights))),
        key=lambda item: -item[1],
    )
    return [{"feature": name, "importance": round(weight, 4)} for name, weight in ranked[:limit]]


def _group_check(scored: DataFrame, group: str, k: int) -> list[dict]:
    if group not in scored.columns:
        return []
    rows = scored.count()
    if rows == 0:
        return []
    cutoff = scored.orderBy(F.desc("score")).limit(min(k, rows)).agg(F.min("score")).first()[0]
    flagged = scored.withColumn("flagged", (F.col("score") >= F.lit(float(cutoff))).cast("int"))
    summary = (
        flagged.groupBy(group)
        .agg(
            F.count(F.lit(1)).alias("taxpayers"),
            F.sum(F.col("label").cast("int")).alias("eligible"),
            F.sum("flagged").alias("flagged"),
            F.sum(F.col("label").cast("int") * F.col("flagged")).alias("caught"),
        )
        .orderBy(F.desc("eligible"))
        .collect()
    )
    records = []
    for row in summary:
        taxpayers = int(row["taxpayers"])
        eligible = 0 if row["eligible"] is None else int(row["eligible"])
        caught = 0 if row["caught"] is None else int(row["caught"])
        records.append(
            {
                "group": group,
                "value": None if row[group] is None else str(row[group]),
                "taxpayers": taxpayers,
                "eligible": eligible,
                "flagged": 0 if row["flagged"] is None else int(row["flagged"]),
                "caught": caught,
                "recall": None if eligible == 0 else round(caught / eligible, 3),
                "flag_rate_pct": round((0 if row["flagged"] is None else int(row["flagged"])) / taxpayers * 100, 3),
            }
        )
    return records


def _tier_cutoffs(scored: DataFrame, config: ProjectConfig) -> tuple[float, float]:
    high, medium = scored.approxQuantile("score", [config.high_quantile, config.medium_quantile], 0.0001)
    return float(high), float(medium)


def _tier_column(score, high: float, medium: float):
    return (
        F.when(score >= F.lit(high), F.lit("High"))
        .when(score >= F.lit(medium), F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )


def _save_models(model_dir: Path, preprocessor, models: dict, plan: dict, high: float, medium: float, serving: str) -> None:
    model_dir.mkdir(parents=True, exist_ok=True)
    _write_json(model_dir / "feature_plan.json", plan)
    _write_json(model_dir / "tier_cutoffs.json", {"high": high, "medium": medium, "model": serving})
    preprocessor.write().overwrite().save(str(model_dir / "preprocessing"))
    for name, model in models.items():
        model.write().overwrite().save(str(model_dir / name))


def score_frame(features: DataFrame, preprocessor, model, high: float, medium: float, model_name: str) -> DataFrame:
    transformed = model.transform(preprocessor.transform(features))
    score = _score_column(transformed)
    return transformed.select(
        F.col("TIN_NO").cast("string").alias("TIN_NO"),
        score.cast("double").alias("score"),
        _tier_column(score, high, medium).alias("tier"),
        F.lit(model_name).alias("model_name"),
    )


def load_scorer(model_dir: Path, serving_model: str):
    plan = json.loads((model_dir / "feature_plan.json").read_text())
    cutoffs = json.loads((model_dir / "tier_cutoffs.json").read_text())
    preprocessor = PipelineModel.load(str(model_dir / "preprocessing"))
    model = MODEL_LOADERS[serving_model].load(str(model_dir / serving_model))
    return plan, preprocessor, model, float(cutoffs["high"]), float(cutoffs["medium"])


def rank_taxpayers(raw: DataFrame, config: ProjectConfig) -> dict:
    """Deduplicate, check, train, and write scores. The grant rules are not changed."""
    if config.serving_model not in MODEL_LOADERS:
        raise ValueError(f"Unknown serving model {config.serving_model!r}")
    source = table_counts(raw)
    prepared = prepare_taxpayers(raw)
    labeled, label_source = with_label(
        prepared,
        assessment_date=config.assessment_date,
        rule_codes=config.cycle.rule_codes,
    )
    labeled = labeled.persist(StorageLevel.MEMORY_AND_DISK)
    profile = profile_taxpayers(labeled)
    profile["source"] = source
    profile["label_source"] = label_source
    plan = discover_features(
        labeled,
        strict=config.strict_features,
        max_cardinality=config.max_categorical_cardinality,
    )
    plan["label_source"] = label_source
    features = apply_features(labeled, plan)
    train, test = _split(features, config)
    preprocessor = _fit_preprocessor(train, plan["num_feats"], plan["cat_feats"])
    prepared_train = preprocessor.transform(train).cache()
    models = _train(prepared_train, config)
    test_features = preprocessor.transform(test).cache()
    metrics = []
    scored_test = {}
    for name, model in models.items():
        transformed = model.transform(test_features)
        score = _score_column(transformed)
        group_columns = [F.col(column) for column in ("REGION_NAME", "TAX_PAYER_TYPE") if column in transformed.columns]
        scored = transformed.select("label", "TIN_NO", score.alias("score"), *group_columns).cache()
        scored_test[name] = scored
        metrics.append(_metrics(scored, name, config.at_k))
    serving_scores = scored_test[config.serving_model]
    if serving_scores.limit(1).count() == 0:
        full = models[config.serving_model].transform(preprocessor.transform(features))
        serving_for_tiers = full.select(_score_column(full).alias("score"))
    else:
        serving_for_tiers = serving_scores
    high, medium = _tier_cutoffs(serving_for_tiers, config)
    importance = _importance(prepared_train, models["random_forest"])
    groups = _group_check(serving_scores, "REGION_NAME", config.group_check_k)
    groups.extend(_group_check(serving_scores, "TAX_PAYER_TYPE", config.group_check_k))
    _save_models(config.models, preprocessor, models, plan, high, medium, config.serving_model)
    scores = score_frame(features, preprocessor, models[config.serving_model], high, medium, config.serving_model)
    config.scores.parent.mkdir(parents=True, exist_ok=True)
    scores.select(*SCORE_COLUMNS).write.mode("overwrite").parquet(str(config.scores))
    tier_counts = {
        row["tier"]: int(row["count"])
        for row in scores.groupBy("tier").count().collect()
    }
    report = {
        "profile": profile,
        "features": {
            "excluded": plan["excluded"],
            "dropped_high_cardinality": plan["dropped_high_cardinality"],
            "numeric_inputs": plan["num_feats"],
            "categorical_inputs": plan["cat_feats"],
        },
        "metrics": metrics,
        "feature_importance": importance,
        "group_check": groups,
        "tiers": {"high": high, "medium": medium, "counts": tier_counts, "model": config.serving_model},
    }
    _write_json(config.reports / "rank_report.json", report)
    labeled.unpersist()
    prepared_train.unpersist()
    test_features.unpersist()
    for frame in scored_test.values():
        frame.unpersist()
    return {
        "serving_model": config.serving_model,
        "taxpayers": profile["taxpayers"],
        "eligible_share_pct": profile["eligible_share_pct"],
        "tier_counts": tier_counts,
        "pr_auc": next(item["pr_auc"] for item in metrics if item["model"] == config.serving_model),
        "models": str(config.models),
        "scores": str(config.scores),
        "report": str(config.reports / "rank_report.json"),
    }


def train(frame=None, config: ProjectConfig | None = None) -> dict:
    """Train the ranker when the taxpayer table is already on disk.

    A missing table returns an untrained result so a checkout can be tested
    before the lake extract is connected. The command-line entry raises instead.
    """
    config = config or ProjectConfig.load()
    if frame is None and not config.taxpayers.exists():
        return {"model_dir": str(config.models), "metrics": {"rows": 0}, "trained": False}
    if frame is None:
        from data_engineering.source import read_taxpayers
        from data_engineering.spark import job_session

        session = job_session("wht-train", delta=config.source_format == "delta")
        try:
            frame = read_taxpayers(session, str(config.taxpayers), config.source_format)
            summary = rank_taxpayers(frame, config)
        finally:
            session.stop()
    else:
        summary = rank_taxpayers(frame, config)
    return {
        "model_dir": summary["models"],
        "metrics": {"rows": summary["taxpayers"], "pr_auc": summary["pr_auc"]},
        "trained": True,
        **summary,
    }


def main() -> None:
    config = ProjectConfig.load()
    if not config.taxpayers.exists():
        raise SystemExit(f"Taxpayer table not found at {config.taxpayers}")
    summary = train(config=config)
    pr_auc = summary["pr_auc"]
    pr_text = "n/a" if pr_auc is None else f"{pr_auc:.4f}"
    print(
        f"Ranker {summary['serving_model']}: {summary['taxpayers']:,} taxpayers, "
        f"test PR-AUC {pr_text}, tiers {summary['tier_counts']}"
    )
    print(f"Scores: {summary['scores']}")
    print(f"Report: {summary['report']}")
    print("The score does not change the five-rule grant decision.")


if __name__ == "__main__":
    main()
