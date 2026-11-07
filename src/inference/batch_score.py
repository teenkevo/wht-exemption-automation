"""Score every TIN with the saved ranker. The grant decision is unchanged.

Live scoring from an online feature store is not implemented. This job reads
the taxpayer table, applies the saved feature plan, and writes batch scores.
"""

from __future__ import annotations

from data_engineering.config import ProjectConfig
from data_engineering.preprocess import prepare_taxpayers
from data_engineering.source import read_taxpayers
from data_engineering.spark import job_session
from features.compute_user_feats import apply_features
from training.train import SCORE_COLUMNS, load_scorer, score_frame


def score(config: ProjectConfig | None = None, spark=None) -> dict:
    config = config or ProjectConfig.load()
    model_dir = config.models / config.serving_model
    if not model_dir.exists():
        raise FileNotFoundError(f"Trained ranker not found at {model_dir}")
    if not config.taxpayers.exists():
        raise FileNotFoundError(f"Taxpayer table not found at {config.taxpayers}")
    own_spark = spark is None
    session = spark or job_session("wht-score", delta=config.source_format == "delta")
    try:
        plan, preprocessor, model, high, medium = load_scorer(config.models, config.serving_model)
        prepared = prepare_taxpayers(read_taxpayers(session, str(config.taxpayers), config.source_format))
        features = apply_features(
            prepared,
            plan,
            assessment_date=config.assessment_date,
            rule_codes=config.cycle.rule_codes,
        )
        scored = score_frame(features, preprocessor, model, high, medium, config.serving_model)
        config.scores.parent.mkdir(parents=True, exist_ok=True)
        scored.select(*SCORE_COLUMNS).write.mode("overwrite").parquet(str(config.scores))
        counts = {
            row["tier"]: int(row["count"])
            for row in scored.groupBy("tier").count().collect()
        }
        return {"scores": str(config.scores), "model_name": config.serving_model, "tiers": counts}
    finally:
        if own_spark:
            session.stop()


def main() -> None:
    from termstyle import fields, heading, note

    config = ProjectConfig.load()
    if not (config.models / config.serving_model).exists():
        heading("Score")
        note("No trained ranker is available. Run make train first.")
        fields([("Scores", str(config.scores), "")])
        return
    summary = score(config)
    heading("Score")
    fields(
        [
            ("Model", summary["model_name"], "cyan"),
            ("Scores", summary["scores"], ""),
            ("Tiers", str(summary["tiers"]), ""),
        ]
    )
    print()
    note("The score does not change the grant decision.")


if __name__ == "__main__":
    main()
