"""Score one taxpayer with the trained ranker.

``serving.model_name`` names the model in ``artifacts/models``. The saved Spark
pipeline is loaded once into a single-core local session and reused for every
request, so the online score matches the batch score. The score does not change
the grant decision.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

from api.core.config import load_config
from data_engineering.config import ProjectConfig


class ModelNotReady(RuntimeError):
    """No trained model is configured for serving."""


@dataclass
class ModelService:
    model_name: str | None
    model_dir: Path
    _scorer: tuple | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @property
    def ready(self) -> bool:
        return bool(self.model_name) and (self.model_dir / str(self.model_name)).exists()

    @classmethod
    def load(cls) -> ModelService:
        name = load_config()["serving"]["model_name"]
        return cls(
            model_name=None if name in (None, "") else str(name),
            model_dir=ProjectConfig.load().models,
        )

    def list_models(self) -> list[str]:
        if self.model_name is None:
            return []
        return [self.model_name]

    def _load(self) -> tuple:
        with self._lock:
            if self._scorer is not None:
                return self._scorer
            if not self.model_name:
                raise ModelNotReady("No model is set in serving.model_name")
            if not self.ready:
                raise ModelNotReady(f"Trained model not found at {self.model_dir / self.model_name}. Run make train")
            from data_engineering.spark import spark_session
            from training.train import MODEL_LOADERS, load_scorer

            if self.model_name not in MODEL_LOADERS:
                raise ModelNotReady(f"Unknown model {self.model_name!r}")
            session = spark_session(
                "wht-predict",
                master="local[1]",
                configs={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "1"},
            )
            plan, preprocessor, model, high, medium = load_scorer(self.model_dir, self.model_name)
            self._scorer = (session, plan, preprocessor, model, high, medium)
            return self._scorer

    def warm(self) -> None:
        self._load()

    def close(self) -> None:
        with self._lock:
            if self._scorer is not None:
                self._scorer[0].stop()
                self._scorer = None

    def predict(self, entity_id: str, row: dict) -> dict:
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        from training.train import score_frame

        session, plan, preprocessor, model, high, medium = self._load()
        numeric = list(plan["num_feats"])
        text = [*plan["cat_feats"], *plan["passthrough"]]
        schema = StructType(
            [StructField("TIN_NO", StringType())]
            + [StructField(name, DoubleType()) for name in numeric]
            + [StructField(name, StringType()) for name in text]
        )
        values = (
            [entity_id]
            + [0.0 if row.get(name) is None else float(row[name]) for name in numeric]
            + ["UNK" if row.get(name) is None else str(row[name]) for name in text]
        )
        frame = session.createDataFrame([values], schema=schema)
        scored = score_frame(frame, preprocessor, model, high, medium, self.model_name).first()
        return {"score": float(scored["score"]), "tier": str(scored["tier"])}
