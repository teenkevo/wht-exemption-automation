"""WHT exemption settings for this pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from data_engineering import load_yaml, resolve_path
from rules_engine.catalog import default_registry
from rules_engine.cycles import Cycle, assessment_date, resolve_cycle

SOURCE_NAMES = (
    "reg",
    "ledger",
    "payments",
    "efris",
    "sector",
    "customs",
    "returns",
)

# Prefix of every path in remote_sources. WHT_LAKE_ROOT replaces it for a local lake.
LAKE_PREFIX = "/data/lakehouse"


def lake_relative(path: str) -> str:
    """The part of a remote source path that follows ``/data/lakehouse``."""
    posix = Path(path).as_posix().rstrip("/")
    prefix = LAKE_PREFIX.rstrip("/")
    if not posix.startswith(prefix + "/"):
        raise ValueError(f"{posix} is outside {prefix}")
    return posix[len(prefix) + 1 :]


def _drill_file() -> dict:
    from data_engineering import REPO_ROOT

    if not (REPO_ROOT / "config" / "drill.yaml").exists():
        return {}
    loaded = load_yaml("drill.yaml")
    if "password" in loaded:
        raise ValueError("config/drill.yaml must not contain a password. Set DRILL_PASSWORD")
    return loaded


def _lake_access(configured: object) -> str:
    """``file`` reads Delta. ``drill`` reads the same paths through Drill JDBC.

    A local mock lake forces ``file``. ``WHT_LAKE_ACCESS`` overrides the file.
    """
    if os.getenv("WHT_LAKE_ROOT", "").strip():
        return "file"
    choice = (os.getenv("WHT_LAKE_ACCESS") or str(configured or "file")).strip().lower()
    if choice not in {"file", "drill"}:
        raise ValueError("lake_access must be file or drill")
    return choice


def _source_paths(raw: object) -> dict[str, Path]:
    if not isinstance(raw, dict):
        raise ValueError("sources must be a mapping of source name to path")
    missing = [name for name in SOURCE_NAMES if not raw.get(name)]
    unknown = sorted(set(raw) - set(SOURCE_NAMES))
    if missing or unknown:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError("WHT sources: " + "; ".join(details))
    paths = {name: resolve_path(str(raw[name])) for name in SOURCE_NAMES}
    root = os.getenv("WHT_LAKE_ROOT", "").strip()
    if not root:
        return paths
    base = resolve_path(root)
    return {name: base / lake_relative(path.as_posix()) for name, path in paths.items()}


@dataclass(frozen=True)
class ProjectConfig:
    project: str
    entity_key: str
    mode: str
    source_format: str
    lake_format: str
    lake_access: str
    drill_url: str
    drill_driver: str
    drill_storage: str
    drill_jar: Path | None
    drill_user: str
    drill_password: str
    drill_fetch_size: int
    dedupe_sector: bool
    source_paths: dict[str, Path]
    cycle: Cycle
    assessment_date: date
    strict_features: bool
    max_categorical_cardinality: int
    split_seed: int
    train_fraction: float
    negative_ratio: float
    serving_model: str
    random_forest_trees: int
    random_forest_depth: int
    logistic_reg_param: float
    logistic_max_iter: int
    gbt_max_iter: int
    gbt_max_depth: int
    gbt_step_size: float
    at_k: tuple[int, ...]
    group_check_k: int
    high_quantile: float
    medium_quantile: float
    taxpayers: Path
    features: Path
    feature_plan: Path
    feast_registry: Path
    online_store: str
    online_connection: str
    grants: Path
    grant_summary: Path
    load_summary: Path
    models: Path
    scores: Path
    reports: Path

    @classmethod
    def load(cls) -> ProjectConfig:
        data = load_yaml("config.yaml")
        paths = data.get("paths") or {}
        load = data.get("load") or {}
        training = data.get("training") or {}
        forest = training.get("random_forest") or {}
        logistic = training.get("logistic_regression") or {}
        gbt = training.get("gbt") or {}
        evaluation = data.get("evaluation") or {}
        tiers = data.get("tiers") or {}
        cycle = resolve_cycle(data, known_rules=set(default_registry().codes()))
        online = data.get("online_store") or {}
        online_type = str(online.get("type", "redis")).strip().lower()
        if online_type not in {"redis", "sqlite"}:
            raise ValueError("online_store.type must be redis or sqlite")
        if online_type == "redis":
            online_connection = os.getenv("REDIS_URL") or str(online.get("connection", "localhost:6379"))
        else:
            online_connection = str(resolve_path(str(online.get("path", "data/online_store.db"))))
        mode = os.getenv("WHT_MODE", "local").strip().lower()
        if mode not in {"local", "remote"}:
            raise ValueError("WHT_MODE must be local or remote")
        # Remote reads the seven lake tables. Local skips them and reads the saved join.
        # A mock root forces Delta files. Otherwise lake_access chooses Drill or Delta.
        access = _lake_access(data.get("lake_access", "file")) if mode == "remote" else "file"
        drill = _drill_file()
        jar_value = (os.getenv("DRILL_JDBC_JAR") or str(drill.get("jar") or "")).strip()
        source_paths = _source_paths(data.get("remote_sources") or {}) if mode == "remote" else {}
        taxpayers = paths.get("taxpayers") or paths.get("source") or "data/delta/wht_base_v1"
        return cls(
            project=str(data.get("project", "wht_exemption")),
            entity_key=str(data.get("entity_key", "TIN_NO")),
            mode=mode,
            source_format=str(data.get("source_format", "parquet")),
            lake_format=str(data.get("lake_format", "delta")),
            lake_access=access,
            drill_url=os.getenv("DRILL_URL") or str(drill.get("url") or ""),
            drill_driver=str(drill.get("driver") or "org.apache.drill.jdbc.Driver"),
            drill_storage=str(drill.get("storage") or "dfs.root"),
            drill_jar=resolve_path(jar_value) if jar_value else None,
            drill_user=os.getenv("DRILL_USER") or str(drill.get("user") or ""),
            drill_password=os.getenv("DRILL_PASSWORD") or "",
            drill_fetch_size=int(drill.get("fetch_size", 10000)),
            dedupe_sector=bool(load.get("dedupe_sector", True)),
            source_paths=source_paths,
            cycle=cycle,
            assessment_date=assessment_date(cycle),
            strict_features=bool(training.get("strict_features", True)),
            max_categorical_cardinality=int(training.get("max_categorical_cardinality", 60)),
            split_seed=int(training.get("seed", 42)),
            train_fraction=float(training.get("train_fraction", 0.8)),
            negative_ratio=float(training.get("negative_ratio", 20)),
            serving_model=str(training.get("serving_model", "random_forest")),
            random_forest_trees=int(forest.get("num_trees", 100)),
            random_forest_depth=int(forest.get("max_depth", 8)),
            logistic_reg_param=float(logistic.get("reg_param", 0.01)),
            logistic_max_iter=int(logistic.get("max_iter", 50)),
            gbt_max_iter=int(gbt.get("max_iter", 100)),
            gbt_max_depth=int(gbt.get("max_depth", 5)),
            gbt_step_size=float(gbt.get("step_size", 0.1)),
            at_k=tuple(int(value) for value in evaluation.get("at_k", [500, 1000, 2000, 5000, 10000])),
            group_check_k=int(evaluation.get("group_check_k", 5000)),
            high_quantile=float(tiers.get("high_quantile", 0.999)),
            medium_quantile=float(tiers.get("medium_quantile", 0.995)),
            taxpayers=resolve_path(str(taxpayers)),
            features=resolve_path(str(paths.get("features", "data/features"))),
            feature_plan=resolve_path(str(paths.get("feature_plan", "artifacts/reports/feature_plan.json"))),
            feast_registry=resolve_path(str(paths.get("feast_registry", "data/registry.db"))),
            online_store=online_type,
            online_connection=online_connection,
            grants=resolve_path(str(paths.get("grants", "data/grants/wht_grants.parquet"))),
            grant_summary=resolve_path(str(paths.get("grant_summary", "artifacts/wht_grant_summary.json"))),
            load_summary=resolve_path(str(paths.get("load_summary", "artifacts/wht_load_summary.json"))),
            models=resolve_path(str(paths.get("models", "artifacts/models"))),
            scores=resolve_path(str(paths.get("scores", "data/scores/wht_scores.parquet"))),
            reports=resolve_path(str(paths.get("reports", "artifacts/reports"))),
        )
