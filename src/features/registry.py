"""Register the feature snapshot in Feast and materialize it online.

Registration stores the view definition. Materialization copies each TIN's
latest values from the parquet snapshot into the online store.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds
from feast import FeatureStore
from feast.repo_config import RepoConfig

from data_engineering import REPO_ROOT
from data_engineering.config import ProjectConfig
from feature_store.features import feature_names, feature_view_for_plan, taxpayer

VIEW_NAME = "taxpayer_features"


def online_store_settings(config: ProjectConfig) -> dict:
    if config.online_store == "redis":
        return {"type": "redis", "connection_string": config.online_connection}
    Path(config.online_connection).parent.mkdir(parents=True, exist_ok=True)
    return {"type": "sqlite", "path": config.online_connection}


def describe_online_store(config: ProjectConfig) -> str:
    if config.online_store == "redis":
        return f"redis at {config.online_connection.split(',')[0]}"
    return f"sqlite at {config.online_connection}"


def check_online_store(config: ProjectConfig) -> None:
    """Fail early with a clear message when Redis is not reachable."""
    if config.online_store != "redis":
        return
    import redis

    address, *options = config.online_connection.split(",")
    host, _, port = address.partition(":")
    settings = dict(option.split("=", 1) for option in options if "=" in option)
    client = redis.Redis(
        host=host or "localhost",
        port=int(port or 6379),
        password=settings.get("password"),
        ssl=settings.get("ssl", "false").lower() == "true",
        socket_connect_timeout=3,
    )
    try:
        client.ping()
    except redis.exceptions.ConnectionError as exc:
        raise ConnectionError(
            f"Redis is not reachable at {address}. Start it with: make redis-up"
        ) from exc
    finally:
        client.close()


def open_store(config: ProjectConfig) -> FeatureStore:
    config.feast_registry.parent.mkdir(parents=True, exist_ok=True)
    repo = RepoConfig(
        project=config.project,
        provider="local",
        registry=str(config.feast_registry),
        online_store=online_store_settings(config),
        offline_store={"type": "file"},
        entity_key_serialization_version=3,
        repo_path=REPO_ROOT / "feature_store",
    )
    return FeatureStore(config=repo)


def register(config: ProjectConfig | None = None) -> dict:
    """Apply the feature plan to the Feast registry."""
    config = config or ProjectConfig.load()
    plan = _load_plan(config.feature_plan)
    if not config.features.exists():
        raise FileNotFoundError(f"Feature snapshot not found at {config.features}")
    names = feature_names(plan)
    view = feature_view_for_plan(plan, config.features)
    _require_columns(config.features, names)
    check_online_store(config)
    store = open_store(config)
    store.apply([taxpayer, view], partial=False)
    return {
        "registry": str(config.feast_registry),
        "online_store": describe_online_store(config),
        "feature_view": view.name,
        "registered_features": names,
    }


BATCH_ROWS = 100_000


def materialize(
    config: ProjectConfig | None = None,
    *,
    batch_rows: int = BATCH_ROWS,
    progress: Callable[[int], None] | None = None,
) -> dict:
    """Copy the registered snapshot into the online store, one batch of TINs at a time.

    Feast's own ``materialize`` loads the whole snapshot into memory before it
    writes. Streaming the parquet in batches keeps memory flat.
    """
    config = config or ProjectConfig.load()
    if not config.feast_registry.exists():
        raise FileNotFoundError(
            f"Feature registry not found at {config.feast_registry}. Register the snapshot first."
        )
    if not config.features.exists():
        raise FileNotFoundError(f"Feature snapshot not found at {config.features}")
    start, end = snapshot_window(config.features)
    check_online_store(config)
    store = open_store(config)
    view = store.get_feature_view(VIEW_NAME)
    columns = ["TIN_NO", "event_timestamp", *(field.name for field in view.features)]
    dataset = ds.dataset(str(config.features), format="parquet")
    written = 0
    for batch in dataset.to_batches(columns=columns, batch_size=batch_rows):
        if batch.num_rows == 0:
            continue
        store.write_to_online_store(VIEW_NAME, batch.to_pandas())
        written += batch.num_rows
        if progress is not None:
            progress(written)
    store.registry.apply_materialization(view, store.project, start, end)
    return {
        "online_store": describe_online_store(config),
        "rows": written,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def snapshot_window(path: Path) -> tuple[datetime, datetime]:
    column = ds.dataset(str(path), format="parquet").to_table(columns=["event_timestamp"]).column(
        "event_timestamp"
    )
    column = column.combine_chunks()
    if len(column) == 0 or pc.all(pc.is_null(column)).as_py():
        raise ValueError(f"Feature snapshot at {path} has no event timestamps")
    start = _aware(pc.min(column).as_py()) - timedelta(seconds=1)
    end = _aware(pc.max(column).as_py()) + timedelta(seconds=1)
    return start, end


def _load_plan(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Feature plan not found at {path}")
    loaded = json.loads(path.read_text())
    if not isinstance(loaded, dict) or "num_feats" not in loaded:
        raise ValueError(f"Feature plan at {path} is missing num_feats")
    return loaded


def _require_columns(path: Path, names: list[str]) -> None:
    present = set(ds.dataset(str(path), format="parquet").schema.names)
    required = ["TIN_NO", "event_timestamp", *names]
    missing = [name for name in required if name not in present]
    if missing:
        raise ValueError("Feature snapshot is missing columns: " + ", ".join(missing))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
