"""Read the seven lake tables through Drill JDBC.

The lake machine does not mount ``/data/lakehouse``. Drill exposes those same
paths as ``dfs.root.`data/lakehouse/...` ``. Spark reads them with the Drill
JDBC driver, so the join stays in Spark and a second JVM is not started.

``WHT_LAKE_ROOT`` keeps remote mode on local Delta files. The password is
``DRILL_PASSWORD`` and is never read from this configuration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STORAGE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_PATH = re.compile(r"^[A-Za-z0-9_./-]+$")


@dataclass(frozen=True)
class DrillConnection:
    url: str
    driver: str
    storage: str
    jar: Path
    user: str
    password: str
    fetch_size: int


def drill_dbtable(storage: str, lake_path: str, columns: tuple[str, ...], limit: int | None = None) -> str:
    """A Spark JDBC subquery for one lake path.

    Drill folds unquoted names to lowercase, so each column is quoted. The
    path drops the leading slash: ``/data/lakehouse/dm_modeled/dim_taxpayer_mst``
    becomes ``dfs.root.`data/lakehouse/dm_modeled/dim_taxpayer_mst` ``.
    """
    if not _STORAGE.match(storage):
        raise ValueError(f"Drill storage must look like dfs.root, got {storage!r}")
    relative = lake_path.replace("\\", "/").lstrip("/")
    if not relative or ".." in relative.split("/") or not _PATH.match(relative):
        raise ValueError(f"Drill cannot read lake path {lake_path!r}")
    if not columns:
        raise ValueError("Drill query needs at least one column")
    bad = [name for name in columns if not _IDENT.match(name)]
    if bad:
        raise ValueError("Drill column names must be identifiers: " + ", ".join(bad))
    if limit is not None and limit < 1:
        raise ValueError("Drill limit must be at least 1")
    selected = ", ".join(f"`{name}`" for name in columns)
    bounded = f" LIMIT {limit}" if limit else ""
    return f"(SELECT {selected} FROM {storage}.`{relative}`{bounded}) AS src"


def open_drill(config) -> DrillConnection:
    """Check the Drill settings. The connection opens on the first read."""
    if not config.drill_url.startswith("jdbc:drill:"):
        raise ValueError("Drill url must start with jdbc:drill:")
    if not config.drill_driver:
        raise ValueError("Drill driver is empty")
    if not config.drill_user:
        raise ValueError("Set DRILL_USER to the Drill account")
    if not config.drill_password:
        raise ValueError("Set DRILL_PASSWORD to the Drill password")
    jar = config.drill_jar
    if jar is None or not jar.is_file():
        where = jar if jar is not None else "DRILL_JDBC_JAR"
        raise FileNotFoundError(
            f"Drill JDBC driver not found at {where}. "
            "Set DRILL_JDBC_JAR to drill-jdbc-all-1.22.0.jar"
        )
    if config.drill_fetch_size < 1:
        raise ValueError("Drill fetch_size must be at least 1")
    return DrillConnection(
        url=config.drill_url,
        driver=config.drill_driver,
        storage=config.drill_storage,
        jar=jar,
        user=config.drill_user,
        password=config.drill_password,
        fetch_size=config.drill_fetch_size,
    )


def read_drill_table(
    spark,
    connection: DrillConnection,
    lake_path: str,
    columns: tuple[str, ...],
    *,
    limit: int | None = None,
) -> DataFrame:
    """Read the named columns from one lake table."""
    query = drill_dbtable(connection.storage, lake_path, columns, limit)
    return (
        spark.read.format("jdbc")
        .option("url", connection.url)
        .option("driver", connection.driver)
        .option("dbtable", query)
        .option("user", connection.user)
        .option("password", connection.password)
        .option("fetchsize", str(connection.fetch_size))
        .load()
    )


def main() -> None:
    """Read one registration row so the Drill connection can be checked."""
    from data_engineering import load_yaml
    from data_engineering.config import ProjectConfig
    from data_engineering.load import _SOURCE_COLUMNS
    from data_engineering.spark import job_session
    from termstyle import fields, heading

    config = ProjectConfig.load()
    connection = open_drill(config)
    sources = load_yaml("config.yaml").get("remote_sources") or {}
    path = str(sources.get("reg") or "")
    if not path:
        raise SystemExit("remote_sources.reg is not set")
    session = job_session("wht-drill", jars=(str(connection.jar),))
    try:
        frame = read_drill_table(session, connection, path, _SOURCE_COLUMNS["reg"], limit=1)
        count = frame.count()
    finally:
        session.stop()
    table = drill_dbtable(connection.storage, path, _SOURCE_COLUMNS["reg"], limit=1)
    heading("Drill")
    fields(
        [
            ("URL", connection.url, ""),
            ("Table", table, ""),
            ("Columns", str(len(_SOURCE_COLUMNS["reg"])), ""),
            ("Rows", f"{count:,}", "green" if count else ""),
        ]
    )


if __name__ == "__main__":
    main()
