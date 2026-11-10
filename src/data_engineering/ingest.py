"""Build the WHT taxpayer table from the seven lakehouse sources."""

from __future__ import annotations

import json

from data_engineering.config import ProjectConfig
from data_engineering.drill import open_drill, read_drill_table
from data_engineering.load import (
    COVERAGE_FIELDS,
    _SOURCE_COLUMNS,
    assemble,
    duplicate_report,
    qa_counts,
    read_sources,
    read_table,
    write_base,
)
from data_engineering.spark import job_session


def _reader(source_format: str):
    def reader(spark, path: str):
        return read_table(spark, path, source_format)

    return reader


def _drill_reader(config: ProjectConfig, connection):
    names = {str(path): name for name, path in config.source_paths.items()}

    def reader(spark, path: str):
        return read_drill_table(spark, connection, path, _SOURCE_COLUMNS[names[str(path)]])

    return reader


def _write_summary(summary: dict, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2) + "\n")


def run(config: ProjectConfig | None = None, spark=None, *, write: bool = True) -> dict:
    """Join the seven sources, label each TIN with the checklist, and write the saved taxpayer table."""
    config = config or ProjectConfig.load()
    table_format = config.source_format.strip().lower()
    if not config.source_paths:
        raise ValueError("Ingest needs the seven source tables. Set WHT_MODE=remote to read them from the lake")
    drill = None
    if config.lake_access == "drill":
        drill = open_drill(config)
        read_format = "drill"
    else:
        read_format = (config.lake_format if config.mode == "remote" else config.source_format).strip().lower()
    if read_format == "parquet":
        missing = [f"{name} ({path})" for name, path in config.source_paths.items() if not path.exists()]
        if missing:
            raise FileNotFoundError("Taxpayer source not found: " + "; ".join(missing))

    own_spark = spark is None
    jars = (str(drill.jar),) if drill is not None else ()
    session = spark or job_session(
        "wht-ingest",
        delta=read_format == "delta" or table_format == "delta",
        jars=jars,
    )
    try:
        reader = _drill_reader(config, drill) if drill is not None else _reader(read_format)
        sources = read_sources(session, config.source_paths, reader)
        duplicates = duplicate_report(sources)
        built = assemble(
            sources,
            dedupe_sector=config.dedupe_sector,
            assessment_date=config.assessment_date,
            rule_codes=config.cycle.rule_codes,
        )
        counts = qa_counts(built)
        if counts["total_rows"] == 0:
            raise ValueError("The taxpayer load produced no rows")
        if write:
            write_base(built, str(config.taxpayers), table_format)
            saved = qa_counts(read_table(session, str(config.taxpayers), table_format))
            if saved["total_rows"] != counts["total_rows"]:
                raise RuntimeError("Written taxpayer table row count does not match the built table")
            counts = saved
        summary = {
            **counts,
            "duplicate_source_tins": duplicates,
            "dedupe_sector": config.dedupe_sector,
            "lake_access": config.lake_access,
            "cycle": config.cycle.id,
            "assessment_date": config.assessment_date.isoformat(),
            "output": str(config.taxpayers),
            "written": write,
        }
        if write:
            _write_summary(summary, config.load_summary)
        return summary
    finally:
        if own_spark:
            session.stop()


def main() -> None:
    from termstyle import fields, heading, note

    config = ProjectConfig.load()
    if config.mode == "local":
        if not config.taxpayers.exists():
            raise SystemExit(f"Joined taxpayer table not found at {config.taxpayers}")
        heading("Ingest")
        note("Local mode reads the joined table")
        fields([("Table", str(config.taxpayers), "")])
        return
    summary = run(config)
    duplicates = summary["duplicate_source_tins"]
    heading("Ingest")
    fields(
        [
            ("Rows", f"{summary['total_rows']:,}", ""),
            ("Unique TINs", f"{summary['unique_tins']:,}", ""),
            ("Eligible", f"{summary['eligible_count']:,}", "green"),
            ("Registration duplicates", f"{duplicates['reg']:,}", ""),
            ("Sector duplicates", f"{duplicates['sector']:,}", ""),
            ("Table", summary["output"], "cyan"),
            ("Access", summary["lake_access"], ""),
        ]
    )
    print()
    note("Coverage")
    fields([(name, f"{summary[name]:,}", "") for name in COVERAGE_FIELDS])


if __name__ == "__main__":
    main()
