"""Copy the registered feature snapshot into the online store."""

from __future__ import annotations

import time

import pyarrow.dataset as ds

from data_engineering.config import ProjectConfig
from features.registry import materialize


def main() -> None:
    config = ProjectConfig.load()
    total = ds.dataset(str(config.features), format="parquet").count_rows() if config.features.exists() else 0
    started = time.monotonic()

    def progress(written: int) -> None:
        elapsed = time.monotonic() - started
        rate = written / elapsed if elapsed else 0
        print(f"  {written:,} of {total:,} TINs written ({rate:,.0f} per second)", flush=True)

    summary = materialize(config, progress=progress)
    print(f"Online store: {summary['online_store']}")
    print(f"Materialized {summary['rows']:,} TINs from {summary['start']} to {summary['end']}")


if __name__ == "__main__":
    main()
