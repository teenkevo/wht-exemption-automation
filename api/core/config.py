"""Read config/config.yaml from the pipeline root."""

from __future__ import annotations

from data_engineering import load_yaml


def load_config() -> dict:
    return load_yaml("config.yaml")
