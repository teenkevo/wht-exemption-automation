"""Training waits until the taxpayer table exists."""

from dataclasses import replace

from data_engineering.config import ProjectConfig
from training.train import train


def test_training_waits_for_the_taxpayer_table(tmp_path):
    config = replace(ProjectConfig.load(), taxpayers=tmp_path / "missing")
    result = train(config=config)
    assert result["trained"] is False
    assert result["metrics"] == {"rows": 0}
    assert result["model_dir"].endswith("artifacts/models")
