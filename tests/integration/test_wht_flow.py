"""Ingest, grant, and ranker on a small taxpayer extract."""

import json
from dataclasses import replace

import pandas as pd
import pyarrow.dataset as ds

from data_engineering.config import ProjectConfig, SOURCE_NAMES
from data_engineering.ingest import run as ingest
from features.build import build as build_features
from data_engineering.load import REG_COLS
from inference.grant import run as grant
from training.train import rank_taxpayers


def _reg(tin: str, registration: str, status: str, started: str = "2018-06-01") -> dict:
    row = {column: "N" for column in REG_COLS}
    row.update(
        {
            "TIN_NO": tin,
            "TAX_PAYER_TYPE": "COMPANY",
            "REG_STATUS": registration,
            "ORG_TYPE": "COMPANY",
            "taxpayer_status": status,
            "BUSINESS_START_DT": started,
            "STATION_NAME": "KAMPALA",
            "REGION_NAME": "CENTRAL",
            "BUSINESS_FISCAL_YEAR": "JUL-JUN",
            "EMAIL": "hidden@example.com",
        }
    )
    return row


def _sources(spark, root):
    frames = {
        "reg": pd.DataFrame(
            [
                _reg("PIPE-TIN-0001", "REGD", "Existing Taxpayer"),
                _reg("PIPE-TIN-0002", "REGD", "Existing Taxpayer", "2016-01-01"),
            ]
        ),
        "ledger": pd.DataFrame(
            [
                {"TPIN": "PIPE-TIN-0001", "OUTSTANDING_LIABILITY": 10.0, "TOTAL_WAIVER": 1.0},
                {"TPIN": "PIPE-TIN-0001", "OUTSTANDING_LIABILITY": -10.0, "TOTAL_WAIVER": 2.0},
            ]
        ),
        "payments": pd.DataFrame(
            [
                {"TIN_NO": "PIPE-TIN-0001", "TOT_PAYMENT_AMT": 60_000_000.0},
                {"TIN_NO": "PIPE-TIN-0001", "TOT_PAYMENT_AMT": 90_000_000.0},
            ]
        ),
        "efris": pd.DataFrame(
            [{"sellertin": "PIPE-TIN-0001", "sales_amount": 4.0, "vat_charged": 1.0}]
        ),
        "sector": pd.DataFrame(
            [
                {"tin_no": "PIPE-TIN-0001", "activity_isic4_cd": "9999", "updated_dt": "2020-01-01"},
                {"tin_no": "PIPE-TIN-0001", "activity_isic4_cd": "1111", "updated_dt": "2024-06-01"},
                {"tin_no": "PIPE-TIN-0002", "activity_isic4_cd": "2222", "updated_dt": "2021-01-01"},
            ]
        ),
        "customs": pd.DataFrame(
            [
                {"taxpayer_tin": "PIPE-TIN-0001", "cif_amt_ugx": 1000.0, "is_red_flg": 1},
                {"taxpayer_tin": "GHOST-TIN", "cif_amt_ugx": 5.0, "is_red_flg": 0},
            ]
        ),
        "returns": pd.DataFrame([{"TAXPAYER_TIN": "PIPE-TIN-0001", "net_tax": 12.0}]),
    }
    paths = {}
    for name in SOURCE_NAMES:
        path = root / name
        spark.createDataFrame(frames[name]).write.mode("overwrite").parquet(str(path))
        paths[name] = path
    return paths


def test_ingest_feeds_the_grant(spark, tmp_path):
    config = replace(
        ProjectConfig.load(),
        source_format="parquet",
        source_paths=_sources(spark, tmp_path / "sources"),
        taxpayers=tmp_path / "wht_base_v1",
        load_summary=tmp_path / "load_summary.json",
        grants=tmp_path / "grants",
        grant_summary=tmp_path / "grant_summary.json",
    )
    loaded = ingest(config, spark=spark)
    assert "PIPE-TIN" not in (tmp_path / "load_summary.json").read_text()
    assert loaded["total_rows"] == 2
    assert loaded["eligible_count"] == 1
    assert loaded["duplicate_source_tins"] == {"reg": 0, "sector": 1}

    table = spark.read.parquet(str(config.taxpayers))
    assert "EMAIL" not in table.columns
    eligible = table.filter(table.TIN_NO == "PIPE-TIN-0001").collect()[0]
    assert eligible.activity_isic4_cd == "1111"
    assert float(eligible.wht_eligible) == 1.0
    assert eligible.is_registered == 1 and eligible.has_filed_returns == 1
    assert table.filter(table.TIN_NO == "GHOST-TIN").count() == 0

    summary = grant(config, spark=spark)
    assert summary["taxpayer_count"] == 2
    assert summary["eligible"] == 1
    assert "PIPE-TIN" not in (tmp_path / "grant_summary.json").read_text()
    decisions = ds.dataset(str(config.grants), format="parquet").to_table().to_pandas().set_index("TIN_NO")
    assert decisions.loc["PIPE-TIN-0001", "decision"] == "ELIGIBLE"
    assert decisions.loc["PIPE-TIN-0002", "decision"] == "NOT ELIGIBLE"
    assert decisions.loc["PIPE-TIN-0002", "failed_rules"] == "RETURNS_FILED,MIN_TAX_COLLECTED"
    assert decisions.loc["PIPE-TIN-0002", "rule_min_tax_collected"] == "FAIL"
    assert decisions.loc["PIPE-TIN-0001", "tax_collected"] == 150_000_000.0
    assert decisions.loc["PIPE-TIN-0001", "rules_applied"].startswith("REGISTERED:v1,")

    featured = replace(
        config,
        features=tmp_path / "features",
        feature_plan=tmp_path / "feature_plan.json",
        feast_registry=tmp_path / "registry.db",
        online_store="sqlite",
        online_connection=str(tmp_path / "online_store.db"),
    )
    snapshot = build_features(featured, spark=spark)
    assert snapshot["rows"] == 2
    assert (tmp_path / "registry.db").exists()
    assert "TIN_NO" not in snapshot["registered_features"]
    assert "log_total_tax_collected" not in snapshot["registered_features"]
    plan = json.loads((tmp_path / "feature_plan.json").read_text())
    assert "TIN_NO" in plan["excluded"]
    assert "log_total_tax_collected" not in plan["num_feats"]
    assert "PIPE-TIN" not in (tmp_path / "feature_plan.json").read_text()


def _rank_rows(n: int = 32) -> pd.DataFrame:
    rows = []
    for i in range(n):
        eligible = i % 4 == 0
        rows.append(
            {
                "TIN_NO": f"DEMO-RANK-{i:04d}",
                "REG_STATUS": "REGD" if eligible else "CNCL",
                "taxpayer_status": "Existing Taxpayer" if eligible else "New Taxpayer",
                "total_outstanding_liability": 0.0 if eligible else 500.0,
                "total_returns_count": 3.0 if eligible else 0.0,
                "total_tax_collected": 150_000_000.0 if eligible else 1_000.0,
                "activity_isic4_cd": f"{4711 + (i % 3)}",
                "BUSINESS_START_DT": f"{2000 + (i % 12):04d}-06-01",
                "REGION_NAME": "DEMO-NORTH" if i % 2 == 0 else "DEMO-SOUTH",
                "TAX_PAYER_TYPE": "DEMO-A" if eligible else "DEMO-B",
                "total_customs_value": float(5000 + i) if eligible else float(i),
                "customs_entry_count": float(4 + (i % 3)) if eligible else 0.0,
                "free_text": f"note-{i}",
                "wht_eligible": 1 if eligible else 0,
            }
        )
    return pd.DataFrame(rows)


def test_ranker_hides_rule_inputs(spark, tmp_path):
    config = replace(
        ProjectConfig.load(),
        strict_features=True,
        max_categorical_cardinality=8,
        random_forest_trees=4,
        random_forest_depth=3,
        logistic_max_iter=10,
        gbt_max_iter=4,
        gbt_max_depth=2,
        at_k=(5,),
        group_check_k=5,
        models=tmp_path / "models",
        scores=tmp_path / "scores",
        reports=tmp_path / "reports",
    )
    raw = spark.createDataFrame(_rank_rows())
    summary = rank_taxpayers(raw, config)
    report = json.loads((config.reports / "rank_report.json").read_text())
    assert "DEMO-RANK-" not in json.dumps(report)
    assert "log_total_tax_collected" not in report["features"]["numeric_inputs"]
    assert "log_total_customs_value" in report["features"]["numeric_inputs"]
    assert summary["serving_model"] == "random_forest"
    assert set(summary["tier_counts"]) <= {"High", "Medium", "Low"}
    saved = ds.dataset(str(config.scores), format="parquet").to_table().to_pandas()
    assert set(saved.columns) == {"TIN_NO", "score", "tier", "model_name"}
    assert "decision" not in saved.columns
