from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
import shutil
import uuid

import polars as pl

import config
import core.duckdb_query as duckdb_query_module
import core.pipeline_builder as pipeline_builder_module
import core.quality_reporter as quality_reporter_module
import core.tushare_fetcher as tushare_fetcher_module
import core.utils as utils_module
import init_history as init_history_module
import run_daily_update as run_daily_update_module
from core.data_cleaner import clean_table, deduplicate_by_primary_key
from core.quality_reporter import QualityReporter
from core.utils import partition_path, temp_partition_path


def workspace_tmp_dir() -> Path:
    path = Path("state") / "test_runs" / f"unit_{uuid.uuid4().hex[:8]}"
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=False)
    return path


def patch_settings(monkeypatch, tmp_path: Path):
    data_root = tmp_path / "data"
    settings = replace(
        config.SETTINGS,
        project_root=tmp_path,
        data_root=data_root,
        logs_root=tmp_path / "logs",
        state_root=tmp_path / "state",
        reports_root=tmp_path / "reports",
        temp_root=tmp_path / "_tmp",
        layers={
            "ods": data_root / "1_ods_raw",
            "dwd": data_root / "2_dwd_detail",
            "dws": data_root / "3_dws_summary",
        },
    )
    for module in (
        config,
        utils_module,
        quality_reporter_module,
        pipeline_builder_module,
        duckdb_query_module,
        tushare_fetcher_module,
        init_history_module,
        run_daily_update_module,
    ):
        monkeypatch.setattr(module, "SETTINGS", settings, raising=False)
    return settings


def test_primary_key_dedup_prefers_more_complete_row():
    frame = pl.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": [date(2025, 1, 3), date(2025, 1, 3)],
            "close": [None, 10.5],
            "vol": [1000.0, 1000.0],
        }
    )
    result = deduplicate_by_primary_key(frame, ("ts_code", "trade_date"))
    assert result.height == 1
    assert result.select("close").item() == 10.5


def test_financial_cleaning_filters_missing_ann_date_and_builds_real_ann_date():
    frame = pl.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20250106", None],
            "f_ann_date": ["20250105", "20250107"],
            "end_date": ["20241231", "20241231"],
            "report_type": ["1", "1"],
            "comp_type": ["1", "1"],
            "update_flag": ["0", "0"],
            "roe": ["12.34", "11.11"],
        }
    )
    cleaned = clean_table("fina_indicator", frame)
    assert cleaned.frame.height == 1
    assert cleaned.frame.select("real_ann_date").item().isoformat() == "2025-01-05"
    assert cleaned.frame.select("source_table").item() == "fina_indicator"


def test_daily_cleaning_casts_dates_and_numeric_strings():
    frame = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20250103"],
            "close": ["10.50"],
            "vol": ["1000"],
        }
    )
    cleaned = clean_table("daily", frame)
    assert cleaned.frame.schema["trade_date"] == pl.Date
    assert cleaned.frame.schema["close"] == pl.Float64
    assert cleaned.frame.schema["vol"] == pl.Float64


def test_partition_path_generation(monkeypatch):
    tmp_path = workspace_tmp_dir()
    patch_settings(monkeypatch, tmp_path)
    assert str(partition_path("dwd", "daily", 2024)).endswith("data\\2_dwd_detail\\daily\\year=2024")
    assert "year=2024__" in str(temp_partition_path("dwd", "daily", 2024))


def test_quality_report_writes_json_and_markdown(monkeypatch):
    tmp_path = workspace_tmp_dir()
    patch_settings(monkeypatch, tmp_path)
    reporter = QualityReporter(run_mode="unit_test", started_at=datetime(2025, 1, 7, 15, 0, 0))
    reporter.add_fetch_stats(
        "daily",
        fetched_rows=10,
        request_count=2,
        success_count=2,
        retry_count=1,
    )
    reporter.add_clean_stats(
        "daily",
        cleaned_rows=9,
        rows_before_dedup=10,
        rows_after_dedup=9,
        null_counts={"close": 0},
        primary_key_duplicates=1,
    )
    json_path, md_path = reporter.write_outputs()
    assert json_path.exists()
    assert md_path.exists()
    payload = json_path.read_text(encoding="utf-8")
    assert '"run_mode": "unit_test"' in payload
    assert '"daily"' in payload
