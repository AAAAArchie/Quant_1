from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
import shutil
import sys
import uuid

import polars as pl

import config
import core.duckdb_query as duckdb_query_module
import core.pipeline_builder as pipeline_builder_module
import core.quality_reporter as quality_reporter_module
import core.tushare_fetcher as tushare_fetcher_module
import core.utils as utils_module
import init_history
import run_daily_update
from core.pipeline_builder import read_year_partition
from core.tushare_fetcher import FetchStats


def workspace_tmp_dir() -> Path:
    path = Path("state") / "test_runs" / f"it_{uuid.uuid4().hex[:8]}"
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
        init_history,
        run_daily_update,
    ):
        monkeypatch.setattr(module, "SETTINGS", settings, raising=False)
    return settings


def make_mock_payload(update: bool = False) -> dict[str, pl.DataFrame]:
    close_20250107 = "11.00" if update else "10.00"
    revenue_20250106 = "260" if update else "240"
    return {
        "trade_cal": pl.DataFrame(
            {
                "exchange": ["SSE", "SSE", "SSE", "SSE", "SSE", "SSE"],
                "cal_date": ["20241230", "20241231", "20250102", "20250103", "20250106", "20250107"],
                "is_open": [1, 1, 1, 1, 1, 1],
                "pretrade_date": ["20241227", "20241230", "20241231", "20250102", "20250103", "20250106"],
            }
        ),
        "stock_basic": pl.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                "name": ["Ping An Bank"],
                "area": ["Shenzhen"],
                "industry": ["Bank"],
                "market": ["Main"],
                "list_status": ["L"],
                "list_date": ["19910403"],
            }
        ),
        "daily": pl.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 6,
                "trade_date": ["20241230", "20241231", "20250102", "20250103", "20250106", "20250107"],
                "open": ["9.8", "9.9", "10.0", "10.1", "10.2", close_20250107],
                "close": ["9.9", "10.0", "10.1", "10.2", "10.3", close_20250107],
                "vol": ["100", "110", "120", "130", "140", "150"],
            }
        ),
        "daily_basic": pl.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 6,
                "trade_date": ["20241230", "20241231", "20250102", "20250103", "20250106", "20250107"],
                "turnover_rate": ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6"],
                "pe": ["5.1", "5.2", "5.3", "5.4", "5.5", "5.6"],
            }
        ),
        "adj_factor": pl.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 6,
                "trade_date": ["20241230", "20241231", "20250102", "20250103", "20250106", "20250107"],
                "adj_factor": ["1", "1", "1", "1", "1", "1"],
            }
        ),
        "suspend_d": pl.DataFrame(
            {
                "ts_code": [],
                "trade_date": [],
                "suspend_timing": [],
            }
        ),
        "stk_limit": pl.DataFrame(
            {
                "ts_code": ["000001.SZ"] * 6,
                "trade_date": ["20241230", "20241231", "20250102", "20250103", "20250106", "20250107"],
                "up_limit": ["12", "12", "12", "12", "12", "12"],
                "down_limit": ["8", "8", "8", "8", "8", "8"],
            }
        ),
        "fina_indicator": pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20241231", "20250106"],
                "f_ann_date": ["20241230", "20250105"],
                "end_date": ["20240930", "20241231"],
                "report_type": ["1", "1"],
                "comp_type": ["1", "1"],
                "update_flag": ["0", "0"],
                "roe": ["10.5", "11.0"],
            }
        ),
        "income": pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20241231", "20250106"],
                "f_ann_date": ["20241230", "20250105"],
                "end_date": ["20240930", "20241231"],
                "report_type": ["1", "1"],
                "comp_type": ["1", "1"],
                "update_flag": ["0", "0"],
                "revenue": ["200", revenue_20250106],
            }
        ),
        "balancesheet": pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20241231", None],
                "f_ann_date": ["20241230", "20250105"],
                "end_date": ["20240930", "20241231"],
                "report_type": ["1", "1"],
                "comp_type": ["1", "1"],
                "update_flag": ["0", "0"],
                "total_assets": ["1000", "1100"],
            }
        ),
    }


class MockFetcher:
    trade_date_calls: list[list[str]] = []

    def __init__(self, payload: dict[str, pl.DataFrame]) -> None:
        self.payload = payload

    @staticmethod
    def _stats(table_name: str, frame: pl.DataFrame) -> FetchStats:
        return FetchStats(
            table_name=table_name,
            request_count=1,
            success_count=1,
            row_count=frame.height,
            retry_count=0,
        )

    def fetch_by_date_windows(self, table_name: str, start_date: str, end_date: str, window_days: int, **kwargs):
        frame = self.payload[table_name]
        if table_name == "trade_cal":
            exchange = kwargs.get("base_params", {}).get("exchange")
            frame = frame.filter(
                (pl.col("exchange") == exchange)
                & (pl.col("cal_date") >= start_date)
                & (pl.col("cal_date") <= end_date)
            )
        return frame, self._stats(table_name, frame)

    def fetch_by_trade_date_list(self, table_name: str, trade_dates: list[str], **kwargs):
        MockFetcher.trade_date_calls.append(list(trade_dates))
        frame = self.payload[table_name]
        if "trade_date" in frame.columns:
            frame = frame.filter(pl.col("trade_date").is_in(trade_dates))
        return frame, self._stats(table_name, frame)

    def fetch_by_symbols(self, table_name: str, symbols: list[str], **kwargs):
        frame = self.payload[table_name]
        symbol_field = kwargs.get("symbol_field", "ts_code")
        if symbol_field == "list_status":
            frame = frame.filter(pl.col("list_status").is_in(symbols))
        else:
            frame = frame.filter(pl.col("ts_code").is_in(symbols))
            base_params = kwargs.get("base_params", {})
            start_date = base_params.get("start_date")
            end_date = base_params.get("end_date")
            if start_date and "ann_date" in frame.columns:
                frame = frame.filter(pl.col("ann_date").is_null() | (pl.col("ann_date") >= start_date))
            if end_date and "ann_date" in frame.columns:
                frame = frame.filter(pl.col("ann_date").is_null() | (pl.col("ann_date") <= end_date))
        return frame, self._stats(table_name, frame)


def test_init_history_builds_daily_master_and_financial_events(monkeypatch):
    tmp_path = workspace_tmp_dir()
    patch_settings(monkeypatch, tmp_path)
    payload = make_mock_payload(update=False)
    monkeypatch.setattr(init_history, "TushareFetcher", lambda: MockFetcher(payload))
    monkeypatch.setattr(sys, "argv", ["init_history.py", "--start-date", "20241230", "--end-date", "20250107"])

    init_history.main()

    daily_master_2025 = read_year_partition("dws", "daily_master", 2025)
    financial_2025 = read_year_partition("dws", "financial_events", 2025)
    assert daily_master_2025.height == 4
    assert "adj_factor" in daily_master_2025.columns
    assert financial_2025.filter(pl.col("ann_date").is_null()).height == 0
    assert financial_2025.filter(pl.col("source_table") == "income").height == 1


def test_daily_update_is_sliding_window_idempotent_and_overwrites_partition(monkeypatch):
    tmp_path = workspace_tmp_dir()
    patch_settings(monkeypatch, tmp_path)

    init_payload = make_mock_payload(update=False)
    monkeypatch.setattr(init_history, "TushareFetcher", lambda: MockFetcher(init_payload))
    monkeypatch.setattr(sys, "argv", ["init_history.py", "--start-date", "20241230", "--end-date", "20250107"])
    init_history.main()

    update_payload = make_mock_payload(update=True)
    MockFetcher.trade_date_calls = []
    monkeypatch.setattr(run_daily_update, "TushareFetcher", lambda: MockFetcher(update_payload))
    monkeypatch.setattr(sys, "argv", ["run_daily_update.py", "--as-of-date", "20250107"])
    run_daily_update.main()
    run_daily_update.main()

    daily_2025 = read_year_partition("dwd", "daily", 2025)
    daily_master_2025 = read_year_partition("dws", "daily_master", 2025)
    income_2025 = read_year_partition("dwd", "income", 2025)
    financial_2025 = read_year_partition("dws", "financial_events", 2025)

    assert (
        daily_2025.filter(pl.col("trade_date") == date(2025, 1, 7)).select("close").item() == 11.0
    )
    assert daily_2025.height == 4
    assert (
        daily_master_2025.filter(pl.col("trade_date") == date(2025, 1, 7)).select("close").item()
        == 11.0
    )
    assert income_2025.select("revenue").item() == 260.0
    assert financial_2025.filter(pl.col("source_table") == "income").select("revenue").item() == 260.0
    assert len(MockFetcher.trade_date_calls[0]) == 5
