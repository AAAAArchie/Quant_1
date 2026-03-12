from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class TableSpec:
    """定义单张表的抓取、主键与分区规则。"""

    name: str
    category: str
    primary_key: tuple[str, ...]
    date_columns: tuple[str, ...]
    ods_dataset: str
    dwd_dataset: str
    dws_dataset: str | None = None
    update_window_days: int = 0
    supports_trade_date_window: bool = False
    requires_symbol_loop: bool = False


@dataclass(frozen=True)
class Settings:
    """项目级运行配置。"""

    project_root: Path
    data_root: Path
    logs_root: Path
    state_root: Path
    reports_root: Path
    temp_root: Path
    init_start_date: str = "20150101"
    daily_backfill_days: int = 5
    financial_backfill_days: int = 90
    init_daily_window_days: int = 120
    init_financial_window_days: int = 90
    tushare_token_env: str = "TUSHARE_TOKEN"
    tushare_http_url_env: str = "TUSHARE_HTTP_URL"
    tushare_http_url_default: str = "http://lianghua.nanyangqiankun.top"
    tushare_sleep_seconds: float = 0.15
    tushare_max_retries: int = 3
    tushare_retry_backoff_seconds: float = 1.5
    partition_column: str = "year"
    parquet_compression: str = "zstd"
    max_rows_per_file: int = 250_000
    quality_json_name: str = "quality_report.json"
    quality_md_name: str = "quality_report.md"
    layers: dict[str, Path] = field(default_factory=dict)


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data"
LOGS_ROOT = PROJECT_ROOT / "logs"
STATE_ROOT = PROJECT_ROOT / "state"
REPORTS_ROOT = PROJECT_ROOT / "reports"
TEMP_ROOT = PROJECT_ROOT / "_tmp"


TABLE_SPECS: dict[str, TableSpec] = {
    "daily": TableSpec(
        name="daily",
        category="daily",
        primary_key=("ts_code", "trade_date"),
        date_columns=("trade_date",),
        ods_dataset="daily",
        dwd_dataset="daily",
        dws_dataset="daily_master",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "daily_basic": TableSpec(
        name="daily_basic",
        category="daily",
        primary_key=("ts_code", "trade_date"),
        date_columns=("trade_date",),
        ods_dataset="daily_basic",
        dwd_dataset="daily_basic",
        dws_dataset="daily_master",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "adj_factor": TableSpec(
        name="adj_factor",
        category="daily",
        primary_key=("ts_code", "trade_date"),
        date_columns=("trade_date",),
        ods_dataset="adj_factor",
        dwd_dataset="adj_factor",
        dws_dataset="daily_master",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "trade_cal": TableSpec(
        name="trade_cal",
        category="auxiliary",
        primary_key=("exchange", "cal_date"),
        date_columns=("cal_date", "pretrade_date"),
        ods_dataset="trade_cal",
        dwd_dataset="trade_cal",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "stock_basic": TableSpec(
        name="stock_basic",
        category="auxiliary",
        primary_key=("ts_code",),
        date_columns=("list_date", "delist_date"),
        ods_dataset="stock_basic",
        dwd_dataset="stock_basic",
        update_window_days=30,
    ),
    "suspend_d": TableSpec(
        name="suspend_d",
        category="auxiliary",
        primary_key=("ts_code", "trade_date"),
        date_columns=("trade_date", "suspend_timing"),
        ods_dataset="suspend_d",
        dwd_dataset="suspend_d",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "stk_limit": TableSpec(
        name="stk_limit",
        category="auxiliary",
        primary_key=("ts_code", "trade_date"),
        date_columns=("trade_date",),
        ods_dataset="stk_limit",
        dwd_dataset="stk_limit",
        update_window_days=5,
        supports_trade_date_window=True,
    ),
    "fina_indicator": TableSpec(
        name="fina_indicator",
        category="financial",
        primary_key=(
            "ts_code",
            "end_date",
            "ann_date",
        ),
        date_columns=("ann_date", "f_ann_date", "end_date"),
        ods_dataset="fina_indicator",
        dwd_dataset="fina_indicator",
        dws_dataset="financial_events",
        update_window_days=90,
        requires_symbol_loop=True,
    ),
    "income": TableSpec(
        name="income",
        category="financial",
        primary_key=(
            "ts_code",
            "end_date",
            "ann_date",
        ),
        date_columns=("ann_date", "f_ann_date", "end_date"),
        ods_dataset="income",
        dwd_dataset="income",
        dws_dataset="financial_events",
        update_window_days=90,
        requires_symbol_loop=True,
    ),
    "balancesheet": TableSpec(
        name="balancesheet",
        category="financial",
        primary_key=(
            "ts_code",
            "end_date",
            "ann_date",
        ),
        date_columns=("ann_date", "f_ann_date", "end_date"),
        ods_dataset="balancesheet",
        dwd_dataset="balancesheet",
        dws_dataset="financial_events",
        update_window_days=90,
        requires_symbol_loop=True,
    ),
}


DAILY_MASTER_SOURCES: tuple[str, ...] = ("daily", "daily_basic", "adj_factor")
FINANCIAL_EVENT_SOURCES: tuple[str, ...] = ("fina_indicator", "income", "balancesheet")


def build_settings() -> Settings:
    layers = {
        "ods": DATA_ROOT / "1_ods_raw",
        "dwd": DATA_ROOT / "2_dwd_detail",
        "dws": DATA_ROOT / "3_dws_summary",
    }
    return Settings(
        project_root=PROJECT_ROOT,
        data_root=DATA_ROOT,
        logs_root=LOGS_ROOT,
        state_root=STATE_ROOT,
        reports_root=REPORTS_ROOT,
        temp_root=TEMP_ROOT,
        layers=layers,
    )


SETTINGS = build_settings()
TUSHARE_TOKEN = os.getenv(SETTINGS.tushare_token_env, "")
TUSHARE_HTTP_URL = os.getenv(SETTINGS.tushare_http_url_env, SETTINGS.tushare_http_url_default)
