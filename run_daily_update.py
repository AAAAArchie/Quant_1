from __future__ import annotations

import argparse
from datetime import datetime, timedelta

import polars as pl

from config import FINANCIAL_EVENT_SOURCES, SETTINGS, TABLE_SPECS
from core.data_cleaner import clean_table, deduplicate_by_primary_key
from core.pipeline_builder import (
    build_daily_master,
    build_financial_event_stream,
    merge_and_write_year_partitions,
    read_year_partition,
    scan_dataset,
    write_year_partition,
)
from core.quality_reporter import QualityReporter
from core.trading_calendar import TradingCalendar
from core.tushare_fetcher import TushareFetcher
from core.utils import ensure_project_directories, format_ymd
from init_history import fetch_stock_basic, fetch_trade_cal, write_ods_raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily sliding-window update")
    parser.add_argument("--as-of-date", default=datetime.now().strftime("%Y%m%d"))
    return parser.parse_args()


def _read_all_years(dataset_name: str) -> pl.DataFrame:
    try:
        return scan_dataset("dwd", dataset_name).collect()
    except Exception:
        return pl.DataFrame()


def _persist_dwd(table_name: str, frame: pl.DataFrame, reporter: QualityReporter) -> set[int]:
    if frame.is_empty():
        return set()
    spec = TABLE_SPECS[table_name]
    partition_date = "real_ann_date" if spec.category == "financial" else spec.date_columns[0]
    results = merge_and_write_year_partitions(
        "dwd",
        spec.dwd_dataset,
        frame,
        primary_key=spec.primary_key,
        partition_date_column=partition_date,
    )
    years = set()
    for result in results:
        years.add(result.year)
        reporter.add_partition_write(
            table_name,
            layer=result.layer,
            year=result.year,
            path=str(result.path),
            file_count=result.file_count,
            row_count=result.row_count,
        )
    return years


def _rebuild_daily_master(years: set[int], reporter: QualityReporter) -> None:
    for year in sorted(years):
        daily = read_year_partition("dwd", "daily", year)
        if daily.is_empty():
            continue
        daily_basic = read_year_partition("dwd", "daily_basic", year)
        adj_factor = read_year_partition("dwd", "adj_factor", year)
        result = write_year_partition("dws", "daily_master", year, build_daily_master(daily, daily_basic, adj_factor))
        reporter.add_partition_write(
            "daily_master",
            layer=result.layer,
            year=result.year,
            path=str(result.path),
            file_count=result.file_count,
            row_count=result.row_count,
        )


def _rebuild_financial_events(years: set[int], reporter: QualityReporter) -> None:
    for year in sorted(years):
        frames = {table_name: read_year_partition("dwd", table_name, year) for table_name in FINANCIAL_EVENT_SOURCES}
        if not any(not frame.is_empty() for frame in frames.values()):
            continue
        result = write_year_partition("dws", "financial_events", year, build_financial_event_stream(frames))
        reporter.add_partition_write(
            "financial_events",
            layer=result.layer,
            year=result.year,
            path=str(result.path),
            file_count=result.file_count,
            row_count=result.row_count,
        )


def main() -> None:
    args = parse_args()
    ensure_project_directories()
    reporter = QualityReporter(run_mode="daily_update")
    fetcher = TushareFetcher()

    try:
        trade_cal_dwd = _read_all_years("trade_cal")
        if trade_cal_dwd.is_empty():
            raw_trade_cal, stat = fetch_trade_cal(fetcher, SETTINGS.init_start_date, args.as_of_date)
            reporter.add_fetch_stats(
                "trade_cal",
                fetched_rows=stat["rows"],
                request_count=stat["requests"],
                success_count=stat["success"],
                retry_count=stat["retries"],
            )
            write_ods_raw("trade_cal", raw_trade_cal, fallback_date=args.as_of_date, reporter=reporter)
            cleaned = clean_table("trade_cal", raw_trade_cal)
            reporter.add_clean_stats(
                "trade_cal",
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            _persist_dwd("trade_cal", cleaned.frame, reporter)
            trade_cal_dwd = _read_all_years("trade_cal")

        calendar = TradingCalendar.from_frame(trade_cal_dwd)
        daily_trade_dates = [value.strftime("%Y%m%d") for value in calendar.recent_trade_dates(args.as_of_date, SETTINGS.daily_backfill_days)]

        stock_basic_dwd = _read_all_years("stock_basic")
        if stock_basic_dwd.is_empty():
            raw_stock_basic, stat = fetch_stock_basic(fetcher)
            reporter.add_fetch_stats(
                "stock_basic",
                fetched_rows=stat["rows"],
                request_count=stat["requests"],
                success_count=stat["success"],
                retry_count=stat["retries"],
            )
            write_ods_raw("stock_basic", raw_stock_basic, fallback_date=args.as_of_date, reporter=reporter)
            cleaned = clean_table("stock_basic", raw_stock_basic)
            reporter.add_clean_stats(
                "stock_basic",
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            _persist_dwd("stock_basic", cleaned.frame, reporter)
            stock_basic_dwd = _read_all_years("stock_basic")

        symbols = (
            stock_basic_dwd.get_column("ts_code").drop_nulls().unique().sort().to_list()
            if "ts_code" in stock_basic_dwd.columns
            else []
        )

        daily_years: set[int] = set()
        for table_name in ("daily", "daily_basic", "adj_factor", "suspend_d", "stk_limit"):
            raw_frame, fetch_stats = fetcher.fetch_by_trade_date_list(table_name, daily_trade_dates)
            reporter.add_fetch_stats(
                table_name,
                fetched_rows=fetch_stats.row_count,
                request_count=fetch_stats.request_count,
                success_count=fetch_stats.success_count,
                retry_count=fetch_stats.retry_count,
                exception_messages=fetch_stats.exception_messages,
            )
            fallback_date = daily_trade_dates[0] if daily_trade_dates else args.as_of_date
            write_ods_raw(table_name, raw_frame, fallback_date=fallback_date, reporter=reporter)
            cleaned = clean_table(table_name, raw_frame)
            reporter.add_clean_stats(
                table_name,
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            coverage = calendar.coverage_summary(
                cleaned.frame.get_column("trade_date").dt.strftime("%Y%m%d").to_list()
                if "trade_date" in cleaned.frame.columns and not cleaned.frame.is_empty()
                else [],
                start_date=daily_trade_dates[0] if daily_trade_dates else args.as_of_date,
                end_date=daily_trade_dates[-1] if daily_trade_dates else args.as_of_date,
            )
            reporter.add_trade_date_coverage(table_name, **coverage)
            daily_years.update(_persist_dwd(table_name, cleaned.frame, reporter))

        financial_start = (datetime.strptime(args.as_of_date, "%Y%m%d") - timedelta(days=SETTINGS.financial_backfill_days)).strftime("%Y%m%d")
        financial_years: set[int] = set()
        for table_name in FINANCIAL_EVENT_SOURCES:
            raw_frame, fetch_stats = fetcher.fetch_by_symbols(
                table_name,
                symbols=symbols,
                symbol_batch_size=1,
                base_params={"start_date": format_ymd(financial_start), "end_date": format_ymd(args.as_of_date)},
            )
            reporter.add_fetch_stats(
                table_name,
                fetched_rows=fetch_stats.row_count,
                request_count=fetch_stats.request_count,
                success_count=fetch_stats.success_count,
                retry_count=fetch_stats.retry_count,
                exception_messages=fetch_stats.exception_messages,
            )
            write_ods_raw(table_name, raw_frame, fallback_date=args.as_of_date, reporter=reporter)
            cleaned = clean_table(table_name, raw_frame)
            reporter.add_clean_stats(
                table_name,
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            financial_years.update(_persist_dwd(table_name, cleaned.frame, reporter))

        _rebuild_daily_master(daily_years, reporter)
        _rebuild_financial_events(financial_years, reporter)
    except Exception as exc:  # noqa: BLE001
        reporter.add_exception(str(exc))
        raise
    finally:
        reporter.write_outputs()


if __name__ == "__main__":
    main()
