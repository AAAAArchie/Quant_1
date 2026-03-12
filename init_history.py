from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

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
from core.utils import ensure_project_directories, format_ymd, iter_date_windows, state_file_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize local A-share history from Tushare")
    parser.add_argument("--start-date", default=SETTINGS.init_start_date)
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def load_state(task_name: str) -> dict[str, object]:
    path = state_file_path(task_name)
    if not path.exists():
        return {"completed_tables": []}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(task_name: str, payload: dict[str, object]) -> None:
    path = state_file_path(task_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_progress(payload: dict[str, object]) -> None:
    path = state_file_path("init_history.progress")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def make_progress_callback(
    *,
    stage_label: str,
    table_name: str,
) -> callable:
    def _callback(payload: dict[str, object]) -> None:
        event = payload.get("event")
        request_index = payload.get("request_index")
        request_total = payload.get("request_total")
        row_count = payload.get("row_count")
        api_name = payload.get("api_name")

        progress_payload = {
            "stage": stage_label,
            "table_name": table_name,
            "api_name": api_name,
            "event": event,
            "request_index": request_index,
            "request_total": request_total,
            "row_count": row_count,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

        if payload.get("window_start") and payload.get("window_end"):
            print(
                f"[{stage_label}] table={table_name} api={api_name} "
                f"window={payload['window_start']}->{payload['window_end']} "
                f"request={request_index}/{request_total} rows={row_count}"
            )
            progress_payload["window_start"] = payload["window_start"]
            progress_payload["window_end"] = payload["window_end"]
        elif payload.get("trade_date"):
            print(
                f"[{stage_label}] table={table_name} api={api_name} "
                f"trade_date={payload['trade_date']} request={request_index}/{request_total} rows={row_count}"
            )
            progress_payload["trade_date"] = payload["trade_date"]
        elif payload.get("symbol_value"):
            print(
                f"[{stage_label}] table={table_name} api={api_name} "
                f"{payload.get('symbol_field')}={payload['symbol_value']} "
                f"request={request_index}/{request_total} rows={row_count}"
            )
            progress_payload["symbol_field"] = payload.get("symbol_field")
            progress_payload["symbol_value"] = payload["symbol_value"]

        if payload.get("event") in {"request_retry", "request_failed"}:
            print(
                f"[{stage_label}] table={table_name} api={api_name} "
                f"event={payload['event']} attempt={payload.get('attempt')} error={payload.get('error')}"
            )
            progress_payload["attempt"] = payload.get("attempt")
            progress_payload["error"] = payload.get("error")

        save_progress(progress_payload)

    return _callback


def print_stage_start(
    stage_number: int,
    stage_total: int,
    table_name: str,
    *,
    expected_requests: int,
    mode: str,
) -> None:
    print(
        f"[Stage {stage_number}/{stage_total}] table={table_name} "
        f"mode={mode} estimated_requests={expected_requests}"
    )
    save_progress(
        {
            "stage": f"{stage_number}/{stage_total}",
            "table_name": table_name,
            "mode": mode,
            "estimated_requests": expected_requests,
            "event": "stage_start",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    )


def count_windows(start_date: str, end_date: str, window_days: int) -> int:
    return len(list(iter_date_windows(start_date, end_date, window_days)))


def write_ods_raw(
    table_name: str,
    raw_frame: pl.DataFrame,
    *,
    fallback_date: str,
    reporter: QualityReporter,
) -> None:
    if raw_frame.is_empty():
        return

    spec = TABLE_SPECS[table_name]
    if spec.category == "financial" and "source_table" not in raw_frame.columns:
        raw_frame = raw_frame.with_columns(pl.lit(table_name).alias("source_table"))
    partition_column = next((column for column in spec.date_columns if column in raw_frame.columns), None)
    if partition_column is None:
        staged = raw_frame.with_columns(pl.lit(fallback_date).str.strptime(pl.Date, "%Y%m%d").alias("__year_date"))
    else:
        staged = raw_frame.with_columns(
            pl.coalesce(
                [
                    pl.col(partition_column).cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False),
                    pl.col(partition_column).cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                ]
            ).fill_null(pl.lit(fallback_date).str.strptime(pl.Date, "%Y%m%d"))
            .alias("__year_date")
        )

    years = staged.select(pl.col("__year_date").dt.year().unique().sort()).get_column("__year_date").to_list()
    for year in years:
        new_year_frame = staged.filter(pl.col("__year_date").dt.year() == year).drop("__year_date")
        existing = read_year_partition("ods", spec.ods_dataset, year)
        merged = pl.concat([new_year_frame, existing], how="diagonal_relaxed")
        merged = deduplicate_by_primary_key(merged, spec.primary_key)
        result = write_year_partition("ods", spec.ods_dataset, year, merged)
        reporter.add_partition_write(
            table_name,
            layer="ods",
            year=year,
            path=str(result.path),
            file_count=result.file_count,
            row_count=result.row_count,
        )


def fetch_trade_cal(fetcher: TushareFetcher, start_date: str, end_date: str) -> tuple[pl.DataFrame, dict[str, int]]:
    frames: list[pl.DataFrame] = []
    stats = {"rows": 0, "requests": 0, "success": 0, "retries": 0}
    for exchange in ("SSE", "SZSE", "BSE"):
        frame, fetch_stats = fetcher.fetch_by_date_windows(
            "trade_cal",
            start_date,
            end_date,
            window_days=365,
            base_params={"exchange": exchange},
        )
        frames.append(frame)
        stats["rows"] += fetch_stats.row_count
        stats["requests"] += fetch_stats.request_count
        stats["success"] += fetch_stats.success_count
        stats["retries"] += fetch_stats.retry_count
    return pl.concat(frames, how="diagonal_relaxed"), stats


def fetch_stock_basic(fetcher: TushareFetcher) -> tuple[pl.DataFrame, dict[str, int]]:
    frames: list[pl.DataFrame] = []
    stats = {"rows": 0, "requests": 0, "success": 0, "retries": 0}
    for list_status in ("L", "D", "P"):
        frame, fetch_stats = fetcher.fetch_by_symbols(
            "stock_basic",
            symbols=[list_status],
            symbol_batch_size=1,
            symbol_field="list_status",
        )
        frames.append(frame)
        stats["rows"] += fetch_stats.row_count
        stats["requests"] += fetch_stats.request_count
        stats["success"] += fetch_stats.success_count
        stats["retries"] += fetch_stats.retry_count
    return pl.concat(frames, how="diagonal_relaxed"), stats


def persist_dwd_table(
    table_name: str,
    clean_frame: pl.DataFrame,
    reporter: QualityReporter,
) -> None:
    if clean_frame.is_empty():
        return
    spec = TABLE_SPECS[table_name]
    partition_date = "real_ann_date" if spec.category == "financial" else spec.date_columns[0]
    results = merge_and_write_year_partitions(
        "dwd",
        spec.dwd_dataset,
        clean_frame,
        primary_key=spec.primary_key,
        partition_date_column=partition_date,
    )
    for result in results:
        reporter.add_partition_write(
            table_name,
            layer=result.layer,
            year=result.year,
            path=str(result.path),
            file_count=result.file_count,
            row_count=result.row_count,
        )


def persist_dws_outputs(
    years: Iterable[int],
    reporter: QualityReporter,
) -> None:
    years = sorted(set(years))
    if not years:
        return

    for year in years:
        daily = read_year_partition("dwd", "daily", year)
        daily_basic = read_year_partition("dwd", "daily_basic", year)
        adj_factor = read_year_partition("dwd", "adj_factor", year)
        if not daily.is_empty():
            daily_master = build_daily_master(daily, daily_basic, adj_factor)
            result = write_year_partition("dws", "daily_master", year, daily_master)
            reporter.add_partition_write(
                "daily_master",
                layer=result.layer,
                year=result.year,
                path=str(result.path),
                file_count=result.file_count,
                row_count=result.row_count,
            )

        financial_frames = {
            table_name: read_year_partition("dwd", table_name, year)
            for table_name in FINANCIAL_EVENT_SOURCES
        }
        if any(not frame.is_empty() for frame in financial_frames.values()):
            financial_events = build_financial_event_stream(financial_frames)
            result = write_year_partition("dws", "financial_events", year, financial_events)
            reporter.add_partition_write(
                "financial_events",
                layer=result.layer,
                year=result.year,
                path=str(result.path),
                file_count=result.file_count,
                row_count=result.row_count,
            )


def read_all_dwd(dataset_name: str) -> pl.DataFrame:
    try:
        return scan_dataset("dwd", dataset_name).collect()
    except Exception:
        return pl.DataFrame()


def main() -> None:
    args = parse_args()
    ensure_project_directories()
    reporter = QualityReporter(run_mode="init_history")
    state = load_state("init_history")
    completed_tables = set(state.get("completed_tables", []))
    affected_years: set[int] = set()
    stage_total = 2 + 5 + len(FINANCIAL_EVENT_SOURCES)
    stage_number = 0

    try:
        if args.force or "trade_cal" not in completed_tables:
            stage_number += 1
            print_stage_start(
                stage_number,
                stage_total,
                "trade_cal",
                expected_requests=count_windows(args.start_date, args.end_date, 365) * 3,
                mode="window",
            )
            fetcher = TushareFetcher(progress_callback=make_progress_callback(stage_label=f"{stage_number}/{stage_total}", table_name="trade_cal"))
            raw_trade_cal, stat = fetch_trade_cal(fetcher, args.start_date, args.end_date)
            reporter.add_fetch_stats(
                "trade_cal",
                fetched_rows=stat["rows"],
                request_count=stat["requests"],
                success_count=stat["success"],
                retry_count=stat["retries"],
            )
            write_ods_raw("trade_cal", raw_trade_cal, fallback_date=args.start_date, reporter=reporter)
            cleaned = clean_table("trade_cal", raw_trade_cal)
            reporter.add_clean_stats(
                "trade_cal",
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            persist_dwd_table("trade_cal", cleaned.frame, reporter)
            completed_tables.add("trade_cal")
            save_state("init_history", {"completed_tables": sorted(completed_tables)})

        trade_cal_dwd = read_all_dwd("trade_cal")
        calendar = TradingCalendar.from_frame(trade_cal_dwd)

        if args.force or "stock_basic" not in completed_tables:
            stage_number += 1
            print_stage_start(
                stage_number,
                stage_total,
                "stock_basic",
                expected_requests=3,
                mode="list_status",
            )
            fetcher = TushareFetcher(progress_callback=make_progress_callback(stage_label=f"{stage_number}/{stage_total}", table_name="stock_basic"))
            raw_stock_basic, stat = fetch_stock_basic(fetcher)
            reporter.add_fetch_stats(
                "stock_basic",
                fetched_rows=stat["rows"],
                request_count=stat["requests"],
                success_count=stat["success"],
                retry_count=stat["retries"],
            )
            write_ods_raw("stock_basic", raw_stock_basic, fallback_date=args.end_date, reporter=reporter)
            cleaned = clean_table("stock_basic", raw_stock_basic)
            reporter.add_clean_stats(
                "stock_basic",
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            persist_dwd_table("stock_basic", cleaned.frame, reporter)
            completed_tables.add("stock_basic")
            save_state("init_history", {"completed_tables": sorted(completed_tables)})

        stock_basic_dwd = read_all_dwd("stock_basic")
        symbols = []
        if "ts_code" in stock_basic_dwd.columns:
            symbols = stock_basic_dwd.get_column("ts_code").drop_nulls().unique().sort().to_list()

        for table_name in ("daily", "daily_basic", "adj_factor", "suspend_d", "stk_limit"):
            if not args.force and table_name in completed_tables:
                continue
            stage_number += 1
            print_stage_start(
                stage_number,
                stage_total,
                table_name,
                expected_requests=count_windows(args.start_date, args.end_date, SETTINGS.init_daily_window_days),
                mode="window",
            )
            fetcher = TushareFetcher(progress_callback=make_progress_callback(stage_label=f"{stage_number}/{stage_total}", table_name=table_name))
            raw_frame, fetch_stats = fetcher.fetch_by_date_windows(
                table_name,
                args.start_date,
                args.end_date,
                window_days=SETTINGS.init_daily_window_days,
            )
            reporter.add_fetch_stats(
                table_name,
                fetched_rows=fetch_stats.row_count,
                request_count=fetch_stats.request_count,
                success_count=fetch_stats.success_count,
                retry_count=fetch_stats.retry_count,
                exception_messages=fetch_stats.exception_messages,
            )
            write_ods_raw(table_name, raw_frame, fallback_date=args.start_date, reporter=reporter)
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
                start_date=args.start_date,
                end_date=args.end_date,
            )
            reporter.add_trade_date_coverage(table_name, **coverage)
            persist_dwd_table(table_name, cleaned.frame, reporter)
            if not cleaned.frame.is_empty():
                affected_years.update(
                    cleaned.frame.get_column("trade_date").dt.year().unique().to_list()
                )
            completed_tables.add(table_name)
            save_state("init_history", {"completed_tables": sorted(completed_tables)})

        for table_name in FINANCIAL_EVENT_SOURCES:
            if not args.force and table_name in completed_tables:
                continue
            stage_number += 1
            print_stage_start(
                stage_number,
                stage_total,
                table_name,
                expected_requests=count_windows(args.start_date, args.end_date, SETTINGS.init_financial_window_days),
                mode="window_vip_first",
            )
            fetcher = TushareFetcher(progress_callback=make_progress_callback(stage_label=f"{stage_number}/{stage_total}", table_name=table_name))
            raw_frame, fetch_stats = fetcher.fetch_by_date_windows(
                table_name,
                args.start_date,
                args.end_date,
                window_days=SETTINGS.init_financial_window_days,
                base_params={},
                prefer_vip=True,
            )
            reporter.add_fetch_stats(
                table_name,
                fetched_rows=fetch_stats.row_count,
                request_count=fetch_stats.request_count,
                success_count=fetch_stats.success_count,
                retry_count=fetch_stats.retry_count,
                exception_messages=fetch_stats.exception_messages,
            )
            write_ods_raw(table_name, raw_frame, fallback_date=args.end_date, reporter=reporter)
            cleaned = clean_table(table_name, raw_frame)
            reporter.add_clean_stats(
                table_name,
                cleaned_rows=cleaned.frame.height,
                rows_before_dedup=cleaned.rows_before_dedup,
                rows_after_dedup=cleaned.rows_after_dedup,
                null_counts=cleaned.null_counts,
                primary_key_duplicates=cleaned.primary_key_duplicates,
            )
            persist_dwd_table(table_name, cleaned.frame, reporter)
            if "real_ann_date" in cleaned.frame.columns and not cleaned.frame.is_empty():
                affected_years.update(cleaned.frame.get_column("real_ann_date").dt.year().unique().to_list())
            completed_tables.add(table_name)
            save_state("init_history", {"completed_tables": sorted(completed_tables)})

        persist_dws_outputs(affected_years, reporter)
    except Exception as exc:  # noqa: BLE001
        reporter.add_exception(str(exc))
        raise
    finally:
        reporter.write_outputs()


if __name__ == "__main__":
    main()
