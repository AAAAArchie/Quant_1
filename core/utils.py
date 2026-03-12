from __future__ import annotations

import shutil
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from config import SETTINGS, TableSpec


def ensure_project_directories() -> None:
    """统一创建项目运行时需要的目录，避免各模块重复判断。"""

    required_dirs = [
        SETTINGS.data_root,
        SETTINGS.logs_root,
        SETTINGS.state_root,
        SETTINGS.reports_root,
        SETTINGS.temp_root,
        *SETTINGS.layers.values(),
    ]
    for path in required_dirs:
        path.mkdir(parents=True, exist_ok=True)


def parse_ymd(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = value.strip()
    if not text:
        return None
    if "-" in text:
        return datetime.strptime(text, "%Y-%m-%d").date()
    return datetime.strptime(text, "%Y%m%d").date()


def format_ymd(value: str | date | datetime | None) -> str | None:
    parsed = parse_ymd(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y%m%d")


def iter_date_windows(
    start_date: str | date,
    end_date: str | date,
    window_days: int,
) -> Iterator[tuple[str, str]]:
    """
    将长时间区间拆成多个闭区间窗口。

    这样做的目的不是为了业务分组，而是为了控制 Tushare 单次请求规模，
    同时让失败重试可以精确到单个窗口，避免整段历史重抓。
    """

    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    if start is None or end is None:
        raise ValueError("start_date and end_date must be valid dates")
    if start > end:
        raise ValueError("start_date must be earlier than or equal to end_date")
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=window_days - 1), end)
        yield cursor.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        cursor = window_end + timedelta(days=1)


def chunked(items: Sequence[str], chunk_size: int) -> Iterator[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for index in range(0, len(items), chunk_size):
        yield list(items[index : index + chunk_size])


def dataset_root(layer: str, dataset_name: str) -> Path:
    if layer not in SETTINGS.layers:
        raise KeyError(f"unsupported layer: {layer}")
    return SETTINGS.layers[layer] / dataset_name


def partition_path(layer: str, dataset_name: str, year: int) -> Path:
    return dataset_root(layer, dataset_name) / f"{SETTINGS.partition_column}={year}"


def temp_partition_path(layer: str, dataset_name: str, year: int) -> Path:
    token = uuid.uuid4().hex[:8]
    return SETTINGS.temp_root / layer / dataset_name / f"{SETTINGS.partition_column}={year}__{token}"


def state_file_path(task_name: str) -> Path:
    return SETTINGS.state_root / f"{task_name}.json"


def log_file_path(task_name: str) -> Path:
    return SETTINGS.logs_root / f"{task_name}.log"


def quality_report_dir(run_mode: str, started_at: datetime) -> Path:
    stamp = started_at.strftime("%Y%m%d_%H%M%S")
    return SETTINGS.reports_root / run_mode / stamp


def atomic_replace_path(source: Path, target: Path) -> None:
    """
    使用临时目录 + replace 完成分区原子覆盖。

    后续分区写入流程会先把完整年份分区写到 temp 目录，再整体替换目标目录。
    这样可以避免写到一半时留下损坏分区，也满足幂等重跑场景的可恢复性。
    """

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        backup = target.parent / f"{target.name}.__bak__"
        if backup.exists():
            if backup.is_dir():
                shutil.rmtree(backup)
            else:
                backup.unlink()
        target.replace(backup)
        try:
            source.replace(target)
        except Exception:
            backup.replace(target)
            raise
        if backup.is_dir():
            shutil.rmtree(backup)
        else:
            backup.unlink()
        return
    source.replace(target)


def impacted_years(
    start_date: str | date | None,
    end_date: str | date | None,
) -> list[int]:
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    if start is None or end is None:
        return []
    if start > end:
        start, end = end, start
    return list(range(start.year, end.year + 1))


def spec_update_window(spec: TableSpec) -> int:
    if spec.category == "financial":
        return SETTINGS.financial_backfill_days
    if spec.category in {"daily", "auxiliary"}:
        return SETTINGS.daily_backfill_days
    return spec.update_window_days


def flatten_exceptions(errors: Iterable[BaseException]) -> list[str]:
    return [f"{type(error).__name__}: {error}" for error in errors]
