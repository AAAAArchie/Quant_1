from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from config import DAILY_MASTER_SOURCES, SETTINGS, TABLE_SPECS
from core.data_cleaner import deduplicate_by_primary_key
from core.utils import atomic_replace_path, dataset_root, partition_path, temp_partition_path


@dataclass
class PartitionWriteResult:
    layer: str
    dataset_name: str
    year: int
    path: Path
    file_count: int
    row_count: int


def build_daily_master(daily: pl.DataFrame, daily_basic: pl.DataFrame, adj_factor: pl.DataFrame) -> pl.DataFrame:
    """
    daily_master 只允许由三张日线表拼接，不允许夹带任何财务字段。
    """

    merged = (
        daily.lazy()
        .join(
            daily_basic.lazy(),
            on=["ts_code", "trade_date"],
            how="left",
            suffix="_daily_basic",
        )
        .join(
            adj_factor.lazy(),
            on=["ts_code", "trade_date"],
            how="left",
            suffix="_adj_factor",
        )
        .sort(["ts_code", "trade_date"])
        .collect()
    )
    return deduplicate_by_primary_key(merged, TABLE_SPECS["daily"].primary_key)


def build_financial_event_stream(financial_frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for table_name, frame in financial_frames.items():
        if frame.is_empty():
            continue
        if "source_table" not in frame.columns:
            frame = frame.with_columns(pl.lit(table_name).alias("source_table"))
        frames.append(frame)

    if not frames:
        return pl.DataFrame(
            {
                "ts_code": [],
                "ann_date": [],
                "f_ann_date": [],
                "real_ann_date": [],
                "end_date": [],
                "report_type": [],
                "comp_type": [],
                "update_flag": [],
                "source_table": [],
            }
        )

    combined = (
        pl.concat(frames, how="diagonal_relaxed")
        .lazy()
        .sort(["ts_code", "real_ann_date", "end_date", "ann_date", "source_table"])
        .collect()
    )
    return combined


def scan_dataset(layer: str, dataset_name: str) -> pl.LazyFrame:
    root = dataset_root(layer, dataset_name)
    if not root.exists():
        return pl.LazyFrame()
    return pl.scan_parquet(str(root / "**" / "*.parquet"))


def merge_and_write_year_partitions(
    layer: str,
    dataset_name: str,
    new_frame: pl.DataFrame,
    *,
    primary_key: tuple[str, ...],
    partition_date_column: str,
) -> list[PartitionWriteResult]:
    if new_frame.is_empty():
        return []

    if partition_date_column not in new_frame.columns:
        raise KeyError(f"missing partition date column: {partition_date_column}")

    staged = new_frame.with_columns(pl.col(partition_date_column).dt.year().alias("__year"))
    impacted_years = (
        staged.select(pl.col("__year").drop_nulls().unique().sort()).get_column("__year").to_list()
    )

    results: list[PartitionWriteResult] = []
    for year in impacted_years:
        year_frame = staged.filter(pl.col("__year") == year).drop("__year")
        existing = read_year_partition(layer, dataset_name, year)
        merged = pl.concat([year_frame, existing], how="diagonal_relaxed")
        merged = deduplicate_by_primary_key(merged, primary_key)
        results.append(write_year_partition(layer, dataset_name, year, merged))
    return results


def read_year_partition(layer: str, dataset_name: str, year: int) -> pl.DataFrame:
    target = partition_path(layer, dataset_name, year)
    if not target.exists():
        return pl.DataFrame()
    files = sorted(target.glob("*.parquet"))
    if not files:
        return pl.DataFrame()
    return pl.scan_parquet([str(path) for path in files]).collect()


def write_year_partition(
    layer: str,
    dataset_name: str,
    year: int,
    frame: pl.DataFrame,
) -> PartitionWriteResult:
    temp_dir = temp_partition_path(layer, dataset_name, year)
    if temp_dir.exists():
        raise FileExistsError(f"temp partition already exists: {temp_dir}")
    temp_dir.mkdir(parents=True, exist_ok=False)

    file_count = _write_frame_to_parquet_parts(frame, temp_dir)
    target_dir = partition_path(layer, dataset_name, year)
    atomic_replace_path(temp_dir, target_dir)
    return PartitionWriteResult(
        layer=layer,
        dataset_name=dataset_name,
        year=year,
        path=target_dir,
        file_count=file_count,
        row_count=frame.height,
    )


def _write_frame_to_parquet_parts(frame: pl.DataFrame, target_dir: Path) -> int:
    if frame.is_empty():
        empty_path = target_dir / "part-000.parquet"
        frame.write_parquet(empty_path, compression=SETTINGS.parquet_compression)
        return 1

    rows_per_file = SETTINGS.max_rows_per_file
    total_files = max(1, math.ceil(frame.height / rows_per_file))
    for index, offset in enumerate(range(0, frame.height, rows_per_file)):
        file_path = target_dir / f"part-{index:03d}.parquet"
        frame.slice(offset, rows_per_file).write_parquet(
            file_path,
            compression=SETTINGS.parquet_compression,
        )
    return total_files


def build_dws_outputs(dwd_frames: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    outputs: dict[str, pl.DataFrame] = {}
    if all(name in dwd_frames for name in DAILY_MASTER_SOURCES):
        outputs["daily_master"] = build_daily_master(
            dwd_frames["daily"],
            dwd_frames["daily_basic"],
            dwd_frames["adj_factor"],
        )

    financial_inputs = {
        name: dwd_frames[name]
        for name in ("fina_indicator", "income", "balancesheet")
        if name in dwd_frames
    }
    if financial_inputs:
        outputs["financial_events"] = build_financial_event_stream(financial_inputs)
    return outputs


def partition_results_to_dict(results: Iterable[PartitionWriteResult]) -> list[dict[str, object]]:
    return [
        {
            "layer": result.layer,
            "dataset_name": result.dataset_name,
            "year": result.year,
            "path": str(result.path),
            "file_count": result.file_count,
            "row_count": result.row_count,
        }
        for result in results
    ]
