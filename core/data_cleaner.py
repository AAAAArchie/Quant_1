from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from config import TABLE_SPECS, TableSpec


IDENTIFIER_COLUMNS = {
    "ts_code",
    "exchange",
    "symbol",
    "name",
    "industry",
    "market",
    "list_status",
    "is_hs",
    "curr_type",
    "report_type",
    "comp_type",
    "update_flag",
    "source_table",
    "suspend_type",
    "suspend_reason_type",
}

INTEGER_COLUMNS = {"is_open"}
FLOAT_REGEX = r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$"


@dataclass
class CleanResult:
    frame: pl.DataFrame
    rows_before_dedup: int
    rows_after_dedup: int
    null_counts: dict[str, int]
    primary_key_duplicates: int


def clean_table(table_name: str, frame: pl.DataFrame) -> CleanResult:
    spec = TABLE_SPECS[table_name]
    prepared = _prepare_frame(frame, spec)
    rows_before_dedup = prepared.height
    primary_key_duplicates = count_primary_key_duplicates(prepared, spec.primary_key)
    deduped = deduplicate_by_primary_key(prepared, spec.primary_key)
    null_counts = compute_null_counts(deduped)
    return CleanResult(
        frame=deduped,
        rows_before_dedup=rows_before_dedup,
        rows_after_dedup=deduped.height,
        null_counts=null_counts,
        primary_key_duplicates=primary_key_duplicates,
    )


def compute_null_counts(frame: pl.DataFrame) -> dict[str, int]:
    if frame.is_empty():
        return {column: 0 for column in frame.columns}
    result = frame.select([pl.col(column).null_count().alias(column) for column in frame.columns])
    return {column: int(result.item(0, column)) for column in frame.columns}


def count_primary_key_duplicates(frame: pl.DataFrame, primary_key: tuple[str, ...]) -> int:
    if frame.is_empty():
        return 0
    duplicate_groups = (
        frame.lazy()
        .group_by(list(primary_key))
        .agg(pl.len().alias("__row_count"))
        .filter(pl.col("__row_count") > 1)
        .select((pl.col("__row_count") - 1).sum().alias("duplicate_rows"))
        .collect()
    )
    value = duplicate_groups.item(0, 0)
    return int(value or 0)


def deduplicate_by_primary_key(frame: pl.DataFrame, primary_key: tuple[str, ...]) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    """
    去重不能直接调用 unique(primary_key) 草率收尾。

    这里的显式保留规则是：
    1. 先计算每行非空字段数量，优先保留信息更完整的记录；
    2. 再按主键升序稳定排序，保证重复运行结果一致；
    3. 最后只对主键做 unique(keep='first')。
    """

    expressions: list[pl.Expr] = [
        pl.sum_horizontal([pl.col(column).is_not_null().cast(pl.Int32) for column in frame.columns]).alias(
            "__non_null_score"
        )
    ]
    temp_columns = ["__non_null_score"]
    sort_columns = [*primary_key]
    descending = [False] * len(primary_key)

    """
    财务表在主键冲突时，先按 update_flag 保留最新更新，再按非空字段更多优先。

    这样可以满足：
    - 主键统一为 ts_code + end_date + ann_date
    - update_flag 不参与主键，但在去重前决定保留顺序
    """

    if "update_flag" in frame.columns:
        expressions.append(
            pl.when(pl.col("update_flag").cast(pl.Utf8, strict=False) == "1")
            .then(1)
            .otherwise(0)
            .alias("__update_flag_priority")
        )
        temp_columns.append("__update_flag_priority")
        sort_columns.append("__update_flag_priority")
        descending.append(True)

    sort_columns.append("__non_null_score")
    descending.append(True)

    ordered = (
        frame.lazy()
        .with_columns(expressions)
        .sort(by=sort_columns, descending=descending)
        .unique(subset=list(primary_key), keep="first", maintain_order=True)
        .drop(temp_columns)
        .sort(list(primary_key))
        .collect()
    )
    return ordered


def _prepare_frame(frame: pl.DataFrame, spec: TableSpec) -> pl.DataFrame:
    if frame.is_empty():
        empty_columns = {column: [] for column in _expected_columns(frame, spec)}
        return pl.DataFrame(empty_columns)

    lazy = frame.lazy()
    lazy = _cast_date_columns(lazy, spec.date_columns)
    lazy = _cast_identifier_columns(lazy)
    lazy = _cast_integer_columns(lazy)
    prepared = lazy.collect()
    prepared = _cast_numeric_like_string_columns(prepared, protected_columns=set(spec.date_columns))

    if spec.category == "financial":
        prepared = _prepare_financial_frame(prepared, spec)

    return prepared.sort([column for column in spec.primary_key if column in prepared.columns])


def _expected_columns(frame: pl.DataFrame, spec: TableSpec) -> list[str]:
    columns = list(frame.columns)
    for column in spec.primary_key:
        if column not in columns:
            columns.append(column)
    if spec.category == "financial":
        for extra in ("source_table", "real_ann_date"):
            if extra not in columns:
                columns.append(extra)
    return columns


def _cast_date_columns(lazy: pl.LazyFrame, date_columns: tuple[str, ...]) -> pl.LazyFrame:
    expressions: list[pl.Expr] = []
    for column in date_columns:
        if column in lazy.collect_schema().names():
            raw = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
            expressions.append(
                pl.when(raw.is_in(["", "None", "null", "NULL"]))
                .then(None)
                .otherwise(
                    pl.coalesce(
                        [
                            raw.str.strptime(pl.Date, "%Y%m%d", strict=False),
                            raw.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                            raw.str.strptime(pl.Date, "%Y/%m/%d", strict=False),
                        ]
                    )
                )
                .alias(column)
            )
    if not expressions:
        return lazy
    return lazy.with_columns(expressions)


def _cast_identifier_columns(lazy: pl.LazyFrame) -> pl.LazyFrame:
    schema_names = set(lazy.collect_schema().names())
    expressions = [
        pl.col(column).cast(pl.Utf8, strict=False).alias(column)
        for column in IDENTIFIER_COLUMNS
        if column in schema_names
    ]
    if not expressions:
        return lazy
    return lazy.with_columns(expressions)


def _cast_integer_columns(lazy: pl.LazyFrame) -> pl.LazyFrame:
    schema_names = set(lazy.collect_schema().names())
    expressions = [
        pl.col(column).cast(pl.Int32, strict=False).alias(column)
        for column in INTEGER_COLUMNS
        if column in schema_names
    ]
    if not expressions:
        return lazy
    return lazy.with_columns(expressions)


def _cast_numeric_like_string_columns(
    frame: pl.DataFrame,
    *,
    protected_columns: set[str],
) -> pl.DataFrame:
    """
    只把“全列都像数值”的字符串列显式转成 Float64。

    这样可以避免把 `name`、`industry` 这类文本列误伤成全空列，同时满足
    “数值字段显式 cast” 的要求。判断逻辑完全在 Polars 内完成。
    """

    expressions: list[pl.Expr] = []
    for column, dtype in frame.schema.items():
        if dtype != pl.Utf8 or column in protected_columns or column in IDENTIFIER_COLUMNS:
            continue
        numeric_check = (
            frame.lazy()
            .select(
                pl.col(column)
                .drop_nulls()
                .str.strip_chars()
                .str.contains(FLOAT_REGEX)
                .all()
                .alias("all_numeric")
            )
            .collect()
            .item(0, 0)
        )
        if numeric_check:
            expressions.append(pl.col(column).cast(pl.Float64, strict=False).alias(column))
    if not expressions:
        return frame
    return frame.with_columns(expressions)


def _prepare_financial_frame(frame: pl.DataFrame, spec: TableSpec) -> pl.DataFrame:
    """
    财务表严格保留披露事件流。

    - ann_date 缺失直接过滤，避免任何未来函数风险；
    - source_table 明确保留来源表，后续 DWS 事件流可以混合存放；
    - real_ann_date 取 ann_date / f_ann_date 较早值，用于未来的 backward asof 映射。
    """

    working = frame
    if "source_table" not in working.columns:
        working = working.with_columns(pl.lit(spec.name).alias("source_table"))

    if "f_ann_date" not in working.columns:
        working = working.with_columns(pl.lit(None, dtype=pl.Date).alias("f_ann_date"))

    required_categoricals = ["comp_type", "update_flag"]
    for column in required_categoricals:
        if column not in working.columns:
            working = working.with_columns(pl.lit("").alias(column))

    lazy = working.lazy().filter(pl.col("ann_date").is_not_null())
    if "report_type" in working.columns:
        lazy = lazy.filter(pl.col("report_type").cast(pl.Utf8, strict=False) == "1")
    working = lazy.with_columns(
        pl.when(pl.col("f_ann_date").is_not_null())
        .then(pl.min_horizontal("ann_date", "f_ann_date"))
        .otherwise(pl.col("ann_date"))
        .alias("real_ann_date")
    ).collect()
    return working
