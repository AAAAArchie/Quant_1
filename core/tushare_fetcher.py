from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import polars as pl

from config import FINANCIAL_EVENT_SOURCES, SETTINGS, TUSHARE_HTTP_URL
from core.utils import chunked, flatten_exceptions, format_ymd, iter_date_windows

try:
    import tushare as ts
except ImportError:  # pragma: no cover - 测试中可注入 mock client
    ts = None


@dataclass
class FetchStats:
    """记录单张表在一次运行中的抓取统计。"""

    table_name: str
    request_count: int = 0
    success_count: int = 0
    row_count: int = 0
    retry_count: int = 0
    exception_messages: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "request_count": self.request_count,
            "success_count": self.success_count,
            "row_count": self.row_count,
            "retry_count": self.retry_count,
            "exception_messages": list(self.exception_messages),
        }


ProgressCallback = Callable[[dict[str, Any]], None]


def _default_pro_client() -> Any:
    if ts is None:
        raise RuntimeError("tushare is not installed; install requirements or inject a mock client")
    if not SETTINGS.tushare_token_env:
        raise RuntimeError("tushare token env name is empty")
    token = os.getenv(SETTINGS.tushare_token_env, "")
    if not token:
        raise RuntimeError(f"missing Tushare token in env var: {SETTINGS.tushare_token_env}")

    pro = ts.pro_api(token)
    # 私有代理要求显式覆写 token 和 http_url；仅 ts.set_token 不足以生效。
    pro._DataApi__token = token
    pro._DataApi__http_url = os.getenv(SETTINGS.tushare_http_url_env, TUSHARE_HTTP_URL)
    return pro


class TushareFetcher:
    """
    统一封装 Tushare 调用。

    抓取器只负责：
    - 调接口
    - 重试与限流
    - 把结果转换成 Polars
    - 向上层回报窗口/批次级进度
    """

    def __init__(
        self,
        pro_client: Any | None = None,
        sleep_seconds: float | None = None,
        max_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.pro = pro_client or _default_pro_client()
        self.sleep_seconds = SETTINGS.tushare_sleep_seconds if sleep_seconds is None else sleep_seconds
        self.max_retries = SETTINGS.tushare_max_retries if max_retries is None else max_retries
        self.retry_backoff_seconds = (
            SETTINGS.tushare_retry_backoff_seconds
            if retry_backoff_seconds is None
            else retry_backoff_seconds
        )
        self.sleep_fn = sleep_fn or time.sleep
        self.progress_callback = progress_callback

    def fetch_by_date_windows(
        self,
        table_name: str,
        start_date: str,
        end_date: str,
        window_days: int,
        *,
        date_field_start: str = "start_date",
        date_field_end: str = "end_date",
        base_params: dict[str, Any] | None = None,
        prefer_vip: bool = False,
    ) -> tuple[pl.DataFrame, FetchStats]:
        stats = FetchStats(table_name=table_name)
        frames: list[pl.DataFrame] = []
        params = dict(base_params or {})
        windows = list(iter_date_windows(start_date, end_date, window_days))
        api_name = self._resolve_api_name(table_name, prefer_vip=prefer_vip)

        for index, (window_start, window_end) in enumerate(windows, start=1):
            request_params = {
                **params,
                date_field_start: format_ymd(window_start),
                date_field_end: format_ymd(window_end),
            }
            self._emit_progress(
                event="window_start",
                table_name=table_name,
                api_name=api_name,
                request_index=index,
                request_total=len(windows),
                window_start=window_start,
                window_end=window_end,
                row_count=stats.row_count,
            )
            frame = self._call_api(
                table_name,
                stats,
                request_params,
                api_name=api_name,
                request_index=index,
                request_total=len(windows),
                window_start=window_start,
                window_end=window_end,
            )
            frames.append(frame)

        return self._concat_frames(frames), stats

    def fetch_by_trade_date_list(
        self,
        table_name: str,
        trade_dates: list[str],
        *,
        trade_date_field: str = "trade_date",
        base_params: dict[str, Any] | None = None,
    ) -> tuple[pl.DataFrame, FetchStats]:
        stats = FetchStats(table_name=table_name)
        frames: list[pl.DataFrame] = []
        params = dict(base_params or {})

        for index, trade_date in enumerate(trade_dates, start=1):
            request_params = {**params, trade_date_field: format_ymd(trade_date)}
            self._emit_progress(
                event="trade_date_start",
                table_name=table_name,
                api_name=table_name,
                request_index=index,
                request_total=len(trade_dates),
                trade_date=trade_date,
                row_count=stats.row_count,
            )
            frames.append(
                self._call_api(
                    table_name,
                    stats,
                    request_params,
                    api_name=table_name,
                    request_index=index,
                    request_total=len(trade_dates),
                    trade_date=trade_date,
                )
            )

        return self._concat_frames(frames), stats

    def fetch_by_symbols(
        self,
        table_name: str,
        symbols: list[str],
        *,
        symbol_batch_size: int = 1,
        symbol_field: str = "ts_code",
        base_params: dict[str, Any] | None = None,
        prefer_vip: bool = False,
    ) -> tuple[pl.DataFrame, FetchStats]:
        """
        财务类在某些场景仍可按股票池循环抓。

        当前这里只提供通用能力；是否使用按股票循环还是按日期窗口，由上层 pipeline 决定。
        """

        stats = FetchStats(table_name=table_name)
        frames: list[pl.DataFrame] = []
        params = dict(base_params or {})
        batches = list(chunked(symbols, symbol_batch_size))
        api_name = self._resolve_api_name(table_name, prefer_vip=prefer_vip)

        for index, symbol_group in enumerate(batches, start=1):
            symbol_value = ",".join(symbol_group)
            request_params = {**params, symbol_field: symbol_value}
            self._emit_progress(
                event="symbol_batch_start",
                table_name=table_name,
                api_name=api_name,
                request_index=index,
                request_total=len(batches),
                symbol_field=symbol_field,
                symbol_value=symbol_value,
                row_count=stats.row_count,
            )
            frames.append(
                self._call_api(
                    table_name,
                    stats,
                    request_params,
                    api_name=api_name,
                    request_index=index,
                    request_total=len(batches),
                    symbol_field=symbol_field,
                    symbol_value=symbol_value,
                )
            )

        return self._concat_frames(frames), stats

    def _call_api(
        self,
        table_name: str,
        stats: FetchStats,
        request_params: dict[str, Any],
        *,
        api_name: str,
        request_index: int | None = None,
        request_total: int | None = None,
        window_start: str | None = None,
        window_end: str | None = None,
        trade_date: str | None = None,
        symbol_field: str | None = None,
        symbol_value: str | None = None,
    ) -> pl.DataFrame:
        api = getattr(self.pro, api_name, None)
        if api is None:
            raise AttributeError(f"Tushare client has no api named '{api_name}'")

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            stats.request_count += 1
            try:
                raw = api(**request_params)
                frame = self._to_polars(raw)
                stats.success_count += 1
                stats.row_count += frame.height
                self._emit_progress(
                    event="request_success",
                    table_name=table_name,
                    api_name=api_name,
                    request_index=request_index,
                    request_total=request_total,
                    attempt=attempt,
                    window_start=window_start,
                    window_end=window_end,
                    trade_date=trade_date,
                    symbol_field=symbol_field,
                    symbol_value=symbol_value,
                    request_rows=frame.height,
                    row_count=stats.row_count,
                )
                self.sleep_fn(self.sleep_seconds)
                return frame
            except Exception as exc:  # noqa: BLE001 - 需要完整捕获网络/接口异常
                last_error = exc
                if attempt < self.max_retries:
                    stats.retry_count += 1
                    self._emit_progress(
                        event="request_retry",
                        table_name=table_name,
                        api_name=api_name,
                        request_index=request_index,
                        request_total=request_total,
                        attempt=attempt,
                        window_start=window_start,
                        window_end=window_end,
                        trade_date=trade_date,
                        symbol_field=symbol_field,
                        symbol_value=symbol_value,
                        error=str(exc),
                        row_count=stats.row_count,
                    )
                    self.sleep_fn(self.retry_backoff_seconds * attempt)
                    continue

                stats.exception_messages.append(
                    f"{api_name} params={request_params} failed after {attempt} attempts: {exc}"
                )
                self._emit_progress(
                    event="request_failed",
                    table_name=table_name,
                    api_name=api_name,
                    request_index=request_index,
                    request_total=request_total,
                    attempt=attempt,
                    window_start=window_start,
                    window_end=window_end,
                    trade_date=trade_date,
                    symbol_field=symbol_field,
                    symbol_value=symbol_value,
                    error=str(exc),
                    row_count=stats.row_count,
                )
                break

        if last_error is None:
            raise RuntimeError(f"unexpected failure while calling {api_name}")
        raise last_error

    def _resolve_api_name(self, table_name: str, *, prefer_vip: bool) -> str:
        if prefer_vip and table_name in FINANCIAL_EVENT_SOURCES:
            vip_name = f"{table_name}_vip"
            if hasattr(self.pro, vip_name):
                return vip_name
        return table_name

    def _emit_progress(self, **payload: Any) -> None:
        if self.progress_callback is None:
            return
        self.progress_callback(payload)

    @staticmethod
    def _to_polars(raw: Any) -> pl.DataFrame:
        if raw is None:
            return pl.DataFrame()
        if isinstance(raw, pl.DataFrame):
            return raw
        if hasattr(raw, "to_dict"):
            if hasattr(raw, "where") and hasattr(raw, "notna"):
                raw = raw.astype(object).where(raw.notna(), None)
            records = raw.to_dict(orient="records")
            if not records:
                return pl.DataFrame({column: [] for column in raw.columns})
            return pl.from_dicts(records, strict=False, infer_schema_length=None)
        if isinstance(raw, list):
            return pl.DataFrame(raw, strict=False)
        raise TypeError(f"unsupported Tushare payload type: {type(raw)!r}")

    @staticmethod
    def _concat_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
        non_empty = [frame for frame in frames if frame.width > 0 or frame.height > 0]
        if not non_empty:
            return pl.DataFrame()
        return pl.concat(non_empty, how="diagonal_relaxed")


def summarize_fetch_failures(fetch_stats: list[FetchStats]) -> list[str]:
    failures: list[BaseException] = []
    messages: list[str] = []
    for stat in fetch_stats:
        messages.extend(stat.exception_messages)
    return messages + flatten_exceptions(failures)
