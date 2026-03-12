from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from core.utils import parse_ymd


@dataclass(frozen=True)
class TradingCalendar:
    data: pl.DataFrame

    @classmethod
    def from_frame(cls, frame: pl.DataFrame) -> "TradingCalendar":
        if frame.is_empty():
            return cls(pl.DataFrame({"exchange": [], "cal_date": [], "is_open": []}))

        cal_date_expr = pl.coalesce(
            [
                pl.col("cal_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False),
                pl.col("cal_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            ]
        )
        cleaned = (
            frame.lazy()
            .with_columns(
                [
                    pl.col("exchange").cast(pl.Utf8),
                    cal_date_expr.alias("cal_date"),
                    pl.col("is_open").cast(pl.Int8, strict=False),
                ]
            )
            .drop_nulls(["exchange", "cal_date", "is_open"])
            .sort(["exchange", "cal_date"])
            .collect()
        )
        return cls(cleaned)

    @classmethod
    def from_parquet(cls, parquet_path: str | Path) -> "TradingCalendar":
        frame = (
            pl.scan_parquet(str(parquet_path))
            .select(["exchange", "cal_date", "is_open"])
            .collect()
        )
        return cls.from_frame(frame)

    def between(
        self,
        start_date: str,
        end_date: str,
        *,
        exchange: str = "SSE",
        open_only: bool = True,
    ) -> list[date]:
        start = parse_ymd(start_date)
        end = parse_ymd(end_date)
        if start is None or end is None:
            raise ValueError("start_date and end_date must be valid")

        lazy = self.data.lazy().filter(pl.col("exchange") == exchange)
        if open_only:
            lazy = lazy.filter(pl.col("is_open") == 1)
        result = (
            lazy.filter(pl.col("cal_date").is_between(start, end, closed="both"))
            .select("cal_date")
            .sort("cal_date")
            .collect()
        )
        return result.get_column("cal_date").to_list()

    def latest_trade_date_on_or_before(
        self,
        target_date: str,
        *,
        exchange: str = "SSE",
    ) -> date | None:
        target = parse_ymd(target_date)
        if target is None:
            raise ValueError("target_date must be valid")

        result = (
            self.data.lazy()
            .filter(
                (pl.col("exchange") == exchange)
                & (pl.col("is_open") == 1)
                & (pl.col("cal_date") <= target)
            )
            .select(pl.col("cal_date").max())
            .collect()
        )
        value = result.item(0, 0)
        return value

    def recent_trade_dates(
        self,
        end_date: str,
        lookback_days: int,
        *,
        exchange: str = "SSE",
    ) -> list[date]:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be positive")

        anchor = self.latest_trade_date_on_or_before(end_date, exchange=exchange)
        if anchor is None:
            return []
        result = (
            self.data.lazy()
            .filter(
                (pl.col("exchange") == exchange)
                & (pl.col("is_open") == 1)
                & (pl.col("cal_date") <= anchor)
            )
            .sort("cal_date", descending=True)
            .limit(lookback_days)
            .sort("cal_date")
            .collect()
        )
        return result.get_column("cal_date").to_list()

    def coverage_summary(
        self,
        actual_trade_dates: list[str],
        *,
        start_date: str,
        end_date: str,
        exchange: str = "SSE",
    ) -> dict[str, object]:
        expected_dates = self.between(start_date, end_date, exchange=exchange, open_only=True)
        actual = {parse_ymd(value) for value in actual_trade_dates if parse_ymd(value) is not None}
        missing = [value.isoformat() for value in expected_dates if value not in actual]
        return {
            "expected_dates": len(expected_dates),
            "actual_dates": len(actual),
            "missing_dates": missing,
        }
