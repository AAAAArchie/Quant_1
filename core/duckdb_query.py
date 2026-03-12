from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from config import SETTINGS


class DuckDBQueryService:
    def __init__(self, database: str = ":memory:") -> None:
        self.connection = duckdb.connect(database=database)

    def query_daily_master(
        self,
        *,
        year: int,
        ts_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pl.DataFrame:
        dataset_glob = self._dataset_glob("daily_master", year=year)
        filters = ["1=1"]
        params: list[Any] = []

        if ts_code:
            filters.append("ts_code = ?")
            params.append(ts_code)
        if start_date:
            filters.append("trade_date >= CAST(? AS DATE)")
            params.append(start_date)
        if end_date:
            filters.append("trade_date <= CAST(? AS DATE)")
            params.append(end_date)

        sql = f"""
            SELECT *
            FROM read_parquet('{dataset_glob}')
            WHERE {' AND '.join(filters)}
            ORDER BY ts_code, trade_date
        """
        return self._query_polars(sql, params)

    def query_financial_events(
        self,
        *,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pl.DataFrame:
        dataset_glob = self._dataset_glob("financial_events")
        filters = ["ts_code = ?"]
        params: list[Any] = [ts_code]

        if start_date:
            filters.append("real_ann_date >= CAST(? AS DATE)")
            params.append(start_date)
        if end_date:
            filters.append("real_ann_date <= CAST(? AS DATE)")
            params.append(end_date)

        sql = f"""
            SELECT
                ts_code,
                source_table,
                ann_date,
                f_ann_date,
                real_ann_date,
                end_date,
                report_type,
                comp_type,
                update_flag
            FROM read_parquet('{dataset_glob}')
            WHERE {' AND '.join(filters)}
            ORDER BY real_ann_date, end_date, source_table
        """
        return self._query_polars(sql, params)

    def _query_polars(self, sql: str, params: list[Any]) -> pl.DataFrame:
        arrow_table = self.connection.execute(sql, params).fetch_arrow_table()
        return pl.from_arrow(arrow_table)

    @staticmethod
    def _dataset_glob(dataset_name: str, *, year: int | None = None) -> str:
        base = SETTINGS.layers["dws"] / dataset_name
        if year is not None:
            return str(base / f"{SETTINGS.partition_column}={year}" / "*.parquet")
        return str(base / "*" / "*.parquet")

    def close(self) -> None:
        self.connection.close()
