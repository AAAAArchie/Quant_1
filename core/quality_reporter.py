from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from config import SETTINGS
from core.utils import atomic_replace_path, quality_report_dir


@dataclass
class TableQualityStats:
    fetched_rows: int = 0
    cleaned_rows: int = 0
    rows_before_dedup: int = 0
    rows_after_dedup: int = 0
    retry_count: int = 0
    request_count: int = 0
    success_count: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    primary_key_duplicates: int = 0
    trade_date_coverage: dict[str, Any] = field(default_factory=dict)
    partition_writes: list[dict[str, Any]] = field(default_factory=list)
    exception_messages: list[str] = field(default_factory=list)


@dataclass
class RunQualityReport:
    run_mode: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    tables: dict[str, TableQualityStats] = field(default_factory=dict)
    exceptions: list[str] = field(default_factory=list)


class QualityReporter:
    """
    统一维护运行质量报告，并同时输出终端 / JSON / Markdown。

    报告结构先在这里定死，后续任何 pipeline 只负责填数，不再各自拼接散乱日志。
    """

    def __init__(self, run_mode: str, started_at: datetime | None = None) -> None:
        self.started_at = started_at or datetime.now()
        self.report = RunQualityReport(
            run_mode=run_mode,
            started_at=self.started_at.isoformat(timespec="seconds"),
        )

    def ensure_table(self, table_name: str) -> TableQualityStats:
        if table_name not in self.report.tables:
            self.report.tables[table_name] = TableQualityStats()
        return self.report.tables[table_name]

    def add_fetch_stats(
        self,
        table_name: str,
        *,
        fetched_rows: int,
        request_count: int,
        success_count: int,
        retry_count: int,
        exception_messages: list[str] | None = None,
    ) -> None:
        table = self.ensure_table(table_name)
        table.fetched_rows += fetched_rows
        table.request_count += request_count
        table.success_count += success_count
        table.retry_count += retry_count
        if exception_messages:
            table.exception_messages.extend(exception_messages)

    def add_clean_stats(
        self,
        table_name: str,
        *,
        cleaned_rows: int,
        rows_before_dedup: int,
        rows_after_dedup: int,
        null_counts: dict[str, int],
        primary_key_duplicates: int,
    ) -> None:
        table = self.ensure_table(table_name)
        table.cleaned_rows = cleaned_rows
        table.rows_before_dedup = rows_before_dedup
        table.rows_after_dedup = rows_after_dedup
        table.null_counts = dict(null_counts)
        table.primary_key_duplicates = primary_key_duplicates

    def add_trade_date_coverage(
        self,
        table_name: str,
        *,
        expected_dates: int,
        actual_dates: int,
        missing_dates: list[str],
    ) -> None:
        table = self.ensure_table(table_name)
        table.trade_date_coverage = {
            "expected_dates": expected_dates,
            "actual_dates": actual_dates,
            "missing_dates": list(missing_dates),
        }

    def add_partition_write(
        self,
        table_name: str,
        *,
        layer: str,
        year: int,
        path: str,
        file_count: int,
        row_count: int,
    ) -> None:
        table = self.ensure_table(table_name)
        table.partition_writes.append(
            {
                "layer": layer,
                "year": year,
                "path": path,
                "file_count": file_count,
                "row_count": row_count,
            }
        )

    def add_exception(self, message: str, table_name: str | None = None) -> None:
        self.report.exceptions.append(message)
        if table_name:
            table = self.ensure_table(table_name)
            table.exception_messages.append(message)

    def finalize(self, finished_at: datetime | None = None) -> dict[str, Any]:
        ended = finished_at or datetime.now()
        self.report.finished_at = ended.isoformat(timespec="seconds")
        self.report.duration_seconds = round((ended - self.started_at).total_seconds(), 3)
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self.report)
        result["tables"] = {
            table_name: asdict(stats) for table_name, stats in self.report.tables.items()
        }
        return result

    def write_outputs(self) -> tuple[Path, Path]:
        payload = self.finalize()
        report_dir = quality_report_dir(self.report.run_mode, self.started_at)
        report_dir.mkdir(parents=True, exist_ok=True)

        json_path = report_dir / SETTINGS.quality_json_name
        md_path = report_dir / SETTINGS.quality_md_name
        self._write_json_atomic(json_path, payload)
        self._write_markdown_atomic(md_path, payload)
        print(self.render_console_summary(payload))
        return json_path, md_path

    def render_console_summary(self, payload: dict[str, Any] | None = None) -> str:
        data = payload or self.to_dict()
        lines = [
            f"run_mode={data['run_mode']}",
            f"started_at={data['started_at']}",
            f"finished_at={data.get('finished_at')}",
            f"duration_seconds={data.get('duration_seconds')}",
        ]
        for table_name, stats in data["tables"].items():
            lines.append(
                " | ".join(
                    [
                        f"table={table_name}",
                        f"fetched={stats['fetched_rows']}",
                        f"cleaned={stats['cleaned_rows']}",
                        f"dedup={stats['rows_before_dedup']}->{stats['rows_after_dedup']}",
                        f"pk_dup={stats['primary_key_duplicates']}",
                        f"retries={stats['retry_count']}",
                    ]
                )
            )
        if data["exceptions"]:
            lines.append(f"exceptions={len(data['exceptions'])}")
        return "\n".join(lines)

    def _write_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        atomic_replace_path(temp_path, path)

    def _write_markdown_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(self._render_markdown(payload), encoding="utf-8")
        atomic_replace_path(temp_path, path)

    def _render_markdown(self, payload: dict[str, Any]) -> str:
        lines = [
            "# Data Quality Report",
            "",
            f"- Run mode: `{payload['run_mode']}`",
            f"- Started at: `{payload['started_at']}`",
            f"- Finished at: `{payload.get('finished_at')}`",
            f"- Duration seconds: `{payload.get('duration_seconds')}`",
            "",
            "## Tables",
            "",
        ]
        for table_name, stats in payload["tables"].items():
            lines.extend(
                [
                    f"### {table_name}",
                    "",
                    f"- Fetched rows: `{stats['fetched_rows']}`",
                    f"- Cleaned rows: `{stats['cleaned_rows']}`",
                    f"- Dedup: `{stats['rows_before_dedup']} -> {stats['rows_after_dedup']}`",
                    f"- Primary key duplicates: `{stats['primary_key_duplicates']}`",
                    f"- Retries: `{stats['retry_count']}`",
                    f"- Requests: `{stats['request_count']}`",
                    f"- Success count: `{stats['success_count']}`",
                    f"- Null counts: `{json.dumps(stats['null_counts'], ensure_ascii=False)}`",
                    f"- Trade date coverage: `{json.dumps(stats['trade_date_coverage'], ensure_ascii=False)}`",
                    "",
                ]
            )
            if stats["partition_writes"]:
                lines.append("| Layer | Year | Path | Files | Rows |")
                lines.append("| --- | --- | --- | --- | --- |")
                for row in stats["partition_writes"]:
                    lines.append(
                        f"| {row['layer']} | {row['year']} | `{row['path']}` | {row['file_count']} | {row['row_count']} |"
                    )
                lines.append("")
            if stats["exception_messages"]:
                lines.append("Exceptions:")
                for message in stats["exception_messages"]:
                    lines.append(f"- {message}")
                lines.append("")

        if payload["exceptions"]:
            lines.append("## Exceptions")
            lines.append("")
            for message in payload["exceptions"]:
                lines.append(f"- {message}")
        return "\n".join(lines) + "\n"
