from __future__ import annotations

import argparse

from core.duckdb_query import DuckDBQueryService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query financial event stream from DuckDB")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = DuckDBQueryService()
    try:
        frame = service.query_financial_events(
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print(frame.head(args.limit))
    finally:
        service.close()


if __name__ == "__main__":
    main()
