# Data Quality Report

- Run mode: `init_history`
- Started at: `2026-03-12T10:46:48`
- Finished at: `2026-03-12T10:46:48`
- Duration seconds: `0.034`

## Tables

### trade_cal

- Fetched rows: `6`
- Cleaned rows: `6`
- Dedup: `6 -> 6`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `3`
- Success count: `3`
- Null counts: `{"exchange": 0, "cal_date": 0, "is_open": 0, "pretrade_date": 0}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_5e50f5a3\data\1_ods_raw\trade_cal\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_5e50f5a3\data\1_ods_raw\trade_cal\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_5e50f5a3\data\2_dwd_detail\trade_cal\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_5e50f5a3\data\2_dwd_detail\trade_cal\year=2025` | 1 | 4 |

### stock_basic

- Fetched rows: `1`
- Cleaned rows: `1`
- Dedup: `1 -> 1`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `3`
- Success count: `3`
- Null counts: `{"ts_code": 0, "symbol": 0, "name": 0, "area": 0, "industry": 0, "market": 0, "list_status": 0, "list_date": 0}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 1991 | `state\test_runs\it_5e50f5a3\data\1_ods_raw\stock_basic\year=1991` | 1 | 1 |
| dwd | 1991 | `state\test_runs\it_5e50f5a3\data\2_dwd_detail\stock_basic\year=1991` | 1 | 1 |

### daily

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "open": 0, "close": 0, "vol": 0}`
- Trade date coverage: `{"expected_dates": 0, "actual_dates": 0, "missing_dates": []}`

## Exceptions

- `year` operation not supported for dtype `null`
