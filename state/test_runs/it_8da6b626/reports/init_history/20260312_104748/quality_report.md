# Data Quality Report

- Run mode: `init_history`
- Started at: `2026-03-12T10:47:48`
- Finished at: `2026-03-12T10:47:48`
- Duration seconds: `0.026`

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
| ods | 2024 | `state\test_runs\it_8da6b626\data\1_ods_raw\trade_cal\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_8da6b626\data\1_ods_raw\trade_cal\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_8da6b626\data\2_dwd_detail\trade_cal\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_8da6b626\data\2_dwd_detail\trade_cal\year=2025` | 1 | 4 |

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
| ods | 1991 | `state\test_runs\it_8da6b626\data\1_ods_raw\stock_basic\year=1991` | 1 | 1 |
| dwd | 1991 | `state\test_runs\it_8da6b626\data\2_dwd_detail\stock_basic\year=1991` | 1 | 1 |

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

### daily_basic

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "turnover_rate": 0, "pe": 0}`
- Trade date coverage: `{"expected_dates": 0, "actual_dates": 0, "missing_dates": []}`

### adj_factor

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "adj_factor": 0}`
- Trade date coverage: `{"expected_dates": 0, "actual_dates": 0, "missing_dates": []}`

### suspend_d

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "suspend_timing": 0}`
- Trade date coverage: `{"expected_dates": 0, "actual_dates": 0, "missing_dates": []}`

### stk_limit

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "up_limit": 0, "down_limit": 0}`
- Trade date coverage: `{"expected_dates": 0, "actual_dates": 0, "missing_dates": []}`

### fina_indicator

- Fetched rows: `2`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{}`
- Trade date coverage: `{}`

## Exceptions

- unable to find column "source_table"; valid columns: ["ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "update_flag", "roe", "__non_null_score"]

Resolved plan until failure:

	---> FAILED HERE RESOLVING 'sink' <---
 WITH_COLUMNS:
 [col("ts_code").is_not_null().strict_cast(Int32).sum_horizontal([col("ann_date").is_not_null().strict_cast(Int32), col("f_ann_date").is_not_null().strict_cast(Int32), col("end_date").is_not_null().strict_cast(Int32), col("report_type").is_not_null().strict_cast(Int32), col("comp_type").is_not_null().strict_cast(Int32), col("update_flag").is_not_null().strict_cast(Int32), col("roe").is_not_null().strict_cast(Int32)]).alias("__non_null_score")] 
  DF ["ts_code", "ann_date", "f_ann_date", "end_date", ...]; PROJECT */8 COLUMNS
