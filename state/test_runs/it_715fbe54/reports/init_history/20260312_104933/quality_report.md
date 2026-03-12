# Data Quality Report

- Run mode: `init_history`
- Started at: `2026-03-12T10:49:33`
- Finished at: `2026-03-12T10:49:33`
- Duration seconds: `0.114`

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
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\trade_cal\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\trade_cal\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\trade_cal\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\trade_cal\year=2025` | 1 | 4 |

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
| ods | 1991 | `state\test_runs\it_715fbe54\data\1_ods_raw\stock_basic\year=1991` | 1 | 1 |
| dwd | 1991 | `state\test_runs\it_715fbe54\data\2_dwd_detail\stock_basic\year=1991` | 1 | 1 |

### daily

- Fetched rows: `6`
- Cleaned rows: `6`
- Dedup: `6 -> 6`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "open": 0, "close": 0, "vol": 0}`
- Trade date coverage: `{"expected_dates": 6, "actual_dates": 6, "missing_dates": []}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\daily\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\daily\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\daily\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\daily\year=2025` | 1 | 4 |

### daily_basic

- Fetched rows: `6`
- Cleaned rows: `6`
- Dedup: `6 -> 6`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "turnover_rate": 0, "pe": 0}`
- Trade date coverage: `{"expected_dates": 6, "actual_dates": 6, "missing_dates": []}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\daily_basic\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\daily_basic\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\daily_basic\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\daily_basic\year=2025` | 1 | 4 |

### adj_factor

- Fetched rows: `6`
- Cleaned rows: `6`
- Dedup: `6 -> 6`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "adj_factor": 0}`
- Trade date coverage: `{"expected_dates": 6, "actual_dates": 6, "missing_dates": []}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\adj_factor\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\adj_factor\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\adj_factor\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\adj_factor\year=2025` | 1 | 4 |

### suspend_d

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "suspend_timing": 0}`
- Trade date coverage: `{"expected_dates": 6, "actual_dates": 0, "missing_dates": ["2024-12-30", "2024-12-31", "2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]}`

### stk_limit

- Fetched rows: `6`
- Cleaned rows: `6`
- Dedup: `6 -> 6`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "trade_date": 0, "up_limit": 0, "down_limit": 0}`
- Trade date coverage: `{"expected_dates": 6, "actual_dates": 6, "missing_dates": []}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\stk_limit\year=2024` | 1 | 2 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\stk_limit\year=2025` | 1 | 4 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\stk_limit\year=2024` | 1 | 2 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\stk_limit\year=2025` | 1 | 4 |

### fina_indicator

- Fetched rows: `2`
- Cleaned rows: `2`
- Dedup: `2 -> 2`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "ann_date": 0, "f_ann_date": 0, "end_date": 0, "report_type": 0, "comp_type": 0, "update_flag": 0, "roe": 0, "source_table": 0, "real_ann_date": 0}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\fina_indicator\year=2024` | 1 | 1 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\fina_indicator\year=2025` | 1 | 1 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\fina_indicator\year=2024` | 1 | 1 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\fina_indicator\year=2025` | 1 | 1 |

### income

- Fetched rows: `2`
- Cleaned rows: `2`
- Dedup: `2 -> 2`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "ann_date": 0, "f_ann_date": 0, "end_date": 0, "report_type": 0, "comp_type": 0, "update_flag": 0, "revenue": 0, "source_table": 0, "real_ann_date": 0}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\income\year=2024` | 1 | 1 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\income\year=2025` | 1 | 1 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\income\year=2024` | 1 | 1 |
| dwd | 2025 | `state\test_runs\it_715fbe54\data\2_dwd_detail\income\year=2025` | 1 | 1 |

### balancesheet

- Fetched rows: `2`
- Cleaned rows: `1`
- Dedup: `1 -> 1`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `1`
- Success count: `1`
- Null counts: `{"ts_code": 0, "ann_date": 0, "f_ann_date": 0, "end_date": 0, "report_type": 0, "comp_type": 0, "update_flag": 0, "total_assets": 0, "source_table": 0, "real_ann_date": 0}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| ods | 2024 | `state\test_runs\it_715fbe54\data\1_ods_raw\balancesheet\year=2024` | 1 | 1 |
| ods | 2025 | `state\test_runs\it_715fbe54\data\1_ods_raw\balancesheet\year=2025` | 1 | 1 |
| dwd | 2024 | `state\test_runs\it_715fbe54\data\2_dwd_detail\balancesheet\year=2024` | 1 | 1 |

### daily_master

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `0`
- Success count: `0`
- Null counts: `{}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| dws | 2024 | `state\test_runs\it_715fbe54\data\3_dws_summary\daily_master\year=2024` | 1 | 2 |
| dws | 2025 | `state\test_runs\it_715fbe54\data\3_dws_summary\daily_master\year=2025` | 1 | 4 |

### financial_events

- Fetched rows: `0`
- Cleaned rows: `0`
- Dedup: `0 -> 0`
- Primary key duplicates: `0`
- Retries: `0`
- Requests: `0`
- Success count: `0`
- Null counts: `{}`
- Trade date coverage: `{}`

| Layer | Year | Path | Files | Rows |
| --- | --- | --- | --- | --- |
| dws | 2024 | `state\test_runs\it_715fbe54\data\3_dws_summary\financial_events\year=2024` | 1 | 3 |
| dws | 2025 | `state\test_runs\it_715fbe54\data\3_dws_summary\financial_events\year=2025` | 1 | 2 |

