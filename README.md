# A 股本地量化数据模块

基于 `Tushare Pro + Polars + Parquet + DuckDB` 的本地量化数据工程，目标是提供可初始化、可日更、可查询、可测试的 A 股研究数据底座，并严格执行财务 PIT 约束与未来函数防护。

## 项目结构

```text
.
├─ config.py
├─ init_history.py
├─ run_daily_update.py
├─ core/
│  ├─ data_cleaner.py
│  ├─ duckdb_query.py
│  ├─ pipeline_builder.py
│  ├─ quality_reporter.py
│  ├─ trading_calendar.py
│  ├─ tushare_fetcher.py
│  └─ utils.py
├─ scripts/
│  ├─ query_daily_master_demo.py
│  └─ query_financial_simple_demo.py
└─ tests/
   ├─ unit/
   └─ integration/
```

## 环境安装

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Tushare Token 配置

Windows PowerShell:

```powershell
$env:TUSHARE_TOKEN = "your_token_here"
```

如需长期生效，可写入用户环境变量。项目默认读取环境变量 `TUSHARE_TOKEN`。

## 数据分层

- `data/1_ods_raw`: Tushare 原始数据保留层
- `data/2_dwd_detail`: Polars 清洗、统一类型、去重后的明细层
- `data/3_dws_summary`: 研究就绪层

说明：

- `daily_master` 仅由 `daily + daily_basic + adj_factor` 构建
- 财务表在 DWD / DWS 中保留事件流，不会直接拼到 `daily_master`
- 财务映射到交易日时，后续策略层只能基于 `real_ann_date <= trade_date` 做 `backward asof join`

## 历史初始化

从 2015 年起拉全量历史，支持限流、重试、窗口抓取、分阶段落盘和可恢复执行。

```bash
python init_history.py --start-date 20150101 --end-date 20260312
```

如需忽略已有状态强制重跑：

```bash
python init_history.py --start-date 20150101 --end-date 20260312 --force
```

## 每日更新

默认执行滑窗回补：

- 日线类回补最近 5 个交易日
- 财务类回补最近 90 个自然日

```bash
python run_daily_update.py --as-of-date 20260312
```

## 示例查询

查询某年 `daily_master`：

```bash
python scripts/query_daily_master_demo.py --year 2024 --ts-code 000001.SZ --start-date 2024-01-01 --end-date 2024-12-31
```

查询某只股票的财务事件流：

```bash
python scripts/query_financial_simple_demo.py --ts-code 000001.SZ --start-date 2024-01-01 --end-date 2025-12-31
```

## 测试运行

测试全部使用 mock 数据，不依赖真实联网调用 Tushare。

```bash
pytest tests/unit tests/integration -q
```

## 质量报告

每次初始化或日更结束后会输出：

- 终端摘要
- `reports/<run_mode>/<timestamp>/quality_report.json`
- `reports/<run_mode>/<timestamp>/quality_report.md`

报告包含抓取记录数、清洗记录数、去重前后、空值统计、主键重复、交易日覆盖、分区写入、重试次数和异常摘要。
