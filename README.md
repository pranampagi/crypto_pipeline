# 🚀 Real-Time Crypto Market Analytics Pipeline

A production-grade, end-to-end Data Engineering project ingesting live cryptocurrency
market data from the **CoinGecko API**, processing it through a **Bronze → Silver → Gold**
medallion architecture using **PySpark**, and serving analytics via a **Snowflake** star schema.


---

## 🏗️ Architecture

```
CoinGecko Public API  (live, every 60s)
         │
         ▼
┌─────────────────────┐
│  Bronze Layer        │  Raw JSON, partitioned by year/month/day/hour
│  (Landing Zone)      │  ← coingecko_poller.py
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Silver Layer        │  Cleaned Parquet, typed, deduplicated
│  (Curated)           │  ← pyspark_transform.py  + dq_checks.py
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Gold Layer          │  Star Schema in Snowflake
│  (Analytics-Ready)   │  ← snowflake_loader.py
│                      │    FACT_MARKET_SNAPSHOT
│                      │    DIM_COIN  ·  DIM_DATE
└─────────────────────┘
         │
         ▼
  Analytical Queries   ← analytical_queries.sql
  (Dashboards / BI)
```

---

## 📂 Project Structure

```
crypto_pipeline/
├── config/
│   └── pipeline_config.yaml        # All config in one place
├── ingestion/
│   └── coingecko_poller.py         # Polls API → writes Bronze JSON
├── transformation/
│   ├── pyspark_transform.py        # Bronze → Silver (PySpark)
│   └── snowflake_loader.py         # Silver → Snowflake Gold
├── data_quality/
│   └── dq_checks.py                # Custom DQ framework (PySpark)
├── sql/
│   ├── ddl_snowflake.sql           # Star schema DDL + stored procedures
│   └── analytical_queries.sql      # 8 Analytical queries
├── notebooks/                      # Databricks Serverless notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_data_quality.py
│   └── 04_gold_snowflake_load.py
├── utils/
│   └── helpers.py                  # Config loader, logger, path utils
├── requirements.txt
├── .gitignore
└── README.md
```

---

## ⚡ Tech Stack

| Component         | Technology                            |
|-------------------|---------------------------------------|
| Ingestion         | Python · Requests · APScheduler       |
| Processing        | PySpark 3.5 (local) / Databricks Serverless |
| Storage           | Parquet (Bronze/Silver) · JSON        |
| Data Warehouse    | Snowflake (Star Schema)               |
| Data Quality      | Custom PySpark DQ Framework           |
| Version Control   | Git                                   |
| Config            | YAML                                  |
| Cloud             | Databricks Unity Catalog Volumes      |

---

## 🚀 Quick Start — Local

### 1. Clone & Install

```bash
git clone https://github.com/your-username/crypto-pipeline.git
cd crypto-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Set Snowflake Credentials

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
```

### 3. Set Up Snowflake (once)

Run `sql/ddl_snowflake.sql` in your Snowflake worksheet — creates the
warehouse, database, schemas, tables, and stored procedures.

### 4. Run a Single Ingestion Cycle (test)

```bash
python ingestion/coingecko_poller.py --once
```

### 5. Run Continuous Polling (every 60s)

```bash
python ingestion/coingecko_poller.py
```

### 6. Transform Bronze → Silver

```bash
python transformation/pyspark_transform.py
```

### 7. Run Data Quality Checks

```bash
python data_quality/dq_checks.py
```

### 8. Load Silver → Snowflake Gold

```bash
python transformation/snowflake_loader.py
```

---

## 🔷 Databricks Serverless Setup

> **Note**: Databricks Community Edition now uses **Serverless compute only** —
> no cluster configuration is needed. The `spark` session is automatically
> provided in every notebook.

### Steps

1. Create a **Unity Catalog** with:
   - Catalog: `crypto_catalog`
   - Schema: `crypto_schema`
   - Volumes: `bronze`, `silver`

2. Import notebooks from the `notebooks/` folder into your Databricks workspace
   (File → Import → Python).

3. Add Snowflake credentials to **Databricks Secrets**:
   ```bash
   databricks secrets create-scope crypto-pipeline
   databricks secrets put --scope crypto-pipeline --key snowflake-account
   databricks secrets put --scope crypto-pipeline --key snowflake-user
   databricks secrets put --scope crypto-pipeline --key snowflake-password
   ```

4. Run notebooks in order:
   ```
   01_bronze_ingestion.py     → Ingest from CoinGecko
   02_silver_transformation.py → PySpark cleanse
   03_data_quality.py          → Validate Silver
   04_gold_snowflake_load.py   → Load to Snowflake
   ```

5. **Schedule** Notebook 1 as a Databricks Workflow Job (every 1 hour).
   Chain Notebooks 2 → 3 → 4 as downstream tasks.

---

## 🗄️ Snowflake Star Schema

```
         ┌─────────────┐
         │  DIM_DATE   │
         │  date_sk PK │
         └──────┬──────┘
                │
┌──────────┐    │    ┌──────────────────────────┐
│ DIM_COIN │────┼────│  FACT_MARKET_SNAPSHOT    │
│ coin_sk  │    │    │  snapshot_id  PK         │
│ coin_id  │    │    │  coin_sk      FK         │
│ symbol   │    └────│  date_sk      FK         │
│ name     │         │  current_price           │
│ ath/atl  │         │  market_cap              │
└──────────┘         │  total_volume            │
                     │  price_change_pct_24h    │
                     │  is_bullish_24h          │
                     │  ...                     │
                     └──────────────────────────┘
```

---

## 📊 Analytical Queries (Gold Layer)

| # | Query | Concepts Demonstrated |
|---|-------|-----------------------|
| 1 | Top 10 by 24h Volume | Aggregation, CTE, ORDER BY |
| 2 | Price Volatility Ranking | RANK(), derived metrics |
| 3 | Hourly Market Cap Trend | Time-series GROUP BY |
| 4 | Price vs Previous Snapshot | LAG(), LEAD(), PARTITION BY |
| 5 | Market Dominance | Running SUM(), CROSS JOIN |
| 6 | Weekend vs Weekday Volume | Conditional aggregation |
| 7 | Rolling 24h Moving Average | ROWS BETWEEN frame |
| 8 | Pipeline Freshness Monitor | Operational health check |

---

## ✅ Data Engineer Alignment

| Requirement | Implementation |
|---|---|
| ETL Pipelines (SQL + PySpark) | Bronze→Silver (PySpark) + Gold (Snowflake SQL) |
| Semi-structured data | Raw JSON from CoinGecko API |
| Data quality & integrity | Custom DQ framework with 8 check types |
| Data warehousing / Star Schema | Snowflake FACT + 2 DIM tables |
| Query optimization | CLUSTER BY, partition pruning, CTEs |
| Databricks | Serverless notebooks (no cluster needed) |
| Snowflake | Target DWH with stored procedures |
| Cloud exposure | Databricks Unity Catalog Volumes |
| Git / version control | Full repo with .gitignore, clean structure |

---

## 🔐 Security Notes

- Credentials are **never** hardcoded — use env vars or Databricks Secrets.
- `.gitignore` excludes all secret files, raw data, and logs.
- Snowflake uses least-privilege `SYSADMIN` role (adjust for production).

---

## 📄 License

MIT — free to use for portfolio and interview purposes.
