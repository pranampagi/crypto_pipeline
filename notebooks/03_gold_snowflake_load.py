# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Notebook 3 — Gold Load into Snowflake (Serverless)
# MAGIC **Pipeline**: Silver Parquet → Snowflake Staging → DIM/FACT Gold Tables
# MAGIC
# MAGIC **Compute**: Databricks Serverless — no cluster config needed.
# MAGIC
# MAGIC **Prerequisites**: Add Snowflake credentials to Databricks Secrets (see cell below).

# COMMAND ----------

# MAGIC %md ## 1. Install Snowflake Connector

# COMMAND ----------

# %pip install snowflake-connector-python

# COMMAND ----------

# MAGIC %md ## 2. Load Snowflake Credentials from Databricks Secrets
# MAGIC
# MAGIC Create a secret scope first:
# MAGIC ```bash
# MAGIC databricks secrets create-scope crypto-pipeline
# MAGIC databricks secrets put --scope crypto-pipeline --key snowflake-account
# MAGIC databricks secrets put --scope crypto-pipeline --key snowflake-user
# MAGIC databricks secrets put --scope crypto-pipeline --key snowflake-password
# MAGIC ```

# COMMAND ----------

import os

# Try Databricks Secrets first, fall back to env vars
def get_secret(scope: str, key: str, env_var: str) -> str:
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        val = os.environ.get(env_var, "")
        if not val:
            raise EnvironmentError(
                f"Secret '{key}' not found in scope '{scope}' "
                f"and env var '{env_var}' is not set."
            )
        return val

SNOWFLAKE_ACCOUNT   = get_secret("crypto-pipeline", "snowflake-account",   "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = get_secret("crypto-pipeline", "snowflake-user",      "SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = get_secret("crypto-pipeline", "snowflake-password",  "SNOWFLAKE_PASSWORD")

SF_CONFIG = {
    "account":   SNOWFLAKE_ACCOUNT,
    "user":      SNOWFLAKE_USER,
    "password":  SNOWFLAKE_PASSWORD,
    "warehouse": "CRYPTO_WH",
    "database":  "CRYPTO_DB",
    "schema":    "GOLD",
    "role":      "SYSADMIN",
}

print("✅ Snowflake config loaded (credentials hidden).")

# COMMAND ----------

# MAGIC %md ## 3. Read Silver

# COMMAND ----------

SILVER_PATH = "/Volumes/crypto_catalog/crypto_schema/silver"

silver_df = spark.read.parquet(SILVER_PATH)
total_records = silver_df.count()
print(f"Silver records to load: {total_records:,}")

# COMMAND ----------

# MAGIC %md ## 4. Write Silver → Snowflake Staging (via Spark Connector)
# MAGIC
# MAGIC Using the **Snowflake Spark Connector** — available in Databricks Runtime by default.

# COMMAND ----------

SFPARAMS = {
    "sfURL":       f"{SF_CONFIG['account']}.snowflakecomputing.com",
    "sfUser":      SF_CONFIG["user"],
    "sfPassword":  SF_CONFIG["password"],
    "sfDatabase":  SF_CONFIG["database"],
    "sfWarehouse": SF_CONFIG["warehouse"],
    "sfRole":      SF_CONFIG["role"],
}

# Flatten column names to upper-case for Snowflake
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

stage_cols = [
    "coin_id", "symbol", "name",
    "current_price", "high_24h", "low_24h",
    "price_change_24h", "price_change_pct_24h", "price_change_pct_7d",
    "price_range_24h", "market_cap", "market_cap_rank",
    "market_cap_change_24h", "market_cap_change_pct_24h",
    "fully_diluted_valuation", "total_volume", "volume_to_market_cap_ratio",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_pct", "ath_date",
    "atl", "atl_change_pct", "atl_date",
    "is_bullish_24h", "batch_ingested_at", "last_updated", "ingestion_id"
]

available = [c for c in stage_cols if c in silver_df.columns]
stage_df  = silver_df.select(available)

print(f"Writing {stage_df.count():,} rows to Snowflake staging ...")

(
    stage_df.write
    .format("snowflake")
    .options(**SFPARAMS)
    .option("sfSchema", "STAGING")
    .option("dbtable", "STG_MARKET_SNAPSHOT")
    .option("truncate_table", "ON")
    .mode("overwrite")
    .save()
)

print("✅ Staging load complete.")

# COMMAND ----------

# MAGIC %md ## 5. Execute Gold Stored Procedures

# COMMAND ----------

import snowflake.connector
from datetime import datetime, timezone

def run_sf_procedure(conn, proc_call: str) -> str:
    with conn.cursor() as cur:
        cur.execute(proc_call)
        return cur.fetchone()[0]

with snowflake.connector.connect(**SF_CONFIG) as conn:
    conn.cursor().execute("USE DATABASE CRYPTO_DB")

    print("1️⃣  Upserting DIM_COIN ...")
    r = run_sf_procedure(conn, "CALL GOLD.SP_UPSERT_DIM_COIN()")
    print(f"   {r}")

    print("2️⃣  Populating DIM_DATE ...")
    ts = datetime.now(timezone.utc).isoformat()
    r = run_sf_procedure(conn, f"CALL GOLD.SP_POPULATE_DIM_DATE('{ts}'::TIMESTAMP_TZ)")
    print(f"   {r}")

    print("3️⃣  Loading FACT_MARKET_SNAPSHOT ...")
    r = run_sf_procedure(conn, "CALL GOLD.SP_LOAD_FACT()")
    print(f"   {r}")

print("\n✅ Gold load procedures complete.")

# COMMAND ----------

# MAGIC %md ## 6. Verify Row Counts

# COMMAND ----------

with snowflake.connector.connect(**SF_CONFIG) as conn:
    conn.cursor().execute("USE DATABASE CRYPTO_DB; USE SCHEMA GOLD;")
    for tbl in ["FACT_MARKET_SNAPSHOT", "DIM_COIN", "DIM_DATE"]:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            cnt = cur.fetchone()[0]
            print(f"  {tbl:35s}: {cnt:>10,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Run a Sample Analytical Query (Direct Preview)

# COMMAND ----------

with snowflake.connector.connect(**SF_CONFIG) as conn:
    conn.cursor().execute("USE DATABASE CRYPTO_DB; USE SCHEMA GOLD; USE WAREHOUSE CRYPTO_WH;")
    query = """
        WITH latest AS (SELECT MAX(batch_ingested_at) ts FROM FACT_MARKET_SNAPSHOT)
        SELECT dc.name, dc.symbol,
               f.current_price, f.market_cap, f.total_volume,
               f.price_change_pct_24h,
               RANK() OVER (ORDER BY f.market_cap DESC) AS rank
        FROM FACT_MARKET_SNAPSHOT f
        JOIN DIM_COIN dc ON dc.coin_sk = f.coin_sk
        JOIN latest l    ON f.batch_ingested_at = l.ts
        ORDER BY rank
        LIMIT 15
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

import pandas as pd
preview_df = pd.DataFrame(rows, columns=cols)
display(spark.createDataFrame(preview_df))

# COMMAND ----------

print("\n🏆 Gold Layer Pipeline Complete!")
print(f"   Silver records processed : {total_records:,}")
print(f"   Snowflake target         : {SF_CONFIG['database']}.GOLD")
