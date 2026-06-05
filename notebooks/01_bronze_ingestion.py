# Databricks notebook source
# MAGIC %md
# MAGIC # 🟫 Notebook 1 — Bronze Ingestion (Serverless)
# MAGIC **Pipeline**: CoinGecko → Raw JSON → Bronze Volume
# MAGIC
# MAGIC **Compute**: Databricks Serverless (no cluster required)
# MAGIC
# MAGIC Run this notebook on a schedule (every 1 hour) using a Databricks Workflow.

# COMMAND ----------

# MAGIC %md ## 1. Install Dependencies

# COMMAND ----------

# %pip install pyyaml requests   # Uncomment if not available in your environment

# COMMAND ----------

# MAGIC %md ## 2. Configuration

# COMMAND ----------

# ── Inline config (no YAML file needed in Databricks) ──
CONFIG = {
    "api": {
        "base_url": "https://api.coingecko.com/api/v3",
        "endpoint": "/coins/markets",
        "params": {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h,7d"
        },
        "max_retries": 3,
        "retry_backoff_seconds": 5,
    },
    # ── Databricks Unity Catalog Volumes path ──
    # Replace with your actual catalog and schema names
    "bronze_volume": "/Volumes/crypto_catalog/crypto_schema/bronze",
}

# COMMAND ----------

# MAGIC %md ## 3. Create Volume Directory Structure (Unity Catalog)

# COMMAND ----------

import os

bronze_base = CONFIG["bronze_volume"]

# Unity Catalog Volumes support standard file operations via dbutils
try:
    dbutils.fs.mkdirs(bronze_base)
    print(f"✅ Bronze volume ready: {bronze_base}")
except Exception as e:
    print(f"ℹ️  Note: {e}")
    # Fallback to /tmp for local or non-UC environments
    bronze_base = "/tmp/crypto_pipeline/bronze"
    os.makedirs(bronze_base, exist_ok=True)
    print(f"Using fallback path: {bronze_base}")

CONFIG["bronze_volume"] = bronze_base

# COMMAND ----------

# MAGIC %md ## 4. CoinGecko Fetch Function

# COMMAND ----------

import requests
import json
import uuid
import time
from datetime import datetime, timezone


def fetch_coingecko_markets(config: dict) -> list:
    """Fetch live market data from CoinGecko with retry."""
    url = config["api"]["base_url"] + config["api"]["endpoint"]
    params = config["api"]["params"]
    max_retries = config["api"]["max_retries"]
    backoff = config["api"]["retry_backoff_seconds"]

    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Fetching CoinGecko markets (attempt {attempt}/{max_retries})...")
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            print(f"  ✅ Fetched {len(data)} coins.")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = backoff * attempt * 2
                print(f"  ⚠️ Rate limited. Waiting {wait}s ...")
                time.sleep(wait)
            else:
                raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"  ⚠️ {e}. Retrying in {backoff}s ...")
            time.sleep(backoff)

    raise RuntimeError("CoinGecko fetch failed after all retries.")


# COMMAND ----------

# MAGIC %md ## 5. Write to Bronze Volume

# COMMAND ----------

def write_bronze(raw_data: list, bronze_base: str) -> str:
    """Write raw JSON envelope to partitioned Bronze path."""
    ts = datetime.now(timezone.utc)
    partition = (
        f"year={ts.year}/month={ts.month:02d}/"
        f"day={ts.day:02d}/hour={ts.hour:02d}"
    )
    partition_path = f"{bronze_base}/{partition}"

    # Create directory
    try:
        dbutils.fs.mkdirs(partition_path)
    except Exception:
        os.makedirs(partition_path.replace("/dbfs", ""), exist_ok=True)

    ingestion_id = str(uuid.uuid4())
    envelope = {
        "ingestion_id": ingestion_id,
        "source": "coingecko_markets_api",
        "ingested_at": ts.isoformat(),
        "record_count": len(raw_data),
        "data": raw_data
    }

    filename = f"markets_{ts.strftime('%Y%m%d_%H%M%S')}_{ingestion_id[:8]}.json"
    filepath = f"{partition_path}/{filename}"

    # Write file
    json_str = json.dumps(envelope, ensure_ascii=False, indent=2)

    try:
        # Databricks Volumes: use dbutils or direct file write
        with open(filepath, "w") as f:
            f.write(json_str)
    except Exception:
        # Fallback: write to /tmp
        os.makedirs(f"/tmp/crypto_pipeline/bronze/{partition}", exist_ok=True)
        filepath = f"/tmp/crypto_pipeline/bronze/{partition}/{filename}"
        with open(filepath, "w") as f:
            f.write(json_str)

    print(f"  📁 Bronze written → {filepath}")
    return filepath


# COMMAND ----------

# MAGIC %md ## 6. Run Ingestion

# COMMAND ----------

print("=" * 55)
print(f"  🚀 Bronze Ingestion — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("=" * 55)

raw_data = fetch_coingecko_markets(CONFIG)
bronze_file = write_bronze(raw_data, CONFIG["bronze_volume"])

print(f"\n✅ Ingestion complete.")
print(f"   Records  : {len(raw_data)}")
print(f"   File     : {bronze_file}")

# COMMAND ----------

# MAGIC %md ## 7. Quick Validation — Preview Ingested Data

# COMMAND ----------

# Preview using Spark — reads the file we just wrote
from pyspark.sql.functions import col, explode

bronze_df = (
    spark.read
    .option("multiline", "true")
    .json(CONFIG["bronze_volume"])
)

bronze_df.select("ingestion_id", "ingested_at", "record_count").show(5, truncate=False)

print(f"\n📊 Total batches in Bronze: {bronze_df.count()}")

# COMMAND ----------

# Preview first few coin records
coins_preview = (
    bronze_df
    .select(explode(col("data")).alias("coin"))
    .select(
        col("coin.id").alias("coin_id"),
        col("coin.name"),
        col("coin.current_price"),
        col("coin.market_cap"),
        col("coin.total_volume")
    )
    .limit(10)
)

display(coins_preview)