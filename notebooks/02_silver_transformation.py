# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Notebook 2 — Silver Transformation (Serverless)
# MAGIC **Pipeline**: Bronze JSON → PySpark Cleanse → Silver Parquet
# MAGIC
# MAGIC **Compute**: Databricks Serverless — `spark` is pre-injected, no cluster setup needed.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

# ── Update these to match your Unity Catalog ──
BRONZE_PATH = "/Volumes/crypto_catalog/crypto_schema/bronze"
SILVER_PATH = "/Volumes/crypto_catalog/crypto_schema/silver"

# Fallback paths (local / non-UC)
import os
if not os.path.exists(BRONZE_PATH.replace("/Volumes", "/dbfs/mnt")):
    BRONZE_PATH = "/tmp/crypto_pipeline/bronze"
    SILVER_PATH = "/tmp/crypto_pipeline/silver"
    os.makedirs(SILVER_PATH, exist_ok=True)

print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. Read Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

print("Reading Bronze layer ...")
raw_df = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE_PATH)
)

print(f"  Total Bronze batches: {raw_df.count()}")

# Explode the 'data' array → one row per coin per ingestion
exploded_df = (
    raw_df.select(
        F.col("ingestion_id"),
        F.to_timestamp(F.col("ingested_at")).alias("batch_ingested_at"),
        F.explode(F.col("data")).alias("coin")
    )
)

print(f"  Total coin records (after explode): {exploded_df.count()}")

# COMMAND ----------

# MAGIC %md ## 3. Flatten & Cast

# COMMAND ----------

flat_df = (
    exploded_df.select(
        F.col("ingestion_id"),
        F.col("batch_ingested_at"),

        # Identity
        F.col("coin.id").alias("coin_id"),
        F.col("coin.symbol").alias("symbol"),
        F.col("coin.name").alias("name"),

        # Prices
        F.col("coin.current_price").cast(DoubleType()).alias("current_price"),
        F.col("coin.high_24h").cast(DoubleType()).alias("high_24h"),
        F.col("coin.low_24h").cast(DoubleType()).alias("low_24h"),
        F.col("coin.price_change_24h").cast(DoubleType()).alias("price_change_24h"),
        F.col("coin.price_change_percentage_24h").cast(DoubleType()).alias("price_change_pct_24h"),
        F.col("coin.price_change_percentage_7d_in_currency").cast(DoubleType()).alias("price_change_pct_7d"),

        # Market
        F.col("coin.market_cap").cast(DoubleType()).alias("market_cap"),
        F.col("coin.market_cap_rank").cast(LongType()).alias("market_cap_rank"),
        F.col("coin.market_cap_change_24h").cast(DoubleType()).alias("market_cap_change_24h"),
        F.col("coin.market_cap_change_percentage_24h").cast(DoubleType()).alias("market_cap_change_pct_24h"),
        F.col("coin.fully_diluted_valuation").cast(DoubleType()).alias("fully_diluted_valuation"),

        # Volume & Supply
        F.col("coin.total_volume").cast(DoubleType()).alias("total_volume"),
        F.col("coin.circulating_supply").cast(DoubleType()).alias("circulating_supply"),
        F.col("coin.total_supply").cast(DoubleType()).alias("total_supply"),
        F.col("coin.max_supply").cast(DoubleType()).alias("max_supply"),

        # ATH / ATL
        F.col("coin.ath").cast(DoubleType()).alias("ath"),
        F.col("coin.ath_change_percentage").cast(DoubleType()).alias("ath_change_pct"),
        F.to_timestamp(F.col("coin.ath_date")).alias("ath_date"),
        F.col("coin.atl").cast(DoubleType()).alias("atl"),
        F.col("coin.atl_change_percentage").cast(DoubleType()).alias("atl_change_pct"),
        F.to_timestamp(F.col("coin.atl_date")).alias("atl_date"),

        # Timestamps
        F.to_timestamp(F.col("coin.last_updated")).alias("last_updated"),
    )
    # Derived columns
    .withColumn("price_range_24h",
        F.col("high_24h") - F.col("low_24h"))
    .withColumn("volume_to_market_cap_ratio",
        F.when(F.col("market_cap") > 0, F.col("total_volume") / F.col("market_cap"))
         .otherwise(None))
    .withColumn("is_bullish_24h",
        F.col("price_change_pct_24h") > 0)
    # Partition columns
    .withColumn("year",  F.year("batch_ingested_at"))
    .withColumn("month", F.month("batch_ingested_at"))
    .withColumn("day",   F.dayofmonth("batch_ingested_at"))
    .withColumn("hour",  F.hour("batch_ingested_at"))
)

print(f"Flat DataFrame: {flat_df.count()} rows | {len(flat_df.columns)} columns")

# COMMAND ----------

# MAGIC %md ## 4. Deduplication

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("ingestion_id", "coin_id").orderBy(F.col("last_updated").desc())

deduped_df = (
    flat_df
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

before = flat_df.count()
after  = deduped_df.count()
print(f"Deduplication: {before} → {after} rows (removed {before - after} duplicates)")

# COMMAND ----------

# MAGIC %md ## 5. Data Quality Filter

# COMMAND ----------

clean_df = deduped_df.filter(
    F.col("coin_id").isNotNull() &
    F.col("current_price").isNotNull() &
    (F.col("current_price") > 0) &
    F.col("market_cap").isNotNull() &
    F.col("symbol").isNotNull()
)

dropped = after - clean_df.count()
print(f"Sanity filter: removed {dropped} invalid records. Final count: {clean_df.count()}")

# COMMAND ----------

# MAGIC %md ## 6. Write Silver (Parquet, Partitioned)

# COMMAND ----------

(
    clean_df.write
    .mode("append")
    .partitionBy("year", "month", "day", "hour")
    .parquet(SILVER_PATH)
)

print(f"✅ Silver written to: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md ## 7. Preview Silver

# COMMAND ----------

silver_df = spark.read.parquet(SILVER_PATH)
print(f"Total Silver records: {silver_df.count():,}")

display(
    silver_df.select(
        "coin_id", "symbol", "name",
        "current_price", "market_cap", "total_volume",
        "price_change_pct_24h", "is_bullish_24h",
        "batch_ingested_at"
    ).orderBy("market_cap_rank")
    .limit(15)
)

# COMMAND ----------

# MAGIC %md ## 8. Inline Data Quality Summary

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum, avg, when, isnan

dq_summary = silver_df.agg(
    count("*").alias("total_records"),
    _sum(when(F.col("current_price").isNull(), 1).otherwise(0)).alias("null_price_count"),
    _sum(when(F.col("market_cap").isNull(), 1).otherwise(0)).alias("null_mktcap_count"),
    _sum(when(F.col("coin_id").isNull(), 1).otherwise(0)).alias("null_coinid_count"),
    _sum(when(F.col("is_bullish_24h") == True, 1).otherwise(0)).alias("bullish_coins"),
    avg("price_change_pct_24h").alias("avg_24h_change_pct"),
    count(F.col("coin_id").isNotNull()).alias("non_null_coins"),
)

display(dq_summary)

# COMMAND ----------

print("\n🎉 Silver Transformation Complete!")
print(f"   Source : {BRONZE_PATH}")
print(f"   Output : {SILVER_PATH}")
