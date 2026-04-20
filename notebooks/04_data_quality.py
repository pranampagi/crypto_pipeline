# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Notebook 4 — Data Quality Checks (Serverless)
# MAGIC **Layer**: Silver DataFrame
# MAGIC **Output**: DQ Report (JSON) + Pass/Fail summary
# MAGIC
# MAGIC Run after Notebook 2 to validate the Silver layer before loading to Gold.

# COMMAND ----------

SILVER_PATH = "/Volumes/crypto_catalog/crypto_schema/silver"

silver_df = spark.read.parquet(SILVER_PATH)
total = silver_df.count()
print(f"Loaded {total:,} Silver records for DQ validation.")

# COMMAND ----------

# MAGIC %md ## Check 1 — Null Percentages on Critical Columns

# COMMAND ----------

from pyspark.sql import functions as F

critical_cols = ["coin_id", "symbol", "name", "current_price", "market_cap", "total_volume"]

null_report = []
for col in critical_cols:
    null_count = silver_df.filter(F.col(col).isNull()).count()
    null_pct = round(null_count / total * 100, 4) if total > 0 else 0
    status = "✅ PASS" if null_pct <= 5.0 else "❌ FAIL"
    null_report.append({"column": col, "null_count": null_count, "null_pct": null_pct, "status": status})
    print(f"  {status} | {col:35s}: {null_count} nulls ({null_pct:.2f}%)")

# COMMAND ----------

# MAGIC %md ## Check 2 — Value Range (Price & Volume > 0)

# COMMAND ----------

for col, threshold in [("current_price", 0), ("total_volume", 0), ("market_cap", 0)]:
    failed = silver_df.filter((F.col(col).isNull()) | (F.col(col) <= threshold)).count()
    status = "✅ PASS" if failed == 0 else "❌ FAIL"
    print(f"  {status} | {col} > {threshold}: {failed} violations")

# COMMAND ----------

# MAGIC %md ## Check 3 — Deduplication

# COMMAND ----------

distinct = silver_df.dropDuplicates(["coin_id", "batch_ingested_at"]).count()
dup_count = total - distinct
dup_pct = round(dup_count / total * 100, 4) if total > 0 else 0
status = "✅ PASS" if dup_pct <= 1.0 else "❌ FAIL"
print(f"  {status} | Duplicates: {dup_count} ({dup_pct:.2f}%)")

# COMMAND ----------

# MAGIC %md ## Check 4 — Distinct Coin Count (>= 50)

# COMMAND ----------

distinct_coins = silver_df.select("coin_id").distinct().count()
status = "✅ PASS" if distinct_coins >= 50 else "❌ FAIL"
print(f"  {status} | Distinct coins: {distinct_coins} (threshold: >= 50)")

# COMMAND ----------

# MAGIC %md ## Check 5 — Data Freshness

# COMMAND ----------

from datetime import datetime, timezone

latest_ts = silver_df.select(F.max("batch_ingested_at").alias("latest")).collect()[0]["latest"]
if latest_ts:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lag_min = round((now - latest_ts).total_seconds() / 60, 1)
    status = "✅ PASS" if lag_min <= 90 else "⚠️ STALE"
    print(f"  {status} | Latest record: {latest_ts} ({lag_min} min ago)")
else:
    print("  ❌ FAIL | No timestamp data found.")

# COMMAND ----------

# MAGIC %md ## Summary Dashboard

# COMMAND ----------

summary = silver_df.agg(
    F.count("*").alias("total_records"),
    F.countDistinct("coin_id").alias("unique_coins"),
    F.countDistinct("ingestion_id").alias("total_batches"),
    F.avg("current_price").alias("avg_price_usd"),
    F.sum("market_cap").alias("total_market_cap"),
    F.sum("total_volume").alias("total_volume_usd"),
    F.sum(F.when(F.col("is_bullish_24h") == True, 1).otherwise(0)).alias("bullish_count"),
    F.sum(F.when(F.col("is_bullish_24h") == False, 1).otherwise(0)).alias("bearish_count"),
    F.max("batch_ingested_at").alias("latest_ingestion"),
    F.min("batch_ingested_at").alias("earliest_ingestion"),
)

display(summary)

# COMMAND ----------

# MAGIC %md ## Price Change Distribution

# COMMAND ----------

display(
    silver_df.groupBy(
        F.when(F.col("price_change_pct_24h") >= 10, "🚀 > +10%")
         .when(F.col("price_change_pct_24h") >= 5,  "📈 +5% to +10%")
         .when(F.col("price_change_pct_24h") >= 0,  "🟢 0% to +5%")
         .when(F.col("price_change_pct_24h") >= -5, "🔴 -5% to 0%")
         .otherwise("💀 < -5%").alias("price_bucket")
    )
    .agg(
        F.count("*").alias("coin_count"),
        F.avg("price_change_pct_24h").alias("avg_pct_change")
    )
    .orderBy("price_bucket")
)
