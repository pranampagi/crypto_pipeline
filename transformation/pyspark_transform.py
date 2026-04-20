"""
transformation/pyspark_transform.py
────────────────────────────────────────────────────────────────────────────────
Reads raw Bronze JSON files, applies PySpark transformations, enforces schema,
runs deduplication, and writes clean Parquet files to the Silver layer.

Run locally (requires PySpark installed):
    python transformation/pyspark_transform.py

Run in Databricks Serverless:
    Import as a notebook or use %run / dbutils.notebook.run(...)
────────────────────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType, BooleanType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.helpers import load_config, get_logger


# ──────────────────────────────────────────────
# Schema Definition
# ──────────────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("ingestion_id",  StringType(),    nullable=True),
    StructField("source",        StringType(),    nullable=True),
    StructField("ingested_at",   StringType(),    nullable=True),
    StructField("record_count",  LongType(),      nullable=True),
    StructField("data", StructType([]), nullable=True),   # placeholder — we explode it
])

COIN_SCHEMA = StructType([
    StructField("id",                              StringType(),  True),
    StructField("symbol",                          StringType(),  True),
    StructField("name",                            StringType(),  True),
    StructField("image",                           StringType(),  True),
    StructField("current_price",                   DoubleType(),  True),
    StructField("market_cap",                      DoubleType(),  True),
    StructField("market_cap_rank",                 LongType(),    True),
    StructField("fully_diluted_valuation",         DoubleType(),  True),
    StructField("total_volume",                    DoubleType(),  True),
    StructField("high_24h",                        DoubleType(),  True),
    StructField("low_24h",                         DoubleType(),  True),
    StructField("price_change_24h",                DoubleType(),  True),
    StructField("price_change_percentage_24h",     DoubleType(),  True),
    StructField("price_change_percentage_7d_in_currency", DoubleType(), True),
    StructField("market_cap_change_24h",           DoubleType(),  True),
    StructField("market_cap_change_percentage_24h",DoubleType(),  True),
    StructField("circulating_supply",              DoubleType(),  True),
    StructField("total_supply",                    DoubleType(),  True),
    StructField("max_supply",                      DoubleType(),  True),
    StructField("ath",                             DoubleType(),  True),
    StructField("ath_change_percentage",           DoubleType(),  True),
    StructField("ath_date",                        StringType(),  True),
    StructField("atl",                             DoubleType(),  True),
    StructField("atl_change_percentage",           DoubleType(),  True),
    StructField("atl_date",                        StringType(),  True),
    StructField("last_updated",                    StringType(),  True),
])


# ──────────────────────────────────────────────
# Spark Session Factory
# ──────────────────────────────────────────────

def get_spark(app_name: str = "CryptoPipeline_Silver") -> SparkSession:
    """
    Returns active SparkSession.
    - In Databricks Serverless: spark is pre-injected; builder just returns it.
    - Locally: creates a local session.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# Bronze Reader
# ──────────────────────────────────────────────

def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Read all raw Bronze JSON envelopes.
    Each file has an envelope with a 'data' array containing coin records.
    We explode the 'data' array to get one row per coin per ingestion batch.
    """
    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(bronze_path)
    )

    # Explode the 'data' array → one row per coin snapshot
    exploded_df = (
        raw_df
        .select(
            F.col("ingestion_id"),
            F.col("ingested_at").alias("batch_ingested_at"),
            F.explode(F.col("data")).alias("coin")
        )
    )
    return exploded_df


# ──────────────────────────────────────────────
# Flattener + Type Caster
# ──────────────────────────────────────────────

def flatten_and_cast(df: DataFrame) -> DataFrame:
    """
    Flatten the struct column 'coin' into individual typed columns.
    Apply safe casts, rename to snake_case, add derived columns.
    """
    flat_df = (
        df
        .select(
            F.col("ingestion_id"),
            F.to_timestamp(F.col("batch_ingested_at")).alias("batch_ingested_at"),

            # Coin identity
            F.col("coin.id").alias("coin_id"),
            F.col("coin.symbol").alias("symbol"),
            F.col("coin.name").alias("name"),

            # Price metrics
            F.col("coin.current_price").cast(DoubleType()).alias("current_price"),
            F.col("coin.high_24h").cast(DoubleType()).alias("high_24h"),
            F.col("coin.low_24h").cast(DoubleType()).alias("low_24h"),
            F.col("coin.price_change_24h").cast(DoubleType()).alias("price_change_24h"),
            F.col("coin.price_change_percentage_24h").cast(DoubleType()).alias("price_change_pct_24h"),
            F.col("coin.price_change_percentage_7d_in_currency").cast(DoubleType()).alias("price_change_pct_7d"),

            # Market metrics
            F.col("coin.market_cap").cast(DoubleType()).alias("market_cap"),
            F.col("coin.market_cap_rank").cast(LongType()).alias("market_cap_rank"),
            F.col("coin.market_cap_change_24h").cast(DoubleType()).alias("market_cap_change_24h"),
            F.col("coin.market_cap_change_percentage_24h").cast(DoubleType()).alias("market_cap_change_pct_24h"),
            F.col("coin.fully_diluted_valuation").cast(DoubleType()).alias("fully_diluted_valuation"),

            # Volume & supply
            F.col("coin.total_volume").cast(DoubleType()).alias("total_volume"),
            F.col("coin.circulating_supply").cast(DoubleType()).alias("circulating_supply"),
            F.col("coin.total_supply").cast(DoubleType()).alias("total_supply"),
            F.col("coin.max_supply").cast(DoubleType()).alias("max_supply"),

            # All-time high / low
            F.col("coin.ath").cast(DoubleType()).alias("ath"),
            F.col("coin.ath_change_percentage").cast(DoubleType()).alias("ath_change_pct"),
            F.to_timestamp(F.col("coin.ath_date")).alias("ath_date"),
            F.col("coin.atl").cast(DoubleType()).alias("atl"),
            F.col("coin.atl_change_percentage").cast(DoubleType()).alias("atl_change_pct"),
            F.to_timestamp(F.col("coin.atl_date")).alias("atl_date"),

            # Timestamps
            F.to_timestamp(F.col("coin.last_updated")).alias("last_updated"),
        )
        # ── Derived columns ──
        .withColumn("price_range_24h", F.col("high_24h") - F.col("low_24h"))
        .withColumn(
            "volume_to_market_cap_ratio",
            F.when(F.col("market_cap") > 0, F.col("total_volume") / F.col("market_cap"))
             .otherwise(None)
        )
        .withColumn(
            "is_bullish_24h",
            F.col("price_change_pct_24h") > 0
        )
        # ── Partition columns ──
        .withColumn("year",  F.year(F.col("batch_ingested_at")))
        .withColumn("month", F.month(F.col("batch_ingested_at")))
        .withColumn("day",   F.dayofmonth(F.col("batch_ingested_at")))
        .withColumn("hour",  F.hour(F.col("batch_ingested_at")))
    )
    return flat_df


# ──────────────────────────────────────────────
# Deduplication
# ──────────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records: same coin_id in the same ingestion batch.
    Keep the record with the latest last_updated timestamp.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("ingestion_id", "coin_id").orderBy(
        F.col("last_updated").desc()
    )
    deduped_df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    return deduped_df


# ──────────────────────────────────────────────
# Null / Sanity Filter
# ──────────────────────────────────────────────

def filter_invalid_records(df: DataFrame, logger) -> DataFrame:
    """Drop records that fail basic sanity checks."""
    before = df.count()

    clean_df = df.filter(
        F.col("coin_id").isNotNull() &
        F.col("current_price").isNotNull() &
        (F.col("current_price") > 0) &
        F.col("market_cap").isNotNull() &
        F.col("symbol").isNotNull()
    )

    after = clean_df.count()
    dropped = before - after
    if dropped > 0:
        logger.warning(f"Dropped {dropped} invalid records (out of {before}) during Silver filtering.")
    else:
        logger.info(f"All {before} records passed sanity checks.")

    return clean_df


# ──────────────────────────────────────────────
# Silver Writer
# ──────────────────────────────────────────────

def write_silver(df: DataFrame, silver_path: str, logger):
    """Write cleaned DataFrame as partitioned Parquet to Silver layer."""
    (
        df.write
        .mode("append")
        .partitionBy("year", "month", "day", "hour")
        .parquet(silver_path)
    )
    logger.info(f"Silver Parquet written → {silver_path}")


# ──────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────

def run_bronze_to_silver(config: dict = None):
    """Full Bronze → Silver transformation pipeline."""
    config = config or load_config()
    logger = get_logger("BronzeToSilver", config)
    spark = get_spark()

    bronze_path = config["storage"]["bronze_path"]
    silver_path = config["storage"]["silver_path"]

    logger.info(f"Reading Bronze from: {bronze_path}")
    bronze_df = read_bronze(spark, bronze_path)

    logger.info("Flattening and casting types ...")
    flat_df = flatten_and_cast(bronze_df)

    logger.info("Deduplicating ...")
    deduped_df = deduplicate(flat_df)

    logger.info("Filtering invalid records ...")
    clean_df = filter_invalid_records(deduped_df, logger)

    logger.info(f"Writing Silver to: {silver_path}")
    write_silver(clean_df, silver_path, logger)

    logger.info("Bronze → Silver transformation complete ✅")
    return clean_df


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    run_bronze_to_silver()
