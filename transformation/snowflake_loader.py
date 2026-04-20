"""
transformation/snowflake_loader.py
────────────────────────────────────────────────────────────────────────────────
Reads from Silver (Parquet) and loads into Snowflake Gold layer.
Handles: staging load → DIM upserts → FACT insert → staging truncate.

Prerequisites:
    pip install snowflake-connector-python pandas pyarrow

Environment variables required:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
────────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.helpers import load_config, get_logger


# ──────────────────────────────────────────────
# Snowflake Connection Manager
# ──────────────────────────────────────────────

class SnowflakeConnection:
    """Context manager for Snowflake connections."""

    def __init__(self, config: dict):
        sf = config["snowflake"]
        self.conn_params = {
            "account":   sf["account"],
            "user":      sf["user"],
            "password":  sf["password"],
            "warehouse": sf["warehouse"],
            "database":  sf["database"],
            "schema":    sf["schema"],
            "role":      sf["role"],
        }
        self.conn = None

    def __enter__(self):
        self.conn = snowflake.connector.connect(**self.conn_params)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


# ──────────────────────────────────────────────
# Silver Reader (local Parquet)
# ──────────────────────────────────────────────

def read_silver_pandas(silver_path: str, logger) -> pd.DataFrame:
    """
    Read all Parquet files from Silver path into a Pandas DataFrame.
    Works locally and in Databricks (pass DBFS path).
    """
    logger.info(f"Reading Silver Parquet from: {silver_path}")

    parquet_files = list(Path(silver_path).rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {silver_path}")

    dfs = [pd.read_parquet(str(f)) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Read {len(df):,} rows from {len(parquet_files)} Parquet files.")
    return df


# ──────────────────────────────────────────────
# Staging Loader
# ──────────────────────────────────────────────

STAGING_COLUMNS = [
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


def load_staging(conn, df: pd.DataFrame, logger) -> int:
    """Truncate and reload the staging table."""
    cursor = conn.cursor()
    try:
        # Truncate staging first
        cursor.execute("USE SCHEMA STAGING")
        cursor.execute("TRUNCATE TABLE IF EXISTS STG_MARKET_SNAPSHOT")
        logger.info("Staging table truncated.")

        # Select only staging columns that exist in the DataFrame
        available_cols = [c for c in STAGING_COLUMNS if c in df.columns]
        stage_df = df[available_cols].copy()

        # Normalise column names to upper case (Snowflake default)
        stage_df.columns = [c.upper() for c in stage_df.columns]

        # Convert timestamps
        for col in ["BATCH_INGESTED_AT", "LAST_UPDATED", "ATH_DATE", "ATL_DATE"]:
            if col in stage_df.columns:
                stage_df[col] = pd.to_datetime(stage_df[col], utc=True, errors="coerce")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=stage_df,
            table_name="STG_MARKET_SNAPSHOT",
            schema="STAGING",
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )
        logger.info(f"Staging loaded: {nrows:,} rows in {nchunks} chunk(s).")
        return nrows

    finally:
        cursor.close()


# ──────────────────────────────────────────────
# Gold Load Orchestrator
# ──────────────────────────────────────────────

def run_gold_procedures(conn, logger):
    """Call stored procedures to populate DIM and FACT tables."""
    cursor = conn.cursor()
    try:
        cursor.execute("USE SCHEMA GOLD")

        # 1. Upsert DIM_COIN
        logger.info("Running SP_UPSERT_DIM_COIN ...")
        cursor.execute("CALL SP_UPSERT_DIM_COIN()")
        result = cursor.fetchone()
        logger.info(f"DIM_COIN: {result[0]}")

        # 2. Populate DIM_DATE for current hour
        current_ts = datetime.now(timezone.utc).isoformat()
        logger.info(f"Running SP_POPULATE_DIM_DATE for {current_ts} ...")
        cursor.execute(f"CALL SP_POPULATE_DIM_DATE('{current_ts}'::TIMESTAMP_TZ)")
        result = cursor.fetchone()
        logger.info(f"DIM_DATE: {result[0]}")

        # 3. Load FACT table
        logger.info("Running SP_LOAD_FACT ...")
        cursor.execute("CALL SP_LOAD_FACT()")
        result = cursor.fetchone()
        logger.info(f"FACT: {result[0]}")

    finally:
        cursor.close()


def verify_load(conn, logger) -> dict:
    """Quick row count verification after load."""
    cursor = conn.cursor()
    try:
        cursor.execute("USE SCHEMA GOLD")
        counts = {}
        for table in ["FACT_MARKET_SNAPSHOT", "DIM_COIN", "DIM_DATE"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
            logger.info(f"{table}: {counts[table]:,} rows")
        return counts
    finally:
        cursor.close()


# ──────────────────────────────────────────────
# Main Entry Point
# ──────────────────────────────────────────────

def run_silver_to_gold(config: dict = None):
    """Full Silver → Snowflake Gold pipeline."""
    config = config or load_config()
    logger = get_logger("SnowflakeLoader", config)

    logger.info("Starting Silver → Snowflake Gold load ...")

    # Step 1: Read Silver
    silver_df = read_silver_pandas(config["storage"]["silver_path"], logger)

    # Step 2: Connect to Snowflake and load
    with SnowflakeConnection(config) as conn:

        # Step 3: Load staging
        rows_loaded = load_staging(conn, silver_df, logger)
        if rows_loaded == 0:
            logger.warning("No rows loaded to staging. Aborting Gold load.")
            return

        # Step 4: Run Gold procedures
        run_gold_procedures(conn, logger)

        # Step 5: Verify
        counts = verify_load(conn, logger)

    logger.info(f"Silver → Gold load complete ✅ | {counts}")
    return counts


if __name__ == "__main__":
    run_silver_to_gold()
