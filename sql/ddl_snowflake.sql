-- ============================================================
-- sql/ddl_snowflake.sql
-- Star Schema DDL for the Crypto Market Analytics Gold Layer
-- Run this once to set up the Snowflake environment.
-- ============================================================


-- ────────────────────────────────────────────────
-- 1. ENVIRONMENT SETUP
-- ────────────────────────────────────────────────

USE ROLE SYSADMIN;

-- Warehouse (serverless-style, auto-suspend)
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Crypto Market Analytics Pipeline';

-- Database & Schema
CREATE DATABASE IF NOT EXISTS CRYPTO_DB
    COMMENT = 'Crypto Market Data Engineering Project';

CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.GOLD
    COMMENT = 'Gold layer — analytics-ready star schema tables';

CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.STAGING
    COMMENT = 'Staging schema for intermediate loads';

USE DATABASE CRYPTO_DB;
USE SCHEMA GOLD;
USE WAREHOUSE CRYPTO_WH;


-- ────────────────────────────────────────────────
-- 2. DIMENSION: DIM_COIN
-- ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS DIM_COIN (
    coin_sk             NUMBER AUTOINCREMENT PRIMARY KEY,   -- surrogate key
    coin_id             VARCHAR(100)  NOT NULL,             -- natural key (e.g., 'bitcoin')
    symbol              VARCHAR(20)   NOT NULL,             -- e.g., 'btc'
    name                VARCHAR(200)  NOT NULL,             -- e.g., 'Bitcoin'
    ath                 FLOAT,                              -- all-time high price USD
    atl                 FLOAT,                              -- all-time low price USD
    ath_date            TIMESTAMP_TZ,
    atl_date            TIMESTAMP_TZ,
    circulating_supply  FLOAT,
    total_supply        FLOAT,
    max_supply          FLOAT,
    is_active           BOOLEAN       DEFAULT TRUE,
    first_seen_at       TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
    last_updated_at     TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (coin_id)
)
COMMENT = 'Slowly Changing Dimension for cryptocurrency metadata';


-- ────────────────────────────────────────────────
-- 3. DIMENSION: DIM_DATE
-- ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_sk         NUMBER       PRIMARY KEY,    -- surrogate key (YYYYMMDDH format)
    snapshot_hour   TIMESTAMP_TZ NOT NULL,       -- exact hour bucket
    date_actual     DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,       -- 1=Mon, 7=Sun
    day_name        VARCHAR(10)  NOT NULL,
    hour_of_day     SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    UNIQUE (snapshot_hour)
)
COMMENT = 'Date/time dimension — granularity is 1 hour';


-- ────────────────────────────────────────────────
-- 4. FACT TABLE: FACT_MARKET_SNAPSHOT
-- ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS FACT_MARKET_SNAPSHOT (
    snapshot_id                 VARCHAR(256)  NOT NULL PRIMARY KEY,   -- ingestion_id + coin_id
    coin_sk                     NUMBER        NOT NULL REFERENCES DIM_COIN(coin_sk),
    date_sk                     NUMBER        NOT NULL REFERENCES DIM_DATE(date_sk),
    ingestion_id                VARCHAR(64),

    -- Price metrics (USD)
    current_price               FLOAT         NOT NULL,
    high_24h                    FLOAT,
    low_24h                     FLOAT,
    price_change_24h            FLOAT,
    price_change_pct_24h        FLOAT,
    price_change_pct_7d         FLOAT,
    price_range_24h             FLOAT,          -- derived: high - low

    -- Market metrics
    market_cap                  FLOAT,
    market_cap_rank             SMALLINT,
    market_cap_change_24h       FLOAT,
    market_cap_change_pct_24h   FLOAT,
    fully_diluted_valuation     FLOAT,

    -- Volume
    total_volume                FLOAT,
    volume_to_market_cap_ratio  FLOAT,          -- derived: volume / market_cap

    -- ATH deviation
    ath_change_pct              FLOAT,
    atl_change_pct              FLOAT,

    -- Flags
    is_bullish_24h              BOOLEAN,

    -- Audit
    batch_ingested_at           TIMESTAMP_TZ  NOT NULL,
    last_updated                TIMESTAMP_TZ,
    loaded_at                   TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date_sk, coin_sk)
COMMENT = 'Fact table — one row per coin per ingestion batch (hourly granularity)';


-- ────────────────────────────────────────────────
-- 5. STAGING TABLE (for incremental loads)
-- ────────────────────────────────────────────────

USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS STG_MARKET_SNAPSHOT (
    coin_id                     VARCHAR(100),
    symbol                      VARCHAR(20),
    name                        VARCHAR(200),
    current_price               FLOAT,
    high_24h                    FLOAT,
    low_24h                     FLOAT,
    price_change_24h            FLOAT,
    price_change_pct_24h        FLOAT,
    price_change_pct_7d         FLOAT,
    price_range_24h             FLOAT,
    market_cap                  FLOAT,
    market_cap_rank             SMALLINT,
    market_cap_change_24h       FLOAT,
    market_cap_change_pct_24h   FLOAT,
    fully_diluted_valuation     FLOAT,
    total_volume                FLOAT,
    volume_to_market_cap_ratio  FLOAT,
    circulating_supply          FLOAT,
    total_supply                FLOAT,
    max_supply                  FLOAT,
    ath                         FLOAT,
    ath_change_pct              FLOAT,
    ath_date                    TIMESTAMP_TZ,
    atl                         FLOAT,
    atl_change_pct              FLOAT,
    atl_date                    TIMESTAMP_TZ,
    is_bullish_24h              BOOLEAN,
    batch_ingested_at           TIMESTAMP_TZ,
    last_updated                TIMESTAMP_TZ,
    ingestion_id                VARCHAR(64)
)
COMMENT = 'Staging — raw Silver data before Gold merge';


-- ────────────────────────────────────────────────
-- 6. MERGE PROCEDURES
-- ────────────────────────────────────────────────

USE SCHEMA GOLD;

-- Populate DIM_DATE from staging data
CREATE OR REPLACE PROCEDURE SP_POPULATE_DIM_DATE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO DIM_DATE AS tgt
    USING (
        SELECT DISTINCT
            TO_NUMBER(TO_CHAR(DATE_TRUNC('hour', batch_ingested_at), 'YYYYMMDDHH24')) AS date_sk,
            DATE_TRUNC('hour', batch_ingested_at)                                      AS snapshot_hour,
            DATE(batch_ingested_at)                                                    AS date_actual,
            YEAR(batch_ingested_at)                                                    AS year,
            QUARTER(batch_ingested_at)                                                 AS quarter,
            MONTH(batch_ingested_at)                                                   AS month,
            MONTHNAME(batch_ingested_at)                                               AS month_name,
            DAY(batch_ingested_at)                                                     AS day_of_month,
            DAYOFWEEK(batch_ingested_at)                                               AS day_of_week,
            DAYNAME(batch_ingested_at)                                                 AS day_name,
            HOUR(batch_ingested_at)                                                    AS hour_of_day,
            DAYOFWEEK(batch_ingested_at) IN (0, 6)                                     AS is_weekend
        FROM STAGING.STG_MARKET_SNAPSHOT
        WHERE batch_ingested_at IS NOT NULL
    ) AS src
    ON tgt.date_sk = src.date_sk
    WHEN NOT MATCHED THEN INSERT VALUES (
        src.date_sk, src.snapshot_hour, src.date_actual,
        src.year, src.quarter, src.month, src.month_name,
        src.day_of_month, src.day_of_week, src.day_name,
        src.hour_of_day, src.is_weekend
    );

    RETURN 'DIM_DATE populated from STAGING.';
END;
$$;


-- Upsert DIM_COIN
CREATE OR REPLACE PROCEDURE SP_UPSERT_DIM_COIN()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO GOLD.DIM_COIN AS tgt
    USING (
        SELECT * FROM (
            SELECT
                coin_id, symbol, name,
                ath, atl, ath_date, atl_date,
                circulating_supply, total_supply, max_supply,
                ROW_NUMBER() OVER(PARTITION BY coin_id ORDER BY batch_ingested_at DESC) as rn
            FROM STAGING.STG_MARKET_SNAPSHOT
        ) WHERE rn = 1
    ) AS src
    ON tgt.coin_id = src.coin_id
    WHEN MATCHED THEN UPDATE SET
        symbol             = src.symbol,
        name               = src.name,
        ath                = GREATEST(COALESCE(tgt.ath, 0), COALESCE(src.ath, 0)),
        circulating_supply = src.circulating_supply,
        last_updated_at    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        coin_id, symbol, name, ath, atl, ath_date, atl_date,
        circulating_supply, total_supply, max_supply
    ) VALUES (
        src.coin_id, src.symbol, src.name, src.ath, src.atl, src.ath_date, src.atl_date,
        src.circulating_supply, src.total_supply, src.max_supply
    );

    RETURN 'DIM_COIN upsert complete.';
END;
$$;


-- Load FACT_MARKET_SNAPSHOT from staging
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO GOLD.FACT_MARKET_SNAPSHOT
    SELECT
        stg.coin_id || '_' || stg.ingestion_id              AS snapshot_id,
        dc.coin_sk,
        TO_NUMBER(TO_CHAR(DATE_TRUNC('hour', stg.batch_ingested_at), 'YYYYMMDDHH24')) AS date_sk,
        stg.ingestion_id,
        stg.current_price,
        stg.high_24h,
        stg.low_24h,
        stg.price_change_24h,
        stg.price_change_pct_24h,
        stg.price_change_pct_7d,
        stg.price_range_24h,
        stg.market_cap,
        stg.market_cap_rank,
        stg.market_cap_change_24h,
        stg.market_cap_change_pct_24h,
        stg.fully_diluted_valuation,
        stg.total_volume,
        stg.volume_to_market_cap_ratio,
        stg.ath_change_pct,
        stg.atl_change_pct,
        stg.is_bullish_24h,
        stg.batch_ingested_at,
        stg.last_updated,
        CURRENT_TIMESTAMP()
    FROM STAGING.STG_MARKET_SNAPSHOT stg
    JOIN GOLD.DIM_COIN dc ON dc.coin_id = stg.coin_id
    WHERE NOT EXISTS (
        SELECT 1 FROM GOLD.FACT_MARKET_SNAPSHOT f
        WHERE f.snapshot_id = stg.coin_id || '_' || stg.ingestion_id
    );

    RETURN 'FACT_MARKET_SNAPSHOT load complete.';
END;
$$;
