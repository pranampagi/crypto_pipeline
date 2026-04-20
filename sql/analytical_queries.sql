-- ============================================================
-- sql/analytical_queries.sql
-- Gold Layer Analytical Queries — Interview-Ready
-- Showcases: aggregations, window functions, CTEs, time-series
-- ============================================================

USE DATABASE CRYPTO_DB;
USE SCHEMA GOLD;
USE WAREHOUSE CRYPTO_WH;


-- ─────────────────────────────────────────────────────────────
-- Q1. Top 10 Coins by 24h Trading Volume (latest snapshot)
-- Showcases: aggregation, subquery, ORDER BY, LIMIT
-- ─────────────────────────────────────────────────────────────
WITH latest_snapshot AS (
    SELECT MAX(batch_ingested_at) AS latest_ts
    FROM FACT_MARKET_SNAPSHOT
)
SELECT
    dc.name                                     AS coin_name,
    dc.symbol,
    f.current_price,
    f.total_volume,
    f.market_cap,
    ROUND(f.volume_to_market_cap_ratio * 100, 2) AS volume_to_mcap_pct,
    f.price_change_pct_24h
FROM FACT_MARKET_SNAPSHOT f
JOIN DIM_COIN dc         ON dc.coin_sk = f.coin_sk
JOIN latest_snapshot ls  ON f.batch_ingested_at = ls.latest_ts
ORDER BY f.total_volume DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- Q2. Price Volatility Ranking (24h price range / price)
-- Showcases: derived metric, RANK() window function, ROUND
-- ─────────────────────────────────────────────────────────────
WITH latest_snapshot AS (
    SELECT MAX(batch_ingested_at) AS latest_ts
    FROM FACT_MARKET_SNAPSHOT
),
volatility AS (
    SELECT
        dc.name,
        dc.symbol,
        f.current_price,
        f.price_range_24h,
        ROUND((f.price_range_24h / NULLIF(f.current_price, 0)) * 100, 2) AS volatility_pct,
        RANK() OVER (ORDER BY (f.price_range_24h / NULLIF(f.current_price, 0)) DESC) AS volatility_rank
    FROM FACT_MARKET_SNAPSHOT f
    JOIN DIM_COIN dc        ON dc.coin_sk = f.coin_sk
    JOIN latest_snapshot ls ON f.batch_ingested_at = ls.latest_ts
    WHERE f.current_price > 0
)
SELECT *
FROM volatility
ORDER BY volatility_rank
LIMIT 20;


-- ─────────────────────────────────────────────────────────────
-- Q3. Hourly Total Market Cap Trend (last 7 days)
-- Showcases: time-series aggregation, GROUP BY, date functions
-- ─────────────────────────────────────────────────────────────
SELECT
    dd.snapshot_hour,
    dd.year,
    dd.month,
    dd.day_of_month,
    dd.hour_of_day,
    COUNT(DISTINCT f.coin_sk)           AS coins_tracked,
    SUM(f.market_cap)                   AS total_market_cap_usd,
    SUM(f.total_volume)                 AS total_volume_usd,
    AVG(f.price_change_pct_24h)         AS avg_price_change_pct,
    SUM(CASE WHEN f.is_bullish_24h THEN 1 ELSE 0 END) AS bullish_coins,
    SUM(CASE WHEN NOT f.is_bullish_24h THEN 1 ELSE 0 END) AS bearish_coins
FROM FACT_MARKET_SNAPSHOT f
JOIN DIM_DATE dd ON dd.date_sk = f.date_sk
WHERE dd.snapshot_hour >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY
    dd.snapshot_hour, dd.year, dd.month, dd.day_of_month, dd.hour_of_day
ORDER BY dd.snapshot_hour DESC;


-- ─────────────────────────────────────────────────────────────
-- Q4. Price Change vs Previous Snapshot (LAG window function)
-- Showcases: LAG(), LEAD(), price momentum detection
-- ─────────────────────────────────────────────────────────────
WITH price_history AS (
    SELECT
        dc.name,
        dc.symbol,
        f.current_price,
        f.batch_ingested_at,
        LAG(f.current_price) OVER (
            PARTITION BY f.coin_sk
            ORDER BY f.batch_ingested_at
        ) AS prev_price,
        LEAD(f.current_price) OVER (
            PARTITION BY f.coin_sk
            ORDER BY f.batch_ingested_at
        ) AS next_price
    FROM FACT_MARKET_SNAPSHOT f
    JOIN DIM_COIN dc ON dc.coin_sk = f.coin_sk
    WHERE dc.symbol IN ('btc', 'eth', 'sol', 'bnb', 'xrp')
)
SELECT
    name,
    symbol,
    batch_ingested_at,
    current_price,
    prev_price,
    ROUND(current_price - COALESCE(prev_price, current_price), 4)   AS price_delta,
    ROUND(
        ((current_price - COALESCE(prev_price, current_price))
         / NULLIF(prev_price, 0)) * 100
    , 4) AS pct_change_from_prev_snapshot
FROM price_history
ORDER BY name, batch_ingested_at DESC;


-- ─────────────────────────────────────────────────────────────
-- Q5. Market Dominance by Coin (BTC Dominance style)
-- Showcases: ratio calculation, total window aggregation
-- ─────────────────────────────────────────────────────────────
WITH latest_snapshot AS (
    SELECT MAX(batch_ingested_at) AS latest_ts FROM FACT_MARKET_SNAPSHOT
),
totals AS (
    SELECT SUM(market_cap) AS total_market_cap
    FROM FACT_MARKET_SNAPSHOT f
    JOIN latest_snapshot ls ON f.batch_ingested_at = ls.latest_ts
)
SELECT
    dc.name,
    dc.symbol,
    f.market_cap,
    ROUND((f.market_cap / t.total_market_cap) * 100, 4) AS dominance_pct,
    SUM(ROUND((f.market_cap / t.total_market_cap) * 100, 4)) OVER (
        ORDER BY f.market_cap DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_dominance_pct
FROM FACT_MARKET_SNAPSHOT f
JOIN DIM_COIN dc        ON dc.coin_sk = f.coin_sk
JOIN latest_snapshot ls ON f.batch_ingested_at = ls.latest_ts
CROSS JOIN totals t
ORDER BY dominance_pct DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────
-- Q6. Weekend vs Weekday Average Volume
-- Showcases: conditional aggregation, DIM_DATE join
-- ─────────────────────────────────────────────────────────────
SELECT
    dc.name,
    dc.symbol,
    AVG(CASE WHEN dd.is_weekend THEN f.total_volume END) AS avg_weekend_volume,
    AVG(CASE WHEN NOT dd.is_weekend THEN f.total_volume END) AS avg_weekday_volume,
    ROUND(
        (AVG(CASE WHEN dd.is_weekend THEN f.total_volume END) /
         NULLIF(AVG(CASE WHEN NOT dd.is_weekend THEN f.total_volume END), 0) - 1) * 100
    , 2) AS weekend_vs_weekday_pct_diff
FROM FACT_MARKET_SNAPSHOT f
JOIN DIM_COIN dc ON dc.coin_sk = f.coin_sk
JOIN DIM_DATE dd ON dd.date_sk = f.date_sk
GROUP BY dc.name, dc.symbol
HAVING COUNT(*) >= 10   -- only coins with enough history
ORDER BY ABS(weekend_vs_weekday_pct_diff) DESC NULLS LAST
LIMIT 20;


-- ─────────────────────────────────────────────────────────────
-- Q7. Rolling 24h Moving Average Price (ROWS BETWEEN)
-- Showcases: advanced window frames, rolling analytics
-- ─────────────────────────────────────────────────────────────
SELECT
    dc.name,
    dc.symbol,
    f.batch_ingested_at,
    f.current_price,
    ROUND(AVG(f.current_price) OVER (
        PARTITION BY f.coin_sk
        ORDER BY f.batch_ingested_at
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW   -- 24-point rolling avg (hourly data)
    ), 6) AS rolling_24h_avg_price,
    ROUND(f.current_price - AVG(f.current_price) OVER (
        PARTITION BY f.coin_sk
        ORDER BY f.batch_ingested_at
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ), 6) AS deviation_from_rolling_avg
FROM FACT_MARKET_SNAPSHOT f
JOIN DIM_COIN dc ON dc.coin_sk = f.coin_sk
WHERE dc.symbol IN ('btc', 'eth', 'sol')
ORDER BY dc.symbol, f.batch_ingested_at DESC;


-- ─────────────────────────────────────────────────────────────
-- Q8. Data Freshness Monitor (Pipeline Health Check)
-- Showcases: operational monitoring query
-- ─────────────────────────────────────────────────────────────
SELECT
    MAX(batch_ingested_at)                         AS last_ingestion_time,
    CURRENT_TIMESTAMP()                            AS now,
    DATEDIFF('minute', MAX(batch_ingested_at), CURRENT_TIMESTAMP())
                                                   AS lag_minutes,
    COUNT(DISTINCT ingestion_id)                   AS total_batches,
    COUNT(*)                                       AS total_records,
    COUNT(DISTINCT coin_sk)                        AS distinct_coins,
    MIN(batch_ingested_at)                         AS earliest_record,
    CASE
        WHEN DATEDIFF('minute', MAX(batch_ingested_at), CURRENT_TIMESTAMP()) <= 90
        THEN '✅ FRESH'
        ELSE '⚠️ STALE — Check Poller'
    END AS pipeline_status
FROM FACT_MARKET_SNAPSHOT;
