{{ config(
    materialized='table',
    schema='marts'
) }}

/*
    Calculate daily returns for each stock
    Used for: Performance tracking, volatility analysis
*/

WITH stock_prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

daily_metrics AS (
    SELECT
        symbol,
        date,
        close_price,
        
        -- Previous day's close
        LAG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY date
        ) AS prev_close_price,
        
        -- Daily return calculation
        ROUND(
            ((close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY date)) 
             / LAG(close_price) OVER (PARTITION BY symbol ORDER BY date)) * 100,
            2
        ) AS daily_return_pct,
        
        -- Absolute change
        close_price - LAG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY date
        ) AS daily_change_inr,
        
        volume,
        
        -- Metadata
        ingestion_timestamp
        
    FROM stock_prices
)

SELECT
    symbol,
    date,
    close_price,
    prev_close_price,
    daily_change_inr,
    daily_return_pct,
    volume,
    
    -- Classify return magnitude
    CASE 
        WHEN daily_return_pct > 3 THEN 'Large Gain'
        WHEN daily_return_pct > 1 THEN 'Moderate Gain'
        WHEN daily_return_pct >= -1 THEN 'Flat'
        WHEN daily_return_pct >= -3 THEN 'Moderate Loss'
        ELSE 'Large Loss'
    END AS return_category,
    
    ingestion_timestamp

FROM daily_metrics
WHERE prev_close_price IS NOT NULL
ORDER BY symbol, date