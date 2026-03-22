{{ config(
    materialized='table',
    schema='marts'
) }}

/*
    Volume trend analysis
    Used for: Identifying unusual trading activity
*/

WITH stock_prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

volume_metrics AS (
    SELECT
        symbol,
        date,
        close_price,
        volume,
        
        -- 7-day average volume
        ROUND(
            AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        ) AS avg_volume_7d,
        
        -- Volume vs average
        ROUND(
            (volume::DECIMAL / 
             AVG(volume) OVER (
                 PARTITION BY symbol 
                 ORDER BY date 
                 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
             )) * 100,
            0
        ) AS volume_vs_avg_pct,
        
        -- Daily price change
        ROUND(
            ((close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY date)) 
             / LAG(close_price) OVER (PARTITION BY symbol ORDER BY date)) * 100,
            2
        ) AS price_change_pct,
        
        ingestion_timestamp
        
    FROM stock_prices
)

SELECT
    symbol,
    date,
    close_price,
    volume,
    avg_volume_7d,
    volume_vs_avg_pct,
    price_change_pct,
    
    -- Classify volume activity
    CASE
        WHEN volume_vs_avg_pct > 200 THEN 'Very High Volume'
        WHEN volume_vs_avg_pct > 150 THEN 'High Volume'
        WHEN volume_vs_avg_pct > 80 THEN 'Normal'
        WHEN volume_vs_avg_pct > 50 THEN 'Low Volume'
        ELSE 'Very Low Volume'
    END AS volume_category,
    
    -- Price-Volume divergence (unusual patterns)
    CASE
        WHEN price_change_pct > 2 AND volume_vs_avg_pct > 150 THEN 'Strong Uptrend'
        WHEN price_change_pct < -2 AND volume_vs_avg_pct > 150 THEN 'Strong Downtrend'
        WHEN price_change_pct > 2 AND volume_vs_avg_pct < 80 THEN 'Weak Uptrend'
        WHEN price_change_pct < -2 AND volume_vs_avg_pct < 80 THEN 'Weak Downtrend'
        ELSE 'Neutral'
    END AS trend_strength,
    
    ingestion_timestamp

FROM volume_metrics
WHERE avg_volume_7d IS NOT NULL
ORDER BY symbol, date