{{ config(
    materialized='table',
    schema='marts'
) }}

/*
    Calculate moving averages for technical analysis
    - 7-day MA (weekly)
    - 30-day MA (monthly)
    - Trading signals (crossovers)
*/

WITH stock_prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

moving_avg AS (
    SELECT
        symbol,
        date,
        close_price,
        
        -- 7-day moving average
        ROUND(
            AVG(close_price) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS ma_7_day,
        
        -- 30-day moving average
        ROUND(
            AVG(close_price) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS ma_30_day,
        
        -- Volume moving average
        ROUND(
            AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        ) AS avg_volume_7_day,
        
        volume,
        ingestion_timestamp
        
    FROM stock_prices
),

with_signals AS (
    SELECT
        *,
        
        -- Previous day's 7-day MA
        LAG(ma_7_day) OVER (PARTITION BY symbol ORDER BY date) AS prev_ma_7_day,
        
        -- Previous day's 30-day MA
        LAG(ma_30_day) OVER (PARTITION BY symbol ORDER BY date) AS prev_ma_30_day,
        
        -- Golden Cross / Death Cross signals
        CASE
            WHEN ma_7_day > ma_30_day 
                 AND LAG(ma_7_day) OVER (PARTITION BY symbol ORDER BY date) <= LAG(ma_30_day) OVER (PARTITION BY symbol ORDER BY date)
            THEN 'Golden Cross'
            
            WHEN ma_7_day < ma_30_day 
                 AND LAG(ma_7_day) OVER (PARTITION BY symbol ORDER BY date) >= LAG(ma_30_day) OVER (PARTITION BY symbol ORDER BY date)
            THEN 'Death Cross'
            
            ELSE NULL
        END AS crossover_signal
        
    FROM moving_avg
)

SELECT
    symbol,
    date,
    close_price,
    ma_7_day,
    ma_30_day,
    avg_volume_7_day,
    volume,
    
    -- Distance from moving averages
    ROUND(((close_price - ma_7_day) / ma_7_day) * 100, 2) AS distance_from_ma7_pct,
    ROUND(((close_price - ma_30_day) / ma_30_day) * 100, 2) AS distance_from_ma30_pct,
    
    crossover_signal,
    ingestion_timestamp

FROM with_signals
ORDER BY symbol, date