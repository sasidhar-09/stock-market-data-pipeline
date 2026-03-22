{{ config(
    materialized='view',
    schema='staging'
) }}

/*
    Staging layer: Clean and standardize raw stock prices
    - Remove duplicates
    - Type casting
    - Add derived columns
    - Filter invalid records
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'stock_prices_raw') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        symbol,
        date,
        
        -- Price data
        CAST(open AS DECIMAL(18, 2)) AS open_price,
        CAST(high AS DECIMAL(18, 2)) AS high_price,
        CAST(low AS DECIMAL(18, 2)) AS low_price,
        CAST(close AS DECIMAL(18, 2)) AS close_price,
        
        -- Volume
        CAST(volume AS BIGINT) AS volume,
        
        -- Metadata
        ingestion_timestamp,
        source_system,
        pipeline_run_id,
        
        -- Derived columns
        CAST(high AS DECIMAL(18, 2)) - CAST(low AS DECIMAL(18, 2)) AS daily_range,
        ROUND(
            ((CAST(close AS DECIMAL(18, 2)) - CAST(open AS DECIMAL(18, 2))) / CAST(open AS DECIMAL(18, 2))) * 100, 
            2
        ) AS daily_change_pct
        
    FROM source
    
    WHERE 1=1
        -- Data quality filters
        AND close IS NOT NULL
        AND open IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND volume IS NOT NULL
        -- Valid OHLC relationship
        AND CAST(low AS DECIMAL(18, 2)) <= CAST(high AS DECIMAL(18, 2))
        AND CAST(open AS DECIMAL(18, 2)) BETWEEN CAST(low AS DECIMAL(18, 2)) AND CAST(high AS DECIMAL(18, 2))
        AND CAST(close AS DECIMAL(18, 2)) BETWEEN CAST(low AS DECIMAL(18, 2)) AND CAST(high AS DECIMAL(18, 2))
        -- Positive prices
        AND CAST(close AS DECIMAL(18, 2)) > 0
        AND volume > 0
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date 
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM cleaned
)

SELECT
    symbol,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_range,
    daily_change_pct,
    ingestion_timestamp,
    source_system,
    pipeline_run_id
FROM deduped
WHERE row_num = 1