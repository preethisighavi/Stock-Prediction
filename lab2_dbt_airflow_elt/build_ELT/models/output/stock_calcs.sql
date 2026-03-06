WITH stock_data AS (
    SELECT 
        "date",
        "symbol",
        "close"
    FROM {{ ref("stock_prices") }}
),
lagged_data AS (
    SELECT
        "date",
        "symbol",
        "close",
        LAG("close") OVER (PARTITION BY "symbol" ORDER BY "date") AS lag_close
    FROM stock_data
),
moving_averages AS (
    SELECT
        "date",
        "symbol",
        "close",
        AVG("close") OVER (
            PARTITION BY "symbol"
            ORDER BY "date"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d
    FROM lagged_data
),
rsi_calculation AS (
    SELECT
        "date",
        "symbol",
        "close",
        SUM(
            CASE 
                WHEN "close" - lag_close > 0 THEN "close" - lag_close
                ELSE 0 
            END
        ) OVER (
            PARTITION BY "symbol"
            ORDER BY "date"
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS gain,
        SUM(
            CASE 
                WHEN "close" - lag_close < 0 THEN ABS("close" - lag_close)
                ELSE 0 
            END
        ) OVER (
            PARTITION BY "symbol"
            ORDER BY "date"
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS loss
    FROM lagged_data
),
rsi_final AS (
    SELECT
        "date",
        "symbol",
        CASE
            WHEN loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (gain / NULLIF(loss, 0))))
        END AS rsi_14d
    FROM rsi_calculation
)
SELECT 
    ma."date" AS date,
    ma."symbol" AS symbol,
    ma.moving_avg_7d AS moving_avg_7d,
    rsi.rsi_14d AS rsi_14d
FROM moving_averages ma
LEFT JOIN rsi_final rsi
ON ma."date" = rsi."date" AND ma."symbol" = rsi."symbol"