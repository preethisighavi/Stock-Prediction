SELECT
    "open",
    "high",
    "low",
    "close",
    "volume",
    "date",
    "symbol"
FROM {{ source('raw_data', 'stock_prices') }}