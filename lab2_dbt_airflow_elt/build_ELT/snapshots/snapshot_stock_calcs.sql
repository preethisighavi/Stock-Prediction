{% snapshot snapshot_stock_calcs %}

{{
  config(
    target_schema="snapshot",
    unique_key="DATE",
    strategy="check",
    check_cols=["MOVING_AVG_7D", "RSI_14D"],
    invalidate_hard_deletes=True
  )
}}

SELECT 
    DATE,
    SYMBOL,
    MOVING_AVG_7D,
    RSI_14D
FROM {{ ref('stock_calcs') }}

{% endsnapshot %}
