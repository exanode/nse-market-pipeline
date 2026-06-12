-- marts/fct_daily_prices.sql
-- Incremental fact table: one row per symbol per trading date.
-- Grain is enforced via unique test on (symbol, price_date).
-- Re-runs are idempotent: existing partition rows get replaced.

{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'price_date'],
        incremental_strategy='merge',
        cluster_by=['price_year', 'price_month'],
        on_schema_change='fail'
    )
}}

with source as (
    select * from {{ ref('int_prices_enriched') }}

    {% if is_incremental() %}
    -- only process new dates to avoid full-table scans
    where price_date > (select max(price_date) from {{ this }})
    {% endif %}
),

final as (
    select
        {{ surrogate_key(['symbol', 'price_date']) }} as price_key,
        symbol,
        price_date,
        price_year,
        price_month,
        open_price,
        high_price,
        low_price,
        close_price,
        vwap,
        volume,
        delivery_volume,
        pct_delivery,
        no_of_trades,
        traded_value,
        daily_return_pct,
        intraday_range,
        prev_close_price,
        ingested_at,
        run_id

    from source
)

select * from final
