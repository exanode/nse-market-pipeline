-- intermediate/int_prices_enriched.sql
-- Add derived metrics: daily return, price year/month, rolling flags.
-- Keeps EQ series only - other series (BE, SM, etc.) excluded here.

with base as (
    select * from {{ ref('stg_nse_prices') }}
    where series = 'EQ'
),

with_prev as (
    select
        *,
        lag(close_price) over (
            partition by symbol
            order by price_date
        ) as prev_close_price

    from base
),

enriched as (
    select
        symbol,
        series,
        price_date,
        extract(year  from price_date)::int  as price_year,
        extract(month from price_date)::int  as price_month,
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
        prev_close_price,
        case
            when prev_close_price is not null and prev_close_price != 0
            then round((close_price - prev_close_price) / prev_close_price * 100, 4)
        end                                  as daily_return_pct,
        high_price - low_price               as intraday_range,
        ingested_at,
        run_id

    from with_prev
)

select * from enriched
