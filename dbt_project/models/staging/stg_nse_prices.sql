-- staging/stg_nse_prices.sql
-- Normalise raw column names, cast types, drop nulls on required fields.
-- No business logic here - that lives in intermediate.

with source as (
    select * from {{ source('raw', 'raw_nse_prices') }}
),

renamed as (
    select
        upper(trim(symbol))                     as symbol,
        trim(series)                            as series,
        cast(date as date)                      as price_date,
        cast(open as float)                     as open_price,
        cast(high as float)                     as high_price,
        cast(low as float)                      as low_price,
        cast(close as float)                    as close_price,
        cast(vwap as float)                     as vwap,
        cast(volume as bigint)                  as volume,
        cast(delivery_volume as bigint)         as delivery_volume,
        cast(pct_delivery as float)             as pct_delivery,
        cast(no_of_trades as bigint)            as no_of_trades,
        cast(value as float)                    as traded_value,
        ingested_at,
        run_id

    from source
    where
        symbol is not null
        and date is not null
        and close is not null
)

select * from renamed
