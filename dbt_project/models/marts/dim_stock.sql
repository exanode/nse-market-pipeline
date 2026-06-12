-- marts/dim_stock.sql
-- Static dimension: one row per listed symbol.
-- Sourced from the index constituent list; not SCD2 here (see snapshot for history).

with symbols as (
    select distinct
        upper(trim(symbol))  as symbol
    from {{ ref('stg_nse_prices') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['symbol']) }} as stock_key,
        symbol,
        -- sector/industry would come from a separate reference table
        -- left as null until that source is available
        cast(null as varchar) as sector,
        cast(null as varchar) as industry,
        cast(null as varchar) as isin,
        current_timestamp()  as dbt_created_at

    from symbols
)

select * from enriched
