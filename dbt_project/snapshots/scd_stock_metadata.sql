-- snapshots/scd_stock_metadata.sql
-- SCD Type 2 snapshot of stock metadata.
-- dbt_valid_from / dbt_valid_to populated automatically by dbt snapshot.
-- Strategy: check MD5 hashdiff on tracked columns to detect changes.

{% snapshot scd_stock_metadata %}

{{
    config(
        target_schema='snapshots',
        unique_key='symbol',
        strategy='check',
        check_cols=['sector', 'industry', 'isin'],
        invalidate_hard_deletes=True
    )
}}

select
    symbol,
    sector,
    industry,
    isin,
    current_timestamp() as record_loaded_at

from {{ ref('dim_stock') }}

{% endsnapshot %}
