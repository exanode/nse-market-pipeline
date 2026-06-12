-- macros/surrogate_key.sql
-- Thin wrapper so models don't call dbt_utils directly.
-- Swap the underlying impl here without touching model files.

{% macro surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}


{% macro audit_columns() %}
    current_timestamp()  as dbt_created_at,
    current_timestamp()  as dbt_updated_at,
    '{{ invocation_id }}' as dbt_run_id
{% endmacro %}
