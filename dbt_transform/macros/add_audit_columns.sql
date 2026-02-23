{% macro add_audit_columns() %}
    
    -- Captures the exact timestamp the dbt model was executed
    cast(current_timestamp() as timestamp) as dbt_updated_at,
    
    -- Injects the unique dbt run ID so you can trace a row back to a specific dbt execution in your logs
    '{{ invocation_id }}' as dbt_run_id

{% endmacro %}