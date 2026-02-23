{% test is_positive(model, column_name) %}

    -- This query selects any rows where the business logic is violated
    select *
    from {{ model }}
    where {{ column_name }} < 0

{% endtest %}