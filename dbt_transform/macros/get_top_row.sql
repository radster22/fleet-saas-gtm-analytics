{% macro get_top_row(model, partition_by, order_by) %}

    select * from (
        select 
            *,
            row_number() over (
                partition by {{ partition_by }} 
                order by {{ order_by }}
            ) as macro_rn
        from {{ model }}
    )
    where macro_rn = 1

{% endmacro %}