{% test unique_per_key(model, key_column_name, column_name) %}

    {{ config(severity = 'error') }}

    with test as (
        select 
            {{ key_column_name }}, 
            count(distinct {{ column_name }}) as should_be_unique
        from {{ model }}
        where {{ key_column_name }} is not null
        group by 1 
        order by 2 desc 
    )

    select 
        * 
    from test
    where should_be_unique > 1

{% endtest %}