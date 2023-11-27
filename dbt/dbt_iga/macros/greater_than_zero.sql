{% test greater_than_zero(model, column_name) %}
{{ config(severity = 'error') }}

    select {{ column_name }}
    from {{ model }}
    where not {{ column_name }} > 0 

{% endtest %}