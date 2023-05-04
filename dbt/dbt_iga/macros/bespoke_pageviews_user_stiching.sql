{% test bespoke_pageviews_user_stiching(model, column_name) %}

    {{ config(severity = 'error') }}

    with test as (
        select 
            customer_id, 
            count(distinct stiched_visitor_id) as should_be_unique
        from {{ ref('pageviews_user_stiching') }}
        where customer_id is not null
        group by 1 
        order by 2 desc 
    )

    select 
        * 
    from test
    where should_be_unique > 1

{% endtest %}