{{ config(materialized = 'table') }}

with user_stiching as (
    select
        pageviews.id,
        first_value(pageviews.visitor_id) 
            over (partition by customer_id order by timestamp) as stiched_visitor_id,
        pageviews.device_type,
        pageviews.timestamp,
        pageviews.page,
        pageviews.customer_id
    from {{ source('web_tracking', 'pageviews') }}
    where customer_id is not null   
),

not_customers as(
    select 
        *
    from {{ source('web_tracking', 'pageviews') }}
    where customer_id is null   
)

select
    *
from user_stiching
union all
select
    *
from not_customers