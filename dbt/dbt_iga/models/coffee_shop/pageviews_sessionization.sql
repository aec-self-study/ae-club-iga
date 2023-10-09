{{ config(materialized = 'table') }}

with

stiched_visitors as (

    select 
        id,
        stiched_visitor_id,
        device_type,
        timestamp,
        page,
        customer_id
    from {{ ref('pageviews_user_stiching') }}

),

flag_new_session as (

    select 
        id,
        stiched_visitor_id,
        device_type,
        timestamp,
        page,
        customer_id,
        ((unix_seconds(stiched_visitors.timestamp) - lag(unix_seconds(stiched_visitors.timestamp))
                over (partition by stiched_visitor_id order by timestamp)) / 60)  as time_lag,
        case
            when ((unix_seconds(stiched_visitors.timestamp) - lag(unix_seconds(stiched_visitors.timestamp))
                over (partition by stiched_visitor_id order by timestamp)) / 60) >= 30 
            or ((unix_seconds(stiched_visitors.timestamp) - lag(unix_seconds(stiched_visitors.timestamp))
                over (partition by stiched_visitor_id order by timestamp)) / 60) is null
            then 1
            else 0
            end as new_session
    from stiched_visitors

),

add_session_id as (

    select 
        id,
        stiched_visitor_id,
        device_type,
        timestamp,
        page,
        customer_id,
        time_lag,
        sum(new_session) over (partition by stiched_visitor_id order by timestamp)|| '_' || stiched_visitor_id 
            as session_id
    from flag_new_session

)

select 
    id,
    stiched_visitor_id,
    session_id,
    device_type,
    timestamp,
    page,
    customer_id,
    time_lag as session_time_lag,
    case 
        when page = 'order-confirmation' then 1
        else 0
        end as is_checkout
from add_session_id
order by stiched_visitor_id, timestamp asc
