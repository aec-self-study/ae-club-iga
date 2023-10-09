{{ config(materialized = 'table') }}

with

pageviews_sessionization as (

    select 
        session_id,
        min(timestamp) as session_start_time,
        max(timestamp) as session_end_time,
        datetime_diff(max(timestamp), min(timestamp), second) as session_duration_seconds,
        count(distinct page) as number_of_pages_visited,
        sum(is_checkout) as number_of_checkouts
    from {{ ref('pageviews_sessionization') }}
    group by 1

)


select 
    session_id,
    session_start_time,
    session_end_time,
    session_duration_seconds,
    number_of_pages_visited,
    number_of_checkouts
from pageviews_sessionization
order by number_of_checkouts desc