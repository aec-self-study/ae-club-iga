{{ config(materialized = 'table') }}

with

orders as (

    select
        id as order_id,
        customer_id,
        total as order_total,
        created_at
    from {{ source('coffee_shop', 'orders') }}

),

first_purchase_date_per_customer as (

    select
        customer_id,
        min(created_at) as first_purchase_date
    from orders
    group by 1
),

joined as (

    select
        order_id,
        orders.customer_id,
        created_at,
        first_purchase_date,
        cast(round(date_diff(cast(created_at as date), cast(first_purchase_date as date), day)/7) as int) as weeks_since_first_purchase,
        order_total
    from orders
    left join first_purchase_date_per_customer
        on orders.customer_id = first_purchase_date_per_customer.customer_id

),

cumulative as (

    select
        order_id,
        customer_id,
        created_at,
        first_purchase_date,
        weeks_since_first_purchase,
        rank() over (partition by customer_id order by weeks_since_first_purchase) as week_rank_per_customer,
        order_total,
        sum(order_total) over (partition by customer_id order by weeks_since_first_purchase)  as cumulative_revenue
    from joined
    order by customer_id asc
)

select *
from cumulative