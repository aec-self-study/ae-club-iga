{{ config(materialized = 'table') }}

with 

orders as (

    select
        id,
        created_at,
        customer_id,
        total
    from {{ source('coffee_shop', 'orders') }}

),

order_items as (

    select
        order_id,
        product_id
    from {{ source('coffee_shop', 'order_items') }}

),

product_prices as (

    select
        product_id,
        price
    from {{ source('coffee_shop', 'product_prices') }}

)

select
    order_items.product_id,
    orders.id as order_id,
    orders.customer_id,
    product_prices.price as product_price,
    orders.total as order_total
from order_items
left join orders 
    on order_items.order_id = orders.id
left join product_prices
    on order_items.product_id = product_prices.product_id