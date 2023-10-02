{{
    config(
        materialized = 'table'
        ) 
}}

with customers as (

    select
        id,
        name,
        email
    from {{ source('coffee_shop', 'customers') }}

),
  
orders as (
  
    select
        distinct customer_id,
        min(created_at) as first_order_at,
        count(total) as number_of_orders,
    from {{ source('coffee_shop', 'orders') }}
    group by customer_id 

)

select
    customers.id,
    customers.name,
    customers.email,
    orders.first_order_at,
    orders.number_of_orders
from customers
left outer join orders on customers.id = orders.customer_id
where 
    orders.number_of_orders is not null
order by orders.first_order_at

