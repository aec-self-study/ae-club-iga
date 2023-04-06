WITH
  customers AS (
  SELECT
    id,
    name,
    email
  FROM
    `analytics-engineers-club.coffee_shop.customers` ),
  orders AS (
  SELECT
    DISTINCT customer_id,
    MIN(created_at) AS first_order,
    COUNT(total) AS number_of_orders,
  FROM
    `analytics-engineers-club.coffee_shop.orders`
  GROUP BY
    customer_id )
SELECT
  customers.id,
  customers.name,
  customers.email,
  orders.first_order,
  orders.number_of_orders
FROM
  customers
LEFT OUTER JOIN
  orders
ON
  customers.id = orders.customer_id
WHERE
  orders.number_of_orders IS NOT NULL
ORDER BY
  orders.first_order
LIMIT
  5;
