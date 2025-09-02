INSERT INTO sales.fact_orders
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_email,
    o.order_id,
    o.order_date,
    o.order_status,
    p.product_id,
    p.product_name,
    p.product_category,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price as line_total
FROM customers.customers c
INNER JOIN orders.orders o ON c.customer_id = o.customer_id
INNER JOIN orders.order_items oi ON o.order_id = oi.order_id
INNER JOIN products.products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01'
  AND o.order_status = 'COMPLETED';