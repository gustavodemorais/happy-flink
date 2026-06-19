CREATE TABLE customers (
  customer_id BIGINT,
  name STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'customers'
);

CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount INT,
  order_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders'
);

SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id;
