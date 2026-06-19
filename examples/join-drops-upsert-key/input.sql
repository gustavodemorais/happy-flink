-- Users: keyed by user_id.
CREATE TABLE
  users (
    user_id INT NOT NULL,
    other_data STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
  )
WITH
  ('connector' = 'kafka', 'topic' = 'users');

-- Orders: primary key is only order_id.
CREATE TABLE
  orders (
    order_id BIGINT NOT NULL,
    user_id INT NOT NULL,
    item_name STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
  )
WITH
  ('connector' = 'kafka', 'topic' = 'orders');

-- Upsert sink keyed by order_id.
CREATE TABLE
  enriched_orders (
    user_id INT,
    order_id BIGINT,
    other_data STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
  )
WITH
  (
    'connector' = 'upsert-kafka',
    'topic' = 'enriched_orders'
  );

-- Join on user_id. The orders PK (order_id) is not carried by the join key,
-- so the join output has no upsert key matching the sink.
INSERT INTO
  enriched_orders
SELECT
  o.user_id,
  o.order_id,
  u.other_data
FROM
  users u
  JOIN orders o ON o.user_id = u.user_id;
