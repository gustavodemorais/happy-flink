-- Raw user records, may repeat per user_id over time.
CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  email STRING,
  update_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'users'
);

-- One row per user wanted in the sink.
CREATE TABLE deduped_users (
  user_id BIGINT,
  name STRING,
  email STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'deduped_users'
);

-- Dedup by keeping the first value per user_id.
INSERT INTO deduped_users
SELECT user_id, FIRST_VALUE(name), FIRST_VALUE(email)
FROM users
GROUP BY user_id;
