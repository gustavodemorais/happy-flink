# Review: customers JOIN orders (regular join)

`SELECT *` regular join of two unbounded Kafka streams on `customer_id`. Both join inputs kept in state forever, no TTL, no time bound, no watermark. State grow without ceiling.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | Regular join, unbounded state both sides | high | [join-unbounded-state](plan/join-unbounded-state.md) | [#1](#1-regular-join-unbounded-state) |
| 2 | No watermark on either source - blocks temporal/interval fix | high | [require-watermark](sql/require-watermark.md) | [#2](#2-no-watermark) |
| 3 | Join state TTL = 0 ms (no cleanup) | high | [state-ttl](plan/state-ttl.md) | [#3](#3-join-state-ttl--0-ms) |

Root cause = #1. `customers` is a dimension (`customer_id`, `name`); `orders` is the fact stream. Right fix = temporal join, which needs watermark (#2 first). TTL (#3) is fallback only.

## Paths

### Path A - Temporal join (recommended)

`customers` looks like a slowly-changing dimension keyed by `customer_id`. Join `orders` against the latest customer version per key -> customer state bounded to one row per key, order state dropped after match.

1. Add PK + event-time watermark to `customers` -> makes it a versioned table (-> #2, enables #1)
2. Add watermark to `orders` on `order_time` (-> #2)
3. Rewrite as `FOR SYSTEM_TIME AS OF o.order_time` (-> #1, #3)

```sql
CREATE TABLE customers (
  customer_id BIGINT,
  name STRING,
  update_time TIMESTAMP(3),
  PRIMARY KEY (customer_id) NOT ENFORCED,
  WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'customers'
);

CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount INT,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders'
);

SELECT o.order_id, o.customer_id, o.amount, o.order_time, c.name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
  ON o.customer_id = c.customer_id;
```

Note: temporal join needs a versioned right side (PK + watermark). `customers` has no event-time column today -> added `update_time`. If `customers` is truly static, prefer Path B (lookup join).

### Path B - Lookup join (if customers lives in external DB)

If `customers` is a table in an external store (JDBC, etc.), not a Kafka stream, join via processing-time lookup. Zero Flink join state - right side queried on demand.

1. Define `customers` as a lookup connector (jdbc/hbase/...) with PK
2. Add processing-time attr to `orders`
3. Join `FOR SYSTEM_TIME AS OF o.proctime`

```sql
CREATE TABLE customers (
  customer_id BIGINT,
  name STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '...',
  'table-name' = 'customers'
);

CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount INT,
  order_time TIMESTAMP(3),
  proctime AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders'
);

SELECT o.order_id, o.customer_id, o.amount, o.order_time, c.name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.proctime AS c
  ON o.customer_id = c.customer_id;
```

### Path C - Keep regular join, cap with TTL (fallback)

Only if both sides are genuinely streams that update independently and you need full bi-directional join semantics. Does not fix root cause - just bounds state by idle time. Risk: drop state still needed -> miss matches.

```sql
SET 'table.exec.state.ttl' = '36 h';

SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id;
```

## Details

### #1 Regular join, unbounded state

Where: plan node `id 10`, `Join(joinType=[InnerJoin] ... leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])`. Both sources `"bounded" : "false"`. No time predicate in join condition (`ON c.customer_id = o.customer_id` only).

Why it hurts: regular join buffers every row from both sides forever to match future arrivals. `NoUniqueKey` both sides = no dedup, every input row stored. State grows with total input -> slow/big checkpoints, slow restore, eventual OOM.

Fix: convert to temporal (Path A) or lookup (Path B) join. Temporal right side keeps only latest version per key; lookup keeps no Flink state at all. Both need the PK/time setup shown above.

### #2 No watermark

Where: both `CREATE TABLE` (input.sql lines 1-17) - no `WATERMARK` clause. `orders.order_time` is a plain `TIMESTAMP(3)`, not a time attribute.

Why it hurts: temporal and interval joins fire on event time. Without a watermark there is no time attribute -> `FOR SYSTEM_TIME AS OF o.order_time` won't compile, and any interval-join rewrite never advances. So #2 must be fixed before #1 can be fixed semantically.

Fix: add `WATERMARK FOR <ts> AS <ts> - INTERVAL '5' SECOND` to each source (shown in Path A). Keep lateness < expected gap. Path B sidesteps this using processing time instead.

### #3 Join state TTL = 0 ms

Where: plan.json `id 10`, `"state"` array - `leftState` and `rightState` both `"ttl" : "0 ms"`. 0 = never expire.

Why it hurts: confirms no cleanup point. Combined with #1, state is strictly monotonic growth.

Fix: a semantic join (Path A/B) removes the need for TTL entirely. If you must keep the regular join (Path C), set `table.exec.state.ttl` larger than the longest expected gap between matching customer/order events - too short drops still-needed rows and silently loses joins.
