All rules checked. Here's the review.

# Review: customers JOIN orders (regular join)

Enriches each order with its customer via plain regular equi-join on `customer_id`. Both Kafka sources unbounded, join keeps both sides in state forever with no TTL and no time bound - state grows without limit.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | Regular join, both sides unbounded, `ttl=0 ms` | high | [join-unbounded-state](plan/join-unbounded-state.md) | [#1](#1-unbounded-join-state) |
| 2 | `customers` is a dimension - should be temporal join | high | [temporal-join](sql/temporal-join.md) | [#2](#2-should-be-temporal-join) |
| 3 | No `WATERMARK` on either table - blocks temporal/interval fix | high | [require-watermark](sql/require-watermark.md) | [#3](#3-no-watermark) |
| 4 | Join state has no TTL (fallback bound only) | high | [state-ttl](plan/state-ttl.md) | [#4](#4-no-state-ttl) |

All four are the same root knot: a regular join with nothing to bound it. Fix the semantics (#2 + #3) and #1/#4 fall away.

## Paths

### Path A - Temporal join (recommended)

Fixes root cause. `customers` is a slowly-changing dimension; you only need the latest version per `customer_id`, not every customer row ever. Temporal join keeps right side bounded to one version per key.

1. Add primary key + version time + watermark to `customers` (-> #3, makes it a versioned table).
2. Add watermark to `orders.order_time` (-> #3).
3. Rewrite as event-time temporal join, `orders` probe side (-> #2, #1, #4).

Result: dim state = one row per active customer key, order state bounded by watermark. No TTL guessing.

### Path B - Lookup join

Only if `customers` actually lives in an external queryable store (JDBC, HBase) and Kafka here is a stand-in. Then no Flink state for the dim at all - Flink queries per row + cache. Needs the source swapped to a lookup connector + a proctime attribute on `orders`. Not applicable while `customers` is Kafka.

### Path C - State TTL (fallback)

Keep the regular join, just cap state: `SET 'table.exec.state.ttl' = '36 h';`. Cheapest, zero query change. But it only bounds the symptom - too short drops still-needed customers (wrong join misses), too long = state still huge. Use only if temporal semantics genuinely don't fit.

## Details

### #1 Unbounded join state

Where: plan node `id 10`, `Join(joinType=[InnerJoin], ..., leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])`, and `state` block shows `"ttl" : "0 ms"` for both `leftState` and `rightState`. Both sources `"bounded" : "false"`.

Why it hurts: regular join buffers every left and right row to match future arrivals from the other side. `NoUniqueKey` both sides = no per-key collapse, every row retained. State only grows -> checkpoint size grows, slow checkpoints, slow restore, eventual OOM. No time attribute in the join = no point where Flink can ever evict.

Fix: give the join a bound - temporal (Path A), interval, or TTL (Path C). See [join-unbounded-state](plan/join-unbounded-state.md).

### #2 Should be temporal join

Where: SQL line 19, `customers c JOIN orders o ON c.customer_id = o.customer_id`. `customers` (customer_id, name) is a dimension; `orders` carries the event time `order_time` - classic fact-enriches-from-dim.

Why it hurts: regular join stores all dim versions forever. The temporal-join rule notes switching to a temporal join can cut join state dramatically, from huge to small, because only the latest version per key is kept.

Fix: event-time temporal join with `orders` as probe side and `customers` as a versioned table (PK = `customer_id` + event-time watermark).

Before:

```sql
SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id;
```

After (with the DDL changes from #3):

```sql
SELECT o.order_id, o.customer_id, o.amount, o.order_time, c.name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
  ON o.customer_id = c.customer_id;
```

Note: drop `SELECT *` - it currently emits `customer_id` twice (`customer_id` and `customer_id0` in the output type). Project the columns you need.

See [temporal-join](sql/temporal-join.md).

### #3 No watermark

Where: both `CREATE TABLE` (lines 1-17). Neither table defines `WATERMARK`. `order_time` is a plain `TIMESTAMP(3)`, not a time attribute; `customers` has no version time at all.

Why it hurts: the recommended fix (temporal join, also interval join) requires watermarks on both sides. Without them the temporal join can't be planned - this blocks Path A. The INDEX review step is explicit: a fix that needs a watermark when none is defined is its own problem, not a silent assumption.

Fix: make `customers` a versioned table (PK + version column + watermark) and add a watermark to `orders`.

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
```

A versioned `customers` needs a changelog source (`upsert-kafka` or a CDC connector) so Flink tracks versions per PK; a plain append Kafka topic gives an append-only "versioned view" keyed by PK + watermark. See [require-watermark](sql/require-watermark.md).

### #4 No state TTL

Where: plan `id 10`, `"ttl" : "0 ms"` on both join states. `0 ms` = TTL disabled.

Why it hurts: even discounting the dimension semantics, a stateful operator with no TTL and no window keeps state for every key ever seen. High-cardinality `customer_id` = unbounded entries, none evicted.

Fix: Path A removes the need (temporal join bounds state by version + watermark). If you must keep the regular join, set `table.exec.state.ttl` as in Path C - but tune carefully, too short drops customers still receiving orders and silently misses joins. See [state-ttl](plan/state-ttl.md).

---

Bottom line: this is one regular-join-on-an-unbounded-stream problem wearing four hats. Go Path A - add the watermarks/PK (#3), rewrite as a temporal join (#2), and the unbounded-state and TTL issues (#1, #4) resolve themselves.
