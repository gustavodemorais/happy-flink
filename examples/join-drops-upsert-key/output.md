# Review: enriched_orders join

Join `users` to `orders` on `user_id`, write to upsert sink keyed by `order_id`. Join key (`user_id`) does not carry the orders PK (`order_id`), so join output has no upsert key matching sink PK. Planner reject.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | Join drops upsert key - output key ≠ sink PK | high | [preserve-upsert-key-in-join](sql/preserve-upsert-key-in-join.md) | [#1](#1-join-drops-upsert-key) |
| 2 | Sink PK not aligned with join's preserved key | high | [align-sink-pk](sql/align-sink-pk.md) | [#2](#2-sink-pk-misaligned) |
| 3 | Regular join, unbounded state (TTL 0 ms) | high | [join-unbounded-state](plan/join-unbounded-state.md) | [#3](#3-unbounded-join-state) |

## Paths

Problems chain: #1 is root cause of the planner error, #2 is the matching sink change, #3 is a separate state issue the working plan still has.

### Path A - composite key, align sink (recommended)
1. Add `user_id` to `orders` PK -> `(order_id, user_id)`. Join then preserves upsert key `(order_id, user_id)` (-> #1)
2. Set sink PK = `(order_id, user_id)` to match (-> #2)
3. Set join state TTL to bound state (-> #3)

This is exactly the working variant in `plan.json`: orders PK `(order_id, user_id)`, sink PK `(order_id, user_id)`, join emits `rightUpsertKeys [[0,1]]`, sink `inputUpsertKey [0,1]`, no `SinkUpsertMaterializer`.

### Path B - ON CONFLICT (Flink 2.4+, not recommended)
1. Keep schema as-is, add `ON CONFLICT DO DEDUPLICATE` (or `DO ERROR` / `DO NOTHING`) to the INSERT (-> #1, #2)

The error text offers this. `DO DEDUPLICATE` is state-intensive (keeps full history per PK) - caps symptom, not root cause. Use only if you truly cannot change the orders PK.

## Details

### 1. Join drops upsert key

Where: `input.sql:38`, join `ON o.user_id = u.user_id`. orders PK is `order_id` only (`input.sql:16`).

Error:
```
upsert key: . ... Primary key: [order_id], upsert key: .
```
Empty upsert key = join preserved nothing matching the sink.

Why it hurts: join only preserves a side's upsert key if the join key contains that side's **full** PK. Join key is `user_id`, orders PK is `order_id` - `user_id` does not contain `order_id`, so the key is lost. Same `order_id` can update its `user_id`, routing rows for one order to different operator instances - no instance can dedup. Flink must add `SinkUpsertMaterializer` (in-memory row list per key, often the pipeline bottleneck) or emit wrong deletes.

Fix: put the full preserved PK in the key path. Make orders PK composite:
```sql
CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  item_name STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'kafka', 'topic' = 'orders');
```
Now join preserves upsert key `(order_id, user_id)`. Confirmed in `plan.json:135` `rightUpsertKeys: [[0,1]]` and join spec `rightInputSpec=[HasUniqueKey]`.

### 2. Sink PK misaligned

Where: `input.sql:27`, sink PK `(order_id)`.

After fix #1 the query upsert key is `(order_id, user_id)`. Sink PK `(order_id)` still mismatches -> materializer returns. Align them:
```sql
CREATE TABLE enriched_orders (
  user_id INT,
  order_id BIGINT,
  other_data STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'enriched_orders');
```
Working plan: sink PK `PK_order_id_user_id [order_id, user_id]` (`plan.json:212`), `inputUpsertKey [0,1]` (`plan.json:226`), no materializer node. Key alignment done.

### 3. Unbounded join state

Where: `plan.json:137-145` - join `leftState` and `rightState` both `"ttl": "0 ms"`.

Both sources are Kafka changelog (`I,UA,UB,D`). Regular (non-temporal) join keeps every row of both sides in state forever to match future updates. TTL 0 = no expiry -> state grows with cardinality, unbounded, big checkpoints. The working variant still has this.

Fix: bound join state with idle TTL if your join window is logically bounded:
```sql
SET 'table.exec.state.ttl' = '36 h';
```
Pick TTL > max lag between a matching `users` update and `orders` row, else you drop valid matches. If `users` is a true dimension table, a [temporal join](sql/temporal-join.md) or [lookup join](sql/lookup-join.md) avoids holding both sides entirely - better than TTL.
