# Review: enriched_orders join

Join `users` x `orders` on `user_id` to feed upsert sink `enriched_orders`. Join drops the orders upsert key, so query output has no upsert key matching sink PK `order_id`. Flink 2.3+ rejects the INSERT.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | Join on `user_id` drops orders upsert key; output upsert key empty | high | [preserve-upsert-key-in-join](../../skills/flink-sql-table-api-optimizer/references/sql/preserve-upsert-key-in-join.md) | [#1](#1-join-drops-upsert-key) |
| 2 | Query upsert key != sink PK; planner rejects INSERT | high | [sink-on-conflict](../../skills/flink-sql-table-api-optimizer/references/sql/sink-on-conflict.md) | [#2](#2-upsert-key-vs-sink-pk-mismatch) |
| 3 | Sink PK must match preserved upsert key | high | [align-sink-pk](../../skills/flink-sql-table-api-optimizer/references/sql/align-sink-pk.md) | [#3](#3-align-sink-pk) |

Problems chain. #1 is root cause. #2 is the symptom (the error). #3 is the other half of the alignment fix.

## Paths

### Path A - Align keys (recommended)

Root-cause fix. Matches your working variant's plan exactly - no `SinkUpsertMaterializer`, no clause.

1. Add `user_id` to orders PK -> `(order_id, user_id)`. Join key `user_id` now part of the PK, so join preserves upsert key `(order_id, user_id)` (-> #1).
2. Set sink PK to `(order_id, user_id)` to match (-> #2, #3).
3. INSERT runs clean.

```sql
CREATE TABLE users (
  user_id INT NOT NULL,
  other_data STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('connector' = 'kafka', 'topic' = 'users');

CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  item_name STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'kafka', 'topic' = 'orders');

CREATE TABLE enriched_orders (
  user_id INT,
  order_id BIGINT,
  other_data STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'enriched_orders');

INSERT INTO enriched_orders
SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders o ON o.user_id = u.user_id;
```

### Path B - Declare ON CONFLICT (only if keys cannot change)

Caps symptom, does not fix root cause. Use only when you truly cannot touch orders PK or sink PK.

1. Keep DDL as-is.
2. Append `ON CONFLICT DO DEDUPLICATE` to accept latest record per PK. State-intensive: keeps full change history per PK. (`DO ERROR` / `DO NOTHING` cheaper but need watermarks on all sources and change semantics.)

```sql
INSERT INTO enriched_orders
SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders o ON o.user_id = u.user_id
ON CONFLICT DO DEDUPLICATE;
```

Flink version: `ON CONFLICT` needs 2.3+.

## Details

### #1 Join drops upsert key

Where: `input.sql:46` - `JOIN orders o ON o.user_id = u.user_id`.

orders PK is `order_id` only. A join preserves a side's upsert key only when the join condition carries that side's full PK. Join key is `user_id`, which is not the orders PK, so `order_id` is not preserved - the same `order_id` can route to different operator instances by `user_id`, no instance can dedup. Output upsert key = empty (your `error.txt`: `upsert key:` blank).

Fix: put the join key inside the PK. Make orders PK `(order_id, user_id)`. The join then preserves upsert key `(order_id, user_id)`. Your working plan confirms it - `plan.txt:11`: `leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[HasUniqueKey]`, and `plan.json` join node `rightUpsertKeys: [[0,1]]`.

Why `user_id` belongs in the PK: even though `order_id` alone identifies an order, `user_id` is the join/routing key and can change for an order. Without it in the PK the order could move between instances and results go wrong.

### #2 Upsert key vs sink PK mismatch

Where: the INSERT into `enriched_orders` (sink PK `order_id`).

From Flink 2.3 the planner stops silently inserting a correction operator. Empty/different upsert key vs sink PK -> reject with the message in `error.txt`. Two distinct upsert keys mapping to one sink PK = non-deterministic result, possible wrong deletes.

Fix: remove the mismatch (Path A) or declare the trade-off with `ON CONFLICT` (Path B). Prefer removing it.

### #3 Align sink PK

Where: `input.sql:28` - sink `PRIMARY KEY (order_id)`.

Once the join preserves `(order_id, user_id)`, the sink PK must match. Set sink PK to `(order_id, user_id)`. Your working plan's sink (`plan.json` node 18) has PK `PK_order_id_user_id` columns `[order_id, user_id]` and `inputUpsertKey: [0,1]` - aligned, so no materializer in the plan.

If sink PK stayed `order_id`, the mismatch returns even with the composite orders PK.

---
Path A is what your fixed plan already implements. Copy Path A SQL and it runs.
