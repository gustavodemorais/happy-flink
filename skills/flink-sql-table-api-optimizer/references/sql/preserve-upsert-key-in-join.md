---
title: Preserve upsert key in join
id: preserve-upsert-key-in-join
impact: HIGH
impactDescription: "Flink adds a state-heavy SinkUpsertMaterializer, or the sink receives wrong deletes."
requires: sql
operators: [Join, SinkUpsertMaterializer]
match: "A join feeds an upsert sink, but the join output has no upsert key matching the sink primary key."
tags: [join, upsert-key, primary-key, sink, changelog]
---

# Preserve upsert key in join

## Problem

The sink needs an upsert key: it has a `PRIMARY KEY` and applies updates by key. However, the join drops the upsert key, so the query output carries no key that matches the sink primary key. Flink then inserts a `SinkUpsertMaterializer` to correct the ordering or, worse, emits wrong deletes.

A join preserves an upsert key only if the join condition contains the full primary key of the side whose key you want to keep. If part of that primary key is missing, the key is lost: rows for one logical key can be routed to different operator instances, so no single instance can deduplicate them.

## Why it hurts

- `SinkUpsertMaterializer` keeps an in-memory list of rows per key to reorder the changelog. In join pipelines it is often the main bottleneck, worse than the join operators themselves. It is state-intensive and scales with key cardinality.
- Correctness: if the upsert key is wrong, updates land under the wrong key and the sink receives wrong deletes. This is silent data corruption.

The planner warns when the sink primary key does not match the upsert key derived from the query, stating that a state-intensive correction operator must be added. That warning indicates this rule applies.

## How to fix

Make the join preserve an upsert key that matches the sink.

1. **Include the full primary key in the join condition and key routing.** The join condition must contain the complete primary key of the side whose key you want to preserve, so that all rows for that key are routed to the same operator instance.
2. **For outer joins, consider nullability.** Keep the desired upsert key on the side that cannot produce nulls. In a LEFT join, the left side is never null, so keep the key there. In a RIGHT join, use the right side. An upsert key on the nullable side breaks correctness.
3. **Read the plan from source to sink.** Run `EXPLAIN` and follow the nodes from the sources down to the sink. Note where the upsert key is present and where it disappears. The join node shows the upsert key it preserves (for example, `(order_id, user_id)`).

See [align-sink-pk](align-sink-pk.md) for the aggregate and deduplication version of this mismatch, and [sink-upsert-materializer](../plan/sink-upsert-materializer.md) to find the operator in the plan. On Flink 2.3+, the mismatch fails planning until you fix the keys or declare an `ON CONFLICT` clause; see [sink-on-conflict](sink-on-conflict.md).

## Example

### Join loses the upsert key

The primary key of `orders` is only `order_id`. Joining on `user_id` does not carry the full `orders` primary key, so the join output has no upsert key.

```sql
CREATE TABLE users (
  user_id INT NOT NULL,
  other_data STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
);

CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  item_name STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
);

SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders o ON o.user_id = u.user_id;
-- join has NO upsert key
```

### Composite primary key preserves it

Make the `orders` primary key `(order_id, user_id)`. The join then preserves the upsert key `(order_id, user_id)`.

```sql
CREATE TABLE orders_with_composite_key (
  order_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  item_name STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
);

CREATE TABLE users (
  user_id INT NOT NULL,
  other_data STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
);

SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders_with_composite_key o ON o.user_id = u.user_id;
-- preserves upsert key (order_id, user_id)
```

Even though `order_id` alone identifies an order, the join key `user_id` can be updated for an order. Without `user_id` in the primary key, the same `order_id` could move between instances and produce wrong results. Both `order_id` and `user_id` are needed to keep the key stable.

### Outer join: key on the non-null side

In a LEFT join, the left side is never null, so keep the upsert key on the left.

```sql
SELECT o.user_id, o.order_id, u.other_data
FROM orders_with_composite_key o
LEFT JOIN users u ON o.user_id = u.user_id;
-- left (orders) side never null - upsert key preserved on it
```

If the sides are swapped, the keyed side becomes nullable:

```sql
SELECT o.user_id, o.order_id, u.other_data
FROM users u
LEFT JOIN orders_with_composite_key o ON o.user_id = u.user_id;
-- orders now on the nullable right side; output upsert key includes a nullable column
```
