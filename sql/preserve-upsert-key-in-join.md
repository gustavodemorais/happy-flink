# Preserve upsert key in join

| Field | Value |
|-------|-------|
| **ID** | `preserve-upsert-key-in-join` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | Join, SinkUpsertMaterializer |
| **Match** | Join feeds an upsert sink, but join output has no upsert key matching the sink PK |

## Problem

Sink needs an upsert key (it has a `PRIMARY KEY`, updates by key). But the join drops the upsert key - the query output carries no key that matches the sink PK. Flink then insert a `SinkUpsertMaterializer` to correct ordering, or worse, emit wrong deletes.

A join only preserves an upsert key if the join condition carries the full primary key of the side whose key you want to keep. Miss part of that PK and the key is lost - rows for one logical key can route to different operator instances, so no single instance can dedup them.

## Why it hurts

- `SinkUpsertMaterializer` keep an in-memory row list per key to reorder the changelog. In join pipelines it is often the main bottleneck, worse than the join nodes themselves. State-intensive, scales with cardinality.
- Correctness: if the upsert key is wrong, updates land under the wrong key, sink get wrong deletes. Silent data corruption.

Planner warns when sink PK does not match the upsert key derived from the query - it says a state-intensive correction operator must be added. That warning = this rule.

## How to fix

Make the join preserve an upsert key that matches the sink.

1. **Include the full PK in the join condition / key routing.** Join condition must contain the complete primary key of the side whose key you want preserved, so rows for that key route to the same operator instance.
2. **Outer joins - mind nullability.** Keep the desired upsert key on the side that cannot produce nulls. LEFT join -> left side never null, keep the key there. RIGHT join -> right side. An upsert key on the nullable side breaks correctness.
3. **Read the plan source-to-sink.** `EXPLAIN` and follow nodes from sources down to the sink. Watch where the upsert key is present and where it disappears. The join node shows the upsert key it preserves (e.g. `(order_id, user_id)`).

See [align-sink-pk](align-sink-pk.md) for the aggregate / dedup version of the mismatch, and [sink-upsert-materializer](../plan/sink-upsert-materializer.md) to spot the operator in the plan. On Flink 2.3+ the mismatch fails planning until you fix the keys or declare a clause - see [sink-on-conflict](sink-on-conflict.md).

## Example

### Join loses the upsert key

`orders` PK is only `order_id`. Join on `user_id` does not carry the full `orders` PK, so the join output has no upsert key.

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

### Composite PK preserves it

Make `orders` PK `(order_id, user_id)`. The join then preserves upsert key `(order_id, user_id)`.

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

Even though `order_id` alone identifies the order, the join key `user_id` can be updated for an order. Without `user_id` in the PK, the same `order_id` could move between instances and results go wrong. Both `(order_id, user_id)` needed to keep the key stable.

### Outer join - key on the non-null side

LEFT join: left side never null, so keep the upsert key on the left.

```sql
SELECT o.user_id, o.order_id, u.other_data
FROM orders_with_composite_key o
LEFT JOIN users u ON o.user_id = u.user_id;
-- left (orders) side never null - upsert key preserved on it
```

Invert it and the keyed side becomes nullable:

```sql
SELECT o.user_id, o.order_id, u.other_data
FROM users u
LEFT JOIN orders_with_composite_key o ON o.user_id = u.user_id;
-- orders now on the nullable right side; output upsert key includes a nullable column
```
