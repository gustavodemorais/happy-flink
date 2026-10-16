---
title: Sink ON CONFLICT clause
id: sink-on-conflict
impact: MEDIUM
impactDescription: "The INSERT is rejected at planning time, or a key mismatch is papered over with costly or lossy conflict handling."
requires: sql
operators: [Sink]
match: "An INSERT into an upsert sink where the query upsert key differs from the sink primary key."
flink-version: "2.3+"
tags: [sink, on-conflict, upsert-key, primary-key, changelog]
---

# Sink ON CONFLICT clause

## Problem

A query writes to an upsert sink, but the upsert key derived from the query does not match the sink `PRIMARY KEY`. Starting with Flink 2.3, the planner no longer guesses: it rejects the INSERT and asks you to declare an `ON CONFLICT` clause that specifies how to handle two different upsert keys landing on the same sink primary key.

The error looks like this:

```
The query has an upsert key that differs from the primary key of the sink table
'...'. Primary key: [order_id], upsert key: [user_id, order_id]. ... Please specify
an ON CONFLICT clause ...
```

## Why it hurts

A mismatch means that two records with different upsert keys map to one sink primary key. The result is non-deterministic, and wrong deletes are possible. The clause forces you to choose the trade-off explicitly instead of relying on a silent, state-heavy fix.

## How to fix

The best fix is to remove the mismatch rather than hide it: make the join preserve the right upsert key ([preserve-upsert-key-in-join](preserve-upsert-key-in-join.md)) and align the sink primary key ([align-sink-pk](align-sink-pk.md)). Then no `ON CONFLICT` clause is needed.

If you cannot change the keys, add `ON CONFLICT` after the INSERT. There are three options:

- `ON CONFLICT DO DEDUPLICATE`: keeps the latest record per primary key. This was the previous default behavior. It is state-intensive, because it keeps the full change history per primary key to roll back on retraction.
- `ON CONFLICT DO ERROR`: fails the job if two distinct records share a primary key. Requires watermarks on all sources.
- `ON CONFLICT DO NOTHING`: keeps the first record per primary key and drops later conflicting ones. Requires watermarks on all sources.

`DO DEDUPLICATE` contains the symptom at a high state cost, so prefer fixing the keys. `DO ERROR` and `DO NOTHING` are cheaper but change semantics; use them only when that behavior is what you want.

The config `table.exec.sink.require-on-conflict` (default `true`) controls this requirement. Setting it to `false` restores the previous silent behavior, which is not recommended.

See [sink-upsert-materializer](../plan/sink-upsert-materializer.md) for the operator behind the materialization and compaction cost.

## Example

In this mismatch, the query upsert key is `(order_id, user_id)` and the sink primary key is `(order_id)`. The best fix is to align the keys, so no clause is needed:

```sql
CREATE TABLE enriched_orders (
  user_id INT,
  order_id BIGINT,
  other_data STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'enriched_orders');
```

If the keys cannot change, declare the trade-off:

```sql
INSERT INTO enriched_orders
SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders o ON o.user_id = u.user_id
ON CONFLICT DO DEDUPLICATE;
```
