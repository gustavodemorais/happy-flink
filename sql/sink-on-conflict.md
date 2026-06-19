# Sink ON CONFLICT clause

| Field | Value |
|-------|-------|
| **ID** | `sink-on-conflict` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | Sink |
| **Match** | INSERT into upsert sink where query upsert key differs from sink PK |
| **Flink version** | 2.3+ |

## Problem

Query write to an upsert sink, but the upsert key derived from the query does not match the sink `PRIMARY KEY`. From Flink 2.3 the planner stop guessing - it reject the INSERT and ask you to declare an `ON CONFLICT` clause that says how to handle two different upsert keys landing on the same sink PK.

Error looks like:

```
The query has an upsert key that differs from the primary key of the sink table
'...'. Primary key: [order_id], upsert key: [user_id, order_id]. ... Please specify
an ON CONFLICT clause ...
```

## Why it hurts

Mismatch = two records with different upsert keys map to one sink PK. Non-deterministic result, possible wrong deletes. The clause force you to pick the trade-off out loud instead of a silent state-heavy fix.

## How to fix

Best fix is to remove the mismatch, not paper over it - make the join preserve the right upsert key ([preserve-upsert-key-in-join](preserve-upsert-key-in-join.md)) and align the sink PK ([align-sink-pk](align-sink-pk.md)). Then no `ON CONFLICT` is needed.

If you genuinely cannot change the keys, add `ON CONFLICT` after the INSERT. Three options:

- `ON CONFLICT DO DEDUPLICATE` - keep latest record per PK. The old default behavior. State-intensive: keeps full change history per PK to roll back on retraction.
- `ON CONFLICT DO ERROR` - fail the job if two distinct records share a PK. Needs watermarks on all sources.
- `ON CONFLICT DO NOTHING` - keep the first record per PK, drop later conflicting ones. Needs watermarks on all sources.

`DO DEDUPLICATE` caps the symptom at high state cost - prefer fixing the keys. `DO ERROR` / `DO NOTHING` cheaper but change semantics; use only when that is what you want.

Config `table.exec.sink.require-on-conflict` (default `true`) gates the requirement. Setting it `false` restores the old silent behavior - not recommended.

See [sink-upsert-materializer](../plan/sink-upsert-materializer.md) for the operator behind the materialize/compaction cost.

## Example

Mismatch - query upsert key `(order_id, user_id)`, sink PK `(order_id)`. Best fix: align keys, no clause needed:

```sql
CREATE TABLE enriched_orders (
  user_id INT,
  order_id BIGINT,
  other_data STRING,
  PRIMARY KEY (order_id, user_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'enriched_orders');
```

If keys truly cannot change, declare the trade-off:

```sql
INSERT INTO enriched_orders
SELECT o.user_id, o.order_id, u.other_data
FROM users u
JOIN orders o ON o.user_id = u.user_id
ON CONFLICT DO DEDUPLICATE;
```
