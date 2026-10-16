---
title: SinkUpsertMaterializer present
id: sink-upsert-materializer
impact: HIGH
impactDescription: "Extra per-key state grows with cardinality and can hide a real correctness bug such as wrong deletes."
requires: plan
operators: [SinkUpsertMaterializer]
match: "A SinkUpsertMaterializer operator in the plan."
tags: [sink, upsert-key, primary-key, changelog, state-size]
---

# SinkUpsertMaterializer present

## Problem

The plan shows a `SinkUpsertMaterializer` before the sink. Flink adds it to reorder the changelog when the sink's upsert key does not match the query's changelog key, or when the output contains non-deterministic columns.

## Why it hurts

The materializer holds an in-memory list of rows per key to fix the ordering before the sink. This adds state and cost that grow with cardinality. It can also mask a real correctness bug (wrong deletes) instead of fixing the root cause.

## How to fix

- **Primary key mismatch**: align the sink `PRIMARY KEY` with the query's upsert key (the GROUP BY or deduplication key). The materializer disappears. See [align-sink-pk](../sql/align-sink-pk.md).
- **A join dropped the upsert key**: fix the join so that it preserves an upsert key matching the sink (the full primary key in the join condition; for outer joins, the key on the non-null side). This is a common cause in join pipelines. See [preserve-upsert-key-in-join](../sql/preserve-upsert-key-in-join.md).
- **Non-deterministic column in the output** (`CURRENT_TIMESTAMP`, `NOW`, `UUID`): remove it from the sink columns, because it breaks the changelog key.
- **Ordering is already guaranteed**: disable the operator with `table.exec.sink.upsert-materialize = none`. Only do this when you know the upstream ordering is safe; otherwise results may be wrong.

On Flink 2.3+, a mismatch between the upsert key and the sink primary key no longer silently adds this operator. Instead, planning fails until you fix the keys or declare an `ON CONFLICT` clause. See [sink-on-conflict](../sql/sink-on-conflict.md).

## Example

Disable the operator only when ordering is already guaranteed and the primary key is aligned:

```sql
SET 'table.exec.sink.upsert-materialize' = 'none';
```

A better approach is to fix the root cause by aligning the sink primary key with the GROUP BY key. See [align-sink-pk](../sql/align-sink-pk.md).
