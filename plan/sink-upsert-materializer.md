# SinkUpsertMaterializer present

| Field | Value |
|-------|-------|
| **ID** | `sink-upsert-materializer` |
| **Severity** | high |
| **Requires** | plan |
| **Operators** | SinkUpsertMaterializer |
| **Match** | SinkUpsertMaterializer operator in plan |

## Problem

Plan show `SinkUpsertMaterializer` before the sink. Flink add it when sink upsert key not match the query's changelog key, or output has non-deterministic columns, to reorder the changelog.

## Why it hurts

Materializer hold an in-memory row list per key to fix ordering before sink. Extra state, extra cost, grow with cardinality. Can also mask a real correctness bug (wrong deletes) instead of fixing root cause.

## How to fix

- **PK mismatch** -> align sink `PRIMARY KEY` with query upsert key (GROUP BY / dedup key). Materializer disappear. See [align-sink-pk](../sql/align-sink-pk.md).
- **join dropped the upsert key** -> fix the join so it preserves an upsert key matching the sink (full PK in join condition; outer-join key on the non-null side). Common cause in join pipelines. See [preserve-upsert-key-in-join](../sql/preserve-upsert-key-in-join.md).
- **non-deterministic column in output** (CURRENT_TIMESTAMP, NOW, UUID) -> drop it from sink columns; it breaks the changelog key.
- **order truly guaranteed already** -> disable: `table.exec.sink.upsert-materialize = none`. Only when you know upstream order safe, else risk wrong result.

On Flink 2.3+ an upsert-key vs sink-PK mismatch no longer silently adds this operator - it fails planning until you fix the keys or declare a clause. See [sink-on-conflict](../sql/sink-on-conflict.md).

## Example

Disable only when ordering is already guaranteed and PK aligned:

```sql
SET 'table.exec.sink.upsert-materialize' = 'none';
```

Better - fix the root cause by aligning sink PK with GROUP BY key. See [align-sink-pk](../sql/align-sink-pk.md).
