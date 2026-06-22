# Non-deterministic function in update pipeline

| Field | Value |
|-------|-------|
| **ID** | `nondeterministic-in-update-pipeline` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | Join, GroupAggregate, Deduplicate, SinkUpsertMaterializer, LookupJoin |
| **Match** | `NOW()`, `CURRENT_TIMESTAMP`, `RAND()`, `UUID()`, non-deterministic UDF, or a lookup join feeding an updating (upsert / retract) pipeline |

## Problem

Non-deterministic call - `NOW()`, `CURRENT_TIMESTAMP`, `RAND()`, `UUID()`, a non-deterministic UDF, or a lookup join against a changing dimension - in a pipeline that emits updates (upsert or retract). Each time a row recompute, the value differ. Downstream key by it, dedup on it, or sink delete by it - and the delete no longer match the earlier insert.

Append-only pipeline = fine. Value computed once, row never updated. Rule fire only when pipeline is updating.

## Why it hurts

Update pipeline send delete then insert per change. Delete carry the OLD row, must match what was emitted before. Non-deterministic column makes the recomputed value differ from the stored one - delete miss, stale row stay, results silently wrong. No crash, no log. Worst kind: looks correct.

Planner has a check - `StreamNonDeterministicUpdatePlanVisitor` - but it is OFF by default. Config `table.optimizer.non-deterministic-update.strategy` default `IGNORE` = do nothing, risk of wrong result stays. Out of box Flink ship the bug silently.

## How to fix

Two moves. Do both.

1. **Make the column deterministic.** Don't compute time / random per row in an updating query. Stamp it at the source (event-time column in the row, watermark) or push it past the updating part. Replace `NOW()` with an ingestion / event-time field. Drop `RAND()` / `UUID()` from keyed or sinked columns.
2. **Turn the planner check ON.** Set `table.optimizer.non-deterministic-update.strategy = TRY_RESOLVE`. Planner then detect the NDU, auto-resolve where it can (e.g. materialize a lookup join), and raise an error when it cannot - so the bug fail loud at plan time instead of corrupting data at runtime.

```sql
SET 'table.optimizer.non-deterministic-update.strategy' = 'TRY_RESOLVE';
```

`IGNORE` (default) = no check. `TRY_RESOLVE` = enforced.

Lookup join special case: dimension row can change between lookups, so a lookup join in an updating pipeline is non-deterministic too. `TRY_RESOLVE` materializes the lookup result to pin it.

**Best effort, not a guarantee.** The check reads what the engine can see - built-in function determinism, declared traits. UDFs and PTFs are a black box: planner trust the declared `isDeterministic` / determinism traits, can't inspect the body. Lie in the declaration (or a UDF non-deterministic inside but marked deterministic) and the check pass while results stay wrong. `TRY_RESOLVE` on is necessary, not sufficient. With custom functions in an update pipeline, still watch the output yourself.

See [preserve-upsert-key-in-join](preserve-upsert-key-in-join.md) and [sink-upsert-materializer](../plan/sink-upsert-materializer.md) - same family of update-correctness bug.

## Example

Before - `NOW()` in an upsert pipeline, recomputed every update:

```sql
CREATE TABLE clicks (
  user_id INT NOT NULL,
  url STRING
);

CREATE TABLE user_state (
  user_id INT NOT NULL,
  last_url STRING,
  seen_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (user_id) NOT ENFORCED
);

INSERT INTO user_state
SELECT user_id, LAST_VALUE(url), NOW()
FROM clicks
GROUP BY user_id;
-- NOW() differs each update -> sink delete keyed/compared on a moving value -> wrong results, silent
```

After - deterministic event-time column, plus planner check on:

```sql
SET 'table.optimizer.non-deterministic-update.strategy' = 'TRY_RESOLVE';

CREATE TABLE clicks (
  user_id INT NOT NULL,
  url STRING,
  event_time TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

CREATE TABLE user_state (
  user_id INT NOT NULL,
  last_url STRING,
  seen_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (user_id) NOT ENFORCED
);

INSERT INTO user_state
SELECT user_id, LAST_VALUE(url), MAX(event_time)
FROM clicks
GROUP BY user_id;
-- event_time stamped at source, stable across recompute; TRY_RESOLVE would fail plan if any NDU remained
```
