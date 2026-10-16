---
title: Non-deterministic function in update pipeline
id: nondeterministic-in-update-pipeline
impact: HIGH
impactDescription: "Retractions stop matching earlier inserts, silently leaving stale or wrong rows in the sink."
requires: sql
operators: [Join, GroupAggregate, Deduplicate, SinkUpsertMaterializer, LookupJoin]
match: "NOW(), CURRENT_TIMESTAMP, RAND(), UUID(), a non-deterministic UDF, or a lookup join feeds an updating (upsert or retract) pipeline."
tags: [non-determinism, changelog, correctness, upsert, lookup-join]
---

# Non-deterministic function in update pipeline

## Problem

A non-deterministic call (`NOW()`, `CURRENT_TIMESTAMP`, `RAND()`, `UUID()`, a non-deterministic UDF, or a lookup join against a changing dimension) appears in a pipeline that emits updates (upsert or retract). Each time a row is recomputed, the value differs. If downstream operators key by that value, deduplicate on it, or the sink deletes by it, the delete no longer matches the earlier insert.

Append-only pipelines are not affected: the value is computed once and the row is never updated. This rule applies only when the pipeline is updating.

## Why it hurts

An updating pipeline sends a delete followed by an insert for each change. The delete carries the old row and must match what was emitted before. A non-deterministic column makes the recomputed value differ from the stored one, so the delete misses, the stale row stays, and the results are silently wrong. There is no crash and no log entry, and the output looks correct.

The planner has a check for this (`StreamNonDeterministicUpdatePlanVisitor`), but it is off by default. The config `table.optimizer.non-deterministic-update.strategy` defaults to `IGNORE`, which does nothing and leaves the risk of wrong results in place. Out of the box, Flink lets this bug through silently.

## How to fix

Apply both of the following.

1. **Make the column deterministic.** Do not compute time or random values per row in an updating query. Stamp the value at the source (an event-time column in the row, with a watermark) or move the computation past the updating part of the pipeline. Replace `NOW()` with an ingestion-time or event-time field. Remove `RAND()` and `UUID()` from keyed or sink columns.
2. **Turn the planner check on.** Set `table.optimizer.non-deterministic-update.strategy = TRY_RESOLVE`. The planner then detects the non-deterministic update (NDU), resolves it automatically where it can (for example, by materializing a lookup join), and raises an error when it cannot. The bug then fails loudly at planning time instead of corrupting data at runtime.

```sql
SET 'table.optimizer.non-deterministic-update.strategy' = 'TRY_RESOLVE';
```

`IGNORE` (the default) performs no check. `TRY_RESOLVE` enforces it.

Lookup joins are a special case: the dimension row can change between lookups, so a lookup join in an updating pipeline is also non-deterministic. `TRY_RESOLVE` materializes the lookup result to pin it.

**This is best effort, not a guarantee.** The check relies on what the engine can see: built-in function determinism and declared traits. UDFs and PTFs are black boxes; the planner trusts their declared `isDeterministic` or determinism traits and cannot inspect the function body. If the declaration is wrong (for example, a UDF that is non-deterministic internally but marked deterministic), the check passes while the results remain wrong. Enabling `TRY_RESOLVE` is necessary but not sufficient. When custom functions are used in an updating pipeline, monitor the output yourself.

See [preserve-upsert-key-in-join](preserve-upsert-key-in-join.md) and [sink-upsert-materializer](../plan/sink-upsert-materializer.md) for related update-correctness issues.

## Example

Before: `NOW()` in an upsert pipeline is recomputed on every update.

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

After: a deterministic event-time column, with the planner check enabled.

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
