# Review: dedup users by FIRST_VALUE

Query dedups `users` to one row per `user_id` using `GROUP BY user_id` + `FIRST_VALUE`, writes to upsert-kafka sink. Works, but uses heavier GroupAggregate path with unbounded, never-expiring state instead of the cheap Deduplicate operator.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | GroupAggregate state has no TTL - grows forever per user_id | high | [state-ttl](plan/state-ttl.md) | [#1](#1-groupaggregate-state-no-ttl) |
| 2 | Recommended fix (ROW_NUMBER keep-first) needs a time attribute - `users` has none | high | [require-watermark](sql/require-watermark.md) | [#2](#2-no-time-attribute-for-cheap-dedup) |
| 3 | Dedup written as FIRST_VALUE agg, not ROW_NUMBER Deduplicate | medium | [dedup-row-number](sql/dedup-row-number.md) | [#3](#3-firstvalue-instead-of-row_number) |

## Paths

### Path A - switch to keep-first Deduplicate (recommended)

Fixes root cause: turn the GroupAggregate into the cheap append-only Deduplicate operator.

1. Add a time attribute to `users` (proctime computed col, or event-time + `WATERMARK` on `update_time`) -> fixes #2.
2. Rewrite dedup as `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY <time> ASC) = 1` -> fixes #3. Plan becomes `Deduplicate`, emits INSERT-only, smaller per-key state, no sink upsert-materialize.
3. Optionally `SET 'table.exec.state.ttl'` to bound Deduplicate state if you don't want every user_id kept forever -> bounds #1.

### Path B - keep FIRST_VALUE, just cap state

Symptom cap only. Leaves heavier agg state and update-producing changelog.

1. `SET 'table.exec.state.ttl' = '<window>'` -> bounds #1. Does nothing for #2/#3.

## Details

### #1 GroupAggregate state no TTL

Where: plan.json node `stream-exec-group-aggregate_1`, `"ttl" : "0 ms"`. EXPLAIN node `GroupAggregate(groupBy=[user_id], ...)`.

Why it hurts: stateful keyed op, no TTL. Flink keeps FIRST_VALUE accumulator for every `user_id` ever seen, forever. High-cardinality user space -> millions of state entries, none expire. Checkpoint grows without limit -> slow checkpoint, slow restore, OOM risk.

Fix (Path B / step 3 of A):

```sql
SET 'table.exec.state.ttl' = '36 h';
```

Pick TTL > longest gap you expect between repeats of the same `user_id`. Trade-off: too short = dropped key re-emits a duplicate; too long = state still big.

### #2 No time attribute for cheap dedup

Where: `users` DDL - `update_time TIMESTAMP(3)` is a plain column, no `WATERMARK`, no proctime.

Why it matters: keep-first dedup only compiles to the cheap append-only `Deduplicate` operator when `ORDER BY` is on a **time attribute** (rowtime with watermark, or proctime). `ORDER BY` a plain TIMESTAMP column instead produces a `Rank` operator that emits updates + retractions - not what you want. Do not assume the time attribute exists; it must be declared.

Fix - pick one.

Processing time (simplest, nondeterministic which row wins on ties):

```sql
CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  email STRING,
  update_time TIMESTAMP(3),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'users'
);
```

Event time (deterministic on `update_time`):

```sql
CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  email STRING,
  update_time TIMESTAMP(3),
  WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'users'
);
```

### #3 FIRST_VALUE instead of ROW_NUMBER

Where: SQL lines 25-27, `SELECT user_id, FIRST_VALUE(name), FIRST_VALUE(email) ... GROUP BY user_id`. Plan: `GroupAggregate`.

Why it hurts: the agg path stores per-key accumulator state for every column fed to FIRST_VALUE, and isn't recognized as the cheap Deduplicate operator. Deduplicate keeps just one row marker per key and emits INSERT-only, which also lets the upsert-kafka sink skip upsert materialization.

Fix (after #2 gives a time attr):

```sql
INSERT INTO deduped_users
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time ASC) AS rn
  FROM users
)
WHERE rn = 1;
```

`ORDER BY ... ASC` -> keep first row per `user_id`. Use `proc_time` instead of `update_time` if you took the proctime route in #2.

Note: source already plain `kafka` and sink PK matches `user_id` - good, no ChangelogNormalize and sink key aligned. No change needed there.
