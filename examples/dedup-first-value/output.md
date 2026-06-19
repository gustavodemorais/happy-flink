# Review: dedup users via FIRST_VALUE

Query dedups users to one row per `user_id` using `GROUP BY` + `FIRST_VALUE`. Works, but lands on a heavy `GroupAggregate` with no TTL - state grows forever per user.

## Summary

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | `GroupAggregate` state unbounded, `ttl=0 ms` | high | [state-ttl](plan/state-ttl.md) | [#1](#1-no-ttl-on-groupaggregate) |
| 2 | Dedup via `FIRST_VALUE` not cheap `Deduplicate` operator | medium | [dedup-row-number](sql/dedup-row-number.md) | [#2](#2-dedup-should-use-row_number) |
| 3 | Cheap `Deduplicate` path needs time attribute - no `WATERMARK` defined | high | [require-watermark](sql/require-watermark.md) | [#3](#3-no-time-attribute-for-deduplicate) |

## Paths

Problems chain: pick dedup operator (#2) -> that operator decides what state bound applies (#1 TTL, #3 time attribute). Two real choices, split by dedup semantics.

### Path A - ROW_NUMBER on proctime + TTL (recommended)

Matches current semantics exactly. `FIRST_VALUE` with no `ORDER BY` already keeps first *arriving* row, not earliest `update_time`. Proctime `ROW_NUMBER` does the same, on the cheap `Deduplicate` operator. No watermark needed.

1. Rewrite to `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY proc ASC) WHERE rn = 1` -> Deduplicate operator, one row per key (fixes #2)
2. Add `proc AS PROCTIME()` to source so order column is a time attribute (fixes #3)
3. Set `table.exec.state.ttl` so dead-key state drops (fixes #1 - Deduplicate is still stateful)

```sql
SET 'table.exec.state.ttl' = '36 h';

CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  email STRING,
  update_time TIMESTAMP(3),
  proc AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'users'
);

CREATE TABLE deduped_users (
  user_id BIGINT,
  name STRING,
  email STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'deduped_users'
);

INSERT INTO deduped_users
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY proc ASC) AS rn
  FROM users
)
WHERE rn = 1;
```

### Path B - ROW_NUMBER on event time + watermark + TTL

Use when "first value" must mean earliest `update_time`, not first arrival. Needs `update_time` as a rowtime attribute = `WATERMARK`. Without it, `ORDER BY update_time` (plain column) compiles to a heavier `Rank`, not `Deduplicate`.

1. Add `WATERMARK FOR update_time` so `update_time` is a time attribute (fixes #3)
2. `ROW_NUMBER ... ORDER BY update_time ASC`, keep `rn = 1` -> Deduplicate (fixes #2)
3. Set TTL (fixes #1)

```sql
SET 'table.exec.state.ttl' = '36 h';

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

CREATE TABLE deduped_users (
  user_id BIGINT,
  name STRING,
  email STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'deduped_users'
);

INSERT INTO deduped_users
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time ASC) AS rn
  FROM users
)
WHERE rn = 1;
```

## Details

### #1 No TTL on GroupAggregate

Plan node id 7, `stream-exec-group-aggregate`:

```json
"state" : [ { "index" : 0, "ttl" : "0 ms", "name" : "groupAggregateState" } ]
```

`ttl = 0 ms` = state kept forever. One accumulator per `user_id`, never dropped. `user_id` is high cardinality - state and checkpoint grow without limit, slow restore, OOM risk. Dead users (gone forever) still hold state.

Fix: set `table.exec.state.ttl` (e.g. `'36 h'`). Pick TTL longer than the longest gap you expect between events for the same user. Note: TTL applies to the `Deduplicate` operator in both paths too - it is stateful, so set TTL regardless of which dedup operator you pick.

### #2 Dedup should use ROW_NUMBER

`GROUP BY user_id` + `FIRST_VALUE(name)`, `FIRST_VALUE(email)` compiles to `GroupAggregate` (plan node 7). That path holds full per-key aggregate state for every column fed to `FIRST_VALUE`, and is not recognized as the dedicated cheap `Deduplicate` operator.

`ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY <time_attr>)` with `rn = 1` -> Flink picks `Deduplicate`. State = one row per key, can drop later rows for keys already seen. Lighter, same result.

### #3 No time attribute for Deduplicate

`Deduplicate` (cheap path) needs the `ORDER BY` column to be a *time attribute* - proctime or rowtime. Source `users` has `update_time TIMESTAMP(3)` but no `WATERMARK`, so `update_time` is a plain column, not a time attribute. `ORDER BY update_time` on a plain column compiles to a heavier `Rank` operator, not `Deduplicate`.

Two fixes, per path:
- Path A: add `proc AS PROCTIME()`, order by it. First-arrival semantics - matches what the original `FIRST_VALUE` (no `ORDER BY`) does today.
- Path B: add `WATERMARK FOR update_time` to make it rowtime, order by it ASC. Earliest-event-time semantics. Lateness interval is a tuning knob - late rows past the watermark may be dropped.

Recommend Path A - it preserves current behavior, needs no watermark, and the rewrite plus TTL fixes all three.
