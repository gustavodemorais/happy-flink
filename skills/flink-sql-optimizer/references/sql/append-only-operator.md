---
title: Operator needs append-only input
id: append-only-operator
impact: HIGH
impactDescription: "Planning fails and the job does not start when an append-only operator consumes an updating stream."
requires: sql
operators: [OverAggregate, Match, IntervalJoin, WindowJoin, WindowRank, WindowDeduplicate, TemporalSort]
match: "An operator that only accepts append-only input (a time-ordered OVER window, MATCH_RECOGNIZE, an interval join, a window join, rank or deduplication, or a temporal sort) is fed by an updating (upsert or retract) source."
tags: [append-only, changelog, over-window, match-recognize, interval-join, to-changelog]
---

# Operator needs append-only input

## Problem

A whole class of streaming operators accepts only append-only (insert-only) input. If any of them is fed an updating stream, such as the output of a `GROUP BY` aggregation, a keep-last deduplication, a regular or outer join, or an upsert source, planning fails with the same error:

```
<Operator> doesn't support consuming update changes which is produced by node ...
```

Operators in this class:

- An `OVER` window ordered by a time attribute (`ORDER BY rowtime` / `PROCTIME()`). `LAG` and `LEAD` run inside OVER, so they inherit this restriction.
- `MATCH_RECOGNIZE` (CEP)
- Interval join (`ON ... AND ts BETWEEN ...`)
- Window join, window `TOP-N` / rank, and window deduplication
- Temporal sort

This is a hard plan-time error, not a silent bug. However, the cause of the updates is usually an operator further upstream, not the node that fails.

## Why it hurts

These operators assume that each row arrives once, in time order, and is never retracted. An updating stream sends retractions and restatements, which have no defined meaning for these operators, so the planner rejects the plan and the job does not start.

A note on OVER: when it is ordered by a **non-time** column, it does accept updates. This is rare and usually not what is intended. The time-ordered case is the common one, and it is the one that fails.

## How to fix

1. **Run the operator on append-only data.** Apply it directly to the insert-only source, before any operator that makes the stream updating. Do not aggregate, apply a keep-last deduplication, or perform a regular join first and then run the append-only operator on top.
2. **Remove the updating upstream operator.** If an upstream operator only deduplicates, switch it to keep-first (`ROW_NUMBER ... ORDER BY time ASC`, which stays append-only; see [dedup-row-number](dedup-row-number.md) and [dedup-order-asc](dedup-order-asc.md)) instead of a `GROUP BY` / `LAST_VALUE` form that emits updates.
3. **Convert the changelog explicitly with `TO_CHANGELOG`.** When you genuinely need to run an append-only operator over a changelog (for example, `LAG` to track status transitions on an upsert stream), `TO_CHANGELOG` turns the updating stream into append-only rows that carry an explicit op-code column. The append-only operator then runs over those rows.

   Use this deliberately: you are flattening updates into rows, and the op codes become real data. Understand what each changelog event means for your query before mapping it; the result is only correct if the op mapping matches the semantics you want. This requires a Flink version with the `TO_CHANGELOG` PTF, which first became available in Flink 2.3.

See also [require-watermark](require-watermark.md): time-ordered OVER windows, interval joins, and window joins on rowtime also need a watermark.

## Example

Before: the aggregation makes the stream updating, and the OVER window on top of it fails:

```sql
-- GROUP BY emits updates; OVER over PROCTIME() can't consume them
SELECT cnt, SUM(cnt) OVER (ORDER BY PROCTIME()) AS running_total
FROM (
  SELECT a, COUNT(*) AS cnt FROM events GROUP BY a
);
-- ERROR: OverAggregate doesn't support consuming update changes
```

After (reordered): the OVER window runs on the append-only source, with no aggregation in the path:

```sql
CREATE TABLE events (
  a BIGINT,
  amount BIGINT,
  proc AS PROCTIME()
);

SELECT a, amount,
  SUM(amount) OVER (PARTITION BY a ORDER BY proc) AS running_total
FROM events;
-- events is insert-only -> OVER valid
```

After (TO_CHANGELOG): `LAG` over an upsert stream, converted to append-only rows with op codes:

```sql
-- orders is upsert (PRIMARY KEY order_id): INSERT, UPDATE_AFTER, DELETE
CREATE VIEW orders_changelog AS
SELECT order_id, op, status, ts
FROM TO_CHANGELOG(
  input => TABLE orders PARTITION BY order_id,
  op_mapping => MAP['INSERT', 'INSERT', 'UPDATE_AFTER', 'UPDATE_AFTER']);

-- now append-only; LAG can track the previous status per order
SELECT order_id, op, status,
  LAG(status) OVER (PARTITION BY order_id ORDER BY ts) AS prev_status
FROM orders_changelog;
```
