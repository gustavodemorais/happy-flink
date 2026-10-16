---
title: Join unbounded state
id: join-unbounded-state
impact: HIGH
impactDescription: "Join state grows without limit until checkpoints become slow and the job runs out of disk or memory."
requires: plan
operators: [Join]
match: "A regular Join in the plan with no time bound and no state TTL."
tags: [join, state-size, interval-join, temporal-join, lookup-join]
---

# Join unbounded state

## Problem

A regular join (not an interval, temporal, or lookup join) keeps both inputs in state indefinitely. Every row from the left side waits for a match from the right side and is never removed. State only grows.

## Why it hurts

State is never dropped, so it grows with the input size and has no ceiling. Checkpoints become slow and large, and restores become slow. Eventually the disk fills up or the job fails with an OOM. Without a time bound there is no point at which state can be cleaned up.

## How to fix

Pick one of the following:

- **Interval join**: add a time bound between both sides. Rows outside the window are dropped from state. `ON ... AND a.ts BETWEEN b.ts - INTERVAL '1' HOUR AND b.ts`
- **Temporal join**: join against a versioned table with `FOR SYSTEM_TIME AS OF`. The right side keeps only the latest version per key.
- **Lookup join**: the right side is an external table, so no Flink state is kept for it. `FOR SYSTEM_TIME AS OF a.proctime`
- **State TTL**: if the regular join must stay, set `table.exec.state.ttl`. State is dropped after the idle time. The risk is that state dropped too early causes late matches to be missed. See [state-ttl](state-ttl.md).

Interval and temporal joins need a watermark on the time attribute. If the input has no `WATERMARK`, fix that first. See [require-watermark](../sql/require-watermark.md).

Check `EXPLAIN`: a `Join(joinType=...)` with no time attribute is a regular join with unbounded state.

## Example

Before: a regular join with unbounded state.

```sql
SELECT o.id, s.name
FROM orders o
JOIN shipments s ON o.id = s.order_id;
```

After: an interval join, with state bounded to 1 hour.

```sql
SELECT o.id, s.name
FROM orders o
JOIN shipments s
  ON o.id = s.order_id
  AND s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR;
```
