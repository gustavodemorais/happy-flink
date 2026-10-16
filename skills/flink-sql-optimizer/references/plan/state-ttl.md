---
title: Stateful ops need TTL
id: state-ttl
impact: HIGH
impactDescription: "State for every key ever seen is kept forever, so checkpoints grow without limit and the job risks OOM."
requires: plan
operators: [Join, GroupAggregate, GlobalGroupAggregate, IncrementalGroupAggregate, WindowAggregate, Match, Rank, Deduplicate]
match: "A stateful operator in the plan with no TTL and no natural time bound."
tags: [state-ttl, state-size, checkpoint, windowing]
---

# Stateful ops need TTL

## Problem

Stateful operators hold state per key. Without a TTL or a window bound, state is kept forever for every key ever seen. New keys keep arriving, so state keeps growing.

## Why it hurts

A high-cardinality key (user ID, session ID, device ID) produces millions of state entries, none of which are ever removed. Checkpoint size grows without limit, leading to slow checkpoints, slow restores, and a risk of OOM. State for dead keys (users who left, sessions that ended) is kept forever.

## How to fix

- Set `table.exec.state.ttl` (for example `'36 h'`). Idle state is dropped after the TTL. Choose a TTL longer than the longest gap you expect between events for the same key.
- Prefer the windowed version of an operator when the semantics allow it (`WindowAggregate` instead of `GroupAggregate`). Windows clean up state when they close, so no TTL estimate is needed.
- Be aware of the trade-off. A TTL that is too short drops state that is still needed and produces wrong results (missed late matches, aggregates restarting from zero). A TTL that is too long keeps state too large. Tune it to your data.

Operators that need this: `Join`, `GroupAggregate`, `GlobalGroupAggregate`, `IncrementalGroupAggregate`, `WindowAggregate`, `Match`, `Rank`, `Deduplicate`.

Check `EXPLAIN`: if any of the nodes above is present, confirm that a TTL is set or a window is used.

## Example

Before: a group aggregation with no TTL, keeping state per user forever.

```sql
SELECT user_id, COUNT(*)
FROM clicks
GROUP BY user_id;
```

After: a TTL is set, so state for idle users is dropped.

```sql
SET 'table.exec.state.ttl' = '36 h';

SELECT user_id, COUNT(*)
FROM clicks
GROUP BY user_id;
```

Better, when the semantics allow it: a window cleans up its own state, with no TTL estimate needed.

```sql
SELECT user_id, window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(click_time), INTERVAL '1' HOUR))
GROUP BY user_id, window_start, window_end;
```
