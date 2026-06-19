# Join unbounded state

| Field | Value |
|-------|-------|
| **ID** | `join-unbounded-state` |
| **Severity** | high |
| **Requires** | plan |
| **Operators** | Join |
| **Match** | Regular Join in plan, no time bound, no state TTL |

## Problem

Regular join (not interval, not temporal, not lookup) keep both sides in state forever. Every row from left wait for match from right, kept always. State only grow.

## Why it hurts

State never drop. Grows with input size, no ceiling. Checkpoint slow, big. Restore slow. Eventually disk full or job die OOM. No time bound = no cleanup point.

## How to fix

Pick one:

- **Interval join** - add time bound between both sides. State drop rows past window. `ON ... AND a.ts BETWEEN b.ts - INTERVAL '1' HOUR AND b.ts`
- **Temporal join** - join against versioned table `FOR SYSTEM_TIME AS OF`. Right side keep only latest version per key.
- **Lookup join** - right side is external table, no Flink state. `FOR SYSTEM_TIME AS OF a.proctime`
- **State TTL** - if regular join must stay, set `table.exec.state.ttl`. State drop after idle time. Risk: drop too early = miss late match. See [state-ttl](state-ttl.md).

Interval and temporal join need a watermark on the time attribute. If the input has no `WATERMARK`, fix that first. See [require-watermark](../sql/require-watermark.md).

Check `EXPLAIN` - `Join(joinType=...)` with no time attribute = regular join, unbounded.

## Example

Before - regular join, unbounded state:

```sql
SELECT o.id, s.name
FROM orders o
JOIN shipments s ON o.id = s.order_id;
```

After - interval join, state bounded to 1 hour:

```sql
SELECT o.id, s.name
FROM orders o
JOIN shipments s
  ON o.id = s.order_id
  AND s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR;
```
