# Multi-join unbounded state

| Field | Value |
|-------|-------|
| **ID** | `multi-join-unbounded-state` |
| **Severity** | high |
| **Requires** | plan |
| **Operators** | Join |
| **Match** | Multiple Join nodes chained in plan |

## Problem

Many tables joined in one query. Plan show chain of `Join` nodes. Each binary join keep own state. State pile up, multiply down chain.

## Why it hurts

N tables = N-1 join nodes (or more). Each hold state both inputs. Intermediate result feed next join, also kept in state. Total state = sum of all join state, can blow up fast. One unbounded join in chain poison whole pipeline.

## How to fix

- bound every join in chain (interval / temporal / lookup) - see [join-unbounded-state](join-unbounded-state.md)
- use MultiJoin operator if Flink version support it - one operator, shared state, less duplication than join chain. Check plan for single multi-way join node vs chain.
- order joins so smallest / most-filtered side join first - shrink intermediate state
- push filters before joins so each join see fewer rows - see [filter-pushdown](../sql/filter-pushdown.md)
- set state TTL on join state - see [state-ttl](state-ttl.md)

Check `EXPLAIN` - count `Join` nodes. Many stacked = this rule.

## Example

Before - three regular joins, state stack up:

```sql
SELECT *
FROM a
JOIN b ON a.id = b.aid
JOIN c ON b.id = c.bid
JOIN d ON c.id = d.cid;
```

After - bound each join with time, state drop old rows:

```sql
SELECT *
FROM a
JOIN b ON a.id = b.aid AND b.ts BETWEEN a.ts - INTERVAL '1' HOUR AND a.ts
JOIN c ON b.id = c.bid AND c.ts BETWEEN b.ts - INTERVAL '1' HOUR AND b.ts
JOIN d ON c.id = d.cid AND d.ts BETWEEN c.ts - INTERVAL '1' HOUR AND c.ts;
```
