---
title: Multi-join unbounded state
id: multi-join
impact: HIGH
impactDescription: "State from every join in the chain adds up, and a single unbounded join makes the whole pipeline's state grow without limit."
requires: plan
operators: [Join]
match: "Multiple Join nodes chained in the plan."
flink-version: "2.1+"
tags: [join, multi-join, state-size, join-order]
---

# Multi-join unbounded state

## Problem

Many tables are joined in one query, and the plan shows a chain of `Join` nodes. Each binary join keeps its own state, so state piles up and multiplies down the chain.

## Why it hurts

N tables produce N-1 join nodes (or more). Each one holds state for both of its inputs. Intermediate results feed the next join and are also kept in state. Total state is the sum of all join state and can grow very quickly. A single unbounded join in the chain affects the whole pipeline.

## How to fix

- Bound every join in the chain (interval, temporal, or lookup). See [join-unbounded-state](join-unbounded-state.md).
- Use the MultiJoin operator if your Flink version supports it (2.1+). It is a single operator with shared state and less duplication than a join chain. Check the plan for a single multi-way join node instead of a chain. Bounding joins works on any version.
- Order joins so that the smallest or most-filtered side is joined first, to shrink intermediate state.
- Push filters before joins so that each join sees fewer rows. See [filter-pushdown](../sql/filter-pushdown.md).
- Set a state TTL on join state. See [state-ttl](state-ttl.md).

Check `EXPLAIN` and count the `Join` nodes. Many stacked joins means this rule applies.

## Example

Before: three regular joins, with state stacking up.

```sql
SELECT *
FROM a
JOIN b ON a.id = b.aid
JOIN c ON b.id = c.bid
JOIN d ON c.id = d.cid;
```

After: each join is bounded by time, so old rows are dropped from state.

```sql
SELECT *
FROM a
JOIN b ON a.id = b.aid AND b.ts BETWEEN a.ts - INTERVAL '1' HOUR AND a.ts
JOIN c ON b.id = c.bid AND c.ts BETWEEN b.ts - INTERVAL '1' HOUR AND b.ts
JOIN d ON c.id = d.cid AND d.ts BETWEEN c.ts - INTERVAL '1' HOUR AND c.ts;
```
