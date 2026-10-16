---
title: Per-operator state TTL
id: per-operator-state-ttl
impact: MEDIUM
impactDescription: "A single global TTL either wastes state on short-lived inputs or drops state that long-lived inputs still need."
requires: plan
operators: [Join, GroupAggregate]
match: "Asymmetric stateful operators share one global TTL, while join sides need different retention."
flink-version: "1.19+"
tags: [state-ttl, hints, join, state-size]
---

# Per-operator state TTL

## Problem

The job sets one global `table.exec.state.ttl`, but different operators (or the two sides of a join) need different retention. A single value is wrong for all of them.

## Why it hurts

A global TTL that is too long for the short-lived side wastes state. One that is too short for the long-lived side drops state that is still needed and produces wrong results. A per-side TTL can cut total join state dramatically.

## How to fix

Use the `STATE_TTL` hint (FLIP-373, Flink 1.19+) to set retention per operator or per join side. Give each side its own TTL.

- If the left side is short-lived (orders) and the right side is long-lived (customers), give the left side a short TTL and the right side a long TTL.
- The hint key is the alias or side name used in the query.
- Check the plan to confirm the hint was applied to the intended operator.
- For general TTL background, see [state-ttl](state-ttl.md).

## Example

A join with asymmetric sides, using a per-side TTL via the hint:

```sql
SELECT /*+ STATE_TTL('o' = '1 d', 'c' = '20 d') */
  o.id, c.name
FROM orders o
JOIN customers c ON o.cust_id = c.id;
```
