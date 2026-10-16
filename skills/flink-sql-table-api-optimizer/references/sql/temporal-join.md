---
title: Temporal join over regular join
id: temporal-join
impact: HIGH
impactDescription: "Both join sides, including every version of the dimension table, stay in state forever."
requires: sql
operators: [Join]
match: "A regular join against a versioned or slowly changing dimension table."
tags: [join, temporal-join, versioned-table, state-size, event-time]
---

# Temporal join over regular join

## Problem

A stream is joined to a dimension or versioned table with a plain regular join. Both sides are kept in state forever, including all versions of the dimension table.

## Why it hurts

Regular join state is unbounded on both sides. Switching to a temporal join reduces join state dramatically, from very large to small. With a regular join, the dimension table grows in state forever, even though only the latest version per key is needed.

## How to fix

Use an event-time temporal join with `FOR SYSTEM_TIME AS OF`. The right side keeps only the version that matches the event time of the left row, and older versions are dropped. This requires:

- A versioned table (primary key plus event-time watermark) on the right side.
- A watermark on both sides. If an input has no `WATERMARK`, fix that first; see [require-watermark](require-watermark.md).
- The dimension's primary key in the join condition.

See also [lookup-join](lookup-join.md) when the dimension lives in an external system, and [join-unbounded-state](../plan/join-unbounded-state.md) for bounding join state in general.

## Example

Before: a regular join keeps unbounded dimension state.

```sql
SELECT o.id, o.amount * r.rate AS amount_usd
FROM orders o
JOIN rates r ON o.currency = r.currency;
```

After: a temporal join keeps only the matching version on the right side.

```sql
SELECT o.id, o.amount * r.rate AS amount_usd
FROM orders o
JOIN rates FOR SYSTEM_TIME AS OF o.order_time AS r
  ON o.currency = r.currency;
```
