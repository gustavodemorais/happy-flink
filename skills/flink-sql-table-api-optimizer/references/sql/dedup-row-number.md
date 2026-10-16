---
title: Dedup via ROW_NUMBER
id: dedup-row-number
impact: MEDIUM
impactDescription: "Deduplicating with GROUP BY or DISTINCT keeps full per-key aggregation state instead of one row per key."
requires: sql
operators: [GroupAggregate, Deduplicate]
match: "A deduplication written as GROUP BY with FIRST_VALUE / LAST_VALUE, or as DISTINCT."
tags: [deduplication, row-number, group-by, state]
---

# Dedup via ROW_NUMBER

## Problem

Deduplication is written as `GROUP BY key` with `FIRST_VALUE` / `LAST_VALUE`, or as `DISTINCT`. The aggregation operator then keeps full per-key state for all columns.

## Why it hurts

The aggregation path stores more than needed: it keeps state for every column passed to FIRST_VALUE / LAST_VALUE, and that state grows with key cardinality. The query is also not recognized as the cheaper Deduplicate operator.

## How to fix

Use `ROW_NUMBER() OVER (PARTITION BY key ORDER BY time_attr)` and keep `rn = 1`. Flink then picks the Deduplicate operator, whose state is one row per key rather than full aggregation state.

- `ORDER BY time ASC` keeps the first row per key.
- `ORDER BY time DESC` keeps the last row per key.
- The order column should be a time attribute to get the cheap path.

## Example

Before: FIRST_VALUE deduplication with heavy aggregation state:

```sql
SELECT user_id, FIRST_VALUE(name), FIRST_VALUE(email)
FROM users
GROUP BY user_id;
```

After: ROW_NUMBER, using the Deduplicate operator with one row per key:

```sql
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time ASC) AS rn
  FROM users
)
WHERE rn = 1;
```
