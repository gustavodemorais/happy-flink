---
title: FILTER over CASE WHEN for distinct
id: filter-distinct
impact: MEDIUM
impactDescription: "Each CASE WHEN distinct aggregate keeps its own state, duplicating state and CPU for the same column."
requires: sql
operators: [GroupAggregate]
match: "Multiple COUNT(DISTINCT ...) aggregates in one query are wrapped in CASE WHEN."
tags: [distinct, filter-clause, aggregation, state]
---

# FILTER over CASE WHEN for distinct

## Problem

`COUNT(DISTINCT CASE WHEN ... END)` creates separate distinct state for each branch. When many CASE expressions use the same distinct column, the result is many copies of the state with no sharing.

## Why it hurts

Each `CASE WHEN` distinct aggregate keeps its own state map of seen values, so the same `user_id` is tracked many times over. This means more state and more CPU, and it is markedly slower than shared state.

## How to fix

Rewrite the aggregates with the SQL-standard `FILTER (WHERE ...)` clause. Flink then shares one distinct state across all aggregates on the same column, which reduces state and runs faster.

- Rewrite `COUNT(DISTINCT CASE WHEN cond THEN col END)` as `COUNT(DISTINCT col) FILTER (WHERE cond)`.
- This only works when the distinct column is the same across the aggregates.

## Example

Before: CASE WHEN, with state per branch:

```sql
SELECT
  day,
  COUNT(DISTINCT CASE WHEN country = 'US' THEN user_id END) AS us_users,
  COUNT(DISTINCT CASE WHEN country = 'DE' THEN user_id END) AS de_users
FROM clicks
GROUP BY day;
```

After: FILTER, with shared distinct state on `user_id`:

```sql
SELECT
  day,
  COUNT(DISTINCT user_id) FILTER (WHERE country = 'US') AS us_users,
  COUNT(DISTINCT user_id) FILTER (WHERE country = 'DE') AS de_users
FROM clicks
GROUP BY day;
```
