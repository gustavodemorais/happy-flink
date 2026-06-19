# FILTER over CASE WHEN for distinct

| Field | Value |
|-------|-------|
| **ID** | `filter-distinct` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | GroupAggregate |
| **Match** | COUNT(DISTINCT ...) wrapped in CASE WHEN, multiple such in one query |

## Problem

`COUNT(DISTINCT CASE WHEN ... END)` make separate distinct state per branch. Same distinct column, many CASE = many state copies. No sharing.

## Why it hurts

Each `CASE WHEN` distinct keep own state map of seen values. Same `user_id` tracked many times over. More state, more CPU. Roughly 2x slower vs shared state.

## How to fix

Rewrite with SQL-standard `FILTER (WHERE ...)`. Flink share one distinct state across aggs on same column. Less state, faster.

- spot `COUNT(DISTINCT CASE WHEN cond THEN col END)` -> `COUNT(DISTINCT col) FILTER (WHERE cond)`
- works only when distinct column same across the aggs

## Example

Before - CASE WHEN, state per branch:

```sql
SELECT
  day,
  COUNT(DISTINCT CASE WHEN country = 'US' THEN user_id END) AS us_users,
  COUNT(DISTINCT CASE WHEN country = 'DE' THEN user_id END) AS de_users
FROM clicks
GROUP BY day;
```

After - FILTER, shared distinct state on user_id:

```sql
SELECT
  day,
  COUNT(DISTINCT user_id) FILTER (WHERE country = 'US') AS us_users,
  COUNT(DISTINCT user_id) FILTER (WHERE country = 'DE') AS de_users
FROM clicks
GROUP BY day;
```
