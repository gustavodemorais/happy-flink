# Dedup via ROW_NUMBER

| Field | Value |
|-------|-------|
| **ID** | `dedup-row-number` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | GroupAggregate, Deduplicate |
| **Match** | Dedup written as GROUP BY with FIRST_VALUE / LAST_VALUE, or DISTINCT |

## Problem

Dedup done with `GROUP BY key` + `FIRST_VALUE` / `LAST_VALUE`, or `DISTINCT`. Aggregation operator keep full per-key state of all columns.

## Why it hurts

Aggregation path store more than needed - state for every column fed to FIRST/LAST_VALUE. Grows with cardinality. Not recognized as the cheap Deduplicate operator.

## How to fix

Use `ROW_NUMBER() OVER (PARTITION BY key ORDER BY time_attr)` and keep `rn = 1`. Flink pick Deduplicate operator. State = one row per key, not full agg state.

- `ORDER BY time ASC` -> keep first row per key
- `ORDER BY time DESC` -> keep last row per key
- order column should be a time attribute for the cheap path

## Example

Before - FIRST_VALUE dedup, heavy agg state:

```sql
SELECT user_id, FIRST_VALUE(name), FIRST_VALUE(email)
FROM users
GROUP BY user_id;
```

After - ROW_NUMBER, Deduplicate operator, one row per key:

```sql
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time ASC) AS rn
  FROM users
)
WHERE rn = 1;
```
