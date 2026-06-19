# Projection pushdown

| Field | Value |
|-------|-------|
| **ID** | `projection-pushdown` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | TableSourceScan |
| **Match** | SELECT * or unused columns, source support projection pushdown |

## Problem

Query read columns it never use. `SELECT *` or pick wide set, then use few. Source ship all columns anyway.

## Why it hurts

Every extra column = more bytes read, deserialized, moved. Wide rows fill state faster on stateful op. Network and CPU burn on data thrown away.

## How to fix

Select only columns you need. Source push projection down, read fewer columns (`SupportsProjectionPushDown`). Works on columnar source (Parquet, ORC) and many connectors.

- never `SELECT *` in production pipeline
- list exact columns
- drop columns not used downstream
- check `EXPLAIN` - `TableSourceScan` should show short `project=[...]` list, not all fields

## Example

Before - read all columns, use two:

```sql
SELECT * FROM orders;
-- downstream only touch order_id, amount
```

After - read two columns:

```sql
SELECT order_id, amount FROM orders;
```
