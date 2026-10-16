---
title: Projection pushdown
id: projection-pushdown
impact: MEDIUM
impactDescription: "Unused columns are read, deserialized, and shipped, wasting I/O, network, CPU, and state."
requires: sql
operators: [TableSourceScan]
match: "SELECT * or unused columns are read from a source that supports projection pushdown."
tags: [projection-pushdown, source, io, select-star]
---

# Projection pushdown

## Problem

The query reads columns it never uses, either through `SELECT *` or by selecting a wide set of columns and using only a few. The source ships all of those columns anyway.

## Why it hurts

Every extra column means more bytes read, deserialized, and transferred. Wide rows fill state faster in stateful operators. Network and CPU are spent on data that is thrown away.

## How to fix

Select only the columns you need. The source then pushes the projection down and reads fewer columns (`SupportsProjectionPushDown`). This works with columnar formats (Parquet, ORC) and many connectors.

- Avoid `SELECT *` in production pipelines.
- List the exact columns.
- Drop columns that are not used downstream.
- Check `EXPLAIN`: `TableSourceScan` should show a short `project=[...]` list, not all fields.

## Example

Before: all columns are read, but only two are used.

```sql
SELECT * FROM orders;
-- downstream only touch order_id, amount
```

After: only two columns are read.

```sql
SELECT order_id, amount FROM orders;
```
