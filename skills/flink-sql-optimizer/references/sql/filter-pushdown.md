---
title: Filter pushdown
id: filter-pushdown
impact: MEDIUM
impactDescription: "Without pushdown the source reads every row, wasting IO, network, and CPU on data that is later discarded."
requires: sql
operators: [TableSourceScan]
match: "A WHERE filter is present and the source supports filter pushdown."
tags: [filter-pushdown, source, predicate, io]
---

# Filter pushdown

## Problem

The filter sits above the source scan. The source reads all rows, and Flink then discards most of them. This wastes IO, network, and CPU.

## Why it hurts

The source pulls the whole table, which is a large read. The filter runs late, after the data has already crossed the network. More rows means more work for every downstream operator, and state grows larger if the filter feeds a stateful operator.

## How to fix

Push the filter into the source so that it skips rows at read time. Flink does this automatically when the source supports `SupportsFilterPushDown` (Kafka with a format predicate, JDBC, filesystem with partition pruning). You can help it:

- Write the filter directly on a source column, not on a computed column.
- Filter on the partition key when the source is partitioned.
- Avoid wrapping the column in a function in the filter (`WHERE CAST(id AS STRING) = '5'` blocks pushdown; use `WHERE id = 5`).
- Check `EXPLAIN`: the filter should appear inside `TableSourceScan`, not as a separate `Calc` above it.

## Example

Before: a function on the column prevents pushdown:

```sql
SELECT * FROM orders
WHERE CAST(order_id AS STRING) = '1001';
```

After: the filter is on the raw column, so the source pushes it down:

```sql
SELECT * FROM orders
WHERE order_id = 1001;
```
