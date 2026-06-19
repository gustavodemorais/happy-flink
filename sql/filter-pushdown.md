# Filter pushdown

| Field | Value |
|-------|-------|
| **ID** | `filter-pushdown` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | TableSourceScan |
| **Match** | WHERE filter present, source support filter pushdown |

## Problem

Filter sit above source scan. Source read all rows, then Flink throw most away. Waste IO, waste network, waste CPU.

## Why it hurts

Source pull whole table. Big read. Filter run late, after data already cross wire. More rows = more work every operator downstream. State bigger if filter feed stateful op.

## How to fix

Push filter into source. Source skip rows at read time. Flink do this auto when source support `SupportsFilterPushDown` (Kafka with format predicate, JDBC, filesystem with partition prune). Help it:

- write filter on source column direct, not on computed column
- filter on partition key when source partitioned
- avoid wrap column in function on filter side (`WHERE CAST(id AS STRING) = '5'` block pushdown; use `WHERE id = 5`)
- check `EXPLAIN` - want filter inside `TableSourceScan`, not separate `Calc` above it

## Example

Before - filter break pushdown, function on column:

```sql
SELECT * FROM orders
WHERE CAST(order_id AS STRING) = '1001';
```

After - filter on raw column, source push it down:

```sql
SELECT * FROM orders
WHERE order_id = 1001;
```
