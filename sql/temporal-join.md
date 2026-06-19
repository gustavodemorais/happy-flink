# Temporal join over regular join

| Field | Value |
|-------|-------|
| **ID** | `temporal-join` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | Join |
| **Match** | Regular join against a versioned / slowly-changing dimension table |

## Problem

Stream joined to a dimension / versioned table with plain regular join. Both sides kept in state forever. Dim table all versions stay.

## Why it hurts

Regular join state unbounded both sides. Switch to temporal cut join state dramatically - from huge to small. Dim table grow forever in state when only latest version per key needed.

## How to fix

Use event-time temporal join. `FOR SYSTEM_TIME AS OF`. Right side keep only the version matching left row event time. Old versions drop. Needs:

- versioned table (primary key + event-time watermark) on right side
- watermark on both sides - if input has no `WATERMARK`, fix that first. See [require-watermark](require-watermark.md).
- PK of dim in join condition

See also [lookup-join](lookup-join.md) when dim lives in external system, [join-unbounded-state](../plan/join-unbounded-state.md) for the general bound.

## Example

Before - regular join, dim state unbounded:

```sql
SELECT o.id, o.amount * r.rate AS amount_usd
FROM orders o
JOIN rates r ON o.currency = r.currency;
```

After - temporal join, right side keeps only matching version:

```sql
SELECT o.id, o.amount * r.rate AS amount_usd
FROM orders o
JOIN rates FOR SYSTEM_TIME AS OF o.order_time AS r
  ON o.currency = r.currency;
```
