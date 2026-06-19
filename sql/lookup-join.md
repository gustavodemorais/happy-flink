# Lookup join for external dimension

| Field | Value |
|-------|-------|
| **ID** | `lookup-join` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | Join, LookupJoin |
| **Match** | Regular join enriching stream from an external table (JDBC, HBase, etc.) |

## Problem

Stream enriched from external dimension (DB, key-value store) via regular join. Flink pull whole external table into state, keep it.

## Why it hurts

Whole dim table held in Flink state, grow with table size, kept forever. Wasteful when dim lives in queryable external system already.

## How to fix

Use processing-time lookup join. `FOR SYSTEM_TIME AS OF proctime`. No Flink-side state - Flink query external table per row. Add lookup cache to cut external calls.

- right side is a lookup-capable connector (JDBC, HBase, etc.)
- `FOR SYSTEM_TIME AS OF` a processing-time attribute
- configure lookup cache + TTL to avoid per-row external hit
- sync lookup latency-bound -> async lookup hint `/*+ LOOKUP('async'='true','output-mode'='allow_unordered') */`

See [temporal-join](temporal-join.md) when dim is a Flink stream, [join-unbounded-state](../plan/join-unbounded-state.md) for the general bound.

## Example

Before - regular join, whole dim in state:

```sql
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.cust_id = c.id;
```

After - lookup join, no Flink state, dim stays external:

```sql
SELECT o.id, c.name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c
  ON o.cust_id = c.id;
```
