---
title: Lookup join for external dimension
id: lookup-join
impact: HIGH
impactDescription: "The entire external dimension table is copied into Flink state and kept forever."
requires: sql
operators: [Join, LookupJoin]
match: "A regular join enriches a stream from an external table (JDBC, HBase, etc.)."
tags: [join, lookup-join, dimension-table, state-size]
---

# Lookup join for external dimension

## Problem

A stream is enriched from an external dimension (a database or key-value store) with a regular join. Flink pulls the whole external table into state and keeps it there.

## Why it hurts

The entire dimension table is held in Flink state, grows with the table size, and is kept forever. This is wasteful when the dimension already lives in an external system that can be queried directly.

## How to fix

Use a processing-time lookup join with `FOR SYSTEM_TIME AS OF proctime`. Flink keeps no state for the dimension; it queries the external table for each row. Add a lookup cache to reduce external calls.

- The right side must be a lookup-capable connector (JDBC, HBase, etc.).
- Use `FOR SYSTEM_TIME AS OF` with a processing-time attribute.
- Configure a lookup cache and TTL to avoid hitting the external system for every row.
- If synchronous lookups are latency-bound, use the async lookup hint `/*+ LOOKUP('async'='true','output-mode'='allow_unordered') */`.

See [temporal-join](temporal-join.md) when the dimension is a Flink stream, and [join-unbounded-state](../plan/join-unbounded-state.md) for bounding join state in general.

## Example

Before: a regular join keeps the whole dimension in state.

```sql
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.cust_id = c.id;
```

After: a lookup join keeps no Flink state, and the dimension stays external.

```sql
SELECT o.id, c.name
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c
  ON o.cust_id = c.id;
```
