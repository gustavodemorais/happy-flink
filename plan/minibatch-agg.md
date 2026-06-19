# Mini-batch aggregation

| Field | Value |
|-------|-------|
| **ID** | `minibatch-agg` |
| **Severity** | medium |
| **Requires** | plan |
| **Operators** | GroupAggregate, Join |
| **Match** | Stateful agg / join in plan, mini-batch not enabled, high state access load |

## Problem

Aggregation / join access state once per record. Every input row = a read + write to state backend. With RocksDB this is per-record disk hit.

## Why it hurts

Per-record state access dominate cost, especially RocksDB. High CPU, high latency, backpressure under load. State backend overhead scale with record rate.

## How to fix

Enable mini-batch. Flink buffer records a short window, fold them, hit state once per batch per key. Trade tiny latency for big throughput.

- `table.exec.mini-batch.enabled = true`
- `table.exec.mini-batch.allow-latency = 5 s` (added latency budget)
- `table.exec.mini-batch.size = 5000` (max rows per batch)
- prereq for [two-phase-agg](two-phase-agg.md) and [split-distinct-agg](split-distinct-agg.md)
- for joins: `table.exec.stream.join.mini-batch-enabled = true`

## Example

Enable mini-batch on a stateful aggregation:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';

SELECT user_id, COUNT(*)
FROM clicks
GROUP BY user_id;
```
