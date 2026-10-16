---
title: Mini-batch aggregation
id: minibatch-agg
impact: MEDIUM
impactDescription: "Per-record state access drives up CPU and latency and causes backpressure under load."
requires: plan
operators: [GroupAggregate, Join]
match: "A stateful aggregation or join in the plan, mini-batch not enabled, and a high state access load."
tags: [mini-batch, aggregation, join, throughput, rocksdb]
---

# Mini-batch aggregation

## Problem

Aggregations and joins access state once per record. Every input row results in a read and a write to the state backend. With RocksDB, this means a disk access per record.

## Why it hurts

Per-record state access dominates the cost, especially with RocksDB. It leads to high CPU usage, high latency, and backpressure under load. State backend overhead scales with the record rate.

## How to fix

Enable mini-batch. Flink buffers records for a short interval, folds them, and accesses state once per batch per key. This trades a small amount of latency for much higher throughput.

- `table.exec.mini-batch.enabled = true`
- `table.exec.mini-batch.allow-latency = 5 s` (added latency budget)
- `table.exec.mini-batch.size = 5000` (maximum rows per batch)
- Mini-batch is a prerequisite for [two-phase-agg](two-phase-agg.md) and [split-distinct-agg](split-distinct-agg.md).
- For joins: `table.exec.stream.join.mini-batch-enabled = true`

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
