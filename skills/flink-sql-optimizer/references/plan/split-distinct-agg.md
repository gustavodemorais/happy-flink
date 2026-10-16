---
title: Split distinct aggregation
id: split-distinct-agg
impact: MEDIUM
impactDescription: "A hot grouping key overloads a single subtask that has to deduplicate all distinct values on its own."
requires: plan
operators: [GroupAggregate]
match: "COUNT(DISTINCT ...) in the plan with sparse or skewed grouping keys and a hot subtask."
tags: [aggregation, distinct, data-skew, hotspot]
---

# Split distinct aggregation

## Problem

`COUNT(DISTINCT)` defeats plain local-global aggregation. Distinct values must be deduplicated globally, so a small number of grouping keys creates a hotspot on one subtask.

## Why it hurts

The full set of distinct values for a hot key lands on a single subtask. Two-phase aggregation alone does not fix this, because distinct aggregates cannot be pre-combined in the same way. One subtask is overwhelmed.

## How to fix

Enable split distinct aggregation. Flink adds a bucketing layer: it hashes the distinct values into buckets, aggregates per bucket, and then combines the results. This spreads the distinct work across subtasks.

- `table.optimizer.distinct-agg.split.enabled = true`
- `table.optimizer.distinct-agg.split.bucket-num = 1024` (tune as needed)
- Not supported for UDAFs.
- Combine with [two-phase-agg](two-phase-agg.md) and mini-batch.
- For related SQL-level cleanup, see [filter-distinct](../sql/filter-distinct.md).

## Example

The plan shows a `COUNT(DISTINCT)` hotspot. Split it:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
SET 'table.optimizer.distinct-agg.split.bucket-num' = '1024';

SELECT day, COUNT(DISTINCT user_id)
FROM clicks
GROUP BY day;
```
