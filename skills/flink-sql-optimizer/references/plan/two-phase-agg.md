---
title: Two-phase (local-global) aggregation
id: two-phase-agg
impact: MEDIUM
impactDescription: "A skewed key overloads one subtask, which slows the whole job and causes backpressure."
requires: plan
operators: [GroupAggregate]
match: "A single-phase GroupAggregate in the plan with skewed grouping keys."
tags: [aggregation, data-skew, local-global, hotspot]
---

# Two-phase (local-global) aggregation

## Problem

The aggregation runs in a single phase. All rows for a key are shuffled to one subtask and aggregated there. A skewed key creates a hot subtask.

## Why it hurts

A hot key overloads one subtask. That subtask slows down the whole job and causes backpressure upstream. Increasing parallelism does not help, because the skewed key still goes to a single subtask.

## How to fix

Enable two-phase aggregation. Each subtask performs a local pre-aggregation before the shuffle, and a global aggregation then combines the partial results. This reduces the data volume crossing the shuffle and spreads the load.

- `table.optimizer.agg-phase-strategy = TWO_PHASE`
- Requires mini-batch to be enabled (see [minibatch-agg](minibatch-agg.md)).
- The aggregate function must be mergeable. Built-in `SUM`, `COUNT`, and `AVG` are fine; some UDAFs are not.
- Check the plan afterwards: you want `LocalGroupAggregate` + `GlobalGroupAggregate`, not a single `GroupAggregate`.
- For `COUNT(DISTINCT)` skew, also enable [split-distinct-agg](split-distinct-agg.md).

## Example

The plan shows a single `GroupAggregate` on a skewed key. Enable two-phase aggregation:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

SELECT day, COUNT(*)
FROM clicks
GROUP BY day;
```
