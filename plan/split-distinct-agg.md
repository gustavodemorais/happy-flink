# Split distinct aggregation

| Field | Value |
|-------|-------|
| **ID** | `split-distinct-agg` |
| **Severity** | medium |
| **Requires** | plan |
| **Operators** | GroupAggregate |
| **Match** | COUNT(DISTINCT ...) in plan with sparse / skewed grouping, hot subtask |

## Problem

`COUNT(DISTINCT)` defeat plain local-global agg. Distinct need to dedup globally, so few grouping keys hotspot one subtask.

## Why it hurts

Distinct value set for a hot key all land one subtask. Two-phase agg alone not fix - distinct can't pre-combine same way. One subtask drown.

## How to fix

Enable split distinct agg. Flink add a bucket layer: hash distinct value into buckets, agg per bucket, then combine. Spread distinct work across subtasks.

- `table.optimizer.distinct-agg.split.enabled = true`
- `table.optimizer.distinct-agg.split.bucket-num = 1024` (tune)
- not for UDAF
- pair with [two-phase-agg](two-phase-agg.md) + mini-batch
- SQL-level cleanup also: [filter-distinct](../sql/filter-distinct.md)

## Example

Plan shows COUNT(DISTINCT) hotspot - split it:

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
