# Two-phase (local-global) aggregation

| Field | Value |
|-------|-------|
| **ID** | `two-phase-agg` |
| **Severity** | medium |
| **Requires** | plan |
| **Operators** | GroupAggregate |
| **Match** | Single-phase GroupAggregate in plan, grouping keys skewed |

## Problem

Aggregation run single phase. All rows for a key shuffle to one subtask, agg there. Skewed key = hot subtask.

## Why it hurts

Hot key overload one subtask. That subtask slow whole job, backpressure upstream. Adding parallelism not help - skewed key still go one place.

## How to fix

Enable two-phase agg. Local pre-agg on each subtask before shuffle, then global agg combine. Cut data volume crossing the shuffle, spread load.

- `table.optimizer.agg-phase-strategy = TWO_PHASE`
- needs mini-batch on (see [minibatch-agg](minibatch-agg.md))
- agg function must be mergeable (built-in SUM/COUNT/AVG ok; some UDAF not)
- check plan after: want `LocalGroupAggregate` + `GlobalGroupAggregate`, not single `GroupAggregate`
- for COUNT(DISTINCT) skew also enable [split-distinct-agg](split-distinct-agg.md)

## Example

Plan shows single GroupAggregate on skewed key - enable two-phase:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

SELECT day, COUNT(*)
FROM clicks
GROUP BY day;
```
