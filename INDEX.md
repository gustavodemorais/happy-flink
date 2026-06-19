# happy-flink rule index

The full map of Flink SQL optimization rules. Pick rules by the data you have.

## How to review (read this first)

If a user hands you a Flink SQL query - optionally with its compiled plan or
runtime metrics - and asks for help:

**1. Analyze.**

- Pick the rule tables below by the data you have:
  - only SQL text -> SQL rules
  - an EXPLAIN / compiled plan -> SQL + Plan rules
  - a live job with metrics -> all three
- Open the rule files that look relevant and check the input against them.
- For every problem, note: the rule, where it shows up in the input (SQL line
  or plan node), the fix, and why it matters.
- If a fix you recommend depends on something missing from the input (for
  example, a time-based join or window needs a watermark but no `WATERMARK` is
  defined), treat that as its own problem - do not silently assume it.
- If a rule has a `Flink version` field, the fix needs that version. Surface it
  with the fix so the user knows the requirement.

**2. Write the report in this structure:**

```
# Review: <short name of the query>

One line: what the query does + the headline problem.

## Summary

A table of every problem found, highest severity first:

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | ... | high | [rule-id](path) | [#1](#1-...) |

## Paths

The ordered ways to fix things. Problems usually chain (one fix needs another
done first), so group them into paths instead of listing them flat. Mark one
recommended.

### Path A - <name> (recommended)
1. step (-> which problems it fixes)
2. step
...

### Path B - <name>
1. step
...

## Details

One section per problem from the Summary, in the same order. Each has: where it
shows up, why it hurts, the fix with corrected SQL, and the Flink reasoning so
the reader learns something.
```

Pick the recommended path by what fixes the root cause, not just what caps the
symptom. A fallback that only bounds state (e.g. TTL) is rarely the recommended
path when a semantic fix (temporal / lookup join, windowing) exists.

### Output style

Write in caveman lite: drop filler words and pleasantries, keep articles and
normal sentence structure so it stays easy to read. Keep all technical detail
exact. Keep code blocks as normal, valid SQL.

## Jump

- [SQL rules](#sql-rules) - need only SQL text
- [Plan rules](#plan-rules) - need an EXPLAIN or compiled plan
- [Runtime rules](#runtime-rules) - need a job with per-node metrics

## SQL rules

Cheapest. You have the SQL text and nothing else.

| Rule | Severity | Requires | Doc |
|------|----------|----------|-----|
| Filter pushdown | medium | sql | [open](sql/filter-pushdown.md) |
| Projection pushdown | medium | sql | [open](sql/projection-pushdown.md) |
| FILTER over CASE WHEN for distinct | medium | sql | [open](sql/filter-distinct.md) |
| Dedup via ROW_NUMBER | medium | sql | [open](sql/dedup-row-number.md) |
| Temporal join over regular join | high | sql | [open](sql/temporal-join.md) |
| Lookup join for external dimension | high | sql | [open](sql/lookup-join.md) |
| Append kafka, not upsert-kafka | medium | sql | [open](sql/append-not-upsert-kafka.md) |
| Align sink PK with upsert key | high | sql | [open](sql/align-sink-pk.md) |
| Window / time join needs watermark | high | sql | [open](sql/require-watermark.md) |

## Plan rules

Need EXPLAIN output or a compiled plan.

| Rule | Severity | Requires | Doc |
|------|----------|----------|-----|
| Join unbounded state | high | plan | [open](plan/join-unbounded-state.md) |
| Multi-join unbounded state | high | plan | [open](plan/multi-join.md) |
| Stateful ops need TTL | high | plan | [open](plan/state-ttl.md) |
| Two-phase (local-global) aggregation | medium | plan | [open](plan/two-phase-agg.md) |
| Split distinct aggregation | medium | plan | [open](plan/split-distinct-agg.md) |
| Mini-batch aggregation | medium | plan | [open](plan/minibatch-agg.md) |
| Avoid ChangelogNormalize | high | plan | [open](plan/avoid-changelog-normalize.md) |
| SinkUpsertMaterializer present | high | plan | [open](plan/sink-upsert-materializer.md) |
| Per-operator state TTL | medium | plan | [open](plan/per-operator-state-ttl.md) |

## Runtime rules

Richest. Need a live job with per-node metrics. (Stubs - templates in place, content TODO.)

| Rule | Severity | Requires | Doc |
|------|----------|----------|-----|
| Data skew | high | runtime | [open](runtime/data-skew.md) (stub) |
| Backpressure | high | runtime | [open](runtime/backpressure.md) (stub) |
| State size growth | high | runtime | [open](runtime/state-size-growth.md) (stub) |
| Watermark lag / idle partition | high | runtime | [open](runtime/watermark-lag.md) (stub) |
| Slow checkpoints | medium | runtime | [open](runtime/checkpoint-tuning.md) (stub) |
