# happy-flink rule index

The full map of Flink SQL optimization rules. Pick rules by the data you have.

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
