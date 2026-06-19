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

## Plan rules

Need EXPLAIN output or a compiled plan.

| Rule | Severity | Requires | Doc |
|------|----------|----------|-----|
| Join unbounded state | high | plan | [open](plan/join-unbounded-state.md) |
| Multi-join unbounded state | high | plan | [open](plan/multi-join.md) |
| Stateful ops need TTL | high | plan | [open](plan/state-ttl.md) |

## Runtime rules

Richest. Need a live job with per-node metrics. (Stubs for now - fill later.)

| Rule | Severity | Requires | Doc |
|------|----------|----------|-----|
| Data skew | high | runtime | [open](runtime/data-skew.md) |
| Backpressure | high | runtime | [open](runtime/backpressure.md) |
