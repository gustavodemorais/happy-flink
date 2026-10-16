---
name: flink-sql-optimizer
description: >
  Review and optimize Apache Flink SQL and Table API queries. Use when the user
  shares a Flink SQL query, Table API program, EXPLAIN output, or compiled plan
  JSON and asks for a review, optimization, or help with a slow job, state that
  keeps growing, or a planning error. Covers regular, temporal, and lookup
  joins; upsert keys and sink primary keys; changelog modes, ChangelogNormalize,
  and SinkUpsertMaterializer; state TTL; deduplication; aggregation tuning
  (mini-batch, two-phase, split distinct); filter and projection pushdown; and
  watermarks for windows and time-based joins. Troubleshooting a live job from
  runtime metrics (backpressure, checkpoint durations, per-operator metrics) is
  out of scope.
license: Apache-2.0
metadata:
  author: gustavodemorais
  version: "0.1.0"
---

# Flink SQL optimizer

A map of Flink SQL optimization rules, plus the protocol for reviewing a query
with them. Pick rules by the input you have.

## How to review

Use this protocol when a user hands you a Flink SQL query, optionally with its
EXPLAIN output or compiled plan, and asks for help.

**1. Analyze.**

- Choose rule tables by the input you have:
  - SQL text only: SQL rules
  - EXPLAIN output or compiled plan: SQL rules and plan rules
- Scan the `Look here if you see` column of those tables against the input. It
  is each rule's trigger: a SQL pattern or a plan node name. Open only the rule
  files whose trigger matches; do not open every rule.
- For each problem, record the rule, where it appears in the input (SQL line or
  plan node), the fix, and why it matters.
- If a fix depends on something missing from the input (for example, a window
  or time-based join needs a watermark but no `WATERMARK` is defined), report
  that as a separate problem instead of assuming it.
- If a rule's frontmatter has a `flink-version` field, the fix requires that
  version. State the requirement alongside the fix.

**2. Write the report with these sections, in order.**

**Title**: `# Review: <short name of the query>`, followed by one line stating
what the query does and the main problem.

**`## Summary`**: a table of every problem found, highest severity first.
Columns: number, problem, severity, rule (link to the rule file), and jump
(anchor link to its Details subsection). For example:

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | unbounded regular join | high | [join-unbounded-state](references/plan/join-unbounded-state.md) | [#1](#1-unbounded-regular-join) |

**`## Paths`**: the ordered ways to fix the query. Problems usually depend on
each other (one fix requires another first), so group them into paths rather
than listing them flat. Mark one path as recommended. Each path is a numbered
list of steps, each naming the problem it fixes, and ends with one fenced `sql`
block containing the final runnable query for that path. Name them
`### Path A - <name> (recommended)`, `### Path B - <name>`, and so on.

**`## Details`**: one subsection per problem from the Summary, in the same
order, with headings that match the Jump anchors. Each covers where the problem
appears, why it hurts, the fix with corrected SQL, and the underlying Flink
behavior so the reader learns something.

Recommend the path that fixes the root cause, not the one that only limits the
symptom. A fallback that only bounds state (for example, state TTL) is rarely
the recommended path when a semantic fix exists (temporal or lookup join,
windowing).

End each path with the final runnable query so the user can copy and run it.
If the path also needs DDL changes (a new primary key, a watermark), say so in
the steps and include the changed `CREATE TABLE` statements in the same block
so it runs as-is.

### Output style

Write concisely and precisely. Skip filler and pleasantries, keep technical
details exact, and keep every SQL block valid and runnable.

## Jump

- [SQL rules](#sql-rules): need only SQL text
- [Plan rules](#plan-rules): need EXPLAIN output or a compiled plan

## SQL rules

Cheapest level: these need only the SQL text.

**Joins**

| Impact | Look here if you see | Rule |
|--------|----------------------|------|
| HIGH | regular join against a versioned or slowly changing dimension | [Temporal join over regular join](references/sql/temporal-join.md) |
| HIGH | regular join enriching a stream from an external table (JDBC, HBase, ...) | [Lookup join for external dimension](references/sql/lookup-join.md) |

**Deduplication**

| Impact | Look here if you see | Rule |
|--------|----------------------|------|
| MEDIUM | dedup written as `GROUP BY` + `FIRST_VALUE` / `LAST_VALUE`, or `DISTINCT` | [Dedup via ROW_NUMBER](references/sql/dedup-row-number.md) |
| MEDIUM | `ROW_NUMBER ... ORDER BY t DESC` (keep last) where downstream needs append-only | [Dedup ORDER BY sets changelog mode](references/sql/dedup-order-asc.md) |

**Aggregation and pushdown**

| Impact | Look here if you see | Rule |
|--------|----------------------|------|
| MEDIUM | a `WHERE` filter on a source that supports filter pushdown | [Filter pushdown](references/sql/filter-pushdown.md) |
| MEDIUM | `SELECT *` or unused columns on a source that supports projection pushdown | [Projection pushdown](references/sql/projection-pushdown.md) |
| MEDIUM | several `COUNT(DISTINCT CASE WHEN ...)` on the same column | [FILTER over CASE WHEN for distinct](references/sql/filter-distinct.md) |

**Upsert keys and sinks**

| Impact | Look here if you see | Rule |
|--------|----------------------|------|
| HIGH | sink `PRIMARY KEY` differs from the query's `GROUP BY` or derived upsert key | [Align sink PK with upsert key](references/sql/align-sink-pk.md) |
| HIGH | a join feeds an upsert sink but its output key does not match the sink PK | [Preserve upsert key in join](references/sql/preserve-upsert-key-in-join.md) |
| MEDIUM | `INSERT` into an upsert sink whose PK differs from the query's upsert key | [Sink ON CONFLICT clause](references/sql/sink-on-conflict.md) |
| MEDIUM | `upsert-kafka` source on an append-only stream | [Append kafka, not upsert-kafka](references/sql/append-not-upsert-kafka.md) |

**Time and correctness**

| Impact | Look here if you see | Rule |
|--------|----------------------|------|
| HIGH | window TVF or event-time join on a column with no `WATERMARK` | [Window / time join needs watermark](references/sql/require-watermark.md) |
| HIGH | `NOW()` / `CURRENT_TIMESTAMP` / `RAND()` / `UUID()` / non-deterministic UDF / lookup join in an updating pipeline | [Non-determinism in update pipeline](references/sql/nondeterministic-in-update-pipeline.md) |
| HIGH | time-ordered `OVER` window, `MATCH_RECOGNIZE`, interval join, window join, window rank / dedup, or temporal sort fed by an updating input (aggregate, keep-last dedup, regular join, upsert source) | [Operator needs append-only input](references/sql/append-only-operator.md) |

## Plan rules

These need EXPLAIN output or a compiled plan. The trigger is a plan node name
or plan shape; search the plan for it.

| Impact | Look here if you see (plan node / shape) | Rule |
|--------|------------------------------------------|------|
| HIGH | `Join` with no time bound and no state TTL | [Join unbounded state](references/plan/join-unbounded-state.md) |
| HIGH | multiple chained `Join` nodes | [Multi-join unbounded state](references/plan/multi-join.md) |
| HIGH | any stateful operator with no TTL and no natural time bound | [Stateful ops need TTL](references/plan/state-ttl.md) |
| HIGH | `ChangelogNormalize` on an append-only upsert source | [Avoid ChangelogNormalize](references/plan/avoid-changelog-normalize.md) |
| HIGH | `SinkUpsertMaterializer` | [SinkUpsertMaterializer present](references/plan/sink-upsert-materializer.md) |
| MEDIUM | single-phase `GroupAggregate` with skewed keys | [Two-phase (local-global) aggregation](references/plan/two-phase-agg.md) |
| MEDIUM | `COUNT(DISTINCT)` aggregate with sparse or skewed grouping, or a hot subtask | [Split distinct aggregation](references/plan/split-distinct-agg.md) |
| MEDIUM | stateful aggregate or join without mini-batch enabled | [Mini-batch aggregation](references/plan/minibatch-agg.md) |
| MEDIUM | join sides or operators sharing one global TTL but needing different retention | [Per-operator state TTL](references/plan/per-operator-state-ttl.md) |
