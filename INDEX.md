# happy-flink rule index

The full map of Flink SQL optimization rules. Pick rules by the data you have.

## How to review (read this first)

If a user hands you a Flink SQL query - optionally with its compiled plan or
runtime metrics - and asks for help:

**1. Analyze.**

- Pick the rule tables below by the data you have:
  - only SQL text -> SQL rules
  - an EXPLAIN / compiled plan -> SQL + Plan rules
- Scan the `Look here if you see` column of those tables against the input. It is
  the trigger for each rule - a SQL pattern or a plan-node name. Open only the
  rule files whose trigger matches. Do not open every rule.
- For every problem, note: the rule, where it shows up in the input (SQL line
  or plan node), the fix, and why it matters.
- If a fix you recommend depends on something missing from the input (for
  example, a time-based join or window needs a watermark but no `WATERMARK` is
  defined), treat that as its own problem - do not silently assume it.
- If a rule has a `Flink version` field, the fix needs that version. Surface it
  with the fix so the user knows the requirement.

**2. Write the report with these sections, in order:**

**Title** - `# Review: <short name of the query>`, then one line: what the query does + the headline problem.

**`## Summary`** - a table of every problem found, highest severity first. Columns: number, problem, severity, rule (link to the rule file), jump (anchor link to its Details section). For example:

| # | Problem | Severity | Rule | Jump |
|---|---------|----------|------|------|
| 1 | unbounded regular join | high | [join-unbounded-state](plan/join-unbounded-state.md) | [#1](#1-unbounded-regular-join) |

**`## Paths`** - the ordered ways to fix things. Problems usually chain (one fix needs another done first), so group them into paths instead of listing them flat. Mark one recommended. Each path is a numbered step list, each step noting which problem it fixes, and ends with a single fenced `sql` block holding the final runnable query for that path. Name them `### Path A - <name> (recommended)`, `### Path B - <name>`, etc.

**`## Details`** - one subsection per problem from the Summary, same order, heading matching the Jump anchor. Each covers: where it shows up, why it hurts, the fix with corrected SQL, and the Flink reasoning so the reader learns something.

Pick the recommended path by what fixes the root cause, not just what caps the symptom. A fallback that only bounds state (e.g. TTL) is rarely the recommended path when a semantic fix (temporal / lookup join, windowing) exists.

End each path with the final runnable query so the user can copy-paste and try it. If the path also needs DDL changes (a new PK, a watermark), say so in the steps and include the changed `CREATE TABLE` in the block too, so it runs as-is.

### Output style

Write in caveman lite: drop filler words and pleasantries, keep articles and
normal sentence structure so it stays easy to read. Keep all technical detail
exact. Keep code blocks as normal, valid SQL.

## Jump

- [SQL rules](#sql-rules) - need only SQL text
- [Plan rules](#plan-rules) - need an EXPLAIN or compiled plan

## SQL rules

Cheapest. You have the SQL text and nothing else.

**Joins**

| Sev | Look here if you see | Rule |
|-----|----------------------|------|
| high | regular join against a versioned / slowly-changing dimension | [Temporal join over regular join](sql/temporal-join.md) |
| high | regular join enriching a stream from an external table (JDBC, HBase, ...) | [Lookup join for external dimension](sql/lookup-join.md) |

**Dedup**

| Sev | Look here if you see | Rule |
|-----|----------------------|------|
| medium | dedup as `GROUP BY` + `FIRST_VALUE` / `LAST_VALUE`, or `DISTINCT` | [Dedup via ROW_NUMBER](sql/dedup-row-number.md) |
| medium | `ROW_NUMBER ... ORDER BY t DESC` keep-last where downstream wants append-only | [Dedup ORDER BY sets changelog mode](sql/dedup-order-asc.md) |

**Aggregation & pushdown**

| Sev | Look here if you see | Rule |
|-----|----------------------|------|
| medium | a `WHERE` filter, source supports filter pushdown | [Filter pushdown](sql/filter-pushdown.md) |
| medium | `SELECT *` or unused columns, source supports projection pushdown | [Projection pushdown](sql/projection-pushdown.md) |
| medium | several `COUNT(DISTINCT CASE WHEN ...)` on the same column | [FILTER over CASE WHEN for distinct](sql/filter-distinct.md) |

**Upsert keys & sinks**

| Sev | Look here if you see | Rule |
|-----|----------------------|------|
| high | sink `PRIMARY KEY` differs from query `GROUP BY` / derived upsert key | [Align sink PK with upsert key](sql/align-sink-pk.md) |
| high | join feeds an upsert sink but output key does not match the sink PK | [Preserve upsert key in join](sql/preserve-upsert-key-in-join.md) |
| medium | `INSERT` into upsert sink, query upsert key differs from sink PK | [Sink ON CONFLICT clause](sql/sink-on-conflict.md) |
| medium | `upsert-kafka` source on an append-only stream | [Append kafka, not upsert-kafka](sql/append-not-upsert-kafka.md) |

**Time & correctness**

| Sev | Look here if you see | Rule |
|-----|----------------------|------|
| high | window TVF / event-time join on a column with no `WATERMARK` | [Window / time join needs watermark](sql/require-watermark.md) |
| high | `NOW()` / `CURRENT_TIMESTAMP` / `RAND()` / `UUID()` / non-det UDF / lookup join in an updating pipeline | [Non-determinism in update pipeline](sql/nondeterministic-in-update-pipeline.md) |
| high | `OVER` window or `MATCH_RECOGNIZE` (time-ordered) fed by an updating source | [OVER window needs append-only input](sql/over-window-append-only.md) |

## Plan rules

Need EXPLAIN output or a compiled plan. Trigger is a plan-node name or shape - search the plan for it.

| Sev | Look here if you see (plan node / shape) | Rule |
|-----|------------------------------------------|------|
| high | `Join` with no time bound, no state TTL | [Join unbounded state](plan/join-unbounded-state.md) |
| high | `Join` with empty join keys + `SINGLETON` exchange | [Join with no equi-predicate](plan/cross-join-no-predicate.md) |
| high | multiple `Join` nodes chained | [Multi-join unbounded state](plan/multi-join.md) |
| high | any stateful op with no TTL and no natural time bound | [Stateful ops need TTL](plan/state-ttl.md) |
| high | `ChangelogNormalize` on an append-only upsert source | [Avoid ChangelogNormalize](plan/avoid-changelog-normalize.md) |
| high | `SinkUpsertMaterializer` | [SinkUpsertMaterializer present](plan/sink-upsert-materializer.md) |
| medium | single-phase `GroupAggregate`, skewed keys | [Two-phase (local-global) aggregation](plan/two-phase-agg.md) |
| medium | `COUNT(DISTINCT)` agg, sparse / skewed grouping, hot subtask | [Split distinct aggregation](plan/split-distinct-agg.md) |
| medium | stateful agg / join, mini-batch not enabled | [Mini-batch aggregation](plan/minibatch-agg.md) |
| medium | join sides / ops sharing one global TTL but needing different retention | [Per-operator state TTL](plan/per-operator-state-ttl.md) |

Runtime rules (live-job metrics) are not in the index yet - still open, see [TODO.md](TODO.md).
