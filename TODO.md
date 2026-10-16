# TODO

Planned + open work. Keep each item small. Done -> delete the line.

## Concepts

- [ ] Add content explaining changelog mode (append vs updating) in simple terms.
- [ ] Add content explaining watermarks and which operators are sensitive to them.
- [ ] Make rules aware which operators accept append-only vs updating input.
- [ ] Make rules aware which operators are watermark-sensitive.
- [ ] Create a `skills/flink-sql-table-api-optimizer/references/content/` folder for these explainers; link to it from the rules.

## Rules - new (high priority)

Common, correctness/state impact, vanilla Flink.

- [ ] Add rule plan/cross-join-no-predicate (join with empty join keys + SINGLETON exchange) - was listed in the index but never written.
- [ ] `idle-source-heartbeat` - idle source freezes watermark; heartbeat rows / idle-timeout. Top demand: window/join "emits nothing." Cover idle partition, oversized watermark delay, heartbeat via UNION ALL, watermark alignment across sides.
- [ ] `watermark-not-advancing` - diagnostic: window/join/dedup produces no output because watermark stalls (idle partition, bad watermark column/expression, delay too large). Routes to `idle-source-heartbeat` / `require-watermark`.
- [ ] `regular-join-loses-pk` (plan) - regular/outer join drops PK unless join condition uses both sides' PKs = planner adds dedup / SinkUpsertMaterializer = high state. Extends `preserve-upsert-key-in-join`.
- [ ] `temporal-join-requires-pk` - temporal join needs left time attribute + versioned table's full PK in equi-condition; subquery dropping `$rowtime`, or composite-PK partial join, fails.
- [ ] `temporal-join-watermark-alignment` - historical/backfill rows return NULL when dim watermark advances past fact event time, or idle dim stalls join watermark.
- [ ] `interval-vs-temporal-join` - symmetric stream-stream (and `WITHIN`-style windowed joins) want interval join, not temporal/TUMBLE.

## Rules - new (medium priority)

- [ ] `prefer-window-tvf` - legacy `GROUP BY TUMBLE(...)` harder to optimize than window TVF.
- [ ] `statement-set-shared-source` - separate INSERTs re-read source N times; `EXECUTE STATEMENT SET` shares the read.
- [ ] `unbounded-order-by` - streaming ORDER BY on a non-time column is invalid / unbounded.
- [ ] `dedup-before-array-agg` - ARRAY_AGG over un-deduped input regrows state; dedup first.
- [ ] `changelog-mode-sink-compat` (plan) - operator output changelog mode incompatible with sink.
- [ ] `to-from-changelog` - using TO_CHANGELOG / FROM_CHANGELOG to fix or optimize queries.
- [ ] `delta-join-eligible`
- [ ] `incremental-three-phase-agg`
- [ ] `topn-omit-rownum`
- [ ] `topn-small-n`
- [ ] Processing-time temporal join rule (after it is released).
- [ ] `dedup-needs-ttl` - dedup / DISTINCT keeps one entry per key forever without state TTL.
- [ ] `dedup-order-by-time-attribute` - dedup `ORDER BY` a payload column != row time with out-of-order arrival leaks duplicates; define watermark on the payload column.
- [ ] `state-ttl-hint-scoping` - per-operator `STATE_TTL` hint silently ignored on UNNEST / DISTINCT / CTEs / views and on the temporal-join `AS` alias; global TTL evicts static dims = NULL joins, override to `0`.
- [ ] `unnest-left-join-not-null` - `LEFT JOIN UNNEST` over array-of-ROW with NOT NULL fields crashes the planner; use `CROSS JOIN UNNEST` or pre-aggregate.
- [ ] `unnest-duplicates` - `CROSS JOIN UNNEST` of an array with duplicate elements yields duplicate rows; dedup after.
- [ ] `name-row-fields` - nested `ROW(...)` output fields default to `EXPR$0`; `CAST(ROW(a,b) AS ROW<n1 T1, n2 T2>)`.
- [ ] `full-outer-latest-state` - no FULL-OUTER temporal variant; `UNION ALL` + `LAST_VALUE(...) OVER (PARTITION BY key ORDER BY rowtime)` keeps last value per key.
- [ ] `anti-join-not-exists` - `LEFT JOIN ... WHERE rhs IS NULL` / correlated `NOT EXISTS` can return nothing or hit planner errors; `GROUP BY ... HAVING COUNT(key)=0` or `NOT IN`.
- [ ] `json-type-untypable` - polymorphic JSON (`anyOf`/`oneOf`/`allOf`, open object, tuple arrays) has no fixed SQL type; declare STRING + `JSON_VALUE`/`JSON_QUERY`, or type the schema.
- [ ] `udf-call-multiplied-by-planner` (plan) - UDF output reused across many projections is duplicated per-column (calc merge), re-evaluating + re-deserializing each time.
- [ ] `ptf-for-stateful-patterns` - timeout/throttle (emit-first-then-suppress), changelog materialization, dynamic UNNEST need a PTF with timers/state, not pure SQL or TTL.

## Rules - new (verify vendor-neutral first)

Maybe Confluent-specific syntax, not vanilla Flink - confirm before adding.

- [ ] `value-fields-include-pk` - PK excluded from Kafka value (`except-key`) breaks GROUP BY / upsert.
- [ ] `distributed-by-matches-pk` - bucket key must equal PK for upsert; else skew.
- [ ] `snapshot-mode-for-backfill` - one-shot replay keeps growing state; snapshot-mode reads to latest offset.

## Evals

- [ ] Aggregation example (COUNT DISTINCT skew).
- [ ] Multi-join example.
- [ ] Upsert / changelog example.
- [ ] Window example.
- [ ] Example with NO problems - check the tool does not invent issues.
- [ ] Tune output format across examples once we have several.

## Conventions

- [ ] Any number or borrowed material in a rule must link to its source.
