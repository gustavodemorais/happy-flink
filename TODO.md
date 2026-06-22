# TODO

Planned + open work. Keep each item small. Done -> delete the line.

## Concepts

- [ ] Add content explaining changelog mode (append vs updating) in simple terms.
- [ ] Add content explaining watermarks and which operators are sensitive to them.
- [ ] Make rules aware which operators accept append-only vs updating input.
- [ ] Make rules aware which operators are watermark-sensitive.
- [ ] Create a `content/` folder for these explainers; link to it from the rules.

## Rules - new (high priority)

Common, correctness/state impact, vanilla Flink.

- [ ] `over-window-append-only` - OVER window only valid on append-only input.
- [ ] `dedup-order-asc` - dedup `ORDER BY ts DESC` emits retractions; ASC keeps-first stays append-only.
- [ ] `cross-join-no-predicate` (plan) - predicate-less join explodes cardinality + state.
- [ ] `idle-source-heartbeat` - idle source freezes watermark; heartbeat rows / idle-timeout.

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
- [ ] `enable-changelog-state-backend`
- [ ] RocksDB tuning rule set.
- [ ] Processing-time temporal join rule (after it is released).

## Rules - new (verify vendor-neutral first)

Maybe Confluent-specific syntax, not vanilla Flink - confirm before adding.

- [ ] `value-fields-include-pk` - PK excluded from Kafka value (`except-key`) breaks GROUP BY / upsert.
- [ ] `distributed-by-matches-pk` - bucket key must equal PK for upsert; else skew.
- [ ] `snapshot-mode-for-backfill` - one-shot replay keeps growing state; snapshot-mode reads to latest offset.

## Rules - fill stubs

- [ ] `data-skew` - real metric thresholds + how to detect.
- [ ] `backpressure` - real metric thresholds + how to detect.
- [ ] `state-size-growth` - real metric thresholds + how to detect.
- [ ] `watermark-lag` - real metric thresholds + how to detect.
- [ ] `checkpoint-tuning` - fold in: incremental checkpoints, interval >= ~1.5x p99 duration, unaligned under backpressure.

## Rules - new runtime / deploy

- [ ] `source-parallelism-vs-partitions` - source parallelism != Kafka partition count = idle subtasks or underuse.
- [ ] `rocksdb-block-cache-vs-managed` - block-cache-size > managed memory = OOM / ineffective cache.
- [ ] `pod-mem-below-process-size` - pod memory limit < `taskmanager.memory.process.size` = OOMKill.
- [ ] `ephemeral-checkpoint-storage` - local / emptyDir checkpoint dir loses state on pod restart; use durable store.

## Examples

- [ ] Aggregation example (COUNT DISTINCT skew).
- [ ] Multi-join example.
- [ ] Upsert / changelog example.
- [ ] Window example.
- [ ] Example with NO problems - check the tool does not invent issues.
- [ ] Tune output format across examples once we have several.

## Conventions

- [ ] Any number or borrowed material in a rule must link to its source.
