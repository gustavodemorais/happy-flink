# TODO

Planned + open work. Keep short. Done -> delete line.

## General changes

- Changelog mode and watermark awareness. These are two concepts that are not well understood by users, and they are important for understanding the results of the tool. We need to make sure that the rules are aware of these concepts and can provide useful feedback to users. For example, some operators and functions only accept append input and some accept both (append + updating). Some operators are sensitive to watermarks, while others are not. The rules should be able to detect these situations and provide appropriate feedback. The goal is to help users understand these concepts a bit more in a very easy and illustrative way and give good explanations if asked further. Maybe we want to create a content folder where we can put some content about these concepts and link to it from the tool.

## Rules

- Upsert key dropped: help users not to drop upsert keys in their queries if their sink requires an upsert key. If sink-upsert-materializer is present, it means that the sink upsert key does not match the query's changelog key. We should provide a rule that detects this situation and provides feedback to the user on how to fix it. We want to mention this in the sink upsert materializer rule, but we also want to have a separate rule that detects this situation and provides feedback to the user on how to fix it. This is important because it can lead to considerable performance issues and can also mask a real correctness bug (wrong deletes).

- [ ] More SQL/plan rules from research not yet ported: `delta-join-eligible`, `incremental-three-phase-agg`, `nondeterministic-col-in-sink`, `topn-*` (omit rownum, small N), `enable-changelog-state-backend`, RocksDB tuning set.
- [ ] Fill 5 runtime stubs: `data-skew`, `backpressure`, `state-size-growth`, `watermark-lag`, `checkpoint-tuning`. Need real metric thresholds + how-to-detect.

## Examples

- [ ] Add more examples, one per rule family: aggregation (COUNT DISTINCT skew), dedup, multi-join, upsert/changelog, window.
- [ ] Example for a query with NO problems - check tool not invent issues.
- [ ] Tune output format more across examples once we have several.
