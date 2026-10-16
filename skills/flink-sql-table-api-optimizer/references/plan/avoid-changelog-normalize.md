---
title: Avoid ChangelogNormalize
id: avoid-changelog-normalize
impact: HIGH
impactDescription: "Unbounded per-key state is kept for data that never needs updates, inflating state and checkpoints."
requires: plan
operators: [ChangelogNormalize]
match: "The plan contains a ChangelogNormalize operator because the source is declared as upsert, but the stream is actually append-only."
tags: [changelog, upsert-kafka, state-size, source, cdc]
---

# Avoid ChangelogNormalize

## Problem

The plan shows a `ChangelogNormalize` operator. Flink inserts it when the source is an upsert source (for example `upsert-kafka` or some CDC connectors), because it must build a full changelog with before-images.

## Why it hurts

`ChangelogNormalize` keeps state for every primary key, indefinitely, so that it can emit retract and update rows. On append-only data this state is pure waste: it grows with key cardinality and produces large checkpoints.

## How to fix

- If an append-only stream is declared as `upsert-kafka`, switch the source to the plain `kafka` connector. The operator disappears. See [append-not-upsert-kafka](../sql/append-not-upsert-kafka.md).
- If an upsert source is truly needed, the operator is required. Reduce its cost with a TTL on the normalize state, or by narrowing the columns read ([projection-pushdown](../sql/projection-pushdown.md)).
- Newer Flink versions (FLIP-510) allow some upsert sources to skip normalization. Check your version.

## Example

The plan has a `ChangelogNormalize` coming from `upsert-kafka` on append-only data. Fix it at the source:

```sql
-- before: 'connector' = 'upsert-kafka' + PRIMARY KEY -> ChangelogNormalize
-- after: plain kafka, operator gone
CREATE TABLE clicks (
  user_id BIGINT,
  url STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks'
);
```
