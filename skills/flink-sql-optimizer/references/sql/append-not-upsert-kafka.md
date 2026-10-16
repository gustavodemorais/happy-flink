---
title: Append kafka, not upsert-kafka
id: append-not-upsert-kafka
impact: MEDIUM
impactDescription: "An upsert-kafka source on append-only data adds a ChangelogNormalize operator whose state grows with key cardinality for no benefit."
requires: sql
operators: [ChangelogNormalize, TableSourceScan]
match: "A source is declared as upsert-kafka, but the stream is append-only and has no deletes."
tags: [kafka, upsert-kafka, changelog-normalize, source, state]
---

# Append kafka, not upsert-kafka

## Problem

The source table uses `connector = 'upsert-kafka'` even though the data is append-only, with no updates or deletes. An upsert source forces a stateful `ChangelogNormalize` operator into the plan.

## Why it hurts

`ChangelogNormalize` keeps state for every primary key it has seen, indefinitely, in order to build the changelog. Append-only data needs none of this, so state grows with key cardinality for no gain.

## How to fix

Use the plain `connector = 'kafka'` for append-only streams. This avoids `ChangelogNormalize` and its per-key state. Use `upsert-kafka` only when the stream actually contains updates or deletes by key.

- Append-only events (clicks, logs, transactions): use `kafka`.
- Compacted topics or CDC with updates by key: use `upsert-kafka`.

See [avoid-changelog-normalize](../plan/avoid-changelog-normalize.md) to spot the operator in the plan.

## Example

Before: `upsert-kafka` on an append-only stream, with a stateful normalize step:

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  url STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'clicks'
);
```

After: plain `kafka`, with no normalize state:

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  url STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks'
);
```
