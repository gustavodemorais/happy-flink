---
title: Align sink PK with upsert key
id: align-sink-pk
impact: HIGH
impactDescription: "A mismatched sink primary key adds a stateful SinkUpsertMaterializer and can corrupt the sink with wrong deletes."
requires: sql
operators: [Sink, SinkUpsertMaterializer]
match: "The sink PRIMARY KEY differs from the query's GROUP BY columns or derived upsert key."
tags: [sink, primary-key, upsert-key, changelog, state]
---

# Align sink PK with upsert key

## Problem

The sink table's `PRIMARY KEY` does not match the query's upsert key (the GROUP BY columns or the deduplication key). Flink then inserts a `SinkUpsertMaterializer` to fix the ordering of changes.

## Why it hurts

`SinkUpsertMaterializer` keeps an in-memory list of rows per key so it can reorder the changelog before it reaches the sink. This adds state and processing cost. Worse, the mismatch can send incorrect deletes and corrupt the sink.

## How to fix

Make the sink `PRIMARY KEY` match the query's upsert key:

- For an aggregation, the sink PK should be the GROUP BY columns.
- For a deduplication, the sink PK should be the PARTITION BY key.
- Once the keys are aligned, the materializer disappears from the plan.

For joins that drop the upsert key, see [preserve-upsert-key-in-join](preserve-upsert-key-in-join.md). To spot the operator in the plan, see [sink-upsert-materializer](../plan/sink-upsert-materializer.md). On Flink 2.3+, a mismatch fails planning until it is fixed or a conflict clause is declared; see [sink-on-conflict](sink-on-conflict.md).

## Example

Before: the sink PK (`id`) does not match the GROUP BY key (`user_id, day`), so a materializer is added:

```sql
CREATE TABLE daily_counts (
  id STRING,
  user_id BIGINT,
  day DATE,
  cnt BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', ...);

INSERT INTO daily_counts
SELECT CONCAT(...), user_id, day, COUNT(*)
FROM clicks GROUP BY user_id, day;
```

After: the sink PK matches the GROUP BY key, so no materializer is needed:

```sql
CREATE TABLE daily_counts (
  user_id BIGINT,
  day DATE,
  cnt BIGINT,
  PRIMARY KEY (user_id, day) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', ...);

INSERT INTO daily_counts
SELECT user_id, day, COUNT(*)
FROM clicks GROUP BY user_id, day;
```
