# Append kafka, not upsert-kafka

| Field | Value |
|-------|-------|
| **ID** | `append-not-upsert-kafka` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | ChangelogNormalize, TableSourceScan |
| **Match** | Source declared as upsert-kafka but stream is append-only / no deletes |

## Problem

Source table use `connector = 'upsert-kafka'` when data is append-only, no updates, no deletes. Upsert source force a stateful `ChangelogNormalize` operator.

## Why it hurts

`ChangelogNormalize` hold state for every primary key seen, forever, to build the changelog. Append-only data need none of this. State grow with key cardinality for no gain.

## How to fix

Use plain `connector = 'kafka'` for append-only streams. No ChangelogNormalize, no per-key state. Only use `upsert-kafka` when stream truly has updates / deletes by key.

- append-only events (clicks, logs, transactions) -> `kafka`
- compacted topic / CDC with updates by key -> `upsert-kafka`

See [avoid-changelog-normalize](../plan/avoid-changelog-normalize.md) to spot the operator in the plan.

## Example

Before - upsert-kafka on append-only stream, stateful normalize:

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

After - plain kafka, no normalize state:

```sql
CREATE TABLE clicks (
  user_id BIGINT,
  url STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks'
);
```
