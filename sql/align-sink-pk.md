# Align sink PK with upsert key

| Field | Value |
|-------|-------|
| **ID** | `align-sink-pk` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | Sink, SinkUpsertMaterializer |
| **Match** | Sink PRIMARY KEY differs from query GROUP BY / derived upsert key |

## Problem

Sink table `PRIMARY KEY` not match the query upsert key (GROUP BY columns, dedup key). Flink insert a `SinkUpsertMaterializer` to fix ordering.

## Why it hurts

`SinkUpsertMaterializer` keep an in-memory row list per key to reorder changelog before sink. Extra state, extra cost. Worse - mismatch can send wrong deletes, corrupt sink.

## How to fix

Make sink `PRIMARY KEY` match the query's upsert key:

- aggregate -> sink PK = GROUP BY columns
- dedup -> sink PK = PARTITION BY key
- align so the materializer disappear from plan

See [sink-upsert-materializer](../plan/sink-upsert-materializer.md) to spot the operator.

## Example

Before - sink PK (id) not match GROUP BY (user_id, day), materializer added:

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

After - sink PK matches GROUP BY key, no materializer:

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
