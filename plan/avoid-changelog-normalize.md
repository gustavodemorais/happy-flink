# Avoid ChangelogNormalize

| Field | Value |
|-------|-------|
| **ID** | `avoid-changelog-normalize` |
| **Severity** | high |
| **Requires** | plan |
| **Operators** | ChangelogNormalize |
| **Match** | ChangelogNormalize operator in plan, source is upsert but stream append-only |

## Problem

Plan show a `ChangelogNormalize` operator. Inserted because source is upsert (upsert-kafka, some CDC) and Flink must build a full changelog with before-images.

## Why it hurts

`ChangelogNormalize` keep state for every primary key, forever, to emit retract/update rows. On append-only data this state is pure waste - grow with key cardinality, big checkpoint.

## How to fix

- append-only stream declared as `upsert-kafka` -> switch source to plain `kafka`. Operator disappear. See [append-not-upsert-kafka](../sql/append-not-upsert-kafka.md).
- if upsert source truly needed, this op is required - reduce its cost with TTL on the normalize state, or narrow columns ([projection-pushdown](../sql/projection-pushdown.md)).
- newer Flink (FLIP-510) let some upsert sources skip normalize - check version.

## Example

Plan has ChangelogNormalize from upsert-kafka on append-only data. Fix at source:

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
