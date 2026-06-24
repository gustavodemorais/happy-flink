# Operator needs append-only input

| Field | Value |
|-------|-------|
| **ID** | `append-only-operator` |
| **Severity** | high |
| **Requires** | sql |
| **Operators** | OverAggregate, Match, IntervalJoin, WindowJoin, WindowRank, WindowDeduplicate, TemporalSort |
| **Match** | An append-only-only operator - OVER (time-ordered), MATCH_RECOGNIZE, interval join, window join / rank / dedup, temporal sort - fed by an updating (upsert / retract) source |

## Problem

A whole class of streaming operators accept only append-only (insert-only) input. Feed any of them an updating stream - the output of a `GROUP BY` aggregate, a keep-last dedup, a regular / outer join, an upsert source - and planning fails with the same error:

```
<Operator> doesn't support consuming update changes which is produced by node ...
```

Operators in this class:

- `OVER` window when ordered by a time attribute (`ORDER BY rowtime` / `PROCTIME()`); `LAG` / `LEAD` ride inside OVER, so they inherit it
- `MATCH_RECOGNIZE` (CEP)
- interval join (`ON ... AND ts BETWEEN ...`)
- window join, window `TOP-N` / rank, window dedup
- temporal sort

Hard plan-time error, not a silent bug - but the updating cause is usually one operator upstream, not at the failing node.

## Why it hurts

These operators assume each row arrives once, in time order, never retracted. An updating stream sends retractions and restatements - they have no defined meaning over that, so the planner refuses the plan. Job won't start.

Escape note for OVER: ordered by a **non-time** column it does accept updates - rare, usually not intended. The time-ordered case is the normal one and the one that breaks.

## How to fix

1. **Put the operator on append-only data.** Run it directly on the insert-only source, before any operator that turns the stream updating. Don't aggregate / keep-last-dedup / regular-join first, then run the append-only operator on top.
2. **Remove the updating upstream.** If an upstream op only dedups, switch it to keep-first (`ROW_NUMBER ... ORDER BY time ASC`, stays append-only - see [dedup-row-number](dedup-row-number.md) and [dedup-order-asc](dedup-order-asc.md)) instead of a `GROUP BY` / `LAST_VALUE` form that emits updates.
3. **Convert the changelog explicitly with `TO_CHANGELOG`.** When you genuinely need to run an append-only operator over a changelog (e.g. `LAG` to track status transitions on an upsert stream), `TO_CHANGELOG` turns the updating stream into append-only rows carrying an explicit op-code column. The append-only operator then runs over those rows.

   Use this deliberately: you are flattening updates into rows and the op codes become real data. Understand what each changelog event means for your query before mapping it - the result is only correct if the op mapping matches the semantics you want. Requires a Flink version with the `TO_CHANGELOG` PTF, the first version is available on Flink 2.3.

See [require-watermark](require-watermark.md) - time-ordered OVER / interval / window joins on rowtime need a watermark too.

## Example

Before - aggregate makes the stream updating, OVER on top fails:

```sql
-- GROUP BY emits updates; OVER over PROCTIME() can't consume them
SELECT cnt, SUM(cnt) OVER (ORDER BY PROCTIME()) AS running_total
FROM (
  SELECT a, COUNT(*) AS cnt FROM events GROUP BY a
);
-- ERROR: OverAggregate doesn't support consuming update changes
```

After (reorder) - OVER on the append-only source, aggregate not in the path:

```sql
CREATE TABLE events (
  a BIGINT,
  amount BIGINT,
  proc AS PROCTIME()
);

SELECT a, amount,
  SUM(amount) OVER (PARTITION BY a ORDER BY proc) AS running_total
FROM events;
-- events is insert-only -> OVER valid
```

After (TO_CHANGELOG) - LAG over an upsert stream, converted to append-only rows with op codes:

```sql
-- orders is upsert (PRIMARY KEY order_id): INSERT, UPDATE_AFTER, DELETE
CREATE VIEW orders_changelog AS
SELECT order_id, op, status, ts
FROM TO_CHANGELOG(
  input => TABLE orders PARTITION BY order_id,
  op_mapping => MAP['INSERT', 'INSERT', 'UPDATE_AFTER', 'UPDATE_AFTER']);

-- now append-only; LAG can track the previous status per order
SELECT order_id, op, status,
  LAG(status) OVER (PARTITION BY order_id ORDER BY ts) AS prev_status
FROM orders_changelog;
```
