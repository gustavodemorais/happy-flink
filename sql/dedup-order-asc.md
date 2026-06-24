# Dedup ORDER BY direction sets changelog mode

| Field | Value |
|-------|-------|
| **ID** | `dedup-order-asc` |
| **Severity** | medium |
| **Requires** | sql |
| **Operators** | Deduplicate |
| **Match** | `ROW_NUMBER() OVER (PARTITION BY k ORDER BY t DESC) WHERE rn = 1` where downstream wants append-only |

## Problem

Deduplicate `ORDER BY` direction decide the output changelog mode:

- `ORDER BY t ASC` + `rn = 1` = keep FIRST row per key = **append-only** (insert-only). First row arrives, emitted once, never changes.
- `ORDER BY t DESC` + `rn = 1` = keep LAST row per key = **updating** changelog. A newer row replaces the kept one, so the operator retracts the old and emits the new.

Use DESC and the stream becomes updating. Anything downstream needing append-only - an OVER window, MATCH_RECOGNIZE, an append sink - then breaks or forces a stateful correction.

## Why it hurts

Keep-last emits retractions: every later row per key sends a delete of the previous plus an insert. More changelog volume, and it turns the rest of the pipeline updating. Operators that only accept append-only input (see [append-only-operator](append-only-operator.md)) reject it. Append sinks can't take it.

Keep-first emits each key once - cheapest changelog, keeps the pipeline append-only.

Caveat: keep-first is append-only only with mini-batch off. Turn mini-batch on (`table.exec.mini-batch.enabled`) and even keep-first output becomes updating - the operator buffers and may restate within a batch.

## How to fix

If you only need one row per key and don't care which, prefer ASC (keep-first) to stay append-only. Use DESC (keep-last) only when you genuinely need the latest row and downstream can take updates.

- want earliest / any per key, append-only downstream -> `ORDER BY t ASC`
- need the latest per key, downstream takes updates -> `ORDER BY t DESC` (accept the updating changelog)
- order column should be a time attribute for the cheap Deduplicate path - see [dedup-row-number](dedup-row-number.md)

## Example

Keep-last - DESC, updating changelog:

```sql
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time DESC) AS rn
  FROM users
)
WHERE rn = 1;
-- keep=[LastRow], outputInsertOnly=false -> emits retractions
```

Keep-first - ASC, append-only:

```sql
SELECT user_id, name, email
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time ASC) AS rn
  FROM users
)
WHERE rn = 1;
-- keep=[FirstRow], outputInsertOnly=true -> append-only
```
