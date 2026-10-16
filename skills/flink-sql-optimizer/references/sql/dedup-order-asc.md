---
title: Dedup ORDER BY direction sets changelog mode
id: dedup-order-asc
impact: MEDIUM
impactDescription: "Keep-last deduplication emits retractions that make the rest of the pipeline updating and break append-only consumers."
requires: sql
operators: [Deduplicate]
match: "A deduplication written as `ROW_NUMBER() OVER (PARTITION BY k ORDER BY t DESC) WHERE rn = 1` where downstream operators need append-only input."
tags: [deduplication, row-number, changelog, append-only, retraction]
---

# Dedup ORDER BY direction sets changelog mode

## Problem

The `ORDER BY` direction of a deduplication determines its output changelog mode:

- `ORDER BY t ASC` with `rn = 1` keeps the FIRST row per key and is **append-only** (insert-only). The first row arrives, is emitted once, and never changes.
- `ORDER BY t DESC` with `rn = 1` keeps the LAST row per key and produces an **updating** changelog. A newer row replaces the kept one, so the operator retracts the old row and emits the new one.

With DESC, the stream becomes updating. Anything downstream that needs append-only input, such as an OVER window, MATCH_RECOGNIZE, or an append sink, then either fails or forces a stateful correction.

## Why it hurts

Keep-last emits retractions: every later row for a key sends a delete of the previous row plus an insert of the new one. This increases changelog volume and makes the rest of the pipeline updating. Operators that only accept append-only input (see [append-only-operator](append-only-operator.md)) reject it, and append sinks cannot accept it.

Keep-first emits each key once. It produces the cheapest changelog and keeps the pipeline append-only.

Caveat: keep-first is append-only only when mini-batch is disabled. With mini-batch enabled (`table.exec.mini-batch.enabled`), even keep-first output becomes updating, because the operator buffers rows and may restate them within a batch.

## How to fix

If you only need one row per key and do not care which one, prefer ASC (keep-first) to stay append-only. Use DESC (keep-last) only when you genuinely need the latest row and downstream operators can handle updates.

- You want the earliest (or any) row per key and downstream is append-only: use `ORDER BY t ASC`.
- You need the latest row per key and downstream accepts updates: use `ORDER BY t DESC` and accept the updating changelog.
- The order column should be a time attribute to get the cheap Deduplicate path; see [dedup-row-number](dedup-row-number.md).

## Example

Keep-last: DESC, updating changelog:

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

Keep-first: ASC, append-only:

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
