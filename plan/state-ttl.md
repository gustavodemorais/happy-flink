# Stateful ops need TTL

| Field | Value |
|-------|-------|
| **ID** | `state-ttl` |
| **Severity** | high |
| **Requires** | plan |
| **Operators** | Join, GroupAggregate, GlobalGroupAggregate, IncrementalGroupAggregate, WindowAggregate, Match, Rank, Deduplicate |
| **Match** | Stateful operator in plan with no TTL and no natural time bound |

## Problem

Stateful operator hold state per key. No TTL, no window bound = state kept forever for every key ever seen. New keys keep coming, state keep growing.

## Why it hurts

High-cardinality key (user id, session id, device id) = millions of state entries, none ever removed. Checkpoint size grow without limit. Slow checkpoint, slow restore, OOM risk. Dead keys (user gone, session done) waste state forever.

## How to fix

- set `table.exec.state.ttl` (e.g. `'36 h'`). Idle state dropped after TTL. Pick TTL > longest gap you expect between events for same key.
- prefer windowed version when semantics allow - window cleans state at window close, no TTL guess needed (`WindowAggregate` over `GroupAggregate`).
- watch trade-off: TTL too short = drop state still needed, wrong result (miss late match, re-emit aggregate from zero). TTL too long = state too big. Tune to data.

Operators that need this: `Join`, `GroupAggregate`, `GlobalGroupAggregate`, `IncrementalGroupAggregate`, `WindowAggregate`, `Match`, `Rank`, `Deduplicate`.

Check `EXPLAIN` - see any of above nodes, confirm TTL set or window present.

## Example

Before - group aggregate, no TTL, state per user forever:

```sql
SELECT user_id, COUNT(*)
FROM clicks
GROUP BY user_id;
```

After - set TTL so idle user state drop:

```sql
SET 'table.exec.state.ttl' = '36 h';

SELECT user_id, COUNT(*)
FROM clicks
GROUP BY user_id;
```

Better when semantics allow - window cleans itself, no TTL guess:

```sql
SELECT user_id, window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(click_time), INTERVAL '1' HOUR))
GROUP BY user_id, window_start, window_end;
```
