# Per-operator state TTL

| Field | Value |
|-------|-------|
| **ID** | `per-operator-state-ttl` |
| **Severity** | medium |
| **Requires** | plan |
| **Operators** | Join, GroupAggregate |
| **Match** | Asymmetric stateful ops sharing one global TTL; join sides need different retention |
| **Flink version** | 1.19+ (STATE_TTL hint, FLIP-373) |

## Problem

Job set one global `table.exec.state.ttl`. But different operators (or two sides of a join) need different retention. One value wrong for all.

## Why it hurts

Global TTL too long for the short-lived side = wasted state. Too short for the long-lived side = drop state still needed, wrong result. Real case: per-side TTL cut 5.8TB state to 590GB.

## How to fix

Use the `STATE_TTL` hint for per-operator / per-join-side retention (FLIP-373). Give each side its own TTL.

- left side short-lived (orders) -> short TTL; right side long-lived (customers) -> long TTL
- hint name = the alias / side in the query
- check plan: confirm hint applied to the right operator
- general TTL background: [state-ttl](state-ttl.md)

## Example

Join with asymmetric sides - per-side TTL via hint:

```sql
SELECT /*+ STATE_TTL('o' = '1 d', 'c' = '20 d') */
  o.id, c.name
FROM orders o
JOIN customers c ON o.cust_id = c.id;
```
