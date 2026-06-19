# Example: join drops the upsert key

## What it tests

A join into an upsert sink where the join condition does not carry the full
primary key of the keyed side. The join drops the upsert key. Exercises the
upsert-key / SinkUpsertMaterializer rule family.

`orders` PK is only `order_id`. The join is on `user_id`, so the orders PK is
not carried through, and the join output has no upsert key matching the sink PK
(`order_id`).

## Input

- `input.sql` - the DDL + the broken INSERT
- `error.txt` - the planner error the broken query produces (empty derived upsert key)
- `plan.txt` - EXPLAIN of a fixed variant (orders keyed by `(order_id, user_id)`, sink PK matches)
- `plan.json` - compiled plan JSON of the fixed variant
- `prompt.md` - prompt fed to a fresh Claude session

Plan artifacts generated from `flink-table-planner` via the throwaway test
`HappyFlinkExampleGenTest`, not committed to this repo. On this Flink version the
broken query fails validation (empty upsert key) rather than silently inserting a
SinkUpsertMaterializer.

## Expected findings

- `preserve-upsert-key-in-join` - the join must carry the full PK of the keyed
  side; fix is a composite PK `(order_id, user_id)` so the join preserves the key
- `align-sink-pk` - sink PK must then match the preserved key `(order_id, user_id)`
- `sink-upsert-materializer` - the operator / cost this avoids

The fixed `plan.txt` shows no SinkUpsertMaterializer and join inputs
`JoinKeyContainsUniqueKey` / `HasUniqueKey` - the upsert key is preserved.

## Output

`output.md` - raw reply from the fresh session.

## Re-run

```bash
./examples/run.sh examples/join-drops-upsert-key
```
