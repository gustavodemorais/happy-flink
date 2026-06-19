# Example: dedup via FIRST_VALUE

## What it tests

Deduplication written as `GROUP BY` + `FIRST_VALUE`. Common, non-join. The
aggregation path keeps full per-key state instead of the cheap Deduplicate
operator. Exercises the `dedup-row-number` rule.

## Input

- `input.sql` - the DDL + the FIRST_VALUE dedup INSERT
- `plan.txt` - EXPLAIN plan
- `plan.json` - compiled plan JSON
- `prompt.md` - prompt fed to a fresh Claude session

Plan artifacts generated from `flink-table-planner` via the throwaway test
`HappyFlinkExampleGenTest`, not committed to this repo.

## Expected findings

The plan shows `GroupAggregate(groupBy=[user_id], select=[FIRST_VALUE(name), FIRST_VALUE(email)])`
- the aggregation path, not the Deduplicate operator. A good review should flag:

- `dedup-row-number` - rewrite as `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY update_time) = 1`
  so Flink picks the Deduplicate operator, storing one row per key instead of
  full aggregation state

## Output

`output.md` - raw reply from the fresh session.

## Re-run

```bash
./examples/run.sh examples/dedup-first-value
```
