# Example: regular join, unbounded state

## What it tests

A plain regular join with no time bound, no primary key, and `SELECT *`. The
classic unbounded-state trap. Exercises the largest filled rule set.

## Input

- `input.sql` - the DDL + query a user would hand us
- `plan.txt` - EXPLAIN text (full AST + physical + execution plan)
- `plan.json` - compiled plan JSON
- `prompt.md` - prompt fed to a fresh Claude session

Plan artifacts generated from `flink-table-planner` via a throwaway test
(`HappyFlinkExampleGenTest`), not committed to this repo.

## Expected findings

The plan shows `Join(joinType=[InnerJoin] ... leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])`
- a regular join, both sides full state, no bound. A good review should flag:

- `join-unbounded-state` - regular join keeps both sides forever; bound it (interval / temporal / lookup) or set TTL
- `temporal-join` / `lookup-join` - the concrete fixes
- `projection-pushdown` - `SELECT *` pulls all 6 columns into join state
- `require-watermark` - no watermark on `order_time`, blocks interval/temporal fix

## Output

`output.md` - raw reply from the fresh session. This is what we eyeball and tune.

## Re-run

From the repo root:

```bash
./examples/run.sh examples/regular-join-unbounded
```
