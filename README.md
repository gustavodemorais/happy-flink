# happy-flink

We all know Flink is super powerful, but it can be hard to make the squirrel happy. This repo is a collection of rules for spotting Flink SQL and Table API problems and their fixes.

## What

The goal of the repo is to help people fix but also learn some of Flink and stream processing concepts. It is packaged as an [Agent Skill](https://agentskills.io): point your AI to the skill and it'll guide you through the rules.

Rules cover the usual pain: slow jobs, state that only grows, state that never shrinks and so on.

**Scope:** SQL and plan-level analysis.

## Example

With the skill installed (or pointing your AI at https://github.com/gustavodemorais/happy-flink), ask:

> Help me optimize this Flink SQL query.
>
> ```sql
> SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id;
> ```

It will spot that this is a regular join with no time bound - both sides stay in
state forever - and walk you through the fix (a temporal join, since `customers`
is a dimension), plus the watermark you need to make that fix work.

See [evals/](evals/) for full runs with SQL, compiled plan, and saved output.

## Who it's for

You run Flink SQL or Table API and want to know why a job is slow or why its state keeps growing, and you want a fix.

## How to use it

1. Get the skill into your AI, any of these works:
   - Install it as a skill: `npx skills add gustavodemorais/happy-flink`
   - Copy [`skills/flink-sql-table-api-optimizer`](skills/flink-sql-table-api-optimizer) into `~/.claude/skills/` (or the equivalent skills directory of your agent)
   - Or just point any AI at [`skills/flink-sql-table-api-optimizer/SKILL.md`](skills/flink-sql-table-api-optimizer/SKILL.md)
2. Provide the AI with the data you have on hand, ideally both:
   1. only SQL or Table API code -> `references/sql/`
   2. an EXPLAIN or compiled plan -> `references/plan/`
3. The AI will scan the rules and provide you with a list of possible problems and fixes and is ready to explain the reasoning behind each!

Cheaper inputs catch less; richer inputs catch more.


## Layout

```
skills/flink-sql-table-api-optimizer/
  SKILL.md            # entry point: review steps + rule index
  references/
    sql/              # rules you can check from SQL text alone
    plan/             # rules that need an EXPLAIN or compiled plan
evals/                # example queries, plans, prompts, and saved outputs
  run.sh              # re-run one eval in a fresh Claude session
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
