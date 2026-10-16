# happy-flink

A set of rules for spotting problems in Flink SQL and Table API queries and
suggesting fixes, packaged as an [Agent Skill](https://agentskills.io).

An AI tool loads the skill, the user provides SQL and optionally an EXPLAIN or
compiled plan, and the tool scans the rules and reports each problem with its
fix and the reasoning behind it.

See [README.md](README.md) for the project goals and
[skills/flink-sql-optimizer/SKILL.md](skills/flink-sql-optimizer/SKILL.md) for
the rule map.

## Layout

```
skills/flink-sql-optimizer/
  SKILL.md          entry point: frontmatter, review protocol, rule tables
  references/sql/   rules that need only the SQL text (cheapest)
  references/plan/  rules that need an EXPLAIN or compiled plan
evals/              real inputs with saved outputs, plus run.sh
```

`SKILL.md` has two jobs:

1. The "How to review" protocol at the top: the fixed steps an agent follows
   (analysis, then a report with Summary table, Paths, and Details, plus the
   output style).
2. The rule map: one table per input level, listing every rule with its
   trigger.

The protocol lives in `SKILL.md` so a user prompt can stay one line ("use the
flink-sql-optimizer skill to help with this query"). Keep it there, not in eval
prompts.

Its YAML frontmatter `name` must match the directory name
(`flink-sql-optimizer`), and its `description` decides when the skill triggers,
so keep the description in sync when adding rule areas.

Troubleshooting live jobs from runtime metrics is out of scope for this skill.

## Rules

- One rule per Markdown file. The filename is the rule ID in kebab-case (for
  example, `multi-join.md`).
- Every rule file uses the same template, with no section left out:

```markdown
---
title: Short rule title
id: kebab-case-id
impact: HIGH            # HIGH | MEDIUM | LOW
impactDescription: one line on what it costs (state growth, throughput, ...)
requires: sql           # sql | plan; must match the folder the file lives in
operators: [Join, GroupAggregate]   # Flink operator names this rule matches
match: one line describing when this rule fires
flink-version: "2.1+"   # optional; only when the fix needs a specific version
tags: [join, state]
---

# Title

## Problem
What is wrong.

## Why it hurts
The cost: state growth, throughput, checkpoint size, money.

## How to fix
Steps. Link related rules with relative paths, e.g. [state-ttl](state-ttl.md).

## Example
Before and after SQL in fenced blocks.
```

- Omit `flink-version` when the fix is version-agnostic (most SQL rules).
- Cross-link related rules with relative Markdown links within the same or the
  sibling folder (e.g. `../plan/state-ttl.md`).

## Style

Write rule prose in plain, concise English. Keep technical details exact and
every SQL block valid.

## Editing

- New rule: add a file in `references/sql/` or `references/plan/` using the
  full template, and add a row to the matching table in `SKILL.md`.
- Every link in `SKILL.md` and every cross-link between rules must resolve to a
  real file. Verify this after each change.
- Rate impact honestly: `HIGH` means it can kill the job or blow up state;
  `MEDIUM` means wasteful but survivable; `LOW` means minor.

## Evals

`evals/` holds real inputs with saved outputs, used to test the rules and tune
the report format.

- One directory per eval: `input.sql`, `plan.txt` (EXPLAIN text), `plan.json`
  (compiled plan JSON), `prompt.md`, `output.md`, `README.md`.
- `evals/run.sh evals/<dir>` runs the prompt through a fresh Claude session and
  saves the reply to `output.md`. It uses
  `claude -p ... --allowedTools Read Glob Grep` (read-only, headless, no context
  from other sessions).
- `prompt.md` is a minimal user message: it points at
  `skills/flink-sql-optimizer/SKILL.md` and links the input files. Do not copy
  the review protocol into it; the protocol lives in `SKILL.md`.
- The fresh session reads the input files itself. `plan.json` carries signals
  that the EXPLAIN text lacks (`"ttl" : "0 ms"`, `"bounded" : false`, duplicate
  output columns).

Plan artifacts (`plan.txt`, `plan.json`) are generated from a throwaway JUnit
test in the flink repo (`HappyFlinkExampleGenTest` in `flink-table-planner`):
`TableEnvironment.create(inStreamingMode())` with `'connector' = 'values'`
tables, `explainSql(q)` for `plan.txt`, and
`compilePlanSql("INSERT INTO sink " + q).asJsonString()` for `plan.json`.
`compilePlanSql` needs an `INSERT` into a sink, not a bare `SELECT`. The test is
not part of this repo.
