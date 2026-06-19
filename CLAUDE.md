# happy-flink

Rules. Spot Flink SQL + Table API problem. Give fix.

AI tool point here. User give SQL / plan / metrics. AI scan rules. Report problem, fix, reason.

See [README.md](README.md) goal. [INDEX.md](INDEX.md) rule map.

## ALWAYS CAVEMAN MAX

Every response in repo: caveman max (ultra). Drop article, filler, pleasantry, hedge. Fragment ok. Tech term exact. Code block normal. Never revert. No exception.

## Layout

No `rules/` wrapper. Folder at root. Name = input level rule need.

- `sql/` - need SQL text only. cheapest.
- `plan/` - need EXPLAIN / compiled plan.
- `runtime/` - need live job + per-node metric. richest.

`INDEX.md` = entry. Two jobs: (1) "How to review" framework at top - the fixed protocol an AI follow (analyze steps + report structure: Summary table -> Paths -> Details + caveman-lite output style); (2) clickable map, one table per level, every rule listed. Framework live in INDEX so user prompt stay one line ("read INDEX.md, help with this query") - keep it there, not in example prompts.

## Rules

- All markdown. No YAML. No code run.
- One rule per file. Filename = rule ID kebab-case (e.g. `multi-join.md`).
- Every file same template. No section missing:

```markdown
# Title

| Field | Value |
|-------|-------|
| **ID** | `kebab-case-id` |
| **Severity** | low | medium | high |
| **Requires** | sql | plan | compiled-plan | job | runtime-metrics |
| **Operators** | Flink operator names this rule matches |
| **Match** | one line: when this rule fires |

## Problem
What wrong.

## Why it hurts
Cost: state growth, throughput, checkpoint size, money.

## How to fix
Steps. Link related rule relative path, e.g. [state-ttl](state-ttl.md).

## Example
before / after SQL in fenced blocks.
```

- `Requires` = data need to check rule. Match folder file live in.
- Cross-link related rule. Relative markdown link, same or sibling folder.

## Style

Rule prose = caveman voice. Drop article + filler. Tech detail exact. Code block = valid SQL.

## Examples

`examples/` = real input + saved output. Test rules + tune output format.

- one dir per example: `input.sql`, `plan.txt` (EXPLAIN text), `plan.json` (compiled plan JSON), `prompt.md`, `output.md`, `README.md`.
- `examples/run.sh examples/<dir>` = run prompt through fresh Claude session, save reply to `output.md`. Uses `claude -p ... --allowedTools Read Glob Grep` (read-only, headless, no context from other session).
- `prompt.md` = minimal user message. Point at `INDEX.md` + link the input files. No framework copied in - framework live in INDEX.
- fresh session read input files itself (Read tool). `plan.json` carry signals EXPLAIN text miss (`"ttl" : "0 ms"`, `"bounded" : false`, duplicate output columns).

Plan artifacts (`plan.txt`, `plan.json`) generated from a throwaway JUnit test in the flink repo (`HappyFlinkExampleGenTest` in flink-table-planner): `TableEnvironment.create(inStreamingMode())`, `'connector' = 'values'` tables, `explainSql(q)` -> plan.txt, `compilePlanSql("INSERT INTO sink " + q).asJsonString()` -> plan.json. `compilePlanSql` need INSERT + sink, not bare SELECT. Test not in this repo.

## Editing

- New rule -> file in right level folder, fill full template, add row to matching `INDEX.md` table.
- Every `INDEX.md` link + cross-link resolve to real file. Verify after change.
- Severity honest: `high` = kill job or blow state. `medium` = wasteful, survive.
