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

`INDEX.md` = entry. Clickable map. One table per level. Every rule listed.

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

## Editing

- New rule -> file in right level folder, fill full template, add row to matching `INDEX.md` table.
- Every `INDEX.md` link + cross-link resolve to real file. Verify after change.
- Severity honest: `high` = kill job or blow state. `medium` = wasteful, survive.
