#!/usr/bin/env bash
# Run one eval through a fresh Claude session and save the reply.
#
# Usage: ./evals/run.sh evals/<eval-dir>
#
# Runs from the repo root so the fresh session can read
# skills/flink-sql-table-api-optimizer/SKILL.md and the rule files under its references/.
# `claude -p` is headless and carries no context from any other session.
set -euo pipefail

dir="${1:?usage: ./evals/run.sh evals/<eval-dir>}"
prompt="$dir/prompt.md"
out="$dir/output.md"

[ -f "$prompt" ] || { echo "no prompt at $prompt" >&2; exit 1; }

claude -p "$(cat "$prompt")" --allowedTools Read Glob Grep > "$out"
echo "wrote $out"
