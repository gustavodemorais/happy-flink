#!/usr/bin/env bash
# Run one example through a fresh Claude session and save the reply.
#
# Usage: ./examples/run.sh examples/<example-dir>
#
# Runs from the repo root so the fresh session can read INDEX.md and the rule
# files. `claude -p` is headless and carries no context from any other session.
set -euo pipefail

dir="${1:?usage: ./examples/run.sh examples/<example-dir>}"
prompt="$dir/prompt.md"
out="$dir/output.md"

[ -f "$prompt" ] || { echo "no prompt at $prompt" >&2; exit 1; }

claude -p "$(cat "$prompt")" --allowedTools Read Glob Grep > "$out"
echo "wrote $out"
