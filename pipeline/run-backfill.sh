#!/usr/bin/env bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
if [[ -f "$HOME/.config/6893/backfill.env" ]]; then
  source "$HOME/.config/6893/backfill.env"
fi
exec timeout --signal=TERM --kill-after=20s 12m \
  "$HOME/6893/.venv-duckdb/bin/python" "$HOME/6893/pipeline/backfill.py" \
  --max-tasks 60 --max-seconds 600
