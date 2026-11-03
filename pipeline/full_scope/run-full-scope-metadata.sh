#!/usr/bin/env bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
if [[ -f "$HOME/.config/6893/backfill.env" ]]; then
  source "$HOME/.config/6893/backfill.env"
fi
export PYTHONPATH="$HOME/6893/pipeline:$HOME/6893/pipeline/full_scope"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
exec timeout --signal=TERM --kill-after=20s 60m \
  "$HOME/6893/.venv-duckdb/bin/python" "$HOME/6893/pipeline/full_scope/run_metadata_backfill.py" \
  --max-seconds 600
