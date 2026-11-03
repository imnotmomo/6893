#!/usr/bin/env bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
export PYTHONPATH="$HOME/6893/pipeline:$HOME/6893/pipeline/full_scope"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
exec timeout --signal=TERM --kill-after=20s 35m \
  "$HOME/6893/.venv-duckdb/bin/python" "$HOME/6893/pipeline/full_scope/refresh_full_scope.py" --execute
