#!/usr/bin/env bash
set -euo pipefail
umask 077
export PATH=/usr/local/bin:/usr/bin:/bin
export OMP_NUM_THREADS=2
export OPENBLAS_NUM_THREADS=2
export MKL_NUM_THREADS=2
export NUMEXPR_NUM_THREADS=2
# The publisher refuses to advance latest until source-quality, model and Spark
# checks pass. A retrieval-complete snapshot alone is not publishable.
for source_quality_file in source_quality.py source_quality.json; do
  if [[ ! -f "$HOME/6893/study/$source_quality_file" ]]; then
    printf 'Missing required study source-quality artifact: %s\n' "$source_quality_file" >&2
    exit 1
  fi
done
exec "$HOME/6893/.venv-duckdb/bin/python" "$HOME/6893/study/publish_study.py" "$@"
