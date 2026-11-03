#!/usr/bin/env bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
export PYTHONPATH="$HOME/6893/pipeline:$HOME/6893/pipeline/full_scope"
export TMPDIR="$HOME/6893/data/.test-tmp"
request_cap="$("$HOME/6893/.venv-duckdb/bin/python" - <<'PY'
from pathlib import Path
import json
path=Path.home()/'6893/data/raw/github_direct_stars/backfill/collection_manifest.json'
value=json.loads(path.read_text()) if path.exists() else {}
print(4 if value.get('status') in {'complete','passed'} and value.get('tools_passed')==30 else 1)
PY
)"
exec timeout --signal=TERM --kill-after=20s 8m \
  "$HOME/6893/.venv-duckdb/bin/python" "$HOME/6893/pipeline/full_scope/collect_github_releases.py" \
  --project "$HOME/6893" --max-requests "$request_cap" --max-daily-requests 40 --execute
