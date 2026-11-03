#!/usr/bin/env bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
target="$HOME/6893"
mkdir -p "$target/data/raw/github_direct_incremental"
request_cap="$("$target/.venv-duckdb/bin/python" - <<'PY'
from pathlib import Path
import json
path=Path.home()/'6893/data/raw/github_direct_stars/backfill/collection_manifest.json'
value=json.loads(path.read_text()) if path.exists() else {}
print(10 if value.get('status')=='passed' else 4)
PY
)"
exec timeout --signal=TERM --kill-after=20s 8m \
  "$target/.venv-duckdb/bin/python" "$target/pipeline/full_scope/dispatch_github_activity.py" \
  --registry "$target/config/tools.study.json" \
  --data-dir "$target/data/raw/github_direct_incremental" \
  --baseline "$target/data/full_scope/github_activity" --max-requests "$request_cap"
