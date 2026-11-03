"""Resume public star collection within the provider's rate limits."""
import fcntl
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess
import sys

root = Path.home() / '6893'
data = root / 'data/raw/github_direct_stars'
data.mkdir(parents=True, exist_ok=True)
with (data / '.dispatcher.lock').open('a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(0)
    if shutil.disk_usage(data).free < 15 * 1024**3:
        raise SystemExit('Star collection stopped below 15 GiB free')
    backfill = data / 'backfill'
    manifest = backfill / 'collection_manifest.json'
    passed = manifest.exists() and json.loads(manifest.read_text()).get('status') == 'passed'
    output, extra = backfill, []
    if passed:
        observations = data / 'observations'
        observations.mkdir(exist_ok=True)
        incomplete = [path.parent for path in sorted(observations.glob('*/collection_manifest.json'))
                      if json.loads(path.read_text()).get('status') != 'passed']
        output = incomplete[0] if incomplete else observations / datetime.now(timezone.utc).strftime('%Y-%m-%d')
        current = output / 'collection_manifest.json'
        if current.exists() and json.loads(current.read_text()).get('status') == 'passed':
            raise SystemExit(0)
        extra = ['--recent-weeks', '4']
    result = subprocess.run([sys.executable, str(root / 'study/collect_github_stars.py'),
                             '--registry', str(root / 'config/tools.study.json'),
                             '--output', str(output), '--transport', 'urllib',
                             '--max-requests', '10', *extra])
    # The collector preserves explicit deferred/failed state and Retry-After.
    # A partial invocation is expected; only its manifest can claim completion.
    raise SystemExit(result.returncode)
