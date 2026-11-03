"""Publish a new supplementary Git observation after all thirty tools pass."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path.home() / '6893'
base = root / 'data/raw/github_git_history'
base.mkdir(parents=True, exist_ok=True)
with (base / '.refresh.lock').open('a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(0)
    result = subprocess.run([sys.executable, str(root / 'study/collect_git_history.py')],
                            text=True, stdout=subprocess.PIPE)
    print(result.stdout, end='')
    if result.returncode:
        raise SystemExit(result.returncode)
    summary = json.loads(result.stdout.strip().splitlines()[-1])
    run = Path(summary['run']).resolve()
    if not run.is_relative_to((base / 'runs').resolve()):
        raise SystemExit('Git run outside source archive')
    manifest = json.loads((run / 'manifest.json').read_text())
    if manifest['status'] != 'complete' or manifest['completed_tools'] != 30:
        raise SystemExit('Git source incomplete; previous latest retained')
    for name, expected in manifest['files'].items():
        path = (run / name).resolve()
        if not path.is_relative_to(run) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise SystemExit('Git source integrity failure; previous latest retained')
    temporary = base / '.latest.refresh'
    temporary.unlink(missing_ok=True)
    temporary.symlink_to('runs/' + run.name)
    os.replace(temporary, base / 'latest')
    print(json.dumps({'status': 'published_supplementary_only', 'run': str(run), 'primary_changed': False}))
