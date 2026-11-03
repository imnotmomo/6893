#!/usr/bin/env bash
set -euo pipefail

# Run on the c0 VM, from a clone of this repository.
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
target="$HOME/6893"
python="$target/.venv-duckdb/bin/python"
if [[ ! -x "$python" ]]; then
  bash "$repo_root/duckdb/setup.sh"
fi
mkdir -p "$target/pipeline" "$target/config" "$target/logs" "$HOME/.config/6893"
chmod 700 "$target/logs" "$HOME/.config/6893"
if [[ "$repo_root" != "$target" ]]; then
  for name in backfill.py arxiv_backfill.py bigquery_sources.py run-backfill.sh verify_backfill.py requirements-backfill.lock.txt collect_pilot.py arxiv_source.py; do
    install -m 644 "$repo_root/pipeline/$name" "$target/pipeline/$name"
  done
  for name in backfill.json tools.backfill.json; do
    install -m 644 "$repo_root/config/$name" "$target/config/$name"
  done
fi
chmod 755 "$target/pipeline/run-backfill.sh"
"$python" -m pip install --only-binary=:all: -r "$target/pipeline/requirements-backfill.lock.txt"
"$python" "$target/pipeline/verify_backfill.py"
"$python" - <<'PY'
from pathlib import Path
import json,shlex,subprocess
root=Path.home()/'6893'
settings=json.loads((root/'config/backfill.json').read_text())
environment=Path.home()/'.config/6893/backfill.env'
if not environment.exists():
    environment.write_text('export BQ_PROJECT_ID='+shlex.quote(settings['bigquery_project'])+'\nexport GOOGLE_APPLICATION_CREDENTIALS='+shlex.quote(str(environment.parent/'gcp-collector.json'))+'\n')
    environment.chmod(0o600)
log=root/'logs/backfill.log';log.touch(mode=0o600,exist_ok=True);log.chmod(0o600)
p=subprocess.run(['crontab','-l'],capture_output=True,text=True)
if p.returncode not in (0,1): raise RuntimeError(p.stderr)
begin='# BEGIN EECS6893_BACKFILL';end='# END EECS6893_BACKFILL'
lines=[];skip=False
for line in p.stdout.splitlines():
    if line==begin:skip=True;continue
    if line==end:skip=False;continue
    if not skip:lines.append(line)
entry='*/15 * * * * /home/ubuntu/6893/pipeline/run-backfill.sh >> /home/ubuntu/6893/logs/backfill.log 2>&1'
contents='\n'.join(lines).rstrip()+'\n'+begin+'\n'+entry+'\n'+end+'\n'
subprocess.run(['crontab','-'],input=contents,text=True,check=True)
print('Installed the 15-minute dispatcher. Google credentials remain in the private config directory outside Git.')
PY
sudo -n tee /etc/logrotate.d/6893-backfill >/dev/null <<'LOGROTATE'
/home/ubuntu/6893/logs/backfill.log {
    daily
    rotate 7
    maxsize 10M
    compress
    missingok
    notifempty
    copytruncate
    su ubuntu ubuntu
}
LOGROTATE
sudo -n chmod 644 /etc/logrotate.d/6893-backfill
systemctl is-active cron
