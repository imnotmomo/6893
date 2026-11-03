"""Install only the project's resumable full-scope cron block on c0."""
from pathlib import Path
import subprocess

ROOT=Path.home()/'6893'
CODE=ROOT/'pipeline/full_scope'
SCHEDULES=[('*/15 * * * *','run-full-scope-downloads.sh','downloads'),
    ('2,17,32,47 * * * *','run-full-scope-metadata.sh','metadata'),
    ('7,37 * * * *','run-full-scope-materialize.sh','materialize'),
    ('9,24,39,54 * * * *','run-github-activity.sh','github'),
    ('11,26,41,56 * * * *','run-full-scope-public.sh','public'),
    ('13,28,43,58 * * * *','run-github-releases.sh','releases')]
BEGIN='# BEGIN EECS6893_FULL_SCOPE'
END='# END EECS6893_FULL_SCOPE'


def replace_block(current,entries):
    lines=[];inside=False
    for line in current.splitlines():
        if line==BEGIN:
            if inside:raise ValueError('Nested full-scope cron block')
            inside=True;continue
        if line==END:
            if not inside:raise ValueError('Unmatched full-scope cron end')
            inside=False;continue
        if not inside:lines.append(line)
    if inside:raise ValueError('Unclosed full-scope cron block')
    return '\n'.join(lines).rstrip()+'\n'+BEGIN+'\n'+'\n'.join(entries)+'\n'+END+'\n'


def main():
    for required in ['run_download_backfill.py','run_metadata_backfill.py','materialize_full_scope.py',
            'refresh_github_activity.py','refresh_full_scope.py','build_dependency_features_streaming.py',
            'collect_github_releases.py','archive_github_releases.py','build_github_release_features.py',
            'collect_github_activity_updates.py','dispatch_github_activity.py','native_pr_updates.py','github_activity_overlay.py','build_github_activity.py',
            'github_visible_cursor.py','github_visible_lists.py','github_jsonp_lists.py','github_pr_history.py']:
        if not (CODE/required).is_file():raise FileNotFoundError(CODE/required)
    logs=ROOT/'logs';logs.mkdir(parents=True,exist_ok=True);entries=[]
    for schedule,name,label in SCHEDULES:
        path=CODE/name
        if not path.is_file():raise FileNotFoundError(path)
        path.chmod(0o755)
        log=logs/('full-scope-'+label+'.log');log.touch(mode=0o600,exist_ok=True);log.chmod(0o600)
        entries.append(schedule+' '+str(path)+' >> '+str(log)+' 2>&1')
    current=subprocess.run(['crontab','-l'],capture_output=True,text=True)
    if current.returncode not in (0,1):raise RuntimeError('Could not read crontab')
    previous=logs/'full-scope-crontab-before.txt'
    if not previous.exists():previous.write_text(current.stdout);previous.chmod(0o600)
    subprocess.run(['crontab','-'],input=replace_block(current.stdout,entries),text=True,check=True)
    rotation=(str(logs/'full-scope-*.log')+' {\n    daily\n    rotate 7\n    maxsize 10M\n'
        '    compress\n    missingok\n    notifempty\n    copytruncate\n    su ubuntu ubuntu\n}\n')
    subprocess.run(['sudo','-n','tee','/etc/logrotate.d/6893-full-scope'],input=rotation,
        text=True,capture_output=True,check=True)
    subprocess.run(['sudo','-n','chmod','644','/etc/logrotate.d/6893-full-scope'],check=True)
    print('Installed six bounded full-scope workers; existing cron entries preserved.')


if __name__=='__main__':main()
