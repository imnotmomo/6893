#!/usr/bin/env python3
"""Collect surviving default-branch Git commit metadata, without GitHub events.

This is a separate retrospective source. It cannot establish when a commit was
publicly observable, recover rewritten/deleted history, or reconstruct pushes.
No GitHub credentials, REST requests, BigQuery jobs or model fitting are used.
"""
import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid


ROOT=Path.home()/'6893'
SOURCE='github_git_default_branch'
PROFILE='retrospective_current_default_branch'
HEX=re.compile(r'^[0-9a-f]{40}(?:[0-9a-f]{24})?$')
REPO=re.compile(r'^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')
TOOL=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')
UTC=timezone.utc
WEEK_FIELDS=['tool_id','week_start_utc','window_start_utc','window_end_exclusive_utc',
             'git_reachable_commit_count','git_first_parent_commit_count',
             'reachable_coverage_days','first_parent_coverage_days','complete_utc_week',
             'historical_public_availability','primary_eligible']


def stamp(value=None):
    if value is None:return datetime.now(UTC)
    parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('An explicit timezone is required')
    return parsed.astimezone(UTC)


def iso(value):return value.astimezone(UTC).isoformat()


def digest(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):result.update(chunk)
    return result.hexdigest()


def write_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():raise FileExistsError('Immutable artifact already exists: '+str(path))
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('x') as stream:
            json.dump(value,stream,sort_keys=True,indent=2,allow_nan=False);stream.write('\n')
            stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:temporary.unlink(missing_ok=True)


def git_environment():
    # Public HTTPS only. Ignore local credential helpers, extra auth headers and
    # environment-injected Git configuration; do not inspect any credential file.
    env={key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0',GIT_ASKPASS='/bin/false',SSH_ASKPASS='/bin/false',
               GIT_OPTIONAL_LOCKS='0',GIT_NO_REPLACE_OBJECTS='1',LC_ALL='C')
    return env


def disk_bytes(root):
    total=0
    for directory,_,files in os.walk(root,followlinks=False):
        for name in files:
            path=Path(directory)/name
            if path.is_symlink():continue
            try:total+=path.stat().st_size
            except FileNotFoundError:continue
    return total


def guard(root,minimum_free_gib,max_run_gib):
    if shutil.disk_usage(root).free<minimum_free_gib*1024**3:
        raise RuntimeError('Free disk space fell below the configured guard')
    if disk_bytes(root)>max_run_gib*1024**3:
        raise RuntimeError('Collection run exceeded the configured disk cap')


def stop(process):
    if process.poll() is not None:return
    os.killpg(process.pid,signal.SIGTERM)
    try:process.wait(timeout=3)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()


def command(args,deadline,env,stdout=None,log=None,check_guard=None):
    """All commands for one tool share the same deadline and disk budget."""
    if time.monotonic()>=deadline:raise TimeoutError('Tool collection deadline elapsed')
    capture=stdout is None
    # Small Git metadata queries are bounded; only git log uses a disk stream.
    error_file=tempfile.TemporaryFile() if not capture and log is None else None
    process=subprocess.Popen(args,stdout=subprocess.PIPE if capture else stdout,
                             stderr=log or error_file or subprocess.PIPE,env=env,start_new_session=True)
    try:
        if capture:
            out,err=process.communicate(timeout=max(0.01,deadline-time.monotonic()))
        else:
            while process.poll() is None:
                if time.monotonic()>=deadline:raise TimeoutError('Tool collection exceeded its timeout')
                if check_guard:check_guard()
                time.sleep(0.2)
            out,err=b'',b''
            if error_file:
                error_file.seek(0);err=error_file.read()[-1000:]
        if process.returncode:
            detail=(err or b'').decode('utf-8','replace')[-1000:]
            raise RuntimeError('Git command failed with exit '+str(process.returncode)+': '+detail)
        if check_guard:check_guard()
        return out.decode('utf-8','strict').strip() if capture else None
    except BaseException:
        stop(process);raise
    finally:
        for stream in (process.stdout,process.stderr,error_file):
            if stream is not None:stream.close()


def git_args(repository,*args):
    return ['git','--no-pager','--git-dir='+str(repository),*args]


def epoch_date(value):
    try:return datetime.fromtimestamp(value,UTC).date()
    except (OverflowError,OSError,ValueError):return None


def parse_log(path,check_guard=None):
    records={}
    with Path(path).open() as stream:
        for index,line in enumerate(stream):
            if check_guard and index%4096==0:check_guard()
            fields=line.rstrip('\n').split('\t')
            if len(fields)!=4:raise ValueError('Malformed Git metadata row')
            sha,committer,author,parent_text=fields
            parents=parent_text.split() if parent_text else []
            if not HEX.fullmatch(sha) or any(not HEX.fullmatch(parent) for parent in parents):
                raise ValueError('Invalid commit or parent SHA')
            if sha in records:raise ValueError('Duplicate commit SHA')
            try:committer,author=int(committer),int(author)
            except ValueError as error:raise ValueError('Invalid Git timestamp') from error
            records[sha]={'sha':sha,'committer_unix':committer,'author_unix':author,'parents':parents}
    if not records:raise ValueError('Empty commit history')
    return records


def verify_graph(records,tip,check_guard=None):
    if tip not in records:raise ValueError('Frozen tip is absent from the commit metadata')
    for index,record in enumerate(records.values()):
        if check_guard and index%4096==0:check_guard()
        if any(parent not in records for parent in record['parents']):
            raise ValueError('Commit parent is absent; history is not a complete reachable graph')
    visited=set();active=set();stack=[(tip,False)];steps=0
    while stack:
        if check_guard and steps%4096==0:check_guard()
        steps+=1
        sha,leaving=stack.pop()
        if leaving:active.remove(sha);visited.add(sha);continue
        if sha in active:raise ValueError('Commit graph contains a cycle')
        if sha in visited:continue
        active.add(sha);stack.append((sha,True))
        stack.extend((parent,False) for parent in records[sha]['parents'])
    if visited!=set(records):raise ValueError('Parsed metadata contains commits unreachable from the frozen tip')
    first_parent=set();current=tip
    while current:
        if check_guard and len(first_parent)%4096==0:check_guard()
        if current in first_parent:raise ValueError('First-parent chain contains a cycle')
        first_parent.add(current)
        parents=records[current]['parents'];current=parents[0] if parents else None
    return first_parent


def annotate(records,first_parent,observed_at,check_guard=None):
    summary={'author_after_committer':0,'author_after_observed':0,'committer_after_observed':0,
             'child_committer_before_parent':0,'unrepresentable_committer_date':0,'unrepresentable_author_date':0}
    observed_unix=int(observed_at.timestamp())
    for index,record in enumerate(records.values()):
        if check_guard and index%4096==0:check_guard()
        flags=[]
        for name,test in [('author_after_committer',record['author_unix']>record['committer_unix']),
                          ('author_after_observed',record['author_unix']>observed_unix),
                          ('committer_after_observed',record['committer_unix']>observed_unix),
                          ('unrepresentable_committer_date',epoch_date(record['committer_unix']) is None),
                          ('unrepresentable_author_date',epoch_date(record['author_unix']) is None)]:
            if test:flags.append(name);summary[name]+=1
        inversions=[parent for parent in record['parents'] if record['committer_unix']<records[parent]['committer_unix']]
        if inversions:flags.append('child_committer_before_parent');summary['child_committer_before_parent']+=len(inversions)
        record.update(first_parent=record['sha'] in first_parent,date_anomaly_flags=flags,
                      parent_time_inversions=inversions)
    return summary


def weekly_rows(tool_id,records,first_parent,start,end):
    """UTC committer-date counts; no zero is filled before retained history."""
    dated=[(record,epoch_date(record['committer_unix'])) for record in records.values()]
    first_all=min((day for _,day in dated if day is not None),default=None)
    first_main=min((day for record,day in dated if day is not None and record['sha'] in first_parent),default=None)
    all_counts={};main_counts={}
    for record,day in dated:
        if day is None or not start<=day<end:continue
        week=day-timedelta(days=day.weekday())
        all_counts[week]=all_counts.get(week,0)+1
        if record['sha'] in first_parent:main_counts[week]=main_counts.get(week,0)+1
    rows=[];week=start-timedelta(days=start.weekday())
    while week<end:
        lower=max(start,week);upper=min(end,week+timedelta(days=7))
        def covered(first):return max(0,(upper-max(lower,first)).days) if first is not None else 0
        all_days,main_days=covered(first_all),covered(first_main)
        rows.append({'tool_id':tool_id,'week_start_utc':str(week),'window_start_utc':str(lower),
                     'window_end_exclusive_utc':str(upper),
                     'git_reachable_commit_count':all_counts.get(week,0) if all_days else None,
                     'git_first_parent_commit_count':main_counts.get(week,0) if main_days else None,
                     'reachable_coverage_days':all_days,'first_parent_coverage_days':main_days,
                     'complete_utc_week':all_days==main_days==7,
                     'historical_public_availability':'unknown','primary_eligible':False})
        week+=timedelta(days=7)
    return rows


def inspect_repository(repository,metadata_path,deadline,env,observed_at,check_guard=None):
    shallow=command(git_args(repository,'rev-parse','--is-shallow-repository'),deadline,env,check_guard=check_guard)
    if shallow!='false':raise ValueError('Shallow repositories are not eligible for historical collection')
    tip=command(git_args(repository,'rev-parse','HEAD'),deadline,env,check_guard=check_guard)
    if not HEX.fullmatch(tip):raise ValueError('Invalid frozen HEAD SHA')
    branch=command(git_args(repository,'symbolic-ref','--short','HEAD'),deadline,env,check_guard=check_guard)
    total=int(command(git_args(repository,'rev-list','--count',tip),deadline,env,check_guard=check_guard))
    main_count=int(command(git_args(repository,'rev-list','--first-parent','--count',tip),deadline,env,check_guard=check_guard))
    with Path(metadata_path).open('xb') as output:
        command(git_args(repository,'log',tip,'--no-show-signature','--no-notes','--no-decorate',
                         '--format=%H%x09%ct%x09%at%x09%P'),deadline,env,stdout=output,check_guard=check_guard)
    records=parse_log(metadata_path,check_guard)
    if len(records)!=total:raise ValueError('Parsed metadata count differs from git rev-list count')
    first_parent=verify_graph(records,tip,check_guard)
    if len(first_parent)!=main_count:raise ValueError('Parsed first-parent chain differs from git rev-list count')
    anomalies=annotate(records,first_parent,observed_at,check_guard)
    return records,first_parent,{'tip_sha':tip,'default_branch_at_collection':branch,
                                'reachable_commits':total,'first_parent_commits':main_count,
                                'shallow':False,'date_anomalies':anomalies}


def write_commits(path,records,check_guard=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():raise FileExistsError('Immutable artifact already exists')
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('xb') as raw:
            with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as zipped:
                for index,sha in enumerate(sorted(records)):
                    if check_guard and index%4096==0:check_guard()
                    zipped.write((json.dumps(records[sha],sort_keys=True,separators=(',',':'))+'\n').encode())
            raw.flush();os.fsync(raw.fileno())
        os.replace(temporary,path)
    finally:temporary.unlink(missing_ok=True)


def collect_tool(tool,run,start,end,registry_sha,args):
    tid=tool['tool_id'];repo=tool['github_repo'];url='https://github.com/'+repo+'.git'
    if not TOOL.fullmatch(tid) or not REPO.fullmatch(repo) or any(part in {'.','..'} for part in repo.split('/')):
        raise ValueError('Invalid frozen tool/repository identifier')
    work=run/'.work'/tid;work.mkdir(parents=True,exist_ok=False)
    clone=work/'repository.git';metadata=work/'commits.tsv';deadline=time.monotonic()+args.timeout_seconds
    env=git_environment()
    def budget():
        if time.monotonic()>=deadline:raise TimeoutError('Tool collection exceeded its timeout')
        guard(run,args.minimum_free_gib,args.max_disk_gib)
    started=stamp();log_path=run/'logs'/(tid+'.log');log_path.parent.mkdir(exist_ok=True)
    try:
        with log_path.open('xb') as log:
            command(['git','-c','credential.helper=','-c','http.extraHeader=','-c','protocol.version=2',
                     'clone','--bare','--single-branch','--no-tags','--filter=tree:0',url,str(clone)],
                    deadline,env,stdout=log,log=log,check_guard=budget)
        observed=stamp()
        clone_seconds=(observed-started).total_seconds()
        records,first_parent,evidence=inspect_repository(clone,metadata,deadline,env,observed,budget)
        rows=weekly_rows(tid,records,first_parent,start,end)
        budget();output=run/'commits'/(tid+'.jsonl.gz');write_commits(output,records,budget);budget()
        metadata.unlink()
        info={'tool_id':tid,'github_repo':repo,'github_repo_id':tool.get('github_repo_id'),
              'repository_url':url,'registry_sha256':registry_sha,'started_at':iso(started),
              'observed_at':iso(observed),'finished_at':iso(stamp()),'status':'complete',
              'source':SOURCE,'profile':PROFILE,'primary_eligible':False,
              'historical_public_availability':'unknown','retrospective':True,
              'date_basis':'committer_unix converted to UTC; author timestamps retained separately',
              'history_start':str(start),'history_end_exclusive':str(end),
              'metadata_path':str(output.relative_to(run)),'metadata_sha256':digest(output),
              'metadata_compressed_bytes':output.stat().st_size,'weekly_rows':len(rows),
              'requested_clone_filter':'tree:0','clone_bytes':disk_bytes(clone),
              'clone_seconds':clone_seconds,'total_elapsed_seconds':(stamp()-started).total_seconds(),
              'in_range_reachable_commits':sum(row['git_reachable_commit_count'] or 0 for row in rows),
              'in_range_first_parent_commits':sum(row['git_first_parent_commit_count'] or 0 for row in rows),**evidence}
        if args.keep_clones:info['retained_clone_path']=str(clone.relative_to(run))
        write_json(run/'tool_manifests'/(tid+'.json'),info)
        return info,rows
    finally:
        if not args.keep_clones:shutil.rmtree(work,ignore_errors=True)


def collect(args):
    registry_path=args.registry.expanduser().resolve();raw=registry_path.read_bytes()
    registry=json.loads(raw);tools=registry.get('tools',[])
    if not tools or len({tool['tool_id'] for tool in tools})!=len(tools):raise ValueError('Missing or duplicate frozen tools')
    if args.tool:
        selected=set(args.tool);tools=[tool for tool in tools if tool['tool_id'] in selected]
        if {tool['tool_id'] for tool in tools}!=selected:raise ValueError('Requested tool is absent from the registry')
    launched=stamp();asof=stamp(args.as_of) if args.as_of else launched;start=date.fromisoformat(args.start)
    if asof>launched:raise ValueError('An as-of time in the future is not a completed observation')
    end=date.fromisoformat(args.end_exclusive) if args.end_exclusive else asof.date()
    if not start<end<=asof.date():raise ValueError('Use a nonempty range ending no later than the last complete UTC day')
    identifier=stamp().strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    run=(args.output or args.data_dir.expanduser()/'raw/github_git_history/runs'/identifier).expanduser().resolve()
    run.parent.mkdir(parents=True,exist_ok=True);run.mkdir(exist_ok=False)
    guard(run,args.minimum_free_gib,args.max_disk_gib)
    (run/'tools.study.json').write_bytes(raw);registry_sha=hashlib.sha256(raw).hexdigest()
    outcomes=[];weekly=[]
    for tool in tools:
        try:
            guard(run,args.minimum_free_gib,args.max_disk_gib)
            result,rows=collect_tool(tool,run,start,end,registry_sha,args);weekly.extend(rows)
        except Exception as error:
            result={'tool_id':tool['tool_id'],'github_repo':tool.get('github_repo'),'status':'failed',
                    'error':type(error).__name__+': '+str(error),'at':iso(stamp()),'primary_eligible':False}
        outcomes.append(result)
        print(json.dumps({'phase':'git_history','tool_id':tool['tool_id'],'status':result['status'],
                          'reachable_commits':result.get('reachable_commits'),'error':result.get('error')},sort_keys=True),flush=True)
    csv_path=run/'weekly_git_history.csv'
    with csv_path.open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=WEEK_FIELDS);writer.writeheader();writer.writerows(weekly)
        stream.flush();os.fsync(stream.fileno())
    complete=all(result['status']=='complete' for result in outcomes)
    manifest={'schema_version':1,'source':SOURCE,'profile':PROFILE,'run_id':run.name,
              'status':'complete' if complete else 'partial_failed','created_at':iso(stamp()),'as_of':iso(asof),
              'history_start':str(start),'history_end_exclusive':str(end),'registry_sha256':registry_sha,
              'requested_tools':len(tools),'completed_tools':sum(result['status']=='complete' for result in outcomes),
              'primary_eligible':False,'historical_public_availability':'unknown','retrospective':True,
              'no_bigquery_queries':True,'no_github_credentials':True,'clones_retained':args.keep_clones,
              'limits':{'per_tool_timeout_seconds':args.timeout_seconds,'minimum_free_gib':args.minimum_free_gib,'maximum_run_gib':args.max_disk_gib,'clone_concurrency':1},
              'measurement':'Unique surviving commits reachable from the current default-branch tip, and its first-parent chain, grouped by UTC committer date. Neither is a GitHub PushEvent count.',
              'limitations':['Current reachable history can omit deleted branches, force-pushed commits and removed repositories.',
                             'Author and committer dates are supplied by commit creators and may be reordered, backdated or future-dated; anomaly counts are retained.',
                             'Commit date does not certify its historical public availability or the time it joined the default branch. No real-time vintage is reconstructed.',
                             'First-parent counts describe surviving mainline commits under the current graph; merge strategies differ between projects.',
                             'Null counts before the first retained commit are unknown; zero later means no matching surviving commit by this date definition, not no development.',
                             'This independent retrospective source is excluded from existing primary forecasting recipes.'],
              'tools':outcomes,'files':{str(path.relative_to(run)):digest(path) for path in sorted(run.rglob('*')) if path.is_file()}}
    write_json(run/'manifest.json',manifest)
    print(json.dumps({'status':manifest['status'],'run':str(run),'completed_tools':manifest['completed_tools'],'requested_tools':len(tools)},sort_keys=True),flush=True)
    return 0 if complete else 1


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry',type=Path,default=ROOT/'config/tools.study.json')
    parser.add_argument('--data-dir',type=Path,default=ROOT/'data')
    parser.add_argument('--output',type=Path,help='New immutable run directory; it must not exist')
    parser.add_argument('--tool',action='append',help='Optional bounded pilot tool ID; repeat for a subset')
    parser.add_argument('--start',default='2023-01-01')
    parser.add_argument('--end-exclusive',help='UTC date, no later than the as-of UTC date')
    parser.add_argument('--as-of',help='Explicit timezone timestamp; defaults to launch time')
    parser.add_argument('--timeout-seconds',type=int,default=120)
    parser.add_argument('--minimum-free-gib',type=float,default=15)
    parser.add_argument('--max-disk-gib',type=float,default=2)
    parser.add_argument('--keep-clones',action='store_true',help='Retain graphs within the same total run disk cap')
    args=parser.parse_args()
    if not 1<=args.timeout_seconds<=120:parser.error('Timeout must be between1 and120 seconds per tool')
    if args.minimum_free_gib<15 or not 0<args.max_disk_gib<=2:parser.error('Keep at least15 GiB free and cap this run at2 GiB or less')
    try:return collect(args)
    except Exception as error:
        print(json.dumps({'status':'failed','error':type(error).__name__+': '+str(error)}),file=sys.stderr);return 1


if __name__=='__main__':raise SystemExit(main())
