#!/usr/bin/env python3
"""Materialize completed local exports; this program never submits cloud work.

Read-only plan by default. --execute builds a rich-download version when its
completed receipts change and either the frozen history is covered or six hours
have elapsed since the previous partial build. Changed-source spines publish at
most hourly. Every publication uses a new immutable directory and atomic symlink.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid

UTC=timezone.utc
RICH_BUILD_TIMEOUT_SECONDS=7200
MATERIALIZE_MAXIMUM_RUNTIME_SECONDS=8100


def read(path):return json.loads(Path(path).read_text()) if Path(path).is_file() else {}
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def signature(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def instant(value):
    result=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)
def save(path,value):
    path=Path(path);temp=path.with_name(path.name+'.tmp')
    temp.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');temp.replace(path)


def receipt_inventory(root,start,end):
    """Cheap stability gate; the builder independently hashes and parses bodies."""
    inventory=[];covered=set();left_bound=date.fromisoformat(start);right_bound=date.fromisoformat(end)
    for plan_path in sorted(Path(root).rglob('query_plan.json')):
        plan=read(plan_path)
        if plan.get('source')!='pypi_download_dimensions':continue
        left=date.fromisoformat(plan['start']);right=date.fromisoformat(plan['end_exclusive'])
        if right<=left_bound or left>=right_bound:continue
        folder=plan_path.parent;manifest=read(folder/'manifest.json');receipt=read(folder/'completion_receipt.json')
        if manifest.get('status')!='complete':continue
        if (receipt.get('all_pages_fetched') is not True or receipt.get('job_complete') is not True
                or manifest.get('all_pages_fetched') is not True or manifest.get('job_complete') is not True
                or receipt.get('query_sha256')!=plan.get('query_sha256')
                or manifest.get('query_sha256')!=plan.get('query_sha256')
                or receipt.get('total_rows')!=manifest.get('rows_fetched')
                or receipt.get('total_rows')!=manifest.get('total_rows')
                or not isinstance(receipt.get('total_rows'),int) or isinstance(receipt.get('total_rows'),bool)
                or receipt['total_rows']<0 or not (folder/'decoded_records.jsonl.gz').is_file()
                or not receipt.get('output_sha256')):
            raise ValueError('Completed local query has an inconsistent receipt: '+str(folder))
        inventory.append({'path':str(folder.resolve()),'plan_sha256':sha(plan_path),
            'manifest_sha256':sha(folder/'manifest.json'),'receipt_sha256':sha(folder/'completion_receipt.json'),
            'query_sha256':plan['query_sha256'],'input_sha256':receipt['output_sha256'],
            'start':str(left),'end_exclusive':str(right)})
        covered.update(left+timedelta(days=n) for n in range((right-left).days)
                       if left_bound<=left+timedelta(days=n)<right_bound)
    return inventory,len(covered)==(right_bound-left_bound).days


def latest_git(data,cohort):
    candidates=[]
    for path in (data/'raw/github_git_contributors/runs').glob('*/manifest.json'):
        body=read(path)
        if (body.get('status')=='complete' and body.get('registry_sha256')==cohort
                and body.get('requested_tools')==30 and body.get('completed_tools')==30):
            candidates.append((instant(body['created_at']),path.parent))
    return max(candidates,default=(None,None),key=lambda pair:pair[0])[1]


def sources(project,github_api=None):
    data=project/'data';base=data/'full_scope';cohort=sha(project/'config/tools.study.json')
    rich=base/'download_dimensions'
    if not rich.exists():rich=base/'download_dimension_features'
    research=data/'raw/research_v1/latest'
    if not research.exists():research=data/'raw/research_v1/backfill'
    depsdev=data/'raw/depsdev/latest'
    if not depsdev.exists():depsdev=data/'raw/depsdev/full-scope-initial'
    github=base/'github_activity';updated=base/'github_activity_updated'
    # A newly published baseline must not be hidden by an older overlay while
    # the next unpaid materialization is still pending.
    if (github/'manifest.json').is_file() and (updated/'manifest.json').is_file():
        try:
            overlay_manifest=read(updated/'manifest.json');quality_path=updated/'source_quality.json';overlay_quality=read(quality_path)
            if (overlay_manifest.get('source')==overlay_quality.get('source')=='github_direct_issues_pulls'
                    and overlay_manifest.get('registry_sha256')==overlay_quality.get('registry_sha256')==cohort
                    and overlay_manifest.get('files',{}).get('source_quality.json')==sha(quality_path)
                    and overlay_quality.get('baseline_manifest_sha256')==sha(github/'manifest.json')):github=updated
        except (ValueError,TypeError,AttributeError,OSError):pass
    return {'research':research,'git':latest_git(data,cohort),
        'dependency':base/'dependency_history','downloads':rich,'github_api':github_api if github_api is not None else github,
        'stars':data/'raw/github_direct_stars/backfill','depsdev':depsdev,'releases':base/'github_releases'}


def checkpoint_signature(project,inputs):
    payload={'primary':sha(project/'data/study/latest/study_manifest.json')}
    assembler=Path(__file__).resolve().parent/'assemble_full_scope.py'
    payload['assembler_sha256']=sha(assembler) if assembler.is_file() else None
    names={'research':'collection_manifest.json','git':'manifest.json','dependency':'manifest.json',
        'downloads':'manifest.json','github_api':'manifest.json','stars':'collection_manifest.json','depsdev':'manifest.json','releases':'manifest.json'}
    for kind,path in inputs.items():
        candidate=Path(path)/names[kind] if path else None
        payload[kind]={'path':str(candidate.resolve()),'sha256':sha(candidate)} if candidate and candidate.is_file() else None
    return signature(payload)


def github_overlay_recipe(project,state,now):
    project=Path(project);base=project/'data/full_scope';baseline=base/'github_activity';raw=project/'data/raw/github_direct_incremental'
    state_path=raw/'activity_update_state.json'
    if not state_path.is_file():state_path=raw/'refresh_state.json'
    if not (baseline/'manifest.json').is_file() or not state_path.is_file() and not (raw/'native_pr_update_state.json').is_file():return {'build':False,'reason':'awaiting_baseline_or_update_state'}
    body=read(state_path);cycles=body.get('completed_cycles',[]);native=read(raw/'native_pr_update_state.json');native_cycles=native.get('completed_cycles',[])
    if not cycles and not native_cycles and not (base/'github_activity_updated').exists():return {'build':False,'reason':'no_completed_update_cycles'}
    references={name for cycle in cycles for name in cycle.get('receipt_paths',[])}
    references.update(d['raw_path'] for d in body.get('completed_details',{}).values())
    receipts=[r for r in body.get('requests',[]) if r['raw_path'] in references]
    native_references={name for cycle in native_cycles for name in cycle.get('receipt_paths',[])}|{c['identity_receipt_path'] for c in native_cycles if c.get('identity_receipt_path')}
    native_receipts=[r for r in native.get('requests',[]) if r['raw_path'] in native_references]
    code=Path(__file__).resolve().parent
    known_end=max([read(baseline/'manifest.json').get('history_end_exclusive') or body.get('baseline_end_exclusive') or now.date().isoformat(),*[c['end_exclusive'] for c in cycles if c.get('end_exclusive')],*[c['end_exclusive'] for c in native_cycles if c.get('end_exclusive')]],key=instant)
    end=min(instant(known_end).date().isoformat(),now.date().isoformat(),'2027-01-01')+'T00:00:00Z'
    binding={'baseline':sha(baseline/'manifest.json'),'cohort':body.get('registry_sha256') or native.get('registry_sha256'),'cycles':cycles,'details':body.get('completed_details',{}),'receipts':receipts,'native_cycles':native_cycles,'native_receipts':native_receipts,'end_exclusive':end,
        'code':{n:sha(code/n) if (code/n).is_file() else None for n in ['github_activity_overlay.py','build_github_activity.py','refresh_github_activity.py','native_pr_updates.py']}}
    fingerprint=signature(binding)
    return {'build':fingerprint!=state.get('github_overlay_signature'),'signature':fingerprint,
        'baseline':str(baseline),'incremental_root':str(raw),'end_exclusive':end,'completed_cycles':len(cycles)}


def plan(project,state,now,github_api=None):
    project=Path(project).resolve();data=project/'data';base=data/'full_scope';inputs=sources(project,github_api)
    status=read(data/'study/latest/study_status.json');end=instant(status['as_of']).date().isoformat()
    inventory,complete=receipt_inventory(data/'raw/full_scope/bigquery','2023-01-01',end)
    rich=inputs['downloads'];quality=read(rich/'download_dimension_quality.json')
    dependency=inputs['dependency']/'manifest.json'
    code=Path(__file__).resolve().parent/'build_download_dimension_features.py'
    binding={'receipts':inventory,'primary':sha(data/'study/latest/study_manifest.json'),
        'dependency':sha(dependency) if dependency.is_file() else None,'builder':sha(code),'end_exclusive':end}
    coverage_helper=code.with_name('download_split_coverage.py')
    binding['coverage_classifier']=sha(coverage_helper) if coverage_helper.is_file() else None
    current=signature(binding)
    previous=state.get('rich_signature')
    # Adopt an independently completed first build rather than duplicate it.
    if previous is None and quality:
        actual={(q['path'],q['query_sha256'],q['receipt_sha256'],q['input_sha256']) for q in quality.get('queries',[])}
        expected={(q['path'],q['query_sha256'],q['receipt_sha256'],q['input_sha256']) for q in inventory}
        known_builder=quality.get('code_sha256',{}).get(code.name)
        release=quality.get('release_history_binding') or {}
        if (actual==expected and quality.get('end_exclusive')==end and known_builder==sha(code)
                and release.get('manifest_sha256')==binding['dependency']
                and quality.get('code_sha256',{}).get('download_split_coverage.py')==binding['coverage_classifier']):previous=current
    built_at=state.get('rich_built_at') or quality.get('built_at')
    partial_due=not built_at or now-instant(built_at)>=timedelta(hours=6)
    build=bool(inventory) and current!=previous and (complete or partial_due)
    overlay=github_overlay_recipe(project,state,now) if github_api is None else {'build':False,'reason':'explicit_source_override'}
    checkpoint=checkpoint_signature(project,inputs)
    last_assembly=state.get('assembled_at')
    assembly_due=(not last_assembly or now-instant(last_assembly)>=timedelta(hours=1))
    return {'paid_queries_enabled':False,'source_inputs':{k:str(v) if v else None for k,v in inputs.items()},
        'completed_query_receipts':len(inventory),'history_covered':complete,'end_exclusive':end,
        'build_rich_features':build,'rich_signature':current,'adopt_existing_rich_signature':previous==current,
        'rich_partial_interval_hours':6,'rich_resource_profile':{'memory_limit':'3GB','threads':1,'max_temp_gib':10,'timeout_seconds':RICH_BUILD_TIMEOUT_SECONDS},'assemble_changed_sources':assembly_due and checkpoint!=state.get('spine_signature'),
        'source_checkpoint_signature':checkpoint,'github_overlay':overlay,'maximum_runtime_seconds':MATERIALIZE_MAXIMUM_RUNTIME_SECONDS}


def atomic_link(link,target):
    link=Path(link);target=Path(target).resolve()
    if link.exists() and not link.is_symlink():raise ValueError('Canonical publication path is not a symlink: '+str(link))
    temporary=link.with_name('.'+link.name+'.next-'+uuid.uuid4().hex[:8])
    temporary.symlink_to(target,target_is_directory=True);os.replace(temporary,link)


def bounded(command,timeout,log,env):
    with Path(log).open('wb') as stream:
        process=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,env=env)
        def stop_group():
            if process.poll() is not None:return
            try:os.killpg(process.pid,signal.SIGTERM)
            except ProcessLookupError:return
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:os.killpg(process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                process.wait()
        def relay(signum,frame):
            stop_group();raise SystemExit(128+signum)
        prior={s:signal.signal(s,relay) for s in [signal.SIGTERM,signal.SIGINT]}
        try:
            try:return process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:stop_group();return 124
        finally:
            for signum,handler in prior.items():signal.signal(signum,handler)


def execute(project,github_api=None):
    project=Path(project).resolve();data=project/'data';base=data/'full_scope';base.mkdir(parents=True,exist_ok=True)
    with (data/'.full-scope-materialize.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'skipped_materializer_busy','paid_queries_enabled':False}
        state_path=base/'materialize_state.json';state=read(state_path);now=datetime.now(UTC)
        recipe=plan(project,state,now,github_api)
        code=Path(__file__).resolve().parent;run=now.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        if recipe['adopt_existing_rich_signature']:state['rich_signature']=recipe['rich_signature']
        if recipe['build_rich_features']:
            output=base/'download_feature_runs'/run;output.parent.mkdir(parents=True,exist_ok=True)
            command=[sys.executable,str(code/'build_download_dimension_features.py'),'--query-root',str(data/'raw/full_scope/bigquery'),
                '--registry',str(project/'config/tools.study.json'),'--primary-snapshot',str(data/'study/latest'),
                '--output',str(output),'--end-exclusive',recipe['end_exclusive'],'--memory-limit','3GB','--threads','1',
                '--max-temp-gib','10','--minimum-free-gib','15']
            dependency=base/'dependency_history'
            if (dependency/'manifest.json').is_file():command+=['--dependency-history',str(dependency)]
            env={**os.environ,'PYTHONPATH':str(project/'pipeline')+os.pathsep+str(code),
                'TMPDIR':str(data/'.test-tmp'),'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}
            (data/'.test-tmp').mkdir(exist_ok=True)
            result=bounded(command,RICH_BUILD_TIMEOUT_SECONDS,output.parent/(run+'.log'),env)
            if result:raise RuntimeError('Rich feature materialization failed; preserved previous canonical pointer, exit '+str(result))
            from assemble_full_scope import inspect_source
            registry=read(project/'config/tools.study.json');ids={v['tool_id'] for v in registry['tools']}
            features,quality,_=inspect_source('downloads',output,sha(project/'config/tools.study.json'),ids)
            if features is None:raise ValueError('Rich output lacks verified weekly features; previous publication retained')
            if quality.get('referenced_primary_manifest_sha256')!=sha(data/'study/latest/study_manifest.json'):
                raise ValueError('Rich output references a different frozen primary; previous publication retained')
            atomic_link(base/'download_dimensions',output)
            state.update(rich_signature=recipe['rich_signature'],rich_built_at=datetime.now(UTC).isoformat(),rich_output=str(output))
            save(state_path,state)
        overlay=github_overlay_recipe(project,state,datetime.now(UTC)) if github_api is None else {'build':False}
        if overlay['build']:
            output=base/'github_activity_update_runs'/(run+'-updated');output.parent.mkdir(parents=True,exist_ok=True)
            command=[sys.executable,str(code/'github_activity_overlay.py'),'--baseline',overlay['baseline'],
                '--incremental-root',overlay['incremental_root'],'--registry',str(project/'config/tools.study.json'),
                '--output',str(output),'--end-exclusive',overlay['end_exclusive']]
            env={**os.environ,'PYTHONPATH':str(project/'pipeline')+os.pathsep+str(code),'TMPDIR':str(data/'.test-tmp')}
            (data/'.test-tmp').mkdir(exist_ok=True)
            result=bounded(command,1200,output.parent/(run+'.log'),env)
            if result:raise RuntimeError('GitHub update overlay failed; previous publication retained, exit '+str(result))
            from assemble_full_scope import inspect_source
            registry=read(project/'config/tools.study.json');ids={v['tool_id'] for v in registry['tools']}
            frame,quality,_=inspect_source('github_api',output,sha(project/'config/tools.study.json'),ids)
            q=read(output/'source_quality.json')
            if frame is None or q.get('baseline_manifest_sha256')!=sha(Path(overlay['baseline'])/'manifest.json'):
                raise ValueError('GitHub update overlay lacks verified features or its baseline changed')
            # A concurrent public collector may append partial progress. Only a
            # newly completed cycle changes this binding and prevents promotion.
            current=github_overlay_recipe(project,state,datetime.now(UTC))
            if current.get('signature')!=overlay['signature']:raise ValueError('Completed GitHub update inputs changed during build')
            atomic_link(base/'github_activity_updated',output)
            state.update(github_overlay_signature=overlay['signature'],github_overlay_output=str(output),github_overlay_built_at=datetime.now(UTC).isoformat())
            save(state_path,state)
        inputs=sources(project,github_api);checkpoint=checkpoint_signature(project,inputs)
        last=state.get('assembled_at');due=not last or datetime.now(UTC)-instant(last)>=timedelta(hours=1)
        if due and checkpoint!=state.get('spine_signature'):
            from assemble_full_scope import assemble,verify_files
            output=base/'runs'/(run+'-materialized');manifest=assemble(data/'study/latest',output,inputs)
            verify_files(output,manifest['files']);atomic_link(base/'latest',output)
            state.update(spine_signature=checkpoint,assembled_at=datetime.now(UTC).isoformat(),spine_output=str(output))
        state.update(last_checked_at=datetime.now(UTC).isoformat(),paid_queries_enabled=False);save(state_path,state)
        return {'status':'checked','paid_queries_enabled':False,'spine_output':state.get('spine_output'),
            'rich_output':state.get('rich_output'),'built_rich_features':recipe['build_rich_features']}


def main():
    os.umask(0o077);parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project',type=Path,default=Path.home()/'6893');parser.add_argument('--github-api',type=Path)
    parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    state=read(args.project/'data/full_scope/materialize_state.json')
    result=execute(args.project,args.github_api) if args.execute else plan(args.project,state,datetime.now(UTC),args.github_api)
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
