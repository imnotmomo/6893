#!/usr/bin/env python3
"""Bound public-source refreshes; the separate materializer owns joined spines.

Default is a read-only plan. Root may install --execute in cron after review.
Research cycles freeze a newly discovered candidate universe no more than weekly;
each invocation resumes at most eight requests in the same frozen cycle. Cached
complete batches within that cycle are never fetched again. A new weekly cycle
retrieves first-version metadata anew; retained candidates are not a recall census.
"""
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from expand_research_pairs import expand_source, KIND as RESEARCH_DERIVATION

UTC=timezone.utc
HISTORY_END_CAP=datetime(2027,1,1,tzinfo=UTC)
SOURCE_MANIFESTS={'research':'collection_manifest.json','depsdev':'manifest.json'}


class ResearchExpansionError(ValueError):pass


def read(path):
    return json.loads(Path(path).read_text()) if Path(path).is_file() else {}


def stamp(value):
    result=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def write(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n');temporary.replace(path)


def source_root(data,kind):return Path(data)/'raw'/('research_v1' if kind=='research' else 'depsdev')


def source_rank(kind,body):
    """Actual observation times outrank checkpoints rewritten without new HTTP."""
    values=[item.get('observed_at') for item in body.get('requests',[])]
    values += [item.get('observed_at_max') for item in body.get('tools',{}).values()]
    observed=max((stamp(value) for value in values if value),default=None)
    observed=observed or stamp(body.get('as_of') or body.get('started_at') or body.get('created_at')
                               or body.get('updated_at') or '1970-01-01T00:00:00Z')
    if kind=='research':
        return (stamp(body['history_end_exclusive']),stamp(body.get('as_of') or body.get('created_at') or observed),observed)
    return (observed,stamp(body.get('started_at') or observed))


def validate_source(project,kind,path,cohort,ids,now=None,_lineage=None):
    """Validate a terminal source before using it as a publication or state anchor."""
    project=Path(project);path=Path(path).resolve();now=now or datetime.now(UTC)
    if not path.is_relative_to(source_root(project/'data',kind).resolve()):
        raise ValueError('Source publication leaves its project source directory')
    body=read(path/SOURCE_MANIFESTS[kind])
    if kind=='research':
        lineage=set(_lineage or [])
        if path in lineage or len(lineage)>=32:raise ValueError('Research derivation ancestry is cyclic or too deep')
        lineage.add(path)
        from assemble_full_scope import inspect_source
        features,quality,_=inspect_source('research',path,cohort,ids)
        if (features is None or not quality.get('collection_complete')
                or set(features.tool_id)!=ids or not features.seed_v1_retrieval_complete.all()):
            raise ValueError('Research source is not fully retrieved; previous latest pointer retained')
        if stamp(body['history_end_exclusive']).date()>min(now.date(),HISTORY_END_CAP.date()):
            raise ValueError('Research publication requests an uncompleted future source cutoff')
        ancestry=body.get('derived_from');derivation=body.get('derivation')
        if ancestry or derivation:
            from assemble_full_scope import sha
            if not isinstance(ancestry,dict) or not isinstance(derivation,dict):raise ValueError('Research derivation ancestry is malformed')
            root=source_root(project/'data','research').resolve()
            name=(ancestry or {}).get('source_relative_path');copy=(ancestry or {}).get('manifest_copy_path')
            if (not isinstance(name,str) or Path(name).is_absolute() or '..' in Path(name).parts
                    or not isinstance(copy,str) or Path(copy).is_absolute() or '..' in Path(copy).parts):
                raise ValueError('Research parent/copy path leaves its source root')
            parent=(root/name).resolve();copy_path=(path/copy).resolve();expected=ancestry.get('manifest_sha256')
            if (not parent.is_relative_to(root) or not copy_path.is_relative_to(path)
                    or not copy_path.is_file() or parent==path
                    or body.get('files',{}).get(copy)!=expected or sha(copy_path)!=expected
                    or not (parent/'collection_manifest.json').is_file()
                    or sha(parent/'collection_manifest.json')!=expected):
                raise ValueError('Research parent manifest/copy hash binding differs')
            parent_body=validate_source(project,'research',parent,cohort,ids,now,lineage)
            if (derivation.get('kind')!=RESEARCH_DERIVATION or derivation.get('candidate_only') is not True
                    or derivation.get('registry_sha256')!=cohort
                    or derivation.get('provider_requests_added')!=0 or derivation.get('new_paper_ids')!=0
                    or derivation.get('original_query_cohort_unchanged') is not True
                    or derivation.get('all_retained_papers_all_tool_aliases_scanned') is not True
                    or derivation.get('tools_scanned')!=len(ids)
                    or type(derivation.get('added_candidate_pairs')) is not int or derivation['added_candidate_pairs']<0):
                raise ValueError('Research derivation scope/eligibility differs')
            inputs={key:derivation.get(key) for key in ['kind','parent_manifest_sha256','registry_sha256','code_sha256','classifier_code_sha256']}
            if inputs['parent_manifest_sha256']!=expected or any(not isinstance(inputs[key],str) or len(inputs[key])!=64
                    or set(inputs[key])-set('0123456789abcdef') for key in ['code_sha256','classifier_code_sha256']):
                raise ValueError('Research derivation code/parent provenance differs')
            fingerprint=hashlib.sha256(json.dumps(inputs,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
            if derivation.get('input_fingerprint')!=fingerprint:raise ValueError('Research derivation input fingerprint differs')
            vintage=['requests','as_of','created_at','started_at','updated_at','history_start','history_end_exclusive','candidate_input_sha256']
            if any(body.get(key)!=parent_body.get(key) for key in vintage) or source_rank('research',body)!=source_rank('research',parent_body):
                raise ValueError('Zero-HTTP derivation changed the provider/query vintage')
            if (body.get('unique_candidate_papers')!=parent_body.get('unique_candidate_papers')
                    or body.get('retrieved_v1_papers')!=parent_body.get('retrieved_v1_papers')
                    or body.get('candidate_pairs')!=parent_body.get('candidate_pairs',0)+derivation['added_candidate_pairs']
                    or any(body.get('files',{}).get(key)!=parent_body.get('files',{}).get(key)
                           for key in ['papers_v1.jsonl','candidates_input.csv','tools.study.json'])):
                raise ValueError('Research derivation changed its retained paper/query universe')
            for request in parent_body.get('requests',[]):
                raw=request.get('raw_path')
                if raw and (body.get('files',{}).get(raw)!=request.get('compressed_sha256')
                        or parent_body.get('files',{}).get(raw)!=request.get('compressed_sha256')):
                    raise ValueError('Research derivation changed a provider response body')
    else:
        from assemble_full_scope import inspect_supplement
        quality,_,_=inspect_supplement('depsdev',path,cohort,ids)
        if not quality.get('retrieval_attempts_complete') or quality.get('rows_available')!=len(ids):
            raise ValueError('Current dependency snapshot is unfinished; previous latest pointer retained')
    if max(source_rank(kind,body))>now:raise ValueError('Source publication has a future observation timestamp')
    return body


def validated_latest(project,kind,cohort,ids,now=None):
    latest=source_root(Path(project)/'data',kind)/'latest'
    if not latest.exists():
        if latest.is_symlink():raise ValueError('Existing latest source pointer is broken')
        return None
    validate_source(project,kind,latest,cohort,ids,now)
    return latest.resolve()


def publish_monotonic(project,kind,candidate,cohort,ids):
    """Caller holds .full-scope-refresh.lock; never roll latest back to stale state."""
    from materialize_full_scope import atomic_link
    candidate=Path(candidate).resolve();body=validate_source(project,kind,candidate,cohort,ids)
    current=validated_latest(project,kind,cohort,ids)
    if kind=='research':
        if current and source_rank(kind,read(current/SOURCE_MANIFESTS[kind]))>source_rank(kind,body):return current
        # A stale ancestor state derives from the already validated descendant,
        # preserving lineage when classifier/expander code changes later.
        if current and source_rank(kind,read(current/SOURCE_MANIFESTS[kind]))>=source_rank(kind,body):
            if current==candidate or research_descends_from(project,current,candidate):candidate=current
        try:
            candidate=expand_source(candidate,Path(project)/'config/tools.study.json',source_root(Path(project)/'data','research'))
            candidate=Path(candidate).resolve();body=validate_source(project,kind,candidate,cohort,ids)
        except Exception as error:
            raise ResearchExpansionError('Research pair expansion failed; previous latest retained: '+str(error)) from error
    if current:
        current_rank=source_rank(kind,read(current/SOURCE_MANIFESTS[kind]));candidate_rank=source_rank(kind,body)
        if current_rank>candidate_rank:return current
        if current_rank==candidate_rank and (kind!='research' or not research_descends_from(project,candidate,current)):return current
    atomic_link(source_root(Path(project)/'data',kind)/'latest',candidate)
    return candidate


def research_descends_from(project,candidate,ancestor):
    """Use only after validate_source has verified every ancestry edge."""
    ancestor=Path(ancestor).resolve();current=Path(candidate).resolve();visited=set()
    root=source_root(Path(project)/'data','research').resolve()
    if current==ancestor:return False
    expected=hashlib.sha256((ancestor/'collection_manifest.json').read_bytes()).hexdigest()
    while current not in visited:
        visited.add(current);body=read(current/'collection_manifest.json');link=body.get('derived_from')
        if not link:return False
        if link['manifest_sha256']==expected:return True
        current=(root/link['source_relative_path']).resolve()
    return False


def adopt_research_derivation(project,candidate):
    """Publish a local derived source only; no collectors, HTTP or spine writes."""
    project=Path(project).resolve();data=project/'data';base=data/'full_scope';base.mkdir(parents=True,exist_ok=True)
    with (data/'.full-scope-refresh.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'skipped_refresh_busy','paid_queries_enabled':False}
        registry=project/'config/tools.study.json';ids={tool['tool_id'] for tool in read(registry)['tools']}
        cohort=hashlib.sha256(registry.read_bytes()).hexdigest()
        selected=publish_monotonic(project,'research',candidate,cohort,ids)
        state_path=base/'refresh_state.json';state=read(state_path)
        active=Path(state.get('research_active') or selected)
        active,completed=adopt_latest(active,state.get('research_last_complete'),selected,'research')
        state['research_active']=str(active);state['research_last_complete']=completed
        state['research_derivation_adopted_at']=datetime.now(UTC).isoformat();state.pop('research_expansion_error',None)
        write(state_path,state)
        return {'status':'research_derivation_published','research_selected':str(selected),'research_active':str(active),
                'provider_requests_added':0,'paid_queries_enabled':False,'spine_or_model_changed':False}


def adopt_latest(current,completed,latest,kind):
    if latest is None:return current,completed
    body=read(current/SOURCE_MANIFESTS[kind]);latest_body=read(latest/SOURCE_MANIFESTS[kind])
    # Keep an in-progress cycle only when its cutoff/vintage is newer than the
    # validated published source. An unstarted or stale cycle may be superseded.
    if not body or source_rank(kind,body)<=source_rank(kind,latest_body):current=latest
    return current,str(latest)


def research_cycle(data,state,instant,latest=None):
    current=Path(state.get('research_active') or data/'raw/research_v1/backfill')
    completed=state.get('research_last_complete')
    current,completed=adopt_latest(current,completed,latest,'research')
    manifest=read(current/'collection_manifest.json')
    if manifest.get('status')=='complete':
        completed=str(current)
        end=stamp(manifest['history_end_exclusive'])
        if end<HISTORY_END_CAP and (end+timedelta(days=7)<=instant or instant>=HISTORY_END_CAP):
            current=data/'raw/research_v1/refresh'/instant.strftime('%Y%m%dT%H%M%SZ')
    return current,completed


def depsdev_cycle(data,state,instant,latest=None):
    current=Path(state.get('depsdev_active') or data/'raw/depsdev/full-scope-initial')
    completed=state.get('depsdev_last_complete')
    current,completed=adopt_latest(current,completed,latest,'depsdev')
    manifest=read(current/'manifest.json')
    if manifest.get('status') in {'complete','complete_with_source_gaps'}:
        completed=str(current)
        observed=source_rank('depsdev',manifest)[0]
        if observed+timedelta(days=7)<=instant:
            current=data/'raw/depsdev/observations'/instant.strftime('%Y%m%dT%H%M%SZ')
    return current,completed


def latest_git(data,cohort):
    valid=[]
    for manifest in (data/'raw/github_git_contributors/runs').glob('*/manifest.json'):
        body=read(manifest)
        if (body.get('status')=='complete' and body.get('registry_sha256')==cohort
                and body.get('requested_tools')==30 and body.get('completed_tools')==30):
            valid.append((stamp(body['created_at']),manifest.parent))
    return max(valid,default=(None,None),key=lambda pair:pair[0])


def plan(project,state,instant):
    project=Path(project);data=project/'data';code=Path(__file__).resolve().parent
    cohort_path=project/'config/tools.study.json'
    import hashlib
    cohort=hashlib.sha256(cohort_path.read_bytes()).hexdigest()
    ids={item['tool_id'] for item in read(cohort_path)['tools']}
    research,completed=research_cycle(data,state,instant,validated_latest(project,'research',cohort,ids,instant))
    depsdev,depsdev_completed=depsdev_cycle(data,state,instant,validated_latest(project,'depsdev',cohort,ids,instant))
    git_created,git=latest_git(data,cohort)
    recent_attempt=state.get('git_last_attempt')
    git_due=(git_created is None or instant-git_created>=timedelta(days=1))
    if recent_attempt and instant-stamp(recent_attempt)<timedelta(days=1):git_due=False
    history_end=min(instant.date(),HISTORY_END_CAP.date()).isoformat()
    if git and instant>=HISTORY_END_CAP and read(git/'manifest.json').get('history_end_exclusive','')>=history_end:git_due=False
    actions=[]
    research_manifest=read(research/'collection_manifest.json')
    research_end=research_manifest.get('history_end_exclusive') or history_end
    if research_manifest.get('status')!='complete':actions.append({'kind':'research','timeout_seconds':240,
        'command':[sys.executable,str(code/'collect_research_v1.py'),'--registry',str(cohort_path),
            '--data-dir',str(data),'--output',str(research),'--max-requests','8','--max-seconds','120',
            '--end-exclusive',research_end]})
    if git_due:
        actions.append({'kind':'git','timeout_seconds':3900,
            'command':[sys.executable,str(code/'collect_git_contributors.py'),'--registry',str(cohort_path),
                '--data-dir',str(data),'--timeout-seconds','120','--minimum-free-gib','15','--max-disk-gib','2',
                '--end-exclusive',history_end]})
    if read(depsdev/'manifest.json').get('status') not in {'complete','complete_with_source_gaps'}:
        actions.append({'kind':'depsdev','timeout_seconds':600,
            'command':[sys.executable,str(code/'collect_depsdev.py'),'--registry',str(cohort_path),
                '--output',str(depsdev),'--max-requests','150','--interval','1']})
    return {'paid_queries_enabled':False,'total_deadline_seconds':5100,'history_end_exclusive':history_end,'actions':actions,
        'research_active':str(research),'research_last_complete':completed,
        'depsdev_active':str(depsdev),'depsdev_last_complete':depsdev_completed,
        'git_selected':str(git) if git else None,'cohort_sha256':cohort,
        'new_research_cycle_reuses_prior_cycle_raw':False,
        'research_cycle_note':'At most weekly new candidate snapshot; within-cycle responses resume by verified cache. New cycles re-fetch v1 metadata within the public request cap.'}


def bounded(command,timeout,log):
    """Kill the whole collector process group if its wall-clock cap expires."""
    with Path(log).open('wb') as stream:
        process=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
        try:return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL);process.wait()
            return 124


def execute(project):
    project=Path(project).resolve();data=project/'data';base=data/'full_scope'
    base.mkdir(parents=True,exist_ok=True)
    with (data/'.full-scope-refresh.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'skipped_refresh_busy','paid_queries_enabled':False}
        state_path=base/'refresh_state.json';state=read(state_path)
        instant=datetime.now(UTC);recipe=plan(project,state,instant);deadline=time.monotonic()+5100
        run_id=instant.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        logs=base/'refresh_logs'/run_id;logs.mkdir(parents=True,exist_ok=False)
        state['research_active']=recipe['research_active']
        state['depsdev_active']=recipe['depsdev_active']
        if recipe['research_last_complete']:state['research_last_complete']=recipe['research_last_complete']
        if recipe['depsdev_last_complete']:state['depsdev_last_complete']=recipe['depsdev_last_complete']
        results=[];write(state_path,state)
        for action in recipe['actions']:
            remaining=int(deadline-time.monotonic())-90
            if remaining<1:break
            if action['kind']=='git':
                state['git_last_attempt']=datetime.now(UTC).isoformat();write(state_path,state)
            result=bounded(action['command'],min(action['timeout_seconds'],remaining),logs/(action['kind']+'.log'))
            results.append({'kind':action['kind'],'exit_code':result})
        research=Path(state['research_active'])
        if read(research/'collection_manifest.json').get('status')=='complete':
            registry=read(project/'config/tools.study.json');ids={item['tool_id'] for item in registry['tools']}
            try:selected=publish_monotonic(project,'research',research,recipe['cohort_sha256'],ids)
            except ResearchExpansionError as error:
                previous=validated_latest(project,'research',recipe['cohort_sha256'],ids)
                if previous:state['research_last_complete']=str(previous)
                else:state.pop('research_last_complete',None)
                state['research_expansion_error']=str(error);write(state_path,state);raise
            state['research_active']=str(selected);state['research_last_complete']=str(selected)
            state.pop('research_expansion_error',None)
        selected_research=Path(state.get('research_last_complete') or research)
        depsdev=Path(state['depsdev_active'])
        if read(depsdev/'manifest.json').get('status') in {'complete','complete_with_source_gaps'}:
            registry=read(project/'config/tools.study.json');ids={item['tool_id'] for item in registry['tools']}
            selected=publish_monotonic(project,'depsdev',depsdev,recipe['cohort_sha256'],ids)
            state['depsdev_active']=str(selected);state['depsdev_last_complete']=str(selected)
        _,git=latest_git(data,recipe['cohort_sha256'])
        state['last_refresh']=datetime.now(UTC).isoformat();state['paid_queries_enabled']=False
        write(state_path,state)
        return {'status':'public_sources_checked','research_selected':str(selected_research),
            'depsdev_selected':state.get('depsdev_last_complete'),
            'git_selected':str(git) if git else None,'actions':results,'paid_queries_enabled':False}


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project',type=Path,default=Path.home()/'6893')
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--execute',action='store_true',help='Run bounded public collectors; never paid queries or model publication')
    mode.add_argument('--publish-research',type=Path,help='Adopt an already built research source with zero HTTP or collector calls')
    args=parser.parse_args()
    if args.publish_research:result=adopt_research_derivation(args.project,args.publish_research)
    elif args.execute:result=execute(args.project)
    else:result=plan(args.project,read(args.project/'data/full_scope/refresh_state.json'),datetime.now(UTC))
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
