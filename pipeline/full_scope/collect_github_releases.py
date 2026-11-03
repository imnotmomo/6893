#!/usr/bin/env python3
"""Bound unsigned release-list refreshes to four requests/run and forty/day.

All visible pages are traversed; publication-date ordering is never assumed.
Old snapshots stay published while a new weekly cycle resumes. HTTP rate headers
and Retry-After stop this worker without advancing a failed page.
"""
from datetime import datetime,timedelta,timezone,date
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import uuid

from archive_github_releases import Archive,atomic,SOURCE
from build_github_release_features import inspect_tool,build,sha
from refresh_github_activity import fetch_public,pause_until

UTC=timezone.utc
CAP=date(2027,1,1)
def stamp(value):
    result=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('UTC release observation required')
    return result.astimezone(UTC)
def read(path):return json.loads(Path(path).read_text()) if Path(path).is_file() else {}
def rank(body):return (body['history_end_exclusive'],body.get('as_of') or body['started_at'])


def summaries(root,tools,now):
    body=read(root/'collection_manifest.json')
    return {tool['tool_id']:inspect_tool(tool,body.get('requests',[]),root.resolve(),now)[2] for tool in tools}


def select_cycle(project,state,registry,now):
    root=project/'data/raw/github_releases';latest=root/'latest'
    active=Path(state['active_root']) if state.get('active_root') else latest.resolve() if latest.exists() else None
    if latest.exists():
        published=read(latest/'collection_manifest.json')
        if published.get('registry_sha256')!=sha(registry):raise ValueError('Published release cohort changed')
        if active is None or not (active/'collection_manifest.json').exists() or rank(read(active/'collection_manifest.json'))<rank(published):
            active=latest.resolve()
    if active and (active/'collection_manifest.json').is_file():
        body=read(active/'collection_manifest.json')
        if body.get('source')!=SOURCE or body.get('registry_sha256')!=sha(registry):
            raise ValueError('Active release source scope changed')
        quality=summaries(active,read(registry)['tools'],now)
        complete=all(q['retrieval_complete'] for q in quality.values())
        end=date.fromisoformat(body['history_end_exclusive'])
        due=end<CAP and (now.date()>=CAP or stamp(body['as_of'])+timedelta(days=7)<=now)
        if not complete or not due:return active,complete
    identifier=now.strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    return root/'runs'/identifier,False


def next_request(archive,tool,now):
    tid=tool['tool_id'];body=archive.state
    marker=body.get('tools',{}).get(tid,{})
    if marker.get('retry_not_before') and stamp(marker['retry_not_before'])>now:return None
    current=max([r.get('attempt',1) for r in body['requests'] if r['tool_id']==tid]+[marker.get('next_attempt',1)])
    items=[r for r in body['requests'] if r['tool_id']==tid and r.get('attempt',1)==current]
    if not any(r['phase']=='identity' for r in items):return {'kind':'repository','phase':'identity','page':1,'attempt':current}
    summary=inspect_tool(tool,body['requests'],archive.root.resolve(),now)[2]
    if summary['selected_attempt']!=current:return {'kind':'repository','phase':'identity','page':1,'attempt':current}
    if not summary['identity_verified']:raise ValueError('Frozen repository identity failed: '+tid)
    if summary['retrieval_complete']:return None
    if summary['terminal_page'] is not None:
        if any(r['phase']=='anchor' for r in items):
            archive.state['tools'][tid]={'next_attempt':current+1,'retry_not_before':(now+timedelta(days=1)).isoformat(),
                'reason':'Unstable release head or page identities; raw attempt preserved'}
            archive.checkpoint();return None
        return {'kind':'releases','phase':'anchor','page':1,'attempt':current}
    pages=[r['page'] for r in items if r['phase']=='list']
    return {'kind':'releases','phase':'list','page':max(pages,default=0)+1,'attempt':current}


def publish(project,source,registry):
    """Publish only a fully traversed thirty-repository raw cycle, with limits."""
    from assemble_full_scope import inspect_source
    from materialize_full_scope import atomic_link
    tools=read(registry)['tools'];ids={t['tool_id'] for t in tools};cohort=sha(registry)
    quality=summaries(source,tools,datetime.now(UTC))
    if not all(q['retrieval_complete'] for q in quality.values()):return None
    base=project/'data/full_scope';pointer=base/'github_releases'
    raw_sha=sha(source/'collection_manifest.json')
    if (pointer/'manifest.json').exists():
        current=read(pointer/'github_release_quality.json')
        if current.get('source_manifest_sha256')==raw_sha and current.get('builder_sha256')==sha(Path(__file__).with_name('build_github_release_features.py')):
            frame,info,_=inspect_source('releases',pointer,cohort,ids)
            if frame is None:raise ValueError('Published release output lacks weekly features')
            return pointer.resolve()
        prior=read(pointer/'source_collection_manifest.json')
        if rank(prior)>rank(read(source/'collection_manifest.json')):return pointer.resolve()
    run=datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')+'-'+raw_sha[:10]
    output=base/'github_release_runs'/run
    build(source,registry,output)
    frame,info,_=inspect_source('releases',output,cohort,ids)
    if frame is None or set(frame.tool_id)!=ids:raise ValueError('Release weekly feature publication is incomplete')
    atomic_link(pointer,output);atomic_link(project/'data/raw/github_releases/latest',source)
    return output


def run(project,max_requests=1,max_daily=40,fetch=fetch_public,now=None,execute=False):
    if not 1<=max_requests<=4 or not 1<=max_daily<=40:raise ValueError('Bound release work to1..4/run and at most40/day')
    project=Path(project).resolve();root=project/'data/raw/github_releases'
    root.mkdir(parents=True,exist_ok=True);registry=project/'config/tools.study.json';tools=read(registry)['tools']
    cohort=sha(registry);instant=now or datetime.now(UTC)
    with (root/'.collector.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'skipped_busy','requests':0}
        state_path=root/'refresh_state.json';state=read(state_path) or {'source':SOURCE,'registry_sha256':cohort}
        if state.get('registry_sha256')!=cohort:raise ValueError('Release refresh cohort changed')
        quota=state.get('quota',{})
        if quota.get('utc_date')!=instant.date().isoformat():quota={'utc_date':instant.date().isoformat(),'used':0}
        state['quota']=quota
        if state.get('not_before') and stamp(state['not_before'])>instant:
            return {'status':'rate_wait','requests':0,'not_before':state['not_before']}
        if quota['used']>=max_daily:return {'status':'daily_quota_wait','requests':0,'daily_used':quota['used']}
        active,complete=select_cycle(project,state,registry,instant)
        if not execute:return {'status':'plan','requests':0,'active_root':str(active),'complete':complete,'maximum_requests':min(max_requests,max_daily-quota['used'])}
        if complete:
            output=publish(project,active,registry)
            state.update(active_root=str(active),last_checked_at=instant.isoformat());atomic(state_path,state)
            return {'status':'complete_not_due','requests':0,'active_root':str(active),'output':str(output) if output else None}
        endpoint=read(active/'collection_manifest.json').get('history_end_exclusive') or min(instant.date(),CAP).isoformat()
        archive=Archive(active,registry,endpoint);state['active_root']=str(active);atomic(state_path,state)
        count=0;cursor=state.get('cursor',0);visited=0
        while count<max_requests and quota['used']<max_daily and visited<len(tools):
            tool=tools[cursor%len(tools)];request=next_request(archive,tool,now or datetime.now(UTC))
            if request is None:cursor+=1;visited+=1;continue
            if shutil.disk_usage(root).free<15*1024**3:raise ValueError('Release collection below15GiB free guard')
            prior=state.get('last_request_started_at')
            if prior:
                delay=1-(datetime.now(UTC)-stamp(prior)).total_seconds()
                if delay>0:time.sleep(delay)
            page=request['page'];size=100;kind=request['kind']
            url='https://api.github.com/repos/'+tool['github_repo']+('/releases?per_page='+str(size)+'&page='+str(page) if kind=='releases' else '')
            state['last_request_started_at']=(now or datetime.now(UTC)).isoformat()
            quota['used']+=1;count+=1;atomic(state_path,state)
            try:response=fetch(url)
            except Exception as error:
                response={'status':0,'body':json.dumps({'transport_error':type(error).__name__}).encode(),'headers':{},'url':url}
            observed=now or datetime.now(UTC);status=response['status']
            phase=request['phase'] if status==200 else 'error'
            archive.save({'tool_id':tool['tool_id'],'kind':kind,'phase':phase,'page':page,'per_page':size,
                'attempt':request['attempt'],'url':url,'response_url':response.get('url'),
                'observed_at':observed.isoformat(),'transport':'unsigned_https_get','http_status':status,
                'headers':response['headers'],'body':response['body'].decode('utf-8',errors='replace')})
            archive.checkpoint()
            pause=pause_until(response['headers'],observed)
            if status in {403,429}:pause=max(pause or observed,observed+timedelta(minutes=1))
            elif status!=200:pause=observed+timedelta(minutes=15)
            if pause:state['not_before']=pause.isoformat();break
        state.update(cursor=cursor%len(tools),last_checked_at=(now or datetime.now(UTC)).isoformat());atomic(state_path,state)
        complete=all(q['retrieval_complete'] for q in summaries(active,tools,now or datetime.now(UTC)).values())
        if complete:
            archive.state['status']='collected_unvalidated';archive.checkpoint()
        output=publish(project,active,registry) if complete else None
        return {'status':'complete' if complete else 'partial','requests':count,'daily_used':quota['used'],
            'active_root':str(active),'output':str(output) if output else None,'not_before':state.get('not_before')}


def main():
    os.umask(0o077);parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project',type=Path,default=Path.home()/'6893')
    parser.add_argument('--max-requests',type=int,default=1);parser.add_argument('--max-daily-requests',type=int,default=40)
    parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    print(json.dumps(run(args.project,args.max_requests,args.max_daily_requests,execute=args.execute),indent=2))


if __name__=='__main__':main()
