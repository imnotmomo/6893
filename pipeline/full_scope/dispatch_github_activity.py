"""Share one public request budget between issue/detail and native-PR streams."""
import argparse,json,fcntl
from datetime import datetime,timedelta,timezone
from pathlib import Path
import collect_github_activity_updates as issues
import native_pr_updates as native
import refresh_github_activity as r


def _run_locked(registry,data_dir,baseline,max_requests=4,minimum_free_gib=15,fetch=r.fetch_public,now=None):
    if not 1<=max_requests<=10:raise ValueError('Activity request cap must be1..10')
    fixed_clock=now is not None;now=now or datetime.now(timezone.utc);root=Path(data_dir);root.mkdir(parents=True,exist_ok=True);path=root/'activity_dispatch_state.json'
    state=json.loads(path.read_text()) if path.is_file() else {'source':'github_activity_dispatcher_v1','turn':0}
    start=now.replace(minute=(now.minute//15)*15,second=0,microsecond=0);window=r.iso(start)
    archived=0
    for filename in [issues.STATE,native.STATE]:
        p=root/filename
        if p.is_file():archived+=sum(start<=r.dt(m['observed_at'])<=now for m in json.loads(p.read_text()).get('requests',[]))
    if state.get('window_start')!=window:state.update(window_start=window,requests_used=archived)
    else:state['requests_used']=max(state.get('requests_used',0),archived)
    pauses=[state.get('not_before')]
    for filename in [issues.STATE,native.STATE]:
        p=root/filename
        if p.is_file():pauses.append(json.loads(p.read_text()).get('not_before'))
    pause=max((r.dt(v) for v in pauses if v),default=None)
    if pause and pause>now:return {'status':'rate_wait','requests':0,'not_before':r.iso(pause)}
    state.pop('not_before',None);remaining=max(0,max_requests-state.get('requests_used',0));results=[]
    if not remaining:return {'status':'activity_window_budget_used','requests':0,'next_window':r.iso(start+timedelta(minutes=15))}
    workers=[('native',native.run),('issues',issues.run)]
    if state.get('turn',0)%2:workers.reverse()
    if state.get('inflight'):
        state.setdefault('uncertain_worker_reservations',[]).append(state.pop('inflight'))
    def invoke(name,worker,cap):
        state['requests_used']=state.get('requests_used',0)+cap
        state['inflight']={'stream':name,'reserved_requests':cap,'window_start':window,'reserved_at':r.iso(now)}
        r.atomic_json(path,state)
        # Uncertain/interrupted calls keep their reservation until the window
        # ends. A normal return releases only the explicitly unused allowance.
        result=worker(registry,root,baseline,cap,minimum_free_gib,fetch,now if fixed_clock else None)
        count=result.get('requests')
        if not isinstance(count,int) or isinstance(count,bool) or not 0<=count<=cap:raise ValueError('Activity worker exceeded shared request cap')
        state['requests_used']-=cap-count;state.pop('inflight',None);r.atomic_json(path,state)
        return result
    state['turn']=state.get('turn',0)+1;used=0
    for index,(name,worker) in enumerate(workers):
        if remaining<=0:break
        cap=min(remaining,max(1,max_requests//2)) if index==0 else remaining
        result=invoke(name,worker,cap)
        used+=result['requests'];remaining-=result['requests'];results.append({'stream':name,**result})
        if result.get('not_before') and r.dt(result['not_before'])>now:state['not_before']=result['not_before'];break
    if remaining>0 and not state.get('not_before'):
        name,worker=workers[0];result=invoke(name,worker,remaining)
        used+=result['requests'];results.append({'stream':name,**result})
        if result.get('not_before') and r.dt(result['not_before'])>now:state['not_before']=result['not_before']
    r.atomic_json(path,state)
    return {'status':'bounded_activity_dispatch','requests':used,'shared_request_cap':max_requests,'requests_used_this_window':state['requests_used'],
        'not_before':state.get('not_before'),'streams':results,'old_issue_state_preserved':True,'paid_queries_enabled':False}

def run(registry,data_dir,baseline,max_requests=4,minimum_free_gib=15,fetch=r.fetch_public,now=None):
    root=Path(data_dir);root.mkdir(parents=True,exist_ok=True)
    with (root/'.dispatch.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return {'status':'skipped_activity_dispatch_busy','requests':0,'paid_queries_enabled':False}
        return _run_locked(registry,root,baseline,max_requests,minimum_free_gib,fetch,now)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--registry',type=Path,required=True);p.add_argument('--data-dir',type=Path,required=True);p.add_argument('--baseline',type=Path,required=True)
    p.add_argument('--max-requests',type=int,default=4);p.add_argument('--minimum-free-gib',type=float,default=15);a=p.parse_args()
    print(json.dumps(run(a.registry,a.data_dir,a.baseline,a.max_requests,a.minimum_free_gib)))
