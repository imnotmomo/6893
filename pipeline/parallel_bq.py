#!/usr/bin/env python3
"""Bounded network concurrency for an already-budgeted historical plan; one DB writer."""
import concurrent.futures, fcntl, hashlib, json, os, shutil, sys, threading, time
from pathlib import Path
sys.path.insert(0,str(Path.home()/'6893/pipeline'))
import backfill as b
from query import connect
ROOT=Path.home()/'6893'
WRITE_LOCK=threading.Lock()

def perform(job,root,config,registry):
    try:
        project=os.environ.get('BQ_PROJECT_ID',config['bigquery_project']);cap=config['max_bytes_per_query']
        if job.get('pending'):
            result=b.resume_aggregate(json.loads(job['pending']),poll_timeout=30)
        else:
            preview=b.fetch_aggregate(job['source'],project,registry,job['start_date'],job['end_date'],cap,dry_run_only=True)
            if preview['status']!='dry_run_ok':result=preview
            elif not config.get('allow_billed_queries',False):result={'ok':False,'status':'blocked_budget','error':{'message':'Execution disabled'}}
            else:
                estimate=int(preview['request_metadata']['estimated_bytes_processed'])
                rounded=max(10*1024**2,((estimate+10*1024**2-1)//(10*1024**2))*10*1024**2)
                with WRITE_LOCK:execution_cap=b.reserve(root,job,min(cap,rounded),config['max_total_query_bytes'])
                if execution_cap is None:result={'ok':False,'status':'blocked_budget','error':{'message':'Cumulative query reservation cap reached'}}
                else:
                    refresh='radar_refresh_'+job['job_id'] if job['generation'].startswith('refresh:') else None
                    result=b.fetch_aggregate(job['source'],project,registry,job['start_date'],job['end_date'],execution_cap,poll_timeout=30,job_id=refresh)
        with WRITE_LOCK:
            if result.get('ok') and result.get('status','ok')=='ok':
                n=b.save(root,job,result);status='done'
            elif (result.get('error') or {}).get('code')=='split_required' or (result.get('error') or {}).get('type')=='bytes_cap_exceeded':b.split(root,job);n=0;status='split'
            elif result.get('status')=='pending':b.defer(root,job,'query_pending',result.get('error'),result,delay=30);n=0;status='query_pending'
            else:
                status='blocked' if 'blocked' in result.get('status','') else 'retry';b.defer(root,job,status,result.get('error') or {'message':result.get('status')},delay=21600 if status=='blocked' else 1800);n=0
        print(b.encode({'source':job['source'],'start':job['start_date'],'end':job['end_date'],'state':status,'records':n,'error':result.get('error')}),flush=True)
    except Exception as e:
        with WRITE_LOCK:b.defer(root,job,'retry',{'type':type(e).__name__,'message':str(e)},delay=1800)
        print(b.encode({'job_id':job['job_id'],'state':'retry','error':str(e)}),flush=True)

def main():
    config=json.loads((ROOT/'config/backfill.json').read_text());root=Path(config['data_dir']);registry=json.loads((ROOT/config['tool_registry']).read_text());config['tool_count']=len(registry['tools'])
    digest=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+b.encode(registry).encode()).hexdigest()
    with (root/'.pilot-collection.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        b.plan(root,config,registry,digest)
        started=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            while time.monotonic()-started<3600 and shutil.disk_usage(root).free>=config.get('minimum_free_GiB',15)*1024**3:
                with connect(read_only=False,data_dir=root) as con:
                    result=con.execute("SELECT * FROM backfill.jobs WHERE plan_sha=? AND source IN ('pypi','gharchive') AND state NOT IN ('done','split','reused') AND next_attempt<=? ORDER BY priority,attempts,start_date,source LIMIT 24",[digest,b.now()]);columns=[c[0] for c in result.description];jobs=[dict(zip(columns,row)) for row in result.fetchall()]
                    pending=con.execute("SELECT count(*) FROM backfill.jobs WHERE plan_sha=? AND source IN ('pypi','gharchive') AND state='query_pending' AND next_attempt<=?",[digest,b.now()+b.timedelta(seconds=60)]).fetchone()[0]
                if not jobs:
                    if pending:time.sleep(5);continue
                    break
                list(pool.map(lambda j:perform(j,root,config,registry),jobs))
                status=b.export_status(root,digest,config)
                print(b.encode({'remaining_bq_states':[x for x in status['states'] if x['source'] in ('pypi','gharchive')],'reserved_query_bytes':status['reserved_query_bytes']}),flush=True)
        print(b.encode(b.export_status(root,digest,config)),flush=True)
if __name__=='__main__':main()
