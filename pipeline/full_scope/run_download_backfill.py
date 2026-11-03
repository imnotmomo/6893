"""Resume all scoped download-dimension windows using the shared BQ budget."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path.home()/'6893'
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pypi_rich_plan import plan
_BODY_HASH_CACHE = {}


def atomic(path, value):
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        temp.write_text(json.dumps(value,indent=2,default=str)+'\n');os.replace(temp,path)
    finally:
        if temp.exists():temp.unlink()


def uncovered_windows(start, stop, covered_days):
    """Nonoverlapping UTC intervals, reusing a completed pilot or daily child."""
    while start < stop:
        if start in covered_days:
            start += timedelta(days=1)
            continue
        end = min(start + timedelta(days=7-start.weekday()), stop)
        if end == stop and stop.weekday() != 0:
            end = start + timedelta(days=1)
        for offset in range(1, (end-start).days):
            if start + timedelta(days=offset) in covered_days:
                end = start + timedelta(days=offset)
                break
        yield start, end
        start = end


def compatible_receipt(value, receipt, registry, has_output):
    source_plan=value.get('source_plan',{})
    try:expected=plan(registry,source_plan['start'],source_plan['end_exclusive'])
    except (KeyError,ValueError):return False
    return (value.get('status')=='complete' and value.get('source')=='pypi_download_dimensions'
        and receipt.get('all_pages_fetched') is True and receipt.get('job_complete') is True
        and value.get('all_pages_fetched') is True and value.get('job_complete') is True
        and value.get('last_page_fetched') is True
        and isinstance(receipt.get('total_rows'),int) and not isinstance(receipt.get('total_rows'),bool)
        and receipt['total_rows']>=0 and bool(receipt.get('cloud_job_id'))
        and receipt.get('total_rows')==value.get('rows_fetched')==value.get('total_rows')
        and receipt.get('query_sha256')==value.get('query_sha256')==expected['query_sha256']
        and source_plan.get('package_to_tool')==expected['package_to_tool']
        and receipt.get('cloud_job_id')==value.get('job_id')
        and receipt.get('actual_billed_bytes')==value.get('actual_billed_bytes')
        and receipt.get('output_sha256')==value.get('decoded_records_sha256')
        and bool(receipt.get('output_sha256')) and has_output)


def verified_download_output(path, expected_sha256):
    """Reuse a body digest only while its complete filesystem identity is stable."""
    def identity(resolved, info):
        return (str(resolved), info.st_dev, info.st_ino, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns)
    try:
        resolved=path.resolve(strict=True)
        before=identity(resolved,resolved.stat())
        digest=_BODY_HASH_CACHE.get(before)
        if digest is None:
            hasher=hashlib.sha256()
            with resolved.open('rb') as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):hasher.update(block)
            digest=hasher.hexdigest()
        after=identity(resolved,resolved.stat())
        if before!=after or path.resolve(strict=True)!=resolved:return False
        _BODY_HASH_CACHE[before]=digest
        return digest==expected_sha256
    except OSError:
        return False


def split_windows(start,end):
    if (end-start).days<=1:return []
    middle=start+timedelta(days=(end-start).days//2)
    return [(start,middle),(middle,end)]


def archive_inventory(archive,registry,start,stop):
    """Keep pending/split intervals intact instead of buying overlapping slices."""
    receipts={};covered=set();pending=[]
    for path in archive.glob('*/manifest.json'):
        try:
            value=json.loads(path.read_text());source=value.get('source_plan',{})
            if value.get('source')!='pypi_download_dimensions':continue
            left=date.fromisoformat(source['start']);right=date.fromisoformat(source['end_exclusive'])
            expected=plan(registry,str(left),str(right))
            if (value.get('query_sha256')!=expected['query_sha256'] or source.get('query_sha256')!=expected['query_sha256']
                    or source.get('package_to_tool')!=expected['package_to_tool']):continue
        except (ValueError,KeyError,OSError):continue
        if left<start or right>stop:continue
        receipt_path=path.parent/'completion_receipt.json'
        if value.get('status')=='complete' and receipt_path.exists():
            receipt=json.loads(receipt_path.read_text())
            body=path.parent/'decoded_records.jsonl.gz'
            if (compatible_receipt(value,receipt,registry,body.is_file())
                    and verified_download_output(body,receipt['output_sha256'])):
                receipts[value['query_sha256']]=str(path.parent)
                covered.update(left+timedelta(days=i) for i in range((right-left).days))
                continue
        # Includes a split parent: its known child topology must win over a
        # newly calendar-aligned interval after one child finishes.
        if value.get('status') in {'planned','dry_run_ok','reserved','submission_started','query_pending','fetching','split','complete'}:
            pending.append({'key':path.parent.name,'plan':path.parent/'query_plan.json',
                'output':path.parent,'start':left,'end':right})
    selected=[];blocked=set(covered)
    for task in sorted(pending,key=lambda t:(-(t['end']-t['start']).days,t['start'],t['key'])):
        days={task['start']+timedelta(days=i) for i in range((task['end']-task['start']).days)}
        if days<=covered or any(parent['start']<=task['start'] and task['end']<=parent['end'] for parent in selected):continue
        selected.append(task);blocked.update(days)
    return receipts,covered,selected,blocked


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers',type=int,default=3)
    parser.add_argument('--max-seconds',type=int,default=3300)
    parser.add_argument('--direction',choices=['forward','reverse'],default='forward',
        help='An optional single-worker reverse lane can use spare capacity; output locks and the budget remain shared')
    args=parser.parse_args()
    if not 1<=args.workers<=4 or not 1<=args.max_seconds<=7200:parser.error('Use1..4 workers and a bounded runtime')
    config=json.loads((ROOT/'config/backfill.json').read_text())
    registry=json.loads((ROOT/config['tool_registry']).read_text())
    data=Path(config['data_dir']);queries=ROOT/'config/full_scope/downloads';queries.mkdir(parents=True,exist_ok=True)
    archive=data/'raw/full_scope/bigquery';archive.mkdir(parents=True,exist_ok=True)
    reverse=args.direction=='reverse'
    if reverse and args.workers!=1:parser.error('The optional reverse lane uses exactly one worker')
    status_path=data/('full_scope_download_auxiliary_status.json' if reverse else 'full_scope_download_status.json')
    with (archive/('.downloads-reverse-dispatch.lock' if reverse else '.downloads-dispatch.lock')).open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return 0
        stop=min(datetime.now(timezone.utc).date(),date(2027,1,1));start=date(2023,1,1);tasks=[]
        receipts,covered_days,tasks,blocked_days=archive_inventory(archive,registry,start,stop)
        def task_for_interval(start,end):
            key='downloads-'+str(start)+'-'+str(end);definition=plan(registry,str(start),str(end))
            plan_path=queries/(key+'.json')
            if plan_path.exists() and json.loads(plan_path.read_text())!=definition:raise ValueError('Frozen query plan changed')
            if not plan_path.exists():atomic(plan_path,definition)
            return {'key':key,'plan':plan_path,'output':archive/key,'start':start,'end':end}
        for left,right in uncovered_windows(start,stop,blocked_days):
            task=task_for_interval(left,right)
            if json.loads(task['plan'].read_text())['query_sha256'] not in receipts:tasks.append(task)
        tasks.sort(key=lambda t:(t['start'],t['end']),reverse=reverse)
        launched=time.monotonic();results=[]
        env={**os.environ,'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}
        def perform(task):
            if time.monotonic()-launched>args.max_seconds:return {'key':task['key'],'status':'not_started_runtime_limit'}
            destination=task['output'];destination.mkdir(exist_ok=True)
            split_path=destination/'split_children.json'
            if split_path.exists():
                binding=json.loads(split_path.read_text());definition=json.loads(task['plan'].read_text())
                windows=split_windows(task['start'],task['end'])
                if (binding.get('query_sha256')!=definition['query_sha256']
                        or binding.get('windows')!=[[str(a),str(b)] for a,b in windows] or not windows):
                    raise ValueError('Saved query split differs from its frozen interval')
                children=[perform(task_for_interval(a,b)) for a,b in windows]
                return {'key':task['key'],'status':'complete' if all(v['status']=='complete' for v in children) else 'partial',
                    'split_parent':True,'children':children,'rows':sum(v.get('rows',0) for v in children)}
            log=destination/'collection.log'
            with log.open('a') as stream:
                child=subprocess.run([sys.executable,str(Path(__file__).with_name('full_scope_bq.py')),
                    '--plan',str(task['plan']),'--output',str(destination),'--execute',
                    '--max-seconds','600','--page-size','100000'],env=env,stdout=stream,stderr=subprocess.STDOUT)
            manifest=destination/'manifest.json'
            value=json.loads(manifest.read_text()) if manifest.exists() else {}
            dry_path=destination/'dry_run.json'
            if child.returncode and value.get('status')=='planned' and dry_path.exists():
                estimate=int(json.loads(dry_path.read_text()).get('statistics',{}).get('query',{}).get('totalBytesProcessed',0))
                windows=split_windows(task['start'],task['end'])
                if estimate>int(config['max_bytes_per_query']) and windows:
                    with (destination/'.collection.lock').open('a') as output_lock:
                        fcntl.flock(output_lock,fcntl.LOCK_EX)
                        latest=json.loads(manifest.read_text())
                        if latest.get('status')=='planned' and not latest.get('reserved_at'):
                            atomic(split_path,{'query_sha256':value['query_sha256'],'estimated_scan_bytes':estimate,
                                'windows':[[str(a),str(b)] for a,b in windows],'reason':'free dry run exceeded per-query cap; parent was not submitted'})
                            latest['status']='split';atomic(manifest,latest)
                    if split_path.exists():return perform(task)
                    value=json.loads(manifest.read_text())
            if split_path.exists():return perform(task)
            result={'key':task['key'],'exit_code':child.returncode,'status':value.get('status','failed'),
                    'rows':value.get('rows_fetched',0),'total_rows':value.get('total_rows'),
                    'actual_billed_bytes':value.get('actual_billed_bytes')}
            print(json.dumps(result),flush=True)
            return result
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending={pool.submit(perform,t):t for t in tasks}
            for future in as_completed(pending):
                results.append(future.result())
                atomic(status_path,{'source':'pypi_download_dimensions','updated_at':datetime.now(timezone.utc).isoformat(),
                    'history_start':'2023-01-01','end_exclusive':str(stop),'completed_before_dispatch':len(receipts),
                    'dispatch_tasks':len(tasks),'dispatch_results':results,'status':'running',
                    'remaining_in_dispatch':len(tasks)-len(results),'query_root':str(archive)})
        _,finished_days,_,_=archive_inventory(archive,registry,start,stop)
        complete=all(start+timedelta(days=i) in finished_days for i in range((stop-start).days))
        atomic(status_path,{'source':'pypi_download_dimensions','updated_at':datetime.now(timezone.utc).isoformat(),
            'history_start':'2023-01-01','end_exclusive':str(stop),'completed_before_dispatch':len(receipts),
            'dispatch_tasks':len(tasks),'dispatch_results':results,'status':'complete' if complete else 'partial',
            'complete_query_days':len(finished_days),'requested_query_days':(stop-start).days,
            'query_root':str(archive)})
        return 0 if complete else 1


if __name__=='__main__':raise SystemExit(main())
