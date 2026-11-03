#!/usr/bin/env python3
"""Audit retained collection evidence offline; never submit queries or fetch data.

The collection lock protects only the database metadata snapshot. Immutable raw
gzip files and sidecar manifests are checked after releasing that lock. A pass
establishes retained collection coverage/integrity, not historical research
eligibility, platform completeness, or the amount on a cloud billing invoice.
"""
import argparse
from collections import Counter,defaultdict
from contextlib import contextmanager
from datetime import date,datetime,timezone
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
import zlib


ROOT=Path.home()/'6893'
FINISHED={'done','reused','split','superseded'}


def now():return datetime.now(timezone.utc).isoformat()


def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,default=str,allow_nan=False)


def read_json(path):
    value=json.loads(path.read_text())
    if not isinstance(value,dict):raise ValueError('Expected JSON object: '+str(path))
    return value


def sha_file(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):value.update(chunk)
    return value.hexdigest()


def day(value):
    if isinstance(value,datetime):return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value,date):return value
    return datetime.fromisoformat(str(value).replace('Z','+00:00')).date()


def timestamp(value):
    result=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def atomic_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('w') as stream:
            stream.write(encode(value)+'\n');stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:temporary.unlink(missing_ok=True)


@contextmanager
def shared_snapshot_lock(path,timeout):
    deadline=time.monotonic()+timeout
    with path.open('a') as lock:
        while True:
            try:fcntl.flock(lock,fcntl.LOCK_SH|fcntl.LOCK_NB);break
            except BlockingIOError:
                if time.monotonic()>=deadline:raise TimeoutError('Collector still holds the collection lock')
                time.sleep(1)
        try:yield
        finally:fcntl.flock(lock,fcntl.LOCK_UN)


def rows(con,sql):
    result=con.execute(sql);columns=[item[0] for item in result.description]
    return [dict(zip(columns,row)) for row in result.fetchall()]


def verify_returned_query(query,cap,expected_parameters):
    # BigQuery omits parameterValue/arrayValues for typed empty arrays and may
    # reorder named parameters. Compare the same typed semantic representation
    # used by the collector; nonempty values, dates, types and caps stay strict.
    from bigquery_sources import _canonical_parameters
    if (int(query['maximumBytesBilled'])!=int(cap)
            or _canonical_parameters(query.get('queryParameters',[]))
                !=_canonical_parameters(expected_parameters)):
        raise ValueError('Actual returned BigQuery cap/parameters differ from the retained request')


def interval_union(intervals,start,end):
    merged=[]
    for left,right in sorted((max(start,day(a)),min(end,day(b))) for a,b in intervals):
        if left>=right:continue
        if merged and left<=merged[-1][1]:merged[-1][1]=max(merged[-1][1],right)
        else:merged.append([left,right])
    gaps=[];cursor=start
    for left,right in merged:
        if cursor<left:gaps.append([cursor,left])
        cursor=max(cursor,right)
    if cursor<end:gaps.append([cursor,end])
    return {'covered_days':sum((right-left).days for left,right in merged),
            'expected_days':(end-start).days,'intervals':merged,'gaps':gaps}


def audit(args):
    sys.path.insert(0,str(ROOT/'duckdb'));sys.path.insert(0,str(ROOT/'pipeline'))
    import backfill as b
    config_path=args.config.expanduser().resolve();config=read_json(config_path);config_sha=sha_file(config_path)
    data=Path(config['data_dir']).expanduser().resolve()
    registry_path=(args.registry or ROOT/config['tool_registry']).expanduser().resolve()
    registry=read_json(registry_path);registry_sha=sha_file(registry_path)
    tools={tool['tool_id']:tool for tool in registry['tools']}
    plan_sha=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+b.encode(registry).encode()).hexdigest()
    start=day(config['start_date']);end=min(day(args.as_of) if args.as_of else datetime.now(timezone.utc).date(),day(config.get('max_end_exclusive','2027-01-01')),datetime.now(timezone.utc).date(),date(2027,1,1))
    report={'schema_version':1,'started_at':now(),'ok':False,'status':'auditing','plan_sha256':plan_sha,
            'as_of_end_exclusive':end,'start_date':start,'cohort_size':len(tools),
            'config_sha256':config_sha,'registry_sha256':registry_sha,'errors':[],'warnings':[],
            'scope':'All backfill.objects and raw/backfill gzip files; collection completeness for the active thirty-tool plan. Pilot snapshots are outside the backfill table.',
            'research_historical_features_eligible':False,
            'limitations':['Current arXiv text remains unreviewed candidate metadata, not historical as-of research evidence.',
                          'Complete retained query windows do not guarantee platform-level completeness or unique users.',
                          'Budget values combine known billed bytes and outstanding reservations; they are not a billing invoice.']}
    def error(code,message,**details):report['errors'].append({'code':code,'message':str(message),**details})
    if len(tools)!=30 or len(registry['tools'])!=30:error('cohort_size','Expected thirty distinct tool IDs')
    if start!=date(2023,1,1) or end<=start:error('date_scope','Expected history beginning 2023-01-01 and a later completed-date endpoint')
    with shared_snapshot_lock(data/'.pilot-collection.lock',args.lock_timeout_seconds):
        with b.connect(data_dir=data) as con:
            jobs=rows(con,'SELECT * FROM backfill.jobs')
            objects=rows(con,'SELECT * FROM backfill.objects')
            reuse=rows(con,'SELECT * FROM backfill.job_reuse')
            budget=rows(con,'SELECT * FROM backfill.budget')
            record_counts=rows(con,'SELECT source,count(*) AS records,count(DISTINCT tool_id) AS distinct_tool_ids FROM backfill.records GROUP BY source ORDER BY source')
            cursor_counts={(row['job_id'],int(row['cursor'])):int(row['records']) for row in rows(con,'SELECT job_id,cursor,count(*) AS records FROM backfill.records GROUP BY job_id,cursor')}
            table_counts={}
            for item in rows(con,"SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema IN ('backfill','pilot') ORDER BY table_schema,table_name"):
                schema=item['table_schema'].replace('"','""');table=item['table_name'].replace('"','""')
                table_counts[schema+'.'+table]=int(con.execute('SELECT count(*) FROM "'+schema+'"."'+table+'"').fetchone()[0])
    report['metadata_snapshot_finished_at']=now()
    by_job={job['job_id']:job for job in jobs};objects_by_job=defaultdict(list);reuse_by_job={item['job_id']:item for item in reuse};budget_by_job={item['job_id']:int(item['reserved_bytes']) for item in budget}
    current=[job for job in jobs if job['plan_sha']==plan_sha]
    report['current_plan_states']=[{'source':source,'state':state,'jobs':count} for (source,state),count in sorted(Counter((job['source'],job['state']) for job in current).items())]
    active=[job for job in current if job['state'] not in FINISHED]
    report['active_unfinished_jobs']=[{key:job[key] for key in ['job_id','source','tool_id','generation','start_date','end_date','state','attempts','next_attempt']} for job in active]
    if active:error('unfinished_jobs',f'{len(active)} active-plan jobs remain unfinished across history/refresh generations')
    if not current:error('missing_plan','No jobs exist for the active registry digest')
    registry_cache={}
    def retained_registry(digest):
        if digest in registry_cache:return registry_cache[digest]
        folder=data/'raw/backfill/registries'/digest
        try:
            value=read_json(folder/'tools.json');metadata=read_json(folder/'manifest.json');raw=b.encode(value).encode()
            expected=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+raw).hexdigest()
            if (expected!=digest or metadata.get('plan_sha256')!=digest or metadata.get('format_version')!=b.FORMAT_VERSION
                    or metadata.get('registry_sha256')!=hashlib.sha256(raw).hexdigest()):raise ValueError('Registry manifest/hash or extraction-format mismatch')
            result={tool['tool_id']:tool for tool in value['tools']}
        except (OSError,ValueError,TypeError,KeyError) as problem:
            error('registry_provenance',problem,plan_sha=digest);result={}
        registry_cache[digest]=result;return result
    retained_registry(plan_sha)
    valid_objects=set();known_queries={};query_caps=defaultdict(list);known_billed=defaultdict(list);referenced_paths=set()
    for obj in objects:
        identifier=obj['object_id'];job=by_job.get(obj['job_id']);objects_by_job[obj['job_id']].append(obj)
        before=len(report['errors'])
        try:
            if not job:raise ValueError('Raw object references a missing job')
            if obj['source']!=job['source']:raise ValueError('Raw object source differs from its job')
            relative=Path(obj['raw_path']);path=(data/relative).resolve()
            if relative.is_absolute() or not path.is_relative_to((data/'raw/backfill').resolve()):raise ValueError('Raw path escapes raw/backfill')
            referenced_paths.add(path)
            if not path.is_file():raise ValueError('Retained gzip is missing')
            if path.stat().st_size!=int(obj['compressed_bytes']):raise ValueError('Compressed byte size differs from the database')
            digest=hashlib.sha256();length=0;query_raw=[]
            with gzip.open(path,'rb') as stream:
                for chunk in iter(lambda:stream.read(1024*1024),b''):
                    digest.update(chunk);length+=len(chunk)
                    if obj['source'] in ('pypi','gharchive'):query_raw.append(chunk)
            if length!=int(obj['raw_bytes']) or digest.hexdigest()!=obj['sha256']:raise ValueError('Decompressed byte count/SHA256 differs from the database')
            metadata=json.loads(obj['metadata']) if isinstance(obj['metadata'],str) else obj['metadata']
            sidecar=read_json(path.with_suffix('.manifest.json'))
            if sidecar!=metadata:raise ValueError('Sidecar manifest differs from database metadata')
            expected={'source':obj['source'],'start':str(job['start_date']),'end_exclusive':str(job['end_date']),
                      'plan_sha256':job['plan_sha'],'format_version':b.FORMAT_VERSION,'raw_sha256':obj['sha256'],'raw_path':obj['raw_path']}
            if any(metadata.get(key)!=value for key,value in expected.items()):raise ValueError('Manifest identity/date/hash differs from the database job')
            if timestamp(metadata['observed_at'])!=timestamp(obj['observed_at']):raise ValueError('Manifest observed time differs from the database')
            if identifier!=b.key(job['job_id'],obj['cursor'],obj['sha256']):raise ValueError('Raw object ID is inconsistent')
            old_registry=retained_registry(job['plan_sha'])
            if not old_registry:raise ValueError('Raw object registry provenance is invalid')
            if obj['source'] in ('pypi','gharchive'):
                request=metadata.get('request') or {};cap=int(request['maximum_bytes_billed'])
                if not 0<cap<=int(config['max_bytes_per_query']):raise ValueError('Missing/invalid per-query bytes cap')
                query_caps[job['job_id']].append(cap)
                external=metadata.get('job_id')
                if not external:raise ValueError('Completed query has no retained external job ID')
                if not metadata.get('coverage',{}).get('query_completed'):raise ValueError('Saved aggregate does not attest query completion')
                if not request.get('project') or not request.get('query_parameters'):raise ValueError('Query project/parameters were not retained')
                source_tools=request.get('tools') or []
                if {item.get('tool_id') for item in source_tools}!=set(old_registry):raise ValueError('Query tool cardinality differs from its frozen registry')
                parameters={item['name']:item.get('parameterValue',{}) for item in request['query_parameters']}
                if (parameters.get('start_date',{}).get('value')!=str(job['start_date'])
                        or parameters.get('end_date',{}).get('value')!=str(job['end_date'])
                        or json.loads(parameters.get('tool_map',{}).get('value','null'))!=source_tools):
                    raise ValueError('Query parameter dates/tool map differ from retained metadata')
                ledger=json.loads(b''.join(query_raw));returned_jobs=[]
                for response in ledger.get('responses',[]):
                    body=response.get('response',{})
                    if body.get('jobReference',{}).get('jobId')==external and body.get('configuration',{}).get('query'):
                        returned_jobs.append(body)
                if not returned_jobs:raise ValueError('No returned BigQuery job configuration is retained in raw evidence')
                for returned in returned_jobs:
                    query=returned['configuration']['query']
                    verify_returned_query(query,cap,request['query_parameters'])
                stats=request.get('statistics') or {}
                if 'totalBytesBilled' in stats:
                    billed=int(stats['totalBytesBilled'])
                    if not 0<=billed<=cap:raise ValueError('Known billed bytes exceed the query cap')
                    known_billed[job['job_id']].append(billed)
                    key=(request['project'],request.get('location','US'),external)
                    if key in known_queries and known_queries[key]!=billed:raise ValueError('Same external query has inconsistent billed-byte evidence')
                    known_queries[key]=billed
            if len(report['errors'])==before:valid_objects.add(identifier)
        except (OSError,EOFError,ValueError,TypeError,KeyError,zlib.error) as problem:error('raw_integrity',problem,object_id=identifier,job_id=obj['job_id'],raw_path=obj['raw_path'])
    report['raw_objects_checked']=len(objects);report['raw_objects_verified']=len(valid_objects)
    report['backfill_raw_bytes']=sum(int(obj['raw_bytes']) for obj in objects)
    report['backfill_compressed_bytes']=sum(int(obj['compressed_bytes']) for obj in objects)
    # Uncommitted files can survive an interrupted write; expose them, never use
    # them as completed coverage. Verify their sidecar hash when one exists.
    untracked=[]
    for path in sorted((data/'raw/backfill').rglob('*.gz')):
        if path.resolve() in referenced_paths:continue
        item={'path':str(path.relative_to(data)),'compressed_bytes':path.stat().st_size}
        try:
            metadata=read_json(path.with_suffix('.manifest.json'));digest=hashlib.sha256();length=0
            with gzip.open(path,'rb') as stream:
                for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk);length+=len(chunk)
            if digest.hexdigest()!=metadata.get('raw_sha256'):raise ValueError('Untracked gzip differs from its sidecar hash')
            item.update(raw_bytes=length,sha256=digest.hexdigest(),manifest_verified=True)
        except (OSError,EOFError,ValueError,TypeError,zlib.error) as problem:error('untracked_raw_integrity',problem,raw_path=item['path'])
        untracked.append(item)
    report['untracked_backfill_gzip_files']=untracked
    if untracked:report['warnings'].append('Untracked raw files are retained but excluded from database-backed coverage; they may be interrupted writes.')
    completed_cache={}
    def completed_raw(job):
        if job['job_id'] in completed_cache:return completed_cache[job['job_id']]
        evidence=objects_by_job.get(job['job_id'],[])
        valid=bool(evidence) and all(obj['object_id'] in valid_objects for obj in evidence)
        if not valid:error('completed_raw_missing','Completed job has missing or invalid retained raw evidence',job_id=job['job_id'])
        if valid and job['source']=='arxiv':
            pages={}
            for obj in evidence:
                cursor=int(obj['cursor'])
                if cursor not in pages or obj['observed_at']>pages[cursor]['observed_at']:pages[cursor]=obj
            cursor=0;seen=set()
            try:
                while True:
                    if cursor in seen:raise ValueError('ArXiv pagination cycles')
                    seen.add(cursor);obj=pages[cursor];metadata=json.loads(obj['metadata']) if isinstance(obj['metadata'],str) else obj['metadata'];pagination=metadata['pagination']
                    returned=int(pagination['returned']);total=int(pagination['total_results'])
                    if int(pagination['start'])!=cursor or not 0<=returned<=100 or not 0<=total<=30000:raise ValueError('Invalid arXiv pagination bounds')
                    if cursor_counts.get((job['job_id'],cursor),0)!=returned:raise ValueError('Stored paper-row count differs from the retained page')
                    if pagination.get('complete'):
                        if cursor+returned<total or pagination.get('next_cursor') is not None:raise ValueError('ArXiv completion marker is inconsistent')
                        break
                    following=int(pagination['next_cursor'])
                    if following!=cursor+returned or following<=cursor:raise ValueError('ArXiv next cursor is inconsistent')
                    cursor=following
            except (ValueError,TypeError,KeyError) as problem:error('arxiv_pagination',problem,job_id=job['job_id']);valid=False
        completed_cache[job['job_id']]=valid;return valid
    intervals=defaultdict(list);evidence_jobs=defaultdict(list);reuse_resolved=[]
    for job in current:
        if job['generation']!='history' or job['state'] not in ('done','reused'):continue
        source,tid=job['source'],job['tool_id']
        if source not in ('arxiv','pypi_releases','pypi','gharchive'):continue
        original=job
        if job['state']=='reused':
            try:
                link=reuse_by_job[job['job_id']];original=by_job[link['reused_job_id']]
                if original['state']!='done' or original['generation']!='history' or original['plan_sha']!=link['reused_plan_sha']:
                    raise ValueError('Reuse target is not the declared completed history job')
                for field in ['source','tool_id','start_date','end_date']:
                    if original[field]!=job[field]:raise ValueError('Reuse target has a different '+field)
                old=retained_registry(original['plan_sha'])
                if source not in ('arxiv','pypi_releases') or b.source_mapping(tools[tid],source)!=b.source_mapping(old[tid],source) or link['mapping_sha256']!=b.source_mapping(tools[tid],source):
                    raise ValueError('Reuse mapping hash does not match both registry snapshots')
                reuse_resolved.append({'job_id':job['job_id'],'original_job_id':original['job_id'],'source':source,'tool_id':tid,'start_date':job['start_date'],'end_date':job['end_date']})
            except (ValueError,KeyError,TypeError) as problem:error('reuse_provenance',problem,job_id=job['job_id']);continue
        if not completed_raw(original):continue
        group=(source,tid)
        intervals[group].append((job['start_date'],job['end_date']));evidence_jobs[group].append(job['job_id'])
    groups=[('arxiv',tid) for tid in sorted(tools)]+[('pypi_releases',tid) for tid in sorted(tools)]+[('pypi','__all__'),('gharchive','__all__')]
    coverage=[]
    for source,tid in groups:
        result=interval_union(intervals[(source,tid)],start,end)
        result.update(source=source,tool_id=tid,evidence_job_count=len(evidence_jobs[(source,tid)]))
        if result['gaps']:error('history_coverage_gap','Retained completed history has uncovered dates',source=source,tool_id=tid,gaps=result['gaps'])
        coverage.append(result)
    report['history_coverage_groups']=coverage;report['expected_history_group_count']=62
    report['complete_history_group_count']=sum(not item['gaps'] for item in coverage)
    report['resolved_reused_windows']=reuse_resolved
    for job in jobs:
        if job['state']=='done':completed_raw(job)
        if job.get('pending') and job['source'] in ('pypi','gharchive'):
            pending=json.loads(job['pending']) if isinstance(job['pending'],str) else job['pending']
            if pending.get('job_id') and job['job_id'] not in budget_by_job:error('pending_query_budget','Pending external query has no ledger reservation',job_id=job['job_id'])
    for identifier,caps in query_caps.items():
        required=max(known_billed[identifier]) if known_billed[identifier] else max(caps)
        if identifier not in budget_by_job or budget_by_job[identifier]<required:error('query_budget_history','Retained query has a missing/insufficient cumulative ledger entry',job_id=identifier,minimum_required_bytes=required)
    reserved=sum(budget_by_job.values());cap=int(config['max_total_query_bytes'])
    if any(value<0 for value in budget_by_job.values()) or reserved>cap:error('global_budget','Global query ledger is negative or exceeds the configured cumulative byte cap')
    unique_known=sum(known_queries.values())
    if reserved<unique_known:error('global_budget_history','Global ledger is below known unique retained-query billed bytes')
    report['budget']={'limit_usd':config.get('query_budget_usd'),'configured_scan_byte_cap':cap,'global_ledger_entries':len(budget),'reserved_and_known_billed_bytes':reserved,'known_billed_bytes_unique_retained_queries':unique_known,'unique_retained_queries_with_billed_bytes':len(known_queries),'conservative_cost_usd_before_free_tier':reserved/1024**4*float(config.get('query_price_usd_per_TiB',6.25)),'is_invoice':False,'history_check':'Each retained completed query must retain a sufficient ledger row; no claim is made about charges outside retained evidence.'}
    report['global_backfill_counts']={'jobs':len(jobs),'objects':len(objects),'records_by_source':record_counts,'table_rows':table_counts}
    report['disk_free_bytes']=shutil.disk_usage(data).free
    report['disk_free_GiB']=round(report['disk_free_bytes']/1024**3,3)
    try:
        size=subprocess.run(['du','-sb','--',str(data)],capture_output=True,text=True,timeout=30,check=True)
        report['projectdata_size_bytes']=int(size.stdout.split()[0])
    except (OSError,ValueError,subprocess.SubprocessError) as problem:
        report['projectdata_size_bytes']=None;report['warnings'].append('Data directory size unavailable: '+str(problem))
    if sha_file(config_path)!=config_sha or sha_file(registry_path)!=registry_sha:error('configuration_changed','Config/registry changed while the audit was running')
    report.update(finished_at=now(),error_count=len(report['errors']),ok=not report['errors'],status='passed' if not report['errors'] else 'failed_or_incomplete')
    output=(args.output or data/'collection_audit.json').expanduser().resolve();atomic_json(output,report)
    print(encode({'status':report['status'],'ok':report['ok'],'history_groups_complete':report['complete_history_group_count'],'history_groups_expected':62,'raw_objects_verified':len(valid_objects),'raw_objects_expected':len(objects),'error_count':len(report['errors']),'output':str(output)}),flush=True)
    return 0 if report['ok'] else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/backfill.json')
    parser.add_argument('--registry',type=Path)
    parser.add_argument('--as-of',help='Completed-date endpoint, exclusive; defaults to current UTC date')
    parser.add_argument('--output',type=Path)
    parser.add_argument('--lock-timeout-seconds',type=int,default=900)
    args=parser.parse_args()
    if not 1<=args.lock_timeout_seconds<=1800:parser.error('Lock wait must be 1–1800 seconds')
    try:return audit(args)
    except Exception as problem:
        failure={'ok':False,'status':'audit_error','finished_at':now(),'error':type(problem).__name__+': '+str(problem)}
        output=args.output
        if output is None:
            try:output=Path(read_json(args.config.expanduser().resolve())['data_dir']).expanduser()/'collection_audit.json'
            except (OSError,ValueError,KeyError,TypeError):pass
        if output:atomic_json(output.expanduser().resolve(),failure)
        print(encode(failure),file=sys.stderr,flush=True);return 1


if __name__=='__main__':raise SystemExit(main())
