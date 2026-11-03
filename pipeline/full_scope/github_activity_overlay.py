"""Join verified completed public update cycles onto an immutable GitHub baseline.

The connector baseline and unsigned update archive keep separate schemas. Only
completed, identity-verified, hash-bound Link chains become observations here.
Neither changing source vintage is eligible as historical primary evidence.
"""
import argparse,csv,gzip,hashlib,json,shutil,tempfile
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
import build_github_activity as b
import refresh_github_activity as refresh
import native_pr_updates


def read(path):return json.loads(Path(path).read_text())
def safe(root,name):
    p=(Path(root)/name).resolve()
    if not p.is_relative_to(Path(root).resolve()) or not p.is_file():raise ValueError('Missing or escaped update source path')
    return p

def baseline(root,registry_sha):
    root=Path(root);m=read(root/'manifest.json')
    if m.get('source')!=b.SOURCE or m.get('registry_sha256')!=registry_sha:raise ValueError('Baseline source/cohort differs')
    for name,digest in m['files'].items():
        if b.sha(safe(root,name))!=digest:raise ValueError('Baseline file hash differs')
    q=read(root/'source_quality.json');records={}
    with gzip.open(root/'normalized_records.jsonl.gz','rt') as f:
        for line in f:
            record=json.loads(line);views=record.get('source_view_observations')
            if not views:
                view='core_pr' if record.get('direct_pr_observation') else 'search_index'
                record.update(source_view=view,issue_created_at=None if view=='core_pr' else record['created_at'],
                    pr_created_at=record['created_at'] if view=='core_pr' else None,
                    merged_at_field_present=record.get('direct_pr_observation',False),closed_at_field_present=True)
                views={view:record}
            for row in views.values():b.merge_record(records,dict(row),2000000)
    weekly={}
    for name in ['github_activity_weekly.csv','github_activity_partial_weeks.csv']:
        if (root/name).is_file():
            with (root/name).open() as f:
                for row in csv.DictReader(f):weekly[(row['tool_id'],row['week_start'])]=row
    return m,q,records,weekly

UPDATE_SOURCE='github_direct_activity_updates_v2'

def body(root,meta,cohort,max_bytes,source=refresh.SOURCE):
    if meta.get('source')!=source or meta.get('status')!=200:raise ValueError('Non-successful or mixed-schema update receipt')
    if meta.get('registry_sha256') not in (None,cohort) or source!=refresh.SOURCE and meta.get('registry_sha256')!=cohort:raise ValueError('Update receipt cohort differs')
    p=safe(root,meta['raw_path']);manifest=read(p.with_suffix(p.suffix+'.manifest.json'))
    if manifest!=meta:raise ValueError('Update receipt differs from immutable sidecar')
    if b.sha(p)!=meta['gzip_sha256']:raise ValueError('Update compressed body hash differs')
    with gzip.open(p,'rb') as f:raw=f.read(max_bytes+1)
    if len(raw)>max_bytes or hashlib.sha256(raw).hexdigest()!=meta['raw_sha256']:raise ValueError('Update decoded body hash/size differs')
    return json.loads(raw)

def completed_updates(root,state,registry,cohort,max_bytes=64*1024**2,known_records=None):
    if state.get('source') not in {refresh.SOURCE,UPDATE_SOURCE} or state.get('registry_sha256')!=cohort:raise ValueError('Incremental source/cohort differs')
    tools={t['tool_id']:t for t in registry['tools']};receipts={};observations=[];coverage=defaultdict(list);verified={};cycles=[];seen_cycles=set()
    for meta in state['requests']:
        if meta['raw_path'] in receipts and receipts[meta['raw_path']]!=meta:raise ValueError('Conflicting update receipt identity')
        receipts[meta['raw_path']]=meta
    details={};new_source=state['source']==UPDATE_SOURCE
    if new_source:
        for key,detail in state.get('completed_details',{}).items():
            name=detail['raw_path'];meta=receipts.get(name)
            if meta is None:raise ValueError('Missing completed PR-detail receipt')
            value=body(root,meta,cohort,max_bytes,state['source']);tid=detail['tool_id']
            if tid not in tools or meta['kind']!='pull_detail' or meta['github_repo_id']!=tools[tid]['github_repo_id']:raise ValueError('PR-detail identity differs')
            expected='https://api.github.com/repos/'+tools[tid]['github_repo']+'/pulls/'+str(detail['number'])
            if meta['request_url']!=expected or meta['response_url']!=expected or key!=tid+'/'+str(detail['number']):raise ValueError('PR-detail endpoint differs')
            item=b.identity(value,tools[tid],{'observed_at':meta['observed_at'],'raw_path':name},True)
            if item['created_at']!=detail['pr_created_at'] or item['observed_at']!=detail['observed_at']:raise ValueError('Completed PR-detail state differs from body')
            item.update(source_schema=UPDATE_SOURCE,source_cycle_id=meta['cycle_id']);details[key]=item
            verified[name]={'gzip_sha256':meta['gzip_sha256'],'raw_sha256':meta['raw_sha256'],'sidecar_sha256':b.sha(safe(root,name+'.manifest.json'))}
    used_details=set()
    for cycle in state.get('completed_cycles',[]):
        tid=cycle['tool_id'];kind=cycle['kind'];cid=cycle['cycle_id']
        if tid not in tools or kind not in {'issues','pulls'} or cid in seen_cycles:raise ValueError('Invalid or duplicate completed cycle')
        seen_cycles.add(cid);tool=tools[tid];left=b.utc(cycle['coverage_start']);right=b.utc(cycle['end_exclusive']);since=b.utc(cycle['since'])
        if not since<=left<=right<=b.utc(cycle['completed_at']) or left<b.utc(state['baseline_end_exclusive']):raise ValueError('Update cycle watermark does not cover its declared interval')
        if cycle.get('registry_sha256')!=cohort or cycle.get('stream_kind')!=kind or not cycle.get('identity_verified'):raise ValueError('Completed cycle identity/cohort is unverified')
        next_url='https://api.github.com/repos/'+tool['github_repo'];identity_ok=False;pages=0;last_edge=None;terminal=None;used=set();used_urls=set();cycle_rows=[]
        for name in cycle['receipt_paths']:
            if name in used or name not in receipts:raise ValueError('Missing/duplicate cycle receipt')
            used.add(name);meta=receipts[name];value=body(root,meta,cohort,max_bytes,state['source'])
            if meta['tool_id']!=tid or meta['github_repo_id']!=tool['github_repo_id'] or meta.get('github_repo','').lower()!=tool['github_repo'].lower():raise ValueError('Incremental numeric repository mapping differs')
            if meta['window_since']!=cycle['since'] or meta['window_end_exclusive']!=cycle['end_exclusive']:raise ValueError('Update cycle window changed')
            if meta.get('cycle_id') not in (None,cid):raise ValueError('Update receipt cycle differs')
            if terminal or meta['request_url']!=next_url or meta['request_url'] in used_urls:raise ValueError('Incremental Link chain differs')
            used_urls.add(meta['request_url'])
            refresh.same_response_query(meta['request_url'],meta['response_url'],tool,None if not identity_ok else kind)
            if b.utc(meta['observed_at'])>b.utc(cycle['completed_at']):raise ValueError('Cycle completion predates its observation')
            if not identity_ok:
                if meta['kind']!='repository' or not isinstance(value,dict) or value.get('id')!=tool['github_repo_id'] or value.get('full_name','').lower()!=tool['github_repo'].lower():raise ValueError('Incremental repository identity body differs')
                identity_ok=True;next_url=refresh.initial_url(tool,kind,cycle['since'])
            else:
                if meta['kind']!=kind or not isinstance(value,list) or len(value)>100:raise ValueError('Incremental list page shape differs')
                query=parse_qs(urlsplit(meta['request_url']).query)
                for key,expected in {'state':'all','sort':'updated','direction':'asc' if kind=='issues' else 'desc','per_page':'100'}.items():
                    if query.get(key)!=[expected]:raise ValueError('Incremental list filter differs')
                if kind=='issues' and query.get('since')!=[cycle['since']]:raise ValueError('Incremental issue watermark differs')
                ids=[r.get('id') for r in value]
                if any(not b.positive(v) for v in ids) or len(ids)!=len(set(ids)):raise ValueError('Duplicate/invalid incremental page IDs')
                dates=[b.utc(r['updated_at']) for r in value]
                if dates!=sorted(dates,reverse=kind=='pulls') or last_edge is not None and dates and (dates[0]<last_edge if kind=='issues' else dates[0]>last_edge):cycle.setdefault('_serialized_order_anomalies',[]).append(name)
                if kind=='issues' and any(d<=since for d in dates):cycle.setdefault('_serialized_since_anomalies',[]).append(name)
                if dates:last_edge=dates[-1]
                nxt=refresh.next_link(meta['headers'].get('Link'),tool,kind)
                terminal='provider_no_next_link' if not nxt else None
                next_url=nxt;pages+=1
                for row in value:
                    item=b.identity(row,tool,{'observed_at':meta['observed_at'],'raw_path':name,'stream':'issues_visible' if kind=='issues' else None},kind=='pulls')
                    if not b.utc(item['created_at'])<=b.utc(row['updated_at'])<=b.utc(meta['observed_at']):raise ValueError('Incremental update timestamp contradicts observation')
                    item['source_schema']=state['source'];item['source_cycle_id']=cid;item['source_view']='core_pr' if kind=='pulls' else 'issues_visible';cycle_rows.append(item)
            verified[name]={'gzip_sha256':meta['gzip_sha256'],'raw_sha256':meta['raw_sha256'],'sidecar_sha256':b.sha(safe(root,name+'.manifest.json'))}
        if not identity_ok or terminal!=cycle.get('terminal_reason') or pages!=cycle['pages']:raise ValueError('Incomplete update cycle cannot advance a feature watermark')
        stream=state.get('streams',{}).get(tid+'/'+kind,{})
        if not stream.get('last_success_end_exclusive') or b.utc(stream['last_success_end_exclusive'])<right:raise ValueError('Completed cycle exceeds stream watermark')
        coverage[(tid,kind)].append((left,right))
        if new_source:
            if kind!='issues':raise ValueError('Changed-PR queue source cannot contain a full PR scan')
            creations_ok=True;merges_ok=True;closes_ok=True;issue_closes_ok=True
            for item in cycle_rows:
                key=tid+'/'+str(item['number']);detail=details.get(key)
                baseline_row=(known_records or {}).get((tid,item['repo_id'],item['number']),{})
                if item['kind']=='pull':
                    creation=baseline_row.get('pr_created_at') or (detail or {}).get('pr_created_at')
                    creations_ok=creations_ok and bool(creation)
                    fresh=detail is not None and b.utc(detail['observed_at'])>=b.utc(item['observed_at'])
                    merges_ok=merges_ok and (item['merged_at_field_present'] or fresh and detail['merged_at_field_present'])
                    closes_ok=closes_ok and (item['closed_at_field_present'] or fresh and detail['closed_at_field_present'])
                    if detail is not None:used_details.add(key)
                else:issue_closes_ok=issue_closes_ok and item['closed_at_field_present']
            if creations_ok:coverage[(tid,'issue_profile_pr_creation')].append((left,right))
            if merges_ok:coverage[(tid,'issue_profile_pr_merge')].append((left,right))
            if closes_ok:coverage[(tid,'issue_profile_pr_close')].append((left,right))
            if issue_closes_ok:coverage[(tid,'issue_close')].append((left,right))
        observations.extend(cycle_rows);cycles.append({'tool_id':tid,'kind':kind,'cycle_id':cid,'coverage_start':cycle['coverage_start'],'end_exclusive':cycle['end_exclusive'],'records':len(cycle_rows),'serialized_order_anomaly_pages':cycle.get('_serialized_order_anomalies',[]),'serialized_since_anomaly_pages':cycle.get('_serialized_since_anomalies',[])})
    observations.extend(details[key] for key in sorted(used_details))
    partial=[{'stream':key,'pages':v['cycle'].get('pages',0),'end_exclusive':v['cycle']['end_exclusive']} for key,v in state.get('streams',{}).items() if v.get('cycle')]
    return observations,coverage,{'completed_cycles':cycles,'partial_cycles_excluded':partial,'verified_incremental_files':verified,'pending_pr_details':len(state.get('pending_details',{})),'source_schema':state['source']}

def build(baseline_root,incremental_root,registry_path,output,end_exclusive=None):
    output=Path(output)
    if output.exists():raise FileExistsError('Use a fresh immutable overlay output')
    registry=read(registry_path);cohort=b.sha(registry_path);base_manifest,base_quality,records,base_weeks=baseline(baseline_root,cohort)
    state_path=Path(incremental_root)/'activity_update_state.json'
    if not state_path.is_file():state_path=Path(incremental_root)/'refresh_state.json'
    if state_path.is_file():state_bytes=state_path.read_bytes();state=json.loads(state_bytes)
    else:
        state={'source':UPDATE_SOURCE,'registry_sha256':cohort,'baseline_end_exclusive':base_manifest['history_end_exclusive'],
            'streams':{},'requests':[],'completed_cycles':[],'completed_details':{},'pending_details':{},'issue_source_absent':True}
        state_bytes=json.dumps(state,sort_keys=True).encode()
    updates,coverage,diag=completed_updates(incremental_root,state,registry,cohort,known_records=records)
    native_path=Path(incremental_root)/native_pr_updates.STATE;native_bytes=None;native_state={};native_diag=None
    if native_path.is_file():
        native_bytes=native_path.read_bytes();native_state=json.loads(native_bytes)
        if native_state.get('baseline_end_exclusive')!=state['baseline_end_exclusive']:raise ValueError('Independent native baseline cutoff differs')
        native_rows,native_coverage,native_diag=native_pr_updates.decode_completed(incremental_root,native_state,registry,cohort,body)
        updates.extend(native_rows)
        for key,periods in native_coverage.items():coverage[key].extend(periods)
        diag['native_pr_updates']=native_diag
    for item in sorted(updates,key=lambda r:(b.utc(r['observed_at']),r['source_path'])):b.merge_record(records,item,2000000)
    now=datetime.now(timezone.utc);latest_end=max([base_manifest['history_end_exclusive'],*[c['end_exclusive'] for c in state.get('completed_cycles',[])],*[c['end_exclusive'] for c in native_state.get('completed_cycles',[])]],key=b.utc)
    end=end_exclusive or latest_end;start=base_manifest['history_start'];base_end=b.utc(base_manifest['history_end_exclusive'])
    if b.utc(end)>now or b.utc(end)<base_end:raise ValueError('Overlay endpoint is future or before immutable baseline')
    dummy_core={t['tool_id']:{'created_census_complete':False} for t in registry['tools']}
    rows=b.weekly_features(records,defaultdict(list),dummy_core,registry,start,end,True)
    # Exact values have independent resource populations. A wrapper-only PR is
    # retained in mixed diagnostics, but cannot enter a certified native series.
    measurement_records={}
    for key,record in records.items():
        views=record.get('_view_records',{})
        if record['kind']=='pull':candidates=[v for v in views.values() if v.get('direct_pr_observation')]
        else:
            candidates=[v for name,v in views.items() if name in {'issues_visible','issues_visible_v3'} and v['kind']=='issue']
            if not candidates:candidates=[record]
        if candidates:measurement_records[key]=dict(max(candidates,key=lambda v:(b.utc(v['observed_at']),v['source_path'])))
    measurement_rows={(r['tool_id'],r['week_start']):r for r in b.weekly_features(measurement_records,defaultdict(list),dummy_core,registry,start,end,True)}
    uncertain_events=defaultdict(list)
    for record in measurement_records.values():
        for anomaly in record.get('event_time_anomalies',[]):
            if b.utc(start)<=b.utc(anomaly['event_at'])<b.utc(end):
                measure='github_pr_merged_count' if anomaly['field']=='merged_at' else 'github_pr_last_reported_closed_count'
                uncertain_events[(record['tool_id'],measure)].append({'number':record['number'],**anomaly})
    diag['native_event_time_uncertainty']={tid+'/'+measure:values for (tid,measure),values in uncertain_events.items()}
    kinds={'github_issue_opened_count':['issues'],'github_pr_opened_count':['pulls'],'github_pr_merged_count':['pulls'],
        'github_issue_last_reported_closed_count':['issues'],'github_pr_last_reported_closed_count':['pulls'],
        'github_issue_creation_authors':['issues'],'github_pr_creation_authors':['pulls'],'github_creation_authors':['issues','pulls']}
    if state['source']==refresh.SOURCE and native_state:
        for (tid,kind),periods in list(coverage.items()):
            if kind=='pulls':
                for feature in ['pr_creation','pr_merge','pr_close']:coverage[(tid,feature)].extend(periods)
    if state['source']==UPDATE_SOURCE or native_state:
        kinds.update(github_pr_opened_count=['pr_creation'],github_pr_creation_authors=['pr_creation'],github_creation_authors=['issues','pr_creation'],
            github_pr_merged_count=['pr_merge'],github_pr_last_reported_closed_count=['pr_close'],github_issue_last_reported_closed_count=['issue_close'])
    truth=lambda v:v is True or v=='True'
    for row in rows:
        tid=row['tool_id'];left=b.utc(row['effective_window_start']);right=b.utc(row['effective_window_end_exclusive']);old=base_weeks.get((tid,row['week_start']),{})
        measured=measurement_rows[(tid,row['week_start'])]
        flags=[]
        for measure in b.MEASURES:
            known=(left>=base_end or truth(old.get(measure+'_complete')))
            known=known and not uncertain_events[(tid,measure)]
            if right>base_end:known=known and all(b.covers(coverage[(tid,k)],max(left,base_end),right) for k in kinds[measure])
            if 'authors' in measure:
                unknown=sum(measured['github_'+k+'_unknown_creation_author_count'] for k in ('issue','pr') if (measure=='github_creation_authors' or k in measure))
                known=known and not unknown
            row[measure]=measured['observed_partial_'+measure] if known else None;row[measure+'_complete']=bool(known);flags.append(bool(known))
        for kind in ['issue','pr']:row['github_'+kind+'_unknown_creation_author_count']=measured['github_'+kind+'_unknown_creation_author_count']
        row['pr_creation_measurement_view']='independent_native_pr_observations'
        row['pr_merge_measurement_view']='independent_native_pr_observations'
        row['pr_last_closed_measurement_view']='independent_native_pr_observations'
        row['union_author_measurement_view']='true_issue_and_native_pr_observations'
        row['coverage_complete']=all(flags);row['retrieval_complete']=all(flags);row['source_vintage']='immutable_baseline_plus_completed_public_update_cycles'
    output.parent.mkdir(parents=True,exist_ok=True);stage=Path(tempfile.mkdtemp(prefix='.github-overlay-',dir=output.parent))
    try:
        with gzip.GzipFile(filename=str(stage/'normalized_records.jsonl.gz'),mode='wb',mtime=0) as f:
            for key,row in sorted(records.items()):f.write((b.encoded({**{k:v for k,v in row.items() if not k.startswith('_')},'source_view_observations':row['_view_records']})+'\n').encode())
        for name,group in [('github_activity_weekly.csv',[r for r in rows if r['week_complete']]),('github_activity_partial_weeks.csv',[r for r in rows if not r['week_complete']])]:
            with (stage/name).open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(rows[0]) if rows else ['tool_id','week_start']);w.writeheader();w.writerows(group)
        observed=max([base_quality['observed_at'],*[r['observed_at'] for r in updates],*[c['completed_at'] for c in state.get('completed_cycles',[])],*[c['completed_at'] for c in native_state.get('completed_cycles',[])]],key=b.utc)
        quality={'source':b.SOURCE,'status':'partial' if any(not r['coverage_complete'] for r in rows) else 'passed','registry_sha256':cohort,
            'history_start':start,'history_end_exclusive':end,'observed_at':observed,'built_at':now.isoformat(),
            'historical_visibility_complete':False,'primary_eligible':False,'collection_complete':all(r['coverage_complete'] for r in rows),
            'baseline_manifest_sha256':b.sha(Path(baseline_root)/'manifest.json'),'baseline_path':str(Path(baseline_root).resolve()),
            'incremental_state_sha256':hashlib.sha256(state_bytes).hexdigest(),
            'native_incremental_state_sha256':hashlib.sha256(native_bytes).hexdigest() if native_bytes is not None else None,'incremental_root':str(Path(incremental_root).resolve()),
            'normalized_records':len(records),'completed_update_observations':len(updates),'weekly_rows':sum(r['week_complete'] for r in rows),
            'diagnostics':diag,'limits':['Public update cycles are mutable retrospective observations, not atomic snapshots or historical publication receipts.',
            'Partial cycles never contribute exact features; unchanged baseline features retain their existing coverage policy.',
            'Issue-wrapper retrieval cannot certify future PR population coverage; independent completed native updated cycles supply those flags.',
            'Old closure state may change on reopen; counts use newest retained last-reported metadata, not a close/reopen event log.'],
            'code_sha256':{n:b.sha(Path(__file__).with_name(n)) for n in ['github_activity_overlay.py','build_github_activity.py','refresh_github_activity.py','native_pr_updates.py']}}
        for name in ['source_quality.json','github_activity_quality.json']:(stage/name).write_text(json.dumps(quality,indent=2)+'\n')
        (stage/'incremental_state.snapshot.json').write_bytes(state_bytes)
        if native_bytes is not None:(stage/'native_incremental_state.snapshot.json').write_bytes(native_bytes)
        manifest={k:quality[k] for k in ['source','status','registry_sha256','history_start','history_end_exclusive','observed_at','historical_visibility_complete','primary_eligible','collection_complete']}
        manifest['files']={p.name:b.sha(p) for p in stage.iterdir() if p.is_file()};(stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');stage.rename(output)
    except BaseException:shutil.rmtree(stage,ignore_errors=True);raise
    return quality

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['baseline','incremental-root','registry','output']:p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--end-exclusive');a=p.parse_args();q=build(a.baseline,a.incremental_root,a.registry,a.output,a.end_exclusive)
    print(json.dumps({'status':q['status'],'completed_update_observations':q['completed_update_observations'],'output':str(a.output)}))
