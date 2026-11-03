"""Bounded header-aware issue updates plus direct details for unknown PRs.

Versioned independently from the old full-PR-list refresh. No full PR scan is
scheduled. A completed issue cycle can publish while PR detail gaps stay unknown.
"""
import argparse,gzip,hashlib,json
from datetime import datetime,timedelta,timezone
from pathlib import Path
import shutil
import build_github_activity as b
import refresh_github_activity as r

SOURCE='github_direct_activity_updates_v2'
STATE='activity_update_state.json'


def baseline_index(baseline,registry_path,root):
    baseline=Path(baseline);manifest=json.loads((baseline/'manifest.json').read_text());cohort=b.sha(registry_path)
    if manifest.get('source')!=b.SOURCE or manifest.get('registry_sha256')!=cohort:raise ValueError('PR baseline index source/cohort differs')
    fingerprint=b.sha(baseline/'manifest.json');directory=Path(root)/'baseline_indexes'/fingerprint;index=directory/'pr_creation.json.gz';proof=directory/'manifest.json'
    if proof.is_file():
        p=json.loads(proof.read_text())
        if p.get('baseline_manifest_sha256')!=fingerprint or p.get('registry_sha256')!=cohort or b.sha(index)!=p.get('index_sha256'):raise ValueError('Cached PR creation index hash differs')
        return json.loads(gzip.decompress(index.read_bytes())),p
    source=baseline/'normalized_records.jsonl.gz'
    if b.sha(source)!=manifest.get('files',{}).get(source.name):raise ValueError('Baseline normalized records hash differs')
    values={}
    with gzip.open(source,'rt') as f:
        for line in f:
            item=json.loads(line)
            if item['kind']!='pull':continue
            created=item.get('pr_created_at') or item.get('source_view_observations',{}).get('core_pr',{}).get('created_at') or (item['created_at'] if item.get('direct_pr_observation') else None)
            if created:values[item['tool_id']+'/'+str(item['number'])]=created
    directory.mkdir(parents=True,exist_ok=True);raw=json.dumps(values,sort_keys=True,separators=(',',':')).encode();index.write_bytes(gzip.compress(raw,mtime=0))
    proof_value={'source':'verified_baseline_pr_creation_index','registry_sha256':cohort,'baseline_manifest_sha256':fingerprint,
        'baseline_normalized_sha256':manifest['files'][source.name],'index_sha256':b.sha(index),'records':len(values)}
    r.atomic_json(proof,proof_value);return values,proof_value


def run(registry_path,data_dir,baseline,max_requests=4,minimum_free_gib=15,fetch=r.fetch_public,now=None):
    if not 1<=max_requests<=10:raise ValueError('max_requests must be 1..10')
    dynamic=now is None;now=now or datetime.now(timezone.utc);end=min(now.replace(hour=0,minute=0,second=0,microsecond=0),r.dt('2027-01-01T00:00:00Z'))
    root=Path(data_dir);root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<minimum_free_gib*1024**3:raise RuntimeError('Free disk guard')
    registry_raw=Path(registry_path).read_bytes();cohort=hashlib.sha256(registry_raw).hexdigest();registry=json.loads(registry_raw);tools={t['tool_id']:t for t in registry['tools']}
    if len(tools)!=len(registry['tools']) or any(not b.positive(t.get('github_repo_id')) for t in tools.values()):raise ValueError('Frozen cohort requires unique tools and numeric repository IDs')
    known,index_proof=baseline_index(baseline,registry_path,root);base_manifest=json.loads((Path(baseline)/'manifest.json').read_text());base_end=base_manifest['history_end_exclusive']
    path=root/STATE
    if path.is_file():state=json.loads(path.read_text())
    else:
        legacy=root/'refresh_state.json'
        state={'source':SOURCE,'registry_sha256':cohort,'baseline_end_exclusive':base_end,'streams':{},'requests':[],'completed_cycles':[],
            'pending_details':{},'completed_details':{},'historical_visibility_complete':False,
            'legacy_source_preserved':{'path':legacy.name,'sha256':b.sha(legacy)} if legacy.is_file() else None}
    if state.get('source')!=SOURCE or state.get('registry_sha256')!=cohort or state.get('baseline_end_exclusive')!=base_end:raise ValueError('Changed update-source schema/cohort/baseline endpoint')
    if state.get('not_before') and r.dt(state['not_before'])>now:return {'status':'rate_wait','requests':0,'not_before':state['not_before']}
    state.pop('not_before',None);state['baseline_index']=index_proof
    # Reconcile creation-only gaps with a newly validated, richer baseline.
    for key,pending in list(state['pending_details'].items()):
        if key in known and set(pending['reasons'])=={'pr_creation'}:
            state.setdefault('resolved_creation_from_baseline',{})[key]={'pr_created_at':known[key],'baseline_manifest_sha256':index_proof['baseline_manifest_sha256']}
            del state['pending_details'][key]
    keys=[(tid,kind) for tid in tools for kind in ('issues','detail')];cursor=state.get('cursor',0)%len(keys);visited=0;attempts=0;errors=[]
    while attempts<max_requests and visited<len(keys):
        tid,kind=keys[cursor];cursor=(cursor+1)%len(keys);visited+=1;tool=tools[tid];stream=state['streams'].setdefault(tid+'/issues',{'last_success_end_exclusive':None})
        if kind=='detail':
            candidates=[(k,v) for k,v in state['pending_details'].items() if v['tool_id']==tid]
            if not candidates:continue
            detail_key,pending=min(candidates,key=lambda item:(r.dt(item[1]['first_observed_at']),item[0]));url='https://api.github.com/repos/'+tool['github_repo']+'/pulls/'+str(pending['number'])
            cycle={'source':SOURCE,'stream_kind':'pull_detail','registry_sha256':cohort,'since':pending['first_observed_at'],'end_exclusive':r.iso(end),
                'cycle_id':'detail-'+detail_key+'-'+pending['latest_observed_at']}
            identity=False
        else:
            if not stream.get('cycle'):
                left=r.dt(stream['last_success_end_exclusive'] or base_end)
                if stream['last_success_end_exclusive'] and left>=end:continue
                since=r.iso(left-timedelta(days=7));cid=r.sha((tid+'|'+r.iso(now)+'|'+r.iso(left)+'|'+r.iso(end)).encode())
                stream['cycle']={'source':SOURCE,'stream_kind':'issues','registry_sha256':cohort,'cycle_id':cid,'coverage_start':r.iso(left),'since':since,
                    'end_exclusive':r.iso(end),'identity_verified':False,'pages':0,'receipt_paths':[],'seen_urls':[],'pr_numbers_seen':[],
                    'next_url':r.initial_url(tool,'issues',since)}
            cycle=stream['cycle'];identity=not cycle['identity_verified'];url='https://api.github.com/repos/'+tool['github_repo'] if identity else r.checked_url(cycle['next_url'],tool,'issues')
        try:
            attempts+=1;response=fetch(url);observed=datetime.now(timezone.utc) if dynamic else now
            if kind=='detail':
                if response['url']!=url:raise ValueError('PR detail redirected outside frozen number')
            else:r.same_response_query(url,response['url'],tool,None if identity else 'issues')
            meta=r.archive(root,tool,'pull_detail' if kind=='detail' else 'repository' if identity else 'issues',url,response,cycle,r.iso(observed));state['requests'].append(meta)
            pause=r.pause_until(response['headers'],observed)
            if pause:state['not_before']=r.iso(pause)
            if response['status']!=200:raise RuntimeError('GitHub HTTP '+str(response['status']))
            value=json.loads(response['body'])
            if kind=='detail':
                row=b.identity(value,tool,{'observed_at':meta['observed_at'],'raw_path':meta['raw_path']},True)
                if row['number']!=pending['number']:raise ValueError('PR detail number differs')
                state['completed_details'][detail_key]={'tool_id':tid,'number':row['number'],'pr_created_at':row['created_at'],'observed_at':meta['observed_at'],'raw_path':meta['raw_path']}
                del state['pending_details'][detail_key]
            elif identity:
                if not isinstance(value,dict) or value.get('id')!=tool['github_repo_id'] or value.get('full_name','').lower()!=tool['github_repo'].lower():raise ValueError('Frozen repository identity mismatch')
                cycle['identity_verified']=True;cycle['receipt_paths'].append(meta['raw_path'])
            else:
                if not isinstance(value,list) or len(value)>100:raise ValueError('Invalid bounded issue response')
                ids=[v.get('id') for v in value]
                if any(not b.positive(v) for v in ids) or len(ids)!=len(set(ids)):raise ValueError('Duplicate/invalid page IDs')
                if url in cycle['seen_urls']:raise ValueError('Actual provider cursor repeated')
                nxt=r.next_link(response['headers'].get('Link'),tool,'issues')
                if nxt and r.parse_qs(r.urlparse(nxt).query).get('since')!=[cycle['since']]:raise ValueError('Actual issue Link changed watermark')
                dates=[r.dt(v['updated_at']) for v in value]
                if dates!=sorted(dates):cycle.setdefault('serialized_update_order_anomalies',[]).append(meta['raw_path'])
                for value_row in value:
                    row=b.identity(value_row,tool,{'observed_at':meta['observed_at'],'raw_path':meta['raw_path'],'stream':'issues_visible'})
                    if row['kind']!='pull':continue
                    number=row['number'];key=tid+'/'+str(number);cycle['pr_numbers_seen'].append(number);reasons=[];completed=state['completed_details'].get(key)
                    if key not in known and not completed:reasons.append('pr_creation')
                    if not row['merged_at_field_present']:reasons.append('merged_at')
                    if not row['closed_at_field_present']:reasons.append('closed_at')
                    if reasons:
                        prior=state['pending_details'].get(key,{})
                        state['pending_details'][key]={'tool_id':tid,'number':number,'reasons':sorted(set(reasons)|set(prior.get('reasons',[]))),
                            'first_observed_at':prior.get('first_observed_at',meta['observed_at']),'latest_observed_at':meta['observed_at'],'wrapper_raw_path':meta['raw_path']}
                cycle['receipt_paths'].append(meta['raw_path']);cycle['seen_urls'].append(url);cycle['pages']+=1
                if not nxt:
                    done={k:v for k,v in cycle.items() if k!='seen_urls'};done.update(tool_id=tid,kind='issues',completed_at=meta['observed_at'],terminal_reason='provider_no_next_link')
                    state['completed_cycles'].append(done);stream['last_success_end_exclusive']=cycle['end_exclusive'];stream['last_completed_cycle_id']=cycle['cycle_id'];del stream['cycle']
                else:cycle['next_url']=nxt
        except Exception as error:
            errors.append({'tool_id':tid,'action':kind,'type':type(error).__name__,'message':str(error)})
            if not state.get('not_before'):state['not_before']=r.iso(observed+timedelta(minutes=15) if 'observed' in locals() else now+timedelta(minutes=15))
        state.update(cursor=cursor,updated_at=r.iso(datetime.now(timezone.utc) if dynamic else now));r.atomic_json(path,state)
        if state.get('not_before') and r.dt(state['not_before'])>now:break
    state.update(cursor=cursor,updated_at=r.iso(datetime.now(timezone.utc) if dynamic else now));r.atomic_json(path,state)
    result={'source':SOURCE,'status':'errors' if errors else 'bounded_success','requests':attempts,'completed_issue_cycles':len(state['completed_cycles']),
        'pending_pr_details':len(state['pending_details']),'errors':errors,'not_before':state.get('not_before')};r.atomic_json(root/'latest_updates_dispatch.json',result);return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--registry',type=Path,required=True);p.add_argument('--data-dir',type=Path,required=True);p.add_argument('--baseline',type=Path,required=True)
    p.add_argument('--max-requests',type=int,default=4);p.add_argument('--minimum-free-gib',type=float,default=15);a=p.parse_args()
    print(json.dumps(run(a.registry,a.data_dir,a.baseline,a.max_requests,a.minimum_free_gib)))
