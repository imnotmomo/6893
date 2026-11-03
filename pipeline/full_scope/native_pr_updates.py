"""Independent, bounded native-PR updated-prefix capture and offline decoder.

No issue-wrapper resultset is used to discover PRs. A native cycle ends only at
an actual Link terminal or a full, ordered page strictly older than its local
lookback boundary. Failed order/boundary proofs never advance a watermark.
"""
import gzip,hashlib,json,re
from datetime import datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
import build_github_activity as b
import refresh_github_activity as r

SOURCE='github_native_pr_updated_observations_v1'
STATE='native_pr_update_state.json'


def native_url(url,tool):
    u=urlsplit(url);q=parse_qs(u.query,keep_blank_values=True)
    if u.scheme!='https' or u.netloc!='api.github.com' or u.fragment or u.path.lower() not in {'/repos/'+tool['github_repo'].lower()+'/pulls','/repositories/'+str(tool['github_repo_id'])+'/pulls'}:raise ValueError('Native updated URL leaves the frozen repository')
    expected={'state':['all'],'sort':['updated'],'direction':['desc'],'per_page':['100']}
    if any(q.get(k)!=v for k,v in expected.items()) or set(q)-set(expected)-{'page','after','before'} or any(len(v)!=1 for v in q.values()):raise ValueError('Native updated URL changes query scope')
    if 'page' in q and (not q['page'][0].isdigit() or int(q['page'][0])<1):raise ValueError('Native offset page is invalid')
    return url

def same_response_query(request,response,tool):
    native_url(request,tool);native_url(response,tool)
    if parse_qs(urlsplit(request).query,keep_blank_values=True)!=parse_qs(urlsplit(response).query,keep_blank_values=True):raise ValueError('Native response redirects to a different query')

def next_url(meta,tool):
    raw=meta.get('Link')
    if raw is None:return None
    if not isinstance(raw,str) or not raw.strip():raise ValueError('Native Link header is empty or malformed')
    links={}
    for part in raw.split(','):
        matched=re.fullmatch(r'\s*<([^<>]+)>\s*;\s*rel="(next|prev|first|last)"\s*',part)
        if not matched:raise ValueError('Native Link header has an unrecognized relation or syntax')
        target,relation=matched.groups()
        if relation in links:raise ValueError('Native Link relation is duplicated')
        links[relation]=native_url(target,tool)
    return links.get('next')

def validate_page(value,tool,observed,since,edge=None,seen=None):
    if not isinstance(value,list) or len(value)>100:raise ValueError('Native updated page shape differs')
    dates=[];numbers=[];items=[]
    for row in value:
        if not b.positive(row.get('id')):raise ValueError('Native provider ID is invalid')
        item=b.identity(row,tool,{'observed_at':observed,'raw_path':'native-page','stream':'native_pr_updated'},True);updated=b.utc(row.get('updated_at'))
        if not b.utc(item['created_at'])<=updated<=b.utc(observed):raise ValueError('Native updated timestamp contradicts creation/observation')
        if any(item[k] is not None and b.utc(item[k])>updated for k in ['merged_at','last_reported_closed_at']):raise ValueError('Native event is later than its updated timestamp')
        dates.append(updated);numbers.append(item['number']);items.append(item)
    if len({row['id'] for row in value})!=len(value):raise ValueError('Native page repeats provider IDs')
    if len(numbers)!=len(set(numbers)) or seen is not None and set(numbers).intersection(seen):raise ValueError('Native pagination repeats canonical PR numbers')
    if dates!=sorted(dates,reverse=True) or dates and edge is not None and dates[0]>b.utc(edge):raise ValueError('Native updated order is not monotone')
    old_boundary=len(value)==100 and bool(dates) and max(dates)<b.utc(since)
    return dates,numbers,items,old_boundary


def run(registry_path,root,baseline,max_requests=2,minimum_free_gib=15,fetch=r.fetch_public,now=None,max_pages=128):
    import shutil
    if not 1<=max_requests<=10 or not 1<=max_pages<=256:raise ValueError('Use bounded request/page caps')
    dynamic=now is None;now=now or datetime.now(timezone.utc);end=min(now.replace(hour=0,minute=0,second=0,microsecond=0),r.dt('2027-01-01T00:00:00Z'))
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<minimum_free_gib*1024**3:raise RuntimeError('Free disk guard')
    registry_raw=Path(registry_path).read_bytes();cohort=hashlib.sha256(registry_raw).hexdigest();tools={x['tool_id']:x for x in json.loads(registry_raw)['tools']}
    if any(not b.positive(t.get('github_repo_id')) for t in tools.values()):raise ValueError('Frozen native repository IDs required')
    base=json.loads((Path(baseline)/'manifest.json').read_text());base_end=base['history_end_exclusive']
    if base.get('source')!=b.SOURCE or base.get('registry_sha256')!=cohort or r.dt(base_end)>end:raise ValueError('Native baseline source/cohort/cutoff differs')
    path=root/STATE;state=json.loads(path.read_text()) if path.is_file() else {'source':SOURCE,'registry_sha256':cohort,'baseline_end_exclusive':base_end,'streams':{},'requests':[],'completed_cycles':[],'failed_cycles':[],'historical_visibility_complete':False}
    if state.get('source')!=SOURCE or state.get('registry_sha256')!=cohort or state['baseline_end_exclusive']!=base_end:raise ValueError('Native update schema/cohort/baseline changed')
    if state.get('not_before') and r.dt(state['not_before'])>now:return {'source':SOURCE,'status':'rate_wait','requests':0,'not_before':state['not_before']}
    state.pop('not_before',None);keys=list(tools);cursor=state.get('cursor',0)%len(keys);visited=0;attempts=0;errors=[]
    while attempts<max_requests and visited<len(keys):
        tid=keys[cursor];cursor=(cursor+1)%len(keys);visited+=1;tool=tools[tid];stream=state['streams'].setdefault(tid,{'last_success_end_exclusive':None})
        if stream.get('blocked_until') and r.dt(stream['blocked_until'])>now:continue
        if not stream.get('cycle'):
            left=r.dt(stream['last_success_end_exclusive'] or base_end)
            if stream['last_success_end_exclusive'] and left>=end:continue
            since=r.iso(left-timedelta(days=7));cid=r.sha((tid+'|native|'+r.iso(now)+'|'+r.iso(end)).encode())
            stream['cycle']={'source':SOURCE,'stream_kind':'native_pr_updates','registry_sha256':cohort,'cycle_id':cid,'coverage_start':r.iso(left),'since':since,
                'end_exclusive':r.iso(end),'pages':0,'receipt_paths':[],'seen_numbers':[],'seen_provider_ids':[],'seen_urls':[],'last_updated_at':None,'next_url':r.initial_url(tool,'pulls',since)}
        cycle=stream['cycle'];identity=bool(cycle.get('needs_empty_identity'));url='https://api.github.com/repos/'+tool['github_repo'] if identity else native_url(cycle['next_url'],tool)
        try:
            attempts+=1;response=fetch(url);observed=datetime.now(timezone.utc) if dynamic else now
            if identity:r.checked_url(response['url'],tool)
            else:same_response_query(url,response['url'],tool)
            meta=r.archive(root,tool,'repository' if identity else 'native_pull_updates',url,response,cycle,r.iso(observed));state['requests'].append(meta)
            pause=r.pause_until(response['headers'],observed)
            if pause:state['not_before']=r.iso(pause)
            if response['status']!=200:raise RuntimeError('Native GitHub HTTP '+str(response['status']))
            value=json.loads(response['body']);done=False;reason=None
            if identity:
                if not isinstance(value,dict) or value.get('id')!=tool['github_repo_id'] or value.get('full_name','').lower()!=tool['github_repo'].lower():raise ValueError('Empty native view lacks frozen numeric identity')
                cycle['identity_receipt_path']=meta['raw_path'];cycle['needs_empty_identity']=False;done=True;reason='provider_no_next_link'
            else:
                if url in cycle['seen_urls']:raise ValueError('Native provider Link repeated')
                dates,numbers,_,old=validate_page(value,tool,meta['observed_at'],cycle['since'],cycle['last_updated_at'],set(cycle['seen_numbers']))
                if set(row['id'] for row in value).intersection(cycle['seen_provider_ids']):raise ValueError('Native pagination repeats provider IDs')
                nxt=next_url(response['headers'],tool)
                if nxt and len(value)<100:raise ValueError('Short native page precedes another provider page')
                current_page=int(parse_qs(urlsplit(url).query).get('page',['1'])[0]);nq=parse_qs(urlsplit(nxt).query) if nxt else {}
                if 'page' in nq and int(nq['page'][0])!=current_page+1:raise ValueError('Native provider offset skips a page')
                cycle['receipt_paths'].append(meta['raw_path']);cycle['pages']+=1;cycle['seen_urls'].append(url);cycle['seen_numbers'].extend(numbers);cycle['seen_provider_ids'].extend(row['id'] for row in value)
                if dates:cycle['last_updated_at']=r.iso(dates[-1])
                done=not nxt or old;reason='verified_old_native_updated_boundary' if old and nxt else 'provider_no_next_link'
                if not value and cycle['pages']==1:
                    cycle['needs_empty_identity']=True;done=False
                elif not done and cycle['pages']>=max_pages:raise ValueError('Native lookback boundary could not be proved within the bounded page cap')
                if nxt:cycle['next_url']=nxt
            if done:
                complete={k:v for k,v in cycle.items() if k not in {'seen_numbers','seen_provider_ids','seen_urls'}};complete.update(tool_id=tid,completed_at=meta['observed_at'],terminal_reason=reason,validated_native_updated_order=True)
                state['completed_cycles'].append(complete);stream['last_success_end_exclusive']=cycle['end_exclusive'];stream['last_completed_cycle_id']=cycle['cycle_id'];stream.pop('cycle',None);stream.pop('blocked_until',None)
        except Exception as error:
            errors.append({'tool_id':tid,'type':type(error).__name__,'message':str(error)})
            if isinstance(error,ValueError):
                state['failed_cycles'].append({**{k:v for k,v in cycle.items() if k not in {'seen_numbers','seen_provider_ids','seen_urls'}},'tool_id':tid,'failed_at':r.iso(observed if 'observed' in locals() else now),'reason':str(error),'capture_complete':False})
                stream.pop('cycle',None);stream['blocked_until']=r.iso(now+timedelta(days=1))
            elif not state.get('not_before'):state['not_before']=r.iso(now+timedelta(minutes=15))
        state.update(cursor=cursor,updated_at=r.iso(datetime.now(timezone.utc) if dynamic else now));r.atomic_json(path,state)
        if state.get('not_before') and r.dt(state['not_before'])>now:break
    state.update(cursor=cursor,updated_at=r.iso(datetime.now(timezone.utc) if dynamic else now));r.atomic_json(path,state)
    return {'source':SOURCE,'status':'unknown_boundaries' if errors else 'bounded_success','requests':attempts,'completed_cycles':len(state['completed_cycles']),'not_before':state.get('not_before'),'errors':errors}


def decode_completed(root,state,registry,cohort,load_body):
    """Separate source decoder: no wrapper-derived PR coverage is accepted."""
    from collections import defaultdict
    if state.get('source')!=SOURCE or state.get('registry_sha256')!=cohort:raise ValueError('Native update source/cohort differs')
    tools={t['tool_id']:t for t in registry['tools']};receipts={};observations=[];coverage=defaultdict(list);verified={};summaries=[];seen_cycles=set()
    for meta in state['requests']:
        if meta['raw_path'] in receipts and receipts[meta['raw_path']]!=meta:raise ValueError('Conflicting native receipt')
        receipts[meta['raw_path']]=meta
    for cycle in state.get('completed_cycles',[]):
        tid=cycle['tool_id'];tool=tools[tid];cid=cycle['cycle_id'];left=b.utc(cycle['coverage_start']);right=b.utc(cycle['end_exclusive']);since=b.utc(cycle['since'])
        if cid in seen_cycles or not since==left-timedelta(days=7) or not b.utc(state['baseline_end_exclusive'])<=left<=right<=b.utc(cycle['completed_at']):raise ValueError('Native completed-cycle watermark differs')
        if cycle.get('source')!=SOURCE or cycle.get('registry_sha256')!=cohort or cycle.get('stream_kind')!='native_pr_updates':raise ValueError('Native cycle schema differs')
        seen_cycles.add(cid);expected=r.initial_url(tool,'pulls',cycle['since']);edge=None;seen=set();seen_ids=set();prior_observed=None;rows=[];reason=None;pages=0;merge_fields=True;close_fields=True;identity_required=False
        for name in cycle['receipt_paths']:
            if name not in receipts or reason:raise ValueError('Missing/extra native cycle page')
            meta=receipts[name];value=load_body(root,meta,cohort,64*1024**2,SOURCE)
            if meta['kind']!='native_pull_updates' or meta['tool_id']!=tid or meta['github_repo_id']!=tool['github_repo_id'] or meta['request_url']!=expected or meta['cycle_id']!=cid or meta['window_since']!=cycle['since'] or meta['window_end_exclusive']!=cycle['end_exclusive']:raise ValueError('Native receipt cycle/URL/window differs')
            same_response_query(meta['request_url'],meta['response_url'],tool)
            observed=b.utc(meta['observed_at'])
            if observed>b.utc(cycle['completed_at']) or prior_observed is not None and observed<prior_observed:raise ValueError('Native cycle observation times differ')
            prior_observed=observed
            if set(row['id'] for row in value).intersection(seen_ids):raise ValueError('Native source repeats provider IDs')
            seen_ids.update(row['id'] for row in value)
            dates,numbers,items,old=validate_page(value,tool,meta['observed_at'],cycle['since'],edge,seen);seen.update(numbers);pages+=1
            nxt=next_url(meta['headers'],tool)
            if nxt and len(value)<100:raise ValueError('Native short intermediate page')
            current_page=int(parse_qs(urlsplit(expected).query).get('page',['1'])[0]);nq=parse_qs(urlsplit(nxt).query) if nxt else {}
            if 'page' in nq and int(nq['page'][0])!=current_page+1:raise ValueError('Native source offset skips a page')
            if dates:edge=r.iso(dates[-1])
            reason='verified_old_native_updated_boundary' if old and nxt else 'provider_no_next_link' if not nxt else None;expected=nxt
            if not value and pages==1:identity_required=True
            for item,updated in zip(items,dates):
                for anomaly in item.get('event_time_anomalies',[]):anomaly['raw_path']=name
                merge_fields=merge_fields and item['merged_at_field_present'];close_fields=close_fields and item['closed_at_field_present']
                for anomaly in item.get('event_time_anomalies',[]):
                    if left<=b.utc(anomaly['event_at'])<right:
                        if anomaly['field']=='merged_at':merge_fields=False
                        else:close_fields=False
                if updated<since:continue
                item.update(source_path=name,source_schema=SOURCE,source_cycle_id=cid,source_stream='native_pr_updated');rows.append(item)
            verified[name]={'gzip_sha256':meta['gzip_sha256'],'raw_sha256':meta['raw_sha256'],'sidecar_sha256':b.sha(Path(root)/(name+'.manifest.json'))}
        if identity_required:
            name=cycle.get('identity_receipt_path');meta=receipts.get(name)
            if not meta:raise ValueError('Empty native source omitted numeric identity evidence')
            value=load_body(root,meta,cohort,64*1024**2,SOURCE)
            expected_identity='https://api.github.com/repos/'+tool['github_repo']
            if (meta.get('tool_id')!=tid or meta.get('github_repo_id')!=tool['github_repo_id'] or meta.get('github_repo','').lower()!=tool['github_repo'].lower()
                    or meta.get('cycle_id')!=cid or meta.get('window_since')!=cycle['since'] or meta.get('window_end_exclusive')!=cycle['end_exclusive']
                    or meta.get('request_url')!=expected_identity or b.utc(meta['observed_at'])<prior_observed or b.utc(meta['observed_at'])>b.utc(cycle['completed_at'])):
                raise ValueError('Empty native identity receipt does not bind the cycle/tool/window/time')
            r.checked_url(meta['response_url'],tool)
            if urlsplit(meta['response_url']).query:raise ValueError('Empty native identity response query differs')
            if meta['kind']!='repository' or value.get('id')!=tool['github_repo_id'] or value.get('full_name','').lower()!=tool['github_repo'].lower():raise ValueError('Empty native numeric identity differs')
            verified[name]={'gzip_sha256':meta['gzip_sha256'],'raw_sha256':meta['raw_sha256']}
        stream=state.get('streams',{}).get(tid,{})
        if reason!=cycle.get('terminal_reason') or pages!=cycle.get('pages') or cycle.get('validated_native_updated_order') is not True or not stream.get('last_success_end_exclusive') or b.utc(stream['last_success_end_exclusive'])<right:raise ValueError('Native cutoff/terminal proof is incomplete')
        coverage[(tid,'pr_creation')].append((left,right))
        if merge_fields:coverage[(tid,'pr_merge')].append((left,right))
        if close_fields:coverage[(tid,'pr_close')].append((left,right))
        observations.extend(rows);summaries.append({'tool_id':tid,'cycle_id':cid,'coverage_start':cycle['coverage_start'],'end_exclusive':cycle['end_exclusive'],'pages':pages,'terminal_reason':reason,'records':len(rows),'merge_fields_complete':merge_fields,'closed_fields_complete':close_fields})
    return observations,coverage,{'source':SOURCE,'completed_native_cycles':summaries,'verified_native_files':verified,'failed_cycle_count':len(state.get('failed_cycles',[])),'partial_streams':[tid for tid,v in state.get('streams',{}).items() if v.get('cycle')],'historical_visibility_complete':False}
