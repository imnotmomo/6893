#!/usr/bin/env python3
"""Unsigned, bounded refresh of public issue/PR metadata; no credential lookup.

Each stream keeps a frozen update overlap window and the actual Link cursor until
its cycle finishes. Mutable fields are observations, never historical snapshots.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import email.utils
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import Request, urlopen

SOURCE='github_direct_issues_pulls_incremental'
SAFE_HEADERS=('ETag','Last-Modified','X-GitHub-Api-Version-Selected','X-RateLimit-Limit','X-RateLimit-Remaining','X-RateLimit-Reset','X-RateLimit-Resource','Retry-After','Link','Date')
MAX_RESPONSE_BYTES=12*1024*1024

def iso(value):
    if value.tzinfo is None:raise ValueError('Naive UTC timestamp')
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def dt(value):
    value=datetime.fromisoformat(value.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('Missing timestamp zone')
    return value.astimezone(timezone.utc)

def sha(raw):return hashlib.sha256(raw).hexdigest()

def atomic_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    temp.replace(path)

def checked_url(url,tool,kind=None):
    parsed=urlparse(url)
    allowed={'/repos/'+tool['github_repo'], '/repositories/'+str(tool['github_repo_id'])}
    if kind:allowed={p+'/'+kind for p in allowed}
    if parsed.scheme!='https' or parsed.netloc!='api.github.com' or parsed.path not in allowed or parsed.fragment:
        raise ValueError('Pagination URL leaves frozen repository scope')
    query=parse_qs(parsed.query,keep_blank_values=True)
    if set(query)-{'state','sort','direction','per_page','page','since','after','before'}:
        raise ValueError('Unexpected pagination query')
    if any(len(v)!=1 for v in query.values()):raise ValueError('Repeated pagination parameter')
    if kind:
        if query.get('per_page')!=['100']:raise ValueError('Page size changed or omitted')
        if query.get('state')!=['all'] or query.get('sort')!=['updated']:
            raise ValueError('Pagination changed collection semantics')
        if query.get('direction')!=[('asc' if kind=='issues' else 'desc')]:
            raise ValueError('Pagination changed update order')
        if kind=='issues':
            if 'since' not in query:raise ValueError('Issue pagination omitted the update watermark')
            dt(query['since'][0])
        elif 'since' in query:raise ValueError('Native pull pagination added an unsupported since filter')
        if 'page' in query and (not query['page'][0].isdigit() or int(query['page'][0])<1):raise ValueError('Invalid offset page')
    elif query:raise ValueError('Repository identity response has an unexpected query')
    return url

def same_response_query(request,response,tool,kind=None):
    checked_url(request,tool,kind);checked_url(response,tool,kind)
    if parse_qs(urlparse(request).query,keep_blank_values=True)!=parse_qs(urlparse(response).query,keep_blank_values=True):
        raise ValueError('Provider response redirected to a different query')

def next_link(value,tool,kind):
    if value is None:return None
    if not isinstance(value,str) or not value.strip():raise ValueError('Malformed provider Link header')
    links={}
    for part in value.split(','):
        matched=re.fullmatch(r'\s*<([^<>]+)>\s*;\s*rel="(next|prev|first|last)"\s*',part)
        if not matched:raise ValueError('Malformed or unsupported provider Link relation')
        target,rel=matched.groups()
        if rel in links:raise ValueError('Duplicate Link relation')
        links[rel]=checked_url(target,tool,kind)
    return links.get('next')

def fetch_public(url):
    req=Request(url,headers={'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','User-Agent':'6893-ai-technology-radar-public-collector/1'})
    try:
        response=urlopen(req,timeout=40)
    except HTTPError as error:
        response=error
    with response:
        raw=response.read(MAX_RESPONSE_BYTES+1)
        if len(raw)>MAX_RESPONSE_BYTES:raise ValueError('Public response exceeds bounded size')
        return {'status':response.status,'body':raw,'headers':{k:response.headers[k] for k in SAFE_HEADERS if k in response.headers},'url':response.geturl()}

def pause_until(headers,now):
    result=[]
    value=headers.get('Retry-After')
    if value:
        try:result.append(now+timedelta(seconds=max(0,int(value))))
        except ValueError:
            parsed=email.utils.parsedate_to_datetime(value)
            if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=timezone.utc)
            result.append(parsed)
    try:
        if int(headers.get('X-RateLimit-Remaining','999'))<=2:
            result.append(datetime.fromtimestamp(int(headers['X-RateLimit-Reset']),timezone.utc)+timedelta(seconds=3))
    except (ValueError,KeyError):pass
    return max(result) if result else None

def archive(root,tool,kind,url,result,cycle,observed_at):
    body=result['body'];zipped=gzip.compress(body,mtime=0)
    identity=json.dumps({'url':url,'observed_at':observed_at,'kind':kind,
        'since':cycle.get('since'),'end_exclusive':cycle.get('end_exclusive'),
        'stream_kind':cycle.get('stream_kind'),'cycle_id':cycle.get('cycle_id')},sort_keys=True)
    key=sha(identity.encode())[:16]+'-'+sha(body)[:16]
    relative=Path('raw')/tool['tool_id']/kind/(key+'.json.gz')
    path=root/relative;path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.read_bytes()!=zipped:raise ValueError('Immutable raw collision')
    if not path.exists():path.write_bytes(zipped)
    meta={'source':cycle.get('source',SOURCE),'tool_id':tool['tool_id'],'github_repo':tool['github_repo'],'github_repo_id':tool['github_repo_id'],
          'kind':kind,'request_url':url,'response_url':result['url'],'status':result['status'],'observed_at':observed_at,
          'window_since':cycle.get('since'),'window_end_exclusive':cycle.get('end_exclusive'),
          'raw_path':str(relative),'raw_sha256':sha(body),'gzip_sha256':sha(zipped),'raw_bytes':len(body),'gzip_bytes':len(zipped),
          'headers':result['headers'],'cycle_id':cycle.get('cycle_id'),'stream_kind':cycle.get('stream_kind'),
          'registry_sha256':cycle.get('registry_sha256'),'historical_public_availability':'current_metadata_observed_at_fetch',
          'closure_semantics':'last reported closed_at; reopen and intermediate closure timeline unavailable'}
    atomic_json(path.with_suffix(path.suffix+'.manifest.json'),meta)
    return meta

def initial_url(tool,kind,since):
    query={'state':'all','sort':'updated','direction':'asc' if kind=='issues' else 'desc','per_page':100}
    if kind=='issues':query['since']=since
    return 'https://api.github.com/repos/'+tool['github_repo']+'/'+kind+'?'+urlencode(query)

def legacy_cycle_receipts(state,tid,kind,cycle):
    candidates=[r for r in state['requests'] if r['tool_id']==tid and r.get('window_since')==cycle['since'] and r.get('window_end_exclusive')==cycle['end_exclusive'] and r.get('status')==200]
    pages=[r for r in candidates if r['kind']==kind]
    identities=[r for r in candidates if r['kind']=='repository' and (not pages or dt(r['observed_at'])<=min(dt(p['observed_at']) for p in pages))]
    identity=max(identities,key=lambda r:dt(r['observed_at'])) if identities else None
    # Pre-v2 repository receipts could be shared by both streams. Keep one
    # identity and each immutable page once; the adapter revalidates every link.
    return list(dict.fromkeys(([identity['raw_path']] if identity else [])+[r['raw_path'] for r in pages]))


def run(registry_path,data_dir,baseline_end,max_requests=4,minimum_free_gib=15,fetch=fetch_public,now=None,end_exclusive=None):
    if not 1<=max_requests<=10:raise ValueError('max_requests must be 1..10')
    dynamic_clock=now is None
    now=now or datetime.now(timezone.utc)
    end=dt(end_exclusive) if end_exclusive else min(now.replace(hour=0,minute=0,second=0,microsecond=0),dt('2027-01-01T00:00:00Z'))
    if end>dt('2027-01-01T00:00:00Z'):raise ValueError('Refresh exceeds the original 2023–2026 study scope')
    if end>now:raise ValueError('Refresh cannot use a future endpoint')
    baseline=dt(baseline_end)
    if baseline>end:raise ValueError('Baseline ends after refresh endpoint')
    root=Path(data_dir);root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<minimum_free_gib*1024**3:raise RuntimeError('Free disk guard')
    registry_raw=Path(registry_path).read_bytes();registry=json.loads(registry_raw);digest=sha(registry_raw)
    tools={x['tool_id']:x for x in registry['tools']}
    if any(not isinstance(t.get('github_repo_id'),int) for t in tools.values()):raise ValueError('Frozen numeric repository IDs are required')
    state_path=root/'refresh_state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {'source':SOURCE,'registry_sha256':digest,'baseline_end_exclusive':iso(baseline),'streams':{},'requests':[],'historical_visibility_complete':False}
    if state['registry_sha256']!=digest or state['baseline_end_exclusive']!=iso(baseline):raise ValueError('Refresh cohort or baseline changed')
    if state.get('not_before') and dt(state['not_before'])>now:return {'requests':0,'status':'rate_wait','not_before':state['not_before']}
    state.pop('not_before',None)
    requests=0;errors=[]
    # Persist a fair cursor; one high-volume repository cannot monopolize dispatches.
    keys=[(tid,kind) for tid in tools for kind in ('issues','pulls')]
    cursor=state.get('cursor',0)%max(1,len(keys));visited=0
    while requests<max_requests and visited<len(keys):
        tid,kind=keys[cursor];cursor=(cursor+1)%len(keys);visited+=1
        tool=tools[tid];key=tid+'/'+kind
        stream=state['streams'].setdefault(key,{'last_success_end_exclusive':None})
        if not stream.get('cycle'):
            last=dt(stream['last_success_end_exclusive']) if stream['last_success_end_exclusive'] else baseline
            if stream['last_success_end_exclusive'] and last>=end:continue
            cycle={'since':iso(last-timedelta(days=7)),'end_exclusive':iso(end),'pages':0,'identity_verified':False,'seen_urls':[],
                'registry_sha256':digest,'stream_kind':kind,'coverage_start':iso(last),
                'cycle_id':sha((key+'|'+iso(now)+'|'+iso(last)+'|'+iso(end)).encode()),'receipt_paths':[]}
            cycle['next_url']=initial_url(tool,kind,cycle['since']);stream['cycle']=cycle
        cycle=stream['cycle']
        if 'receipt_paths' not in cycle:
            # One explicit migration for an unfinished pre-v2 cycle. The overlay
            # still verifies its complete immutable URL/header/body chain.
            cycle.update(registry_sha256=digest,stream_kind=kind,coverage_start=iso(dt(stream['last_success_end_exclusive']) if stream['last_success_end_exclusive'] else baseline),
                cycle_id=sha((key+'|legacy|'+cycle['since']+'|'+cycle['end_exclusive']).encode()),
                receipt_paths=legacy_cycle_receipts(state,tid,kind,cycle),
                migrated_legacy_receipts=True)
        identity=not cycle['identity_verified']
        url='https://api.github.com/repos/'+tool['github_repo'] if identity else checked_url(cycle['next_url'],tool,kind)
        try:
            requests+=1
            response=fetch(url)
            same_response_query(url,response['url'],tool,None if identity else kind)
            observed=datetime.now(timezone.utc) if dynamic_clock else now
            meta=archive(root,tool,'repository' if identity else kind,url,response,cycle,iso(observed));state['requests'].append(meta)
            pause=pause_until(response['headers'],now)
            if pause:state['not_before']=iso(pause)
            if response['status']!=200:
                raise RuntimeError('GitHub HTTP '+str(response['status']))
            parsed=json.loads(response['body'])
            if identity:
                if parsed.get('id')!=tool['github_repo_id'] or parsed.get('full_name','').casefold()!=tool['github_repo'].casefold():
                    raise ValueError('Frozen repository identity mismatch')
                cycle['identity_verified']=True;cycle['receipt_paths'].append(meta['raw_path'])
            else:
                if not isinstance(parsed,list) or len(parsed)>100:raise ValueError('Invalid bounded list response')
                if url in cycle['seen_urls']:raise ValueError('Pagination cursor repeated')
                ids=[x.get('id') for x in parsed]
                if any(not isinstance(v,int) or isinstance(v,bool) or v<=0 for v in ids) or len(ids)!=len(set(ids)):raise ValueError('Duplicate/invalid page IDs')
                dates=[dt(x['updated_at']) for x in parsed]
                expected=sorted(dates,reverse=kind=='pulls')
                if dates!=expected:cycle.setdefault('serialized_update_order_anomalies',[]).append(meta['raw_path'])
                nxt=next_link(response['headers'].get('Link'),tool,kind)
                if nxt and kind=='issues' and parse_qs(urlparse(nxt).query).get('since')!=[cycle['since']]:
                    raise ValueError('Issue cursor changed update watermark')
                cycle['seen_urls'].append(url);cycle['pages']+=1;cycle['receipt_paths'].append(meta['raw_path'])
                # Provider sort keys can differ from JSON timestamps. Actual Link
                # termination is authoritative for both resource types.
                done=not nxt
                if done:
                    cycle['terminal_reason']='provider_no_next_link'
                    stream['last_success_end_exclusive']=cycle['end_exclusive']
                    stream['last_completed_cycle']={k:v for k,v in cycle.items() if k!='seen_urls'}
                    stream['last_completed_cycle']['completed_at']=iso(observed)
                    state.setdefault('completed_cycles',[]).append({'tool_id':tid,'kind':kind,**stream['last_completed_cycle']})
                    del stream['cycle']
                else:cycle['next_url']=nxt
            stream.pop('last_error',None)
        except Exception as error:
            stream['last_error']={'observed_at':iso(now),'type':type(error).__name__,'message':str(error)}
            errors.append({'stream':key,**stream['last_error']})
            # Headerless network failures also back off; no spin or cursor advance.
            if not state.get('not_before'):state['not_before']=iso(now+timedelta(minutes=15))
        state['cursor']=cursor;state['updated_at']=iso(now);atomic_json(state_path,state)
        if state.get('not_before') and dt(state['not_before'])>now:break
    state['cursor']=cursor;state['updated_at']=iso(now);atomic_json(state_path,state)
    result={'requests':requests,'status':'errors' if errors else 'bounded_success','streams_completed':sum(x.get('last_success_end_exclusive')==iso(end) for x in state['streams'].values()),'stream_count':len(keys),'not_before':state.get('not_before'),'errors':errors}
    atomic_json(root/'latest_dispatch.json',result)
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--data-dir',type=Path,required=True)
    parser.add_argument('--baseline-end-exclusive',required=True)
    parser.add_argument('--max-requests',type=int,default=4)
    parser.add_argument('--minimum-free-gib',type=float,default=15)
    parser.add_argument('--end-exclusive')
    args=parser.parse_args()
    print(json.dumps(run(args.registry,args.data_dir,args.baseline_end_exclusive,args.max_requests,args.minimum_free_gib,end_exclusive=args.end_exclusive)))
if __name__=='__main__':main()
