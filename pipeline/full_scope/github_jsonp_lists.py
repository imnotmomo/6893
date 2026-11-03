"""Validate the safe JSONP-envelope archive and actual opaque provider links."""
import gzip,hashlib,json,re
from pathlib import Path
from urllib.parse import urlsplit,urlunsplit,parse_qs
from github_visible_cursor import verify_receipts,digest

SAFE_META={'status','link','etag','date','last-modified','x-ratelimit-limit','x-ratelimit-remaining','x-ratelimit-reset','x-ratelimit-resource','x-github-api-version-selected','x-github-media-type','retry-after'}


def envelope(root,r,cohort,max_bytes):
    if r.get('transport')!='github_connector_jsonp_envelope' or r.get('registry_sha256')!=cohort:raise ValueError('JSONP archive source/cohort differs')
    path=(root/r['raw_path']).resolve()
    if not path.is_relative_to(root) or not path.is_file() or digest(path)!=r.get('compressed_sha256'):raise ValueError('JSONP compressed body path/hash differs')
    with gzip.open(path,'rb') as f:raw=f.read(max_bytes+1)
    if len(raw)>max_bytes or hashlib.sha256(raw).hexdigest()!=r.get('raw_sha256'):raise ValueError('JSONP retained body hash/size differs')
    value=json.loads(raw)
    if not isinstance(value,dict) or set(value)!={'meta','data'} or not isinstance(value['data'],list) or not isinstance(value['meta'],dict):raise ValueError('Expected a safe JSONP envelope')
    if any(str(k).lower() not in SAFE_META for k in value['meta']):raise ValueError('Unsafe/unrecognized JSONP metadata retained')
    meta={k.lower():v for k,v in value['meta'].items()}
    if len(meta)!=len(value['meta']):raise ValueError('Duplicate casefolded JSONP metadata field')
    if r.get('safe_meta') is not None and r['safe_meta']!=value['meta']:raise ValueError('JSONP safe metadata receipt differs')
    if meta.get('status')!=200 or r.get('status',(r.get('safe_meta') or {}).get('status'))!=200:raise ValueError('JSONP status is not successful')
    offset=r.get('data_json_utf8_offset');length=r.get('data_json_utf8_bytes')
    if not isinstance(offset,int) or isinstance(offset,bool) or not isinstance(length,int) or isinstance(length,bool) or offset<0 or length<2 or offset+length>len(raw):raise ValueError('JSONP data slice bounds differ')
    data=raw[offset:offset+length]
    if hashlib.sha256(data).hexdigest()!=r.get('data_sha256') or json.loads(data)!=value['data']:raise ValueError('JSONP exact data-slice hash differs')
    if not re.fullmatch('[a-f0-9]{64}',str(r.get('full_envelope_sha256',''))) or not isinstance(r.get('full_envelope_bytes'),int) or r['full_envelope_bytes']<len(data):raise ValueError('Original JSONP envelope fingerprint is missing')
    if len(value['data'])>100 or not isinstance(r.get('record_count'),int) or isinstance(r.get('record_count'),bool) or r['record_count']!=len(value['data']):raise ValueError('JSONP page count/shape differs')
    links=meta.get('link',[])
    if not isinstance(links,list):raise ValueError('JSONP links are not structured provider relations')
    next_links=[]
    for link in links:
        if not isinstance(link,list) or len(link)!=2 or not isinstance(link[0],str) or not isinstance(link[1],dict):raise ValueError('Invalid JSONP Link relation')
        if link[1].get('rel')=='next':next_links.append(link[0])
    if len(next_links)>1:raise ValueError('Ambiguous JSONP next Link')
    nxt=next_links[0] if next_links else None
    if r.get('provider_next_url')!=nxt:raise ValueError('JSONP next-Link receipt differs from safe envelope')
    return value['data'],nxt

def context(url,tool,since):
    u=urlsplit(url);q=parse_qs(u.query,keep_blank_values=True)
    if u.scheme!='https' or u.netloc!='api.github.com' or u.fragment or u.path.lower() not in {'/repos/'+tool['github_repo'].lower()+'/issues','/repositories/'+str(tool['github_repo_id'])+'/issues'}:raise ValueError('JSONP URL leaves frozen numeric repository')
    expected={'state':'all','since':since,'sort':'updated','direction':'asc','per_page':'100','callback':'radarPagination'}
    if any(q.get(k)!=[v] for k,v in expected.items()) or set(q)-set(expected)-{'after','page','before'} or any(len(v)!=1 for v in q.values()):raise ValueError('JSONP actual-Link filters changed')
    return u

def connector_url(provider,tool,since):
    if provider is None:return None
    u=context(provider,tool,since)
    if u.path.startswith('/repositories/'):
        return urlunsplit((u.scheme,u.netloc,'/repos/'+tool['github_repo']+'/issues',u.query,u.fragment))
    return provider


def evaluate_v3(root,state,registry,normalize,utc,max_bytes):
    tools={t['tool_id']:t for t in registry['tools']};selected={};observations=[];quality={};verified=[];proofs={};cache={}
    for r in sorted(state['requests'],key=lambda r:(utc(r['observed_at']),r['raw_path'])):
        if r.get('phase')=='issues_list_v3' and r.get('stream')=='issues_visible_v3':
            if r.get('kind')!='issues' or not isinstance(r.get('page'),int) or isinstance(r.get('page'),bool) or r['page']<1:raise ValueError('Invalid v3 list receipt')
            selected[(r['tool_id'],r['page'])]=r
    diagnostics={};transport_diagnostics=[]
    for r in state['requests']:
        name=r.get('v3_transport_label_diagnostic_path');expected=r.get('v3_transport_label_diagnostic_sha256')
        if name:
            path=(root/name).resolve()
            if not path.is_relative_to(root) or not path.is_file() or digest(path)!=expected:raise ValueError('V3 transport-label diagnostic hash differs')
            document=json.loads(path.read_text());proofs[name]=expected
            for entry in document['verified_requests']:
                previous=diagnostics.get(entry['raw_path'])
                if previous is not None and previous!=entry:raise ValueError('Conflicting V3 transport-label diagnostic')
                diagnostics[entry['raw_path']]=entry
    if {t for t,_ in selected}-set(tools):raise ValueError('Unknown JSONP source tool')
    for tid,tool in tools.items():
        pages=sorted((p,r) for (t,p),r in selected.items() if t==tid)
        if not pages:continue
        source=state.get('tools',{}).get(tid,{}).get('issues_visible_v3',{});reasons=[];next_request=None;terminal=False;seen_urls=set();last=None;count=0;missing_merge=[];missing_closed=[];previous_time=None
        if [p for p,_ in pages]!=list(range(1,pages[-1][0]+1)):reasons.append('missing_v3_pages')
        for page,r in pages:
            effective=r
            if r.get('transport')!='github_connector_jsonp_envelope':
                diagnostic=diagnostics.get(r['raw_path'])
                if not diagnostic or diagnostic['raw_sha256']!=r['raw_sha256'] or diagnostic['observed_transport_label']!=r.get('transport') or diagnostic['specific_transport']!='github_connector_jsonp_envelope':raise ValueError('V3 generic transport label lacks an exact bound diagnostic')
                effective={**r,'transport':'github_connector_jsonp_envelope'};transport_diagnostics.append(r['raw_path'])
            contract_path=r.get('jsonp_contract_path')
            if contract_path:
                path=(root/contract_path).resolve()
                if not path.is_relative_to(root) or not path.is_file() or digest(path)!=r.get('jsonp_contract_sha256'):raise ValueError('V3 archive contract hash differs')
                contract=json.loads(path.read_text());proofs[contract_path]=digest(path)
                if contract.get('version')!='github_issues_visible_jsonp_v3' or contract.get('callback')!='radarPagination':raise ValueError('V3 archive contract differs')
                code_path=(root/contract['sha256_code_path']).resolve()
                if not code_path.is_relative_to(root) or not code_path.is_file() or digest(code_path)!=contract['sha256_code_sha256']:raise ValueError('V3 hashing implementation evidence differs')
                proofs[contract['sha256_code_path']]=digest(code_path)
            proofs.update(verify_receipts(root,effective,tool,registry,max_bytes,cache))
            body,provider_next=envelope(root,effective,state['registry_sha256'],max_bytes);verified.append(r)
            since=r.get('since');url=r.get('request_url') or r.get('url');context(url,tool,since)
            if not utc(since)<utc(state['start']):raise ValueError('JSONP updated-since boundary omits study start')
            if r.get('url',url)!=url:raise ValueError('JSONP request URL aliases disagree')
            expected=connector_url(provider_next,tool,since)
            if r.get('connector_next_url')!=expected:raise ValueError('JSONP connector URL changes more than the frozen path alias')
            if terminal or url in seen_urls or page>1 and url!=next_request:reasons.append('v3_actual_link_chain_differs')
            if page==1 and any(k in parse_qs(urlsplit(url).query) for k in ['after','before','page']):reasons.append('v3_first_page_has_cursor')
            if previous_time is not None and utc(r['request_started_at'])<previous_time or utc(r['request_started_at'])>utc(r['observed_at']):reasons.append('v3_request_times_overlap_or_reverse')
            previous_time=utc(r['observed_at']);seen_urls.add(url);next_request=expected;terminal=provider_next is None
            if r.get('terminal_page') is not terminal:reasons.append('v3_terminal_receipt_differs')
            ids=[row.get('id') for row in body]
            if any(not isinstance(v,int) or isinstance(v,bool) or v<=0 for v in ids) or len(ids)!=len(set(ids)):raise ValueError('Duplicate/invalid v3 page IDs')
            for row in body:
                item=normalize(row,tool,{**r,'stream':'issues_visible'});item['source_view']='issues_visible_v3';item['source_schema']='github_connector_jsonp_envelope'
                if not utc(item['created_at'])<=utc(row['updated_at'])<=utc(r['observed_at']):raise ValueError('JSONP record timestamp contradicts observation')
                if item['kind']=='pull' and not item['merged_at_field_present']:missing_merge.append(item['number'])
                if not item['closed_at_field_present']:missing_closed.append(item['number'])
                observations.append(item)
            if r.get('validation_error'):reasons.append('v3_collector_reported_validation_error')
            if 'termination_evidence' in r and terminal and r['termination_evidence']!='actual_provider_no_next_link':reasons.append('v3_termination_evidence_differs')
            last=r;count+=len(body)
        if not terminal:reasons.append('v3_provider_has_next_link')
        if source.get('status')!='complete':reasons.append('v3_source_state_incomplete')
        expected_state={'pages_saved':len(pages),'record_observations':count,'next_page':pages[-1][0]+1,'last_receipt_raw_path':last['raw_path']}
        if terminal:expected_state['terminal_raw_path']=last['raw_path']
        for field,value in [('provider_next_url',last['provider_next_url']),('connector_next_url',last['connector_next_url'])]:
            if field in source and source[field]!=value:reasons.append('v3_state_next_link_differs')
        if 'visited_request_url_sha256' in source and source['visited_request_url_sha256']!=[hashlib.sha256((r.get('request_url') or r['url']).encode()).hexdigest() for _,r in pages]:reasons.append('v3_state_visited_urls_differ')
        if any(source.get(k)!=v for k,v in expected_state.items()):reasons.append('v3_state_checkpoint_differs')
        quality[tid]={'visible_list_pagination_complete':not reasons,'reasons':sorted(set(reasons)),'pages_observed':len(pages),
            'actual_provider_link_receipts':len(pages),'body_derived_cursor_method_eligible':False,'pagination_method':'actual_provider_jsonp_links_v3',
            'record_observations':count,'missing_merged_at_numbers':sorted(set(missing_merge)),'missing_closed_at_numbers':sorted(set(missing_closed)),
            'historical_visibility_complete':False,'updated_since_crawl_is_atomic_snapshot':False,
            'scope':'currently_visible_provider_updated_since_resultset; serialized updated_at is not asserted to equal the provider sort key'}
    return observations,quality,{'raw_requests_verified':verified,'proof_file_hashes':proofs,'transport_label_diagnostics_applied':transport_diagnostics}
