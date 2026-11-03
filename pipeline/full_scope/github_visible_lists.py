"""Conservative offline validation of provider-cursor updated-since issue lists."""
from collections import defaultdict
from urllib.parse import urlsplit,parse_qs
from github_issues_cursor import check_context,validate_page
from github_visible_cursor import verify_receipts,digest,cursor_for
import gzip,hashlib,re
import json


def evaluate_visible(root,state,registry,load_body,normalize,utc,max_body_bytes):
    tools={t['tool_id']:t for t in registry['tools']};selected={};verified=[];proofs={};observations=[];quality={};cache={}
    for r in sorted(state['requests'],key=lambda r:(utc(r['observed_at']),r['raw_path'])):
        if r.get('phase')=='issues_list' and r.get('stream')=='issues_visible':
            if r.get('kind')!='issues' or not isinstance(r.get('page'),int) or isinstance(r.get('page'),bool) or r['page']<1:raise ValueError('Invalid visible issue-list request metadata')
            selected[(r['tool_id'],r['page'])]=r
    corrections={};correction_binding=state.get('proof_reference_corrections');applied=[]
    if correction_binding:
        path=(root/correction_binding['path']).resolve()
        if not path.is_relative_to(root) or not path.is_file() or digest(path)!=correction_binding['sha256']:raise ValueError('Explicit proof-reference correction hash differs')
        proofs[correction_binding['path']]=correction_binding['sha256']
        document=json.loads(path.read_text())
        if document.get('correction_version')!=1:raise ValueError('Unknown proof-reference correction format')
        for correction in document['corrections']:
            if correction['raw_path'] in corrections:raise ValueError('Duplicate proof-reference correction')
            corrections[correction['raw_path']]=correction
    disqualification=state.get('method_disqualification');method_evidence=None
    if disqualification:
        path=(root/disqualification['path']).resolve()
        if not path.is_relative_to(root) or not path.is_file() or digest(path)!=disqualification['sha256']:raise ValueError('Cursor method disqualification hash differs')
        method_evidence=json.loads(path.read_text());proofs[disqualification['path']]=disqualification['sha256']
        receipt_path=(root/method_evidence['provider_receipt_path']).resolve();page_path=(root/method_evidence['provider_page_path']).resolve()
        if not receipt_path.is_relative_to(root) or not page_path.is_relative_to(root):raise ValueError('Escaped disqualification evidence path')
        receipt=json.loads(receipt_path.read_text())
        if digest(page_path)!=receipt['compressed_sha256']:raise ValueError('Disqualification provider page hash differs')
        with gzip.open(page_path,'rb') as f:raw=f.read(max_body_bytes+1)
        if len(raw)>max_body_bytes or hashlib.sha256(raw).hexdigest()!=receipt['raw_sha256']:raise ValueError('Disqualification decoded hash differs')
        last=json.loads(raw)[-1];derived=cursor_for(last)
        if derived!=method_evidence['body_derived_token'] or derived==method_evidence['provider_token'] or method_evidence['provider_link'] not in receipt['headers']['Link']:raise ValueError('Disqualification evidence does not reproduce the cursor mismatch')
        proofs[method_evidence['provider_receipt_path']]=digest(receipt_path);proofs[method_evidence['provider_page_path']]=digest(page_path)
    start,end=utc(state['start']),utc(state['end_exclusive'])
    for tid,tool in tools.items():
        source=state.get('tools',{}).get(tid,{}).get('issues_visible',{});reasons=[];anchor=None;cursor_in=None;prior_time=None
        header_receipts=0
        pages=sorted((page,r) for (tool_id,page),r in selected.items() if tool_id==tid)
        total=0;seen=set();repeated=0;terminal=False;last=None;merge_missing=[];closed_missing=[]
        if not pages:reasons.append('visible_list_not_collected')
        if pages and [p for p,_ in pages]!=list(range(1,pages[-1][0]+1)):reasons.append('missing_visible_pages')
        for page,r in pages:
            # Hash, cohort and identity errors are fatal; no apparently valid
            # partial output may hide modified evidence.
            effective=r;correction=corrections.get(r['raw_path'])
            if correction:
                checks={'raw_path':'raw_path','raw_sha256':'raw_sha256','tool_id':'tool_id','page':'page','original_observed_at':'observed_at',
                        'original_path':'cursor_conformance_path','original_sha256':'cursor_conformance_receipt_sha256'}
                if any(correction[k]!=r.get(field) for k,field in checks.items()) or correction['corrected_sha256']!=correction['original_sha256']:raise ValueError('Explicit proof-reference correction does not bind this original observation')
                effective={**r,'cursor_conformance_path':correction['corrected_path'],'cursor_conformance_receipt_sha256':correction['corrected_sha256']}
                applied.append(r['raw_path'])
            proofs.update(verify_receipts(root,effective,tool,registry,max_body_bytes,cache))
            body=load_body(root,r,state['registry_sha256'],max_body_bytes);verified.append(r)
            if not isinstance(body,list) or len(body)>100 or r.get('record_count')!=len(body):raise ValueError('Visible issue page shape/count differs')
            request_start=utc(r.get('request_started_at'));observed=utc(r['observed_at'])
            if observed<request_start or prior_time is not None and request_start<prior_time:reasons.append('overlapping_or_decreasing_request_times')
            prior_time=observed
            try:
                since=r.get('since');check_context(r['url'],tool['github_repo'],since)
                if not utc(since)<start or r.get('sort')!='updated' or r.get('direction')!='asc' or r.get('per_page')!=100:raise ValueError('Visible-list scope does not cover study start')
                if source.get('since')!=since or source.get('sort')!='updated' or source.get('direction')!='asc' or source.get('per_page')!=100 or source.get('github_repo_id')!=tool['github_repo_id']:raise ValueError('Visible-list state scope differs')
                if terminal:raise ValueError('Records after terminal empty page')
                query=parse_qs(urlsplit(r['url']).query)
                if r.get('cursor_in')!=cursor_in or query.get('after')!=([cursor_in] if cursor_in else None):raise ValueError('Visible-list cursor chain differs')
                actual=r.get('provider_link_receipt_path')
                if actual:
                    receipt_path=(root/actual).resolve()
                    if not receipt_path.is_relative_to(root) or not receipt_path.is_file() or digest(receipt_path)!=r.get('provider_link_receipt_sha256'):raise ValueError('Actual provider Link receipt hash differs')
                    receipt=json.loads(receipt_path.read_text());proofs[actual]=digest(receipt_path)
                    if receipt.get('request_url')!=r['url'] or receipt.get('raw_sha256')!=r['raw_sha256'] or receipt.get('compressed_sha256')!=r['compressed_sha256'] or receipt.get('status')!=200:raise ValueError('Actual provider Link receipt does not bind this response')
                    links=re.findall(r'<([^>]+)>;\s*rel="next"',receipt.get('headers',{}).get('Link',''))
                    if len(links)>1:raise ValueError('Ambiguous actual provider Link')
                    out=cursor_in
                    if links:
                        u=urlsplit(links[0]);q=parse_qs(u.query)
                        if u.scheme!='https' or u.netloc!='api.github.com' or u.path.lower() not in {'/repos/'+tool['github_repo'].lower()+'/issues','/repositories/'+str(tool['github_repo_id'])+'/issues'}:raise ValueError('Actual provider Link repository differs')
                        q.pop('page',None);tokens=q.pop('after',None)
                        if not tokens or len(tokens)!=1 or q!={'state':['all'],'since':[since],'sort':['updated'],'direction':['asc'],'per_page':['100']}:raise ValueError('Actual provider Link scope differs')
                        out=tokens[0]
                        if out==cursor_in:raise ValueError('Actual provider cursor did not advance')
                    # JSON updated_at is not the opaque provider sort key.
                    # Never derive or validate that key from serialized dates.
                    ids=[row.get('id') for row in body]
                    if len(ids)!=len(set(ids)):raise ValueError('Duplicate ID inside page')
                    new_anchor=body[-1] if body else anchor
                    checked={'terminal_page':not body and not links}
                    header_receipts+=1
                else:
                    reasons.append('body_derived_cursor_method_disqualified')
                    checked=validate_page(body,anchor,since)
                    out=checked['cursor_out'];new_anchor=checked['last_anchor']
                expected_anchor={k:new_anchor[k] for k in ['id','number','updated_at']} if new_anchor else None
                if r.get('cursor_out')!=out or r.get('last_anchor')!=expected_anchor or r.get('terminal_page') is not checked['terminal_page']:raise ValueError('Visible-list provider receipt boundary differs')
                if r.get('cursor_encoding')!='observed_cursor_v2_msgpack_updated_at_issue_id':raise ValueError('Unverified cursor encoding')
                anchor=new_anchor;cursor_in=out;terminal=checked['terminal_page']
            except (ValueError,KeyError,TypeError) as error:reasons.append(str(error))
            for row in body:
                if not isinstance(row,dict) or not isinstance(row.get('id'),int) or isinstance(row.get('id'),bool) or row['id']<=0:raise ValueError('Visible provider record ID is invalid')
                item=normalize(row,tool,r)
                updated=utc(row.get('updated_at'))
                if not utc(item['created_at'])<=updated<=observed:raise ValueError('Visible record updated_at contradicts creation/observation')
                if item['kind']=='pull' and not item['merged_at_field_present']:merge_missing.append(item['number'])
                if not item['closed_at_field_present']:closed_missing.append(item['number'])
                key=(tid,item['repo_id'],item['number'])
                if key in seen:repeated+=1
                seen.add(key);observations.append(item)
            total+=len(body);last=r
        if pages:
            if not terminal:reasons.append('no_actual_empty_terminal_page')
            if source.get('status')!='complete':reasons.append('visible_list_state_incomplete')
            expected={'pages_saved':len(pages),'record_observations':total,'next_page':pages[-1][0]+1,
                      'cursor_out':cursor_in,'last_anchor':({k:anchor[k] for k in ['id','number','updated_at']} if anchor else None),
                      'last_receipt_raw_path':last['raw_path']}
            if terminal:expected['terminal_raw_path']=last['raw_path']
            if any(source.get(k)!=v for k,v in expected.items()):reasons.append('visible_list_state_checkpoint_differs')
        quality[tid]={'visible_list_pagination_complete':not reasons,'reasons':sorted(set(reasons)),
            'pages_observed':len(pages),'actual_provider_link_receipts':header_receipts,'body_derived_cursor_method_eligible':False,'record_observations':total,'unique_issue_or_pr_numbers':len(seen),
            'repeated_record_observations':repeated,'missing_merged_at_numbers':sorted(set(merge_missing)),
            'missing_closed_at_numbers':sorted(set(closed_missing)),
            'historical_visibility_complete':False,'updated_since_crawl_is_atomic_snapshot':False,
            'scope':'currently_visible_objects_updated_after_since; records may change while pagination runs'}
    unknown=set(tid for tid,_ in selected)-set(tools)
    if unknown:raise ValueError('Unknown visible-list tool')
    return observations,quality,{'raw_requests_verified':verified,'proof_file_hashes':proofs,'explicit_proof_reference_corrections_applied':applied,'cursor_method_disqualification':method_evidence}
