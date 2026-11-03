"""Verify frozen provider cursor and repository-identity evidence without HTTP."""
import gzip,hashlib,json,re
from pathlib import Path
from urllib.parse import urlsplit,parse_qs,urlencode,urlunsplit
from github_issues_cursor import cursor,validate_page,check_context


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1048576),b''):h.update(block)
    return h.hexdigest()

def cursor_for(anchor):
    return cursor(anchor['updated_at'],anchor['id']) if anchor else None

def verify_receipts(root,request,tool,registry,max_body_bytes=64*1024**2,cache=None):
    root=Path(root).resolve();extra={};cache={} if cache is None else cache
    def path_for(name):
        p=(root/name).resolve()
        if not p.is_relative_to(root) or not p.is_file():raise ValueError('Missing or escaped cursor proof path')
        return p
    def bound(name,expected):
        p=path_for(name);actual=digest(p)
        if not expected or actual!=expected:raise ValueError('Cursor/identity proof hash differs: '+name)
        extra[name]=actual;return p
    def document(name,expected):return json.loads(bound(name,expected).read_text())
    def rows(name,compressed,raw):
        p=bound(name,compressed)
        with gzip.open(p,'rb') as f:body=f.read(max_body_bytes+1)
        if len(body)>max_body_bytes or hashlib.sha256(body).hexdigest()!=raw:raise ValueError('Cursor proof decoded body hash/size differs')
        result=json.loads(body)
        if not isinstance(result,list):raise ValueError('Cursor proof response is not an array')
        return result
    def next_url(receipt,anchor,repo,repo_id,per_page):
        check_context(receipt['url'],repo,'2022-12-31T23:59:59Z',per_page)
        link=receipt.get('headers',{}).get('Link','');matches=re.findall(r'<([^>]+)>;\s*rel="next"',link)
        if len(matches)!=1:raise ValueError('Frozen provider next-Link is missing/ambiguous')
        u=urlsplit(matches[0]);q=parse_qs(u.query)
        if u.scheme!='https' or u.netloc!='api.github.com' or u.path!='/repositories/'+str(repo_id)+'/issues':raise ValueError('Provider cursor Link repository differs')
        if q.pop('page',None)!=['2'] or q.pop('after',None)!=[cursor_for(anchor)]:raise ValueError('Provider Link differs from observed cursor encoder')
        expected={'state':['all'],'sort':['updated'],'direction':['asc'],'since':['2022-12-31T23:59:59Z'],'per_page':[str(per_page)]}
        if q!=expected:raise ValueError('Provider Link filters differ')
        return matches[0]
    def proof(name,expected,depth=0):
        if depth>2:raise ValueError('Recursive cursor proof')
        key=('cursor',name,expected)
        if key in cache:extra.update(cache[key]);return
        before=dict(extra);p=document(name,expected)
        if 'base_proof_path' in p:
            proof(p['base_proof_path'],p['base_proof_sha256'],depth+1)
            r=document(p['tie_provider_receipt_path'],p['tie_provider_receipt_sha256'])
            body=rows(p['tie_provider_page_path'],r['compressed_sha256'],r['raw_sha256'])
            anchor=p['tied_anchor'];candidate=p['actual_next'];repo=urlsplit(r['url']).path.split('/repos/',1)[-1].rsplit('/issues',1)[0]
            mapping=[t for t in registry['tools'] if t['github_repo'].lower()==repo.lower()]
            if len(mapping)!=1:raise ValueError('Tie proof repository is outside cohort')
            if not body or body[-1]['id']!=anchor['id'] or body[-1]['updated_at']!=anchor['updated_at']:raise ValueError('Tie proof anchor differs from retained body')
            next_url(r,anchor,repo,mapping[0]['github_repo_id'],r['per_page'])
            resumed=rows(p['tie_connector_resume_raw_path'],p['tie_connector_resume_compressed_sha256'],p['tie_connector_resume_raw_sha256'])
            validate_page(resumed,anchor)
            if not resumed or any(resumed[0][k]!=candidate[k] for k in ['id','updated_at']):raise ValueError('Tie proof resumed body differs')
        else:
            r=document(p['provider_receipt_path'],p['provider_receipt_sha256'])
            body=rows(p['provider_first_page_path'],r['compressed_sha256'],r['raw_sha256']);anchor=p['anchor']
            repo=urlsplit(r['url']).path.split('/repos/',1)[-1].rsplit('/issues',1)[0]
            mapping=[t for t in registry['tools'] if t['github_repo'].lower()==repo.lower()]
            if len(mapping)!=1:raise ValueError('Cursor proof repository is outside cohort')
            if not body or any(body[-1][k]!=anchor[k] for k in ['id','updated_at']):raise ValueError('Cursor proof anchor differs')
            if p['provider_cursor']!=cursor_for(anchor) or p['generated_cursor']!=cursor_for(anchor):raise ValueError('Frozen encoder roundtrip differs')
            if next_url(r,anchor,repo,mapping[0]['github_repo_id'],100)!=p['provider_link']:raise ValueError('Provider Link receipt differs')
            previous=None
            for prefix,urlkey in [('connector_resume','connector_request_url'),('connector_cursor_only_resume','cursor_only_resume_url')]:
                resumed=rows(p[prefix+'_raw_path'],p[prefix+'_compressed_sha256'],p[prefix+'_raw_sha256'])
                validate_page(resumed,anchor)
                u=urlsplit(p[urlkey]);q=parse_qs(u.query);q.pop('page',None)
                normalized=urlunsplit((u.scheme,u.netloc,u.path,urlencode({k:v[0] for k,v in q.items()}),''))
                check_context(normalized,repo,'2022-12-31T23:59:59Z')
                if q.get('after')!=[cursor_for(anchor)] or not resumed:raise ValueError('Frozen connector resume context differs')
                if previous is not None and resumed!=previous:raise ValueError('Cursor-only and offset resume observations differ')
                previous=resumed
            for edge,actual in [('resume_first',previous[0]),('resume_last',previous[-1])]:
                if any(p[edge][k]!=actual[k] for k in ['id','updated_at']):raise ValueError('Frozen resume bounds differ')
        cache[key]=dict(extra)
    if request.get('transport')!='github_connector_jsonp_envelope':
        proof(request['cursor_conformance_path'],request['cursor_conformance_receipt_sha256'])
    name=request['repo_identity_proof_path'];expected=request['repo_identity_proof_sha256'];key=('identity',name,expected,tool['tool_id'])
    if key in cache:extra.update(cache[key])
    else:
        before=dict(extra);p=document(name,expected)
        if p.get('registry_sha256')!=request['registry_sha256']:raise ValueError('Identity proof cohort differs')
        entry=p['tools'][tool['tool_id']]
        if entry.get('github_repo','').lower()!=tool['github_repo'].lower() or entry.get('expected_repo_id')!=tool['github_repo_id'] or entry.get('observed_repo_id')!=tool['github_repo_id'] or entry.get('method')!='direct_PR_base_repo_id':raise ValueError('Identity proof mapping differs')
        body=rows(entry['raw_path'],entry['compressed_sha256'],entry['raw_sha256'])
        if not body or any((r.get('base',{}).get('repo') or {}).get('id')!=tool['github_repo_id'] or str((r.get('base',{}).get('repo') or {}).get('full_name','')).lower()!=tool['github_repo'].lower() for r in body):raise ValueError('Identity proof PR base numeric ID differs')
        cache[key]=dict(extra)
    if request.get('github_repo_id')!=tool['github_repo_id'] or request.get('repo','').lower()!=tool['github_repo'].lower():raise ValueError('Visible request numeric repository identity differs')
    return extra
