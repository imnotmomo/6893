"""Build conservative weekly issue/PR observations from a frozen connector archive.

No requests are made. Search agreement describes index retrieval only. True issue
counts require exhaustive provider-cursor GET lists; independent PR views are
compared without intersecting away objects.
Current closure state is not a full closure/reopen event history. No output is
eligible as historical primary data.
"""
import argparse
from collections import Counter,defaultdict
from datetime import datetime,timedelta,timezone
import csv,gzip,hashlib,json,math,os
from pathlib import Path
import shutil,tempfile
from urllib.parse import urlsplit,parse_qs

from search_plan import plan as search_plan,STREAMS
from github_visible_lists import evaluate_visible
from github_jsonp_lists import evaluate_v3
from github_pr_history import evaluate_pr_history

SOURCE='github_direct_issues_pulls'
VERSION='single_inclusive_date_range_v2'
MEASURES=['github_issue_opened_count','github_pr_opened_count','github_pr_merged_count',
          'github_issue_last_reported_closed_count','github_pr_last_reported_closed_count',
          'github_issue_creation_authors','github_pr_creation_authors','github_creation_authors']


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
    return digest.hexdigest()
def utc(value):
    if not isinstance(value,str):raise ValueError('Required UTC source timestamp is missing')
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Source timestamps must have a UTC offset')
    return stamp.astimezone(timezone.utc)
def stamp(value):return utc(value).isoformat().replace('+00:00','Z') if value is not None else None
def positive(value):return isinstance(value,int) and not isinstance(value,bool) and value>0
def transport_summary(requests):
    transports=Counter(r.get('transport','unreported') for r in requests)
    metadata=Counter();versions=set()
    for request in requests:
        for name,value in (request.get('safe_meta') or {}).items():
            name=name.lower();metadata[name]+=1
            if name=='x-github-api-version-selected' and isinstance(value,str):versions.add(value)
    return {'transport':next(iter(transports)) if len(transports)==1 else 'mixed_connector_profiles',
        'transport_receipt_counts':dict(sorted(transports.items())),
        'safe_metadata_receipt_counts':dict(sorted(metadata.items())),
        'api_version':sorted(versions) if versions else 'unreported',
        'etag_and_rate_headers':'per_receipt_safe_meta_when_reported' if metadata else 'unreported'}
def week(value):
    day=utc(value).replace(hour=0,minute=0,second=0,microsecond=0)
    return (day-timedelta(days=day.weekday())).strftime('%Y-%m-%d')
def monday_intervals(start,end):
    left=utc(start).replace(hour=0,minute=0,second=0,microsecond=0)
    left+=timedelta(days=(-left.weekday())%7)
    if left<utc(start):left+=timedelta(days=7)
    while left+timedelta(days=7)<=utc(end):
        yield left,left+timedelta(days=7);left+=timedelta(days=7)
def covers(intervals,left,right):
    edge=left
    for start,end in sorted(intervals):
        if end<=edge:continue
        if start>edge:return False
        edge=max(edge,end)
        if edge>=right:return True
    return False


def raw_body(root,request,cohort,max_bytes):
    if request.get('registry_sha256')!=cohort:raise ValueError('Raw request cohort hash differs')
    if request.get('transport')!='github_connector_json_body':raise ValueError('Do not mix connector and separately versioned incremental archives')
    path=(root/request['raw_path']).resolve()
    if not path.is_relative_to(root) or not path.is_file():raise ValueError('Missing or escaped raw archive path')
    if sha(path)!=request.get('compressed_sha256'):raise ValueError('Compressed raw response hash differs')
    with gzip.open(path,'rb') as stream:body=stream.read(max_bytes+1)
    if len(body)>max_bytes:raise ValueError('Raw response exceeds the bounded JSON body limit')
    if hashlib.sha256(body).hexdigest()!=request.get('raw_sha256'):raise ValueError('Decoded raw response hash differs')
    return json.loads(body)


def identity(row,tool,request,direct=False):
    if not isinstance(row,dict) or not positive(row.get('number')):raise ValueError('Missing stable GitHub number')
    repo=tool['github_repo'].lower();number=row['number'];url=urlsplit(row.get('url',''))
    expected='/repos/'+repo+('/pulls/' if direct else '/issues/')+str(number)
    if url.scheme!='https' or url.hostname!='api.github.com' or url.path.lower()!=expected:
        raise ValueError('Response URL differs from frozen repository/number')
    if direct:
        base=row.get('base',{}).get('repo') or {}
        if base.get('id')!=tool['github_repo_id']:raise ValueError('PR base repository numeric ID differs')
        if base.get('full_name') and base['full_name'].lower()!=repo:raise ValueError('PR base repository name differs')
    else:
        repository=row.get('repository_url')
        if repository and repository.lower()!='https://api.github.com/repos/'+repo:raise ValueError('Search repository URL differs')
    kind='pull' if direct or 'pull_request' in row else 'issue'
    author_data=row.get('user') or {};reported_author=author_data.get('id');author=reported_author
    if author is not None and not positive(author):raise ValueError('GitHub account ID is invalid')
    # Confirmed in this archive: the shared ghost account represents deleted
    # identities. Its public ID is retained, but is not one exact creator.
    placeholder=author==10137 or str(author_data.get('login','')).lower()=='ghost'
    if placeholder:author=None
    merged=row.get('merged_at') if direct else (row.get('pull_request') or {}).get('merged_at')
    created=utc(row.get('created_at'));observed=utc(request['observed_at'])
    chronology=[]
    for field,value in [('closed_at',row.get('closed_at')),('merged_at',merged)]:
        if value is None:continue
        event=utc(value)
        if not created<=event<=observed:
            if direct and (request.get('phase')=='pr_history_collect' or request.get('stream')=='native_pr_updated') and event<created:
                chronology.append({'field':field,'event_at':stamp(value),'created_at':stamp(row['created_at']),
                    'reason':'native_event_precedes_pr_object_creation','raw_path':request['raw_path']})
            else:raise ValueError('Record event time contradicts creation/observation')
    if created>observed:raise ValueError('Record creation is after source observation')
    return {'tool_id':tool['tool_id'],'repo_id':tool['github_repo_id'],'number':number,'kind':kind,
            'created_at':stamp(row.get('created_at')),'issue_created_at':None if direct else stamp(row.get('created_at')),
            'pr_created_at':stamp(row.get('created_at')) if direct else None,'creation_timestamp_semantics':'pull_request_object' if direct else 'issue_object',
            'last_reported_closed_at':stamp(row.get('closed_at')),
            'merged_at':stamp(merged),'author_account_id':author,'author_reported_account_id':reported_author,
            'author_identity_status':'shared_deleted_placeholder' if placeholder else 'missing' if author is None else 'public_account',
            'provider_state':row.get('state'),'updated_at':stamp(row.get('updated_at')),
            'closed_at_field_present':'closed_at' in row,
            'merged_at_field_present':('merged_at' in row if direct else 'merged_at' in (row.get('pull_request') or {})),
            'event_time_anomalies':chronology,
            'source_view':'core_pr' if direct else 'issues_visible' if request.get('stream')=='issues_visible' else 'search_index',
            'observed_at':stamp(request['observed_at']),'direct_pr_observation':direct,
            'provider_record_id':row.get('id'),'source_path':request['raw_path']}


def validate_request_url(request,tool,leaf=None):
    url=urlsplit(request['url']);query=parse_qs(url.query)
    if url.scheme!='https' or url.hostname!='api.github.com':raise ValueError('Unrecognized provider URL')
    if leaf is not None:
        expected={'q':leaf['query'],'page':str(request['page']),'per_page':str(leaf['per_page'])}
        if url.path!='/search/issues' or any(query.get(k)!=[v] for k,v in expected.items()):
            raise ValueError('Search URL does not match its certified query')
        for field in ['tool_id','repo','stream','window_start','window_end_exclusive','date_field','query','count_only']:
            actual=request.get(field,False) if field=='count_only' else request.get(field)
            if actual!=leaf.get(field):raise ValueError('Search metadata differs from deterministic partition: '+field)
    else:
        expected={'state':'all','sort':'created','direction':'asc','per_page':'100','page':str(request['page'])}
        if url.path.lower()!='/repos/'+tool['github_repo'].lower()+'/pulls' or any(query.get(k)!=[v] for k,v in expected.items()):
            raise ValueError('Core PR URL does not certify ascending created-order pagination')


def merge_record(records,record,max_records):
    key=(record['tool_id'],record['repo_id'],record['number']);previous=records.get(key)
    if previous is None:
        if len(records)>=max_records:raise ValueError('Normalized record bound exceeded')
        records[key]={**record,'first_observed_at':record['observed_at'],'evidence_count':1,
                      '_direct_merged_at':record['merged_at'] if record['direct_pr_observation'] else None,
                      '_direct_observed_at':record['observed_at'] if record['direct_pr_observation'] else None,
                      '_view_records':{record['source_view']:dict(record)}}
        return key
    old_view=previous['_view_records'].get(record['source_view'])
    if previous['kind']!=record['kind'] or old_view and old_view['created_at']!=record['created_at'] or record['kind']=='issue' and previous['created_at']!=record['created_at']:
        raise ValueError('Immutable record identity/creation timestamp changed within the same object view: '+encoded({'key':key,'previous_source':previous['source_path'],'new_source':record['source_path']}))
    first=min(previous['first_observed_at'],record['observed_at'],key=utc);count=previous['evidence_count']+1
    direct_time=previous['_direct_observed_at'];direct_merge=previous['_direct_merged_at']
    if record['direct_pr_observation'] and (direct_time is None or utc(record['observed_at'])>=utc(direct_time)):
        direct_time,direct_merge=record['observed_at'],record['merged_at']
    views=dict(previous['_view_records']);old_view=views.get(record['source_view'])
    if old_view is None or (utc(record['observed_at']),record['source_path'])>=(utc(old_view['observed_at']),old_view['source_path']):views[record['source_view']]=dict(record)
    # Index observations cannot overwrite newer authoritative API fields.
    priority=lambda row:(row['source_view']!='search_index',utc(row['observed_at']),row['source_path'])
    candidate=record if priority(record)>=priority(previous) else previous
    # An explicit direct PR merged_at (including null) outranks Search metadata.
    api_merges=[v for name,v in views.items() if name in {'core_pr','core_pr_history','issues_visible','issues_visible_v3'} and v['merged_at_field_present']]
    merged=max(api_merges,key=lambda r:(utc(r['observed_at']),r['source_path']))['merged_at'] if api_merges else candidate['merged_at']
    if direct_time is None and merged is None:merged=previous['merged_at'] or record['merged_at']
    pr_created=views.get('core_pr',{}).get('created_at') or views.get('core_pr_history',{}).get('created_at')
    issue_views=[v for name,v in views.items() if name not in {'core_pr','core_pr_history'}]
    issue_created=max(issue_views,key=priority)['created_at'] if issue_views else None
    records[key]={**candidate,'merged_at':merged,'created_at':pr_created or issue_created,
                  'pr_created_at':pr_created,'issue_created_at':issue_created,
                  'creation_timestamp_semantics':'pull_request_object' if pr_created else 'issue_object',
                  'first_observed_at':first,'evidence_count':count,
                  '_direct_observed_at':direct_time,'_direct_merged_at':direct_merge,'_view_records':views}
    return key


def evaluate(root,state,registry,max_body_bytes=64*1024**2,max_records=1000000):
    root=Path(root).resolve();cohort=state['registry_sha256'];tools={t['tool_id']:t for t in registry['tools']}
    if len(tools)!=len(registry['tools']) or any(not positive(t.get('github_repo_id')) for t in tools.values()):
        raise ValueError('Frozen cohort must have unique tool IDs and numeric repository IDs')
    start,end=utc(state['start']),utc(state['end_exclusive'])
    if not start<end:raise ValueError('Invalid completed source interval')
    for tid,source in state.get('tools',{}).items():
        if tid not in tools or source.get('repo_id')!=tools[tid]['github_repo_id'] or source.get('repo','').lower()!=tools[tid]['github_repo'].lower():
            raise ValueError('Source repository mapping differs from registry')
    requests=state['requests'];rejected=[r for r in requests if 'search_job_id' in r and r.get('query_semantics_version')!=VERSION]
    eligible=[r for r in requests if 'search_job_id' not in r or r.get('query_semantics_version')==VERSION]
    eligible.sort(key=lambda r:(utc(r['observed_at']),r['raw_path']))
    filtered={**state,'requests':eligible};partition=search_plan(filtered,registry)
    leaves={leaf['search_job_id']:leaf for leaf in partition['leaves']}
    jobs={**{b['search_job_id']:b for b in partition['branches']},**leaves}
    for pending in partition['pending']:jobs.setdefault(pending['search_job_id'],pending)
    selected={};core={}
    for request in eligible:
        if request.get('search_job_id') in jobs:
            selected[(request['search_job_id'],request['page'])]=request
        elif request.get('phase')=='collect' and request.get('kind')=='pulls' and request.get('per_page')==100:
            core[(request['tool_id'],request['page'])]=request
    records={};errors=[];verified=[];leaf_rows=defaultdict(list);core_ids=defaultdict(set);core_pages=defaultdict(set);core_sizes=defaultdict(dict);core_numbers=defaultdict(list);core_bounds=defaultdict(dict)
    leaf_checks={jid:{'complete':False,'reasons':[],'unique_count':0,'total_count':leaf['total_count']} for jid,leaf in leaves.items()}
    # Core observations are processed first so old Search merge records can use
    # a known direct merged_at where a retained boundary page includes that PR.
    for (tid,page),request in sorted(core.items()):
        if tid not in tools:raise ValueError('Unknown core tool')
        validate_request_url(request,tools[tid]);body=raw_body(root,request,cohort,max_body_bytes)
        if not isinstance(body,list) or len(body)>100 or request.get('record_count')!=len(body):raise ValueError('Core PR page shape/count differs')
        verified.append(request);core_pages[tid].add(page);core_sizes[tid][page]=len(body)
        core_bounds[tid][page]={'first_created':body[0].get('created_at') if body else None,'last_created':body[-1].get('created_at') if body else None}
        for row in body:
            item=identity(row,tools[tid],request,True);key=merge_record(records,item,max_records);core_numbers[tid].append(item['number'])
            if start<=utc(item['created_at'])<end:core_ids[tid].add(key)
    search_pages=defaultdict(dict)
    for (jid,page),request in sorted(selected.items()):
        job=jobs[jid];tid=job['tool_id'];validate_request_url(request,tools[tid],job)
        body=raw_body(root,request,cohort,max_body_bytes);verified.append(request)
        if not isinstance(body,dict) or not isinstance(body.get('items'),list):raise ValueError('Search response shape differs')
        total=body.get('total_count');incomplete=body.get('incomplete_results')
        if not isinstance(total,int) or isinstance(total,bool) or total<0 or not isinstance(incomplete,bool):raise ValueError('Search count/complete fields are invalid')
        if request.get('search_total_count')!=total or request.get('search_incomplete')!=incomplete or request.get('record_count')!=len(body['items']):
            raise ValueError('Search receipt differs from archived provider body')
        search_pages[jid][page]={'total_count':total,'incomplete':incomplete,'items':body['items'],'request':request}
        if jid not in leaves or job.get('count_only'):continue
        for row in body['items']:
            try:
                item=identity(row,tools[tid],request)
                expected='pull' if job['kind']=='pulls' else 'issue'
                if item['kind']!=expected:raise ValueError('Search row type differs from stream')
                created=utc(item['created_at'])
                if job['stream']!='issues_created' and not created<start:raise ValueError('Old-created Search row is not before study')
                event={'created':item['created_at'],'closed':item['last_reported_closed_at'],'merged':item['merged_at']}[job['date_field']]
                missing_merge=job['date_field']=='merged' and event is None
                if not missing_merge and (event is None or not utc(job['window_start'])<=utc(event)<utc(job['window_end_exclusive'])):
                    raise ValueError('Search row event is outside certified date partition')
                key=merge_record(records,item,max_records)
                leaf_rows[jid].append((key,missing_merge))
            except ValueError as error:
                leaf_checks[jid]['reasons'].append(str(error))
                errors.append({'tool_id':tid,'search_job_id':jid,'number':row.get('number') if isinstance(row,dict) else None,'error':str(error)})
    intervals=defaultdict(list);census={};missing_merge=[]
    for jid,leaf in leaves.items():
        check=leaf_checks[jid];pages=search_pages[jid];expected=set(range(1,leaf['total_pages']+1))
        if set(pages)!=expected:check['reasons'].append('missing_or_unexpected_pages')
        if any(page['total_count']!=leaf['total_count'] or page['incomplete'] for page in pages.values()):check['reasons'].append('changing_count_or_incomplete_search')
        if not leaf.get('count_only'):
            for page_number,page in pages.items():
                expected_size=max(0,min(100,page['total_count']-(page_number-1)*100))
                if len(page['items'])!=expected_size:check['reasons'].append('invalid_search_page_size')
        if leaf.get('count_only'):
            # The one returned item is a type/date sanity check. It is never the
            # expected count, and the 1,000 Search-result cap does not apply.
            for page in pages.values():
                if len(page['items'])!=min(1,page['total_count']):check['reasons'].append('invalid_census_sample_size')
                for row in page['items']:
                    try:
                        item=identity(row,tools[leaf['tool_id']],page['request'])
                        if item['kind']!='pull' or not start<=utc(item['created_at'])<end:raise ValueError('Invalid census sample type/date')
                    except ValueError as error:check['reasons'].append(str(error))
        else:
            keys=[key for key,_ in leaf_rows[jid]];check['unique_count']=len(set(keys))
            if len(keys)!=len(set(keys)):check['reasons'].append('duplicate_records_across_search_pages')
            if len(set(keys))!=leaf['total_count']:check['reasons'].append('unique_count_differs_from_search_total')
            for key,pending_merge in leaf_rows[jid]:
                if leaf['date_field']=='merged':
                    merged=records[key]['merged_at']
                    if merged is None or not utc(leaf['window_start'])<=utc(merged)<utc(leaf['window_end_exclusive']):
                        check['reasons'].append('missing_exact_merge_timestamp')
                        missing_merge.append({'tool_id':key[0],'repo_id':key[1],'number':key[2],'search_job_id':jid})
        check['reasons']=sorted(set(check['reasons']));check['complete']=not check['reasons']
        if check['complete']:
            if leaf.get('count_only'):census[leaf['tool_id']]=leaf['total_count']
            else:intervals[(leaf['tool_id'],leaf['stream'])].append((utc(leaf['window_start']),utc(leaf['window_end_exclusive'])))
    visible_rows,visible_quality,visible_diag=evaluate_visible(root,state,registry,raw_body,identity,utc,max_body_bytes)
    v3_rows,v3_quality,v3_diag=evaluate_v3(root,state,registry,identity,utc,max_body_bytes)
    visible_rows.extend(v3_rows);visible_quality.update(v3_quality)
    visible_diag['raw_requests_verified'].extend(v3_diag['raw_requests_verified']);visible_diag['proof_file_hashes'].update(v3_diag['proof_file_hashes'])
    for item in visible_rows:merge_record(records,item,max_records)
    verified.extend(visible_diag['raw_requests_verified'])
    core_quality={}
    for tid in tools:
        source=state.get('tools',{}).get(tid,{})
        left,right=source.get('start_page'),source.get('last_page');pages=core_pages[tid]
        page_complete=positive(left) and positive(right) and left<=right and pages==set(range(left,right+1))
        boundary=source.get('boundary_previous_last_created');boundary_receipt=None
        boundary_ok=left==1
        if positive(left) and left>1:
            candidates=[r for r in eligible if r.get('tool_id')==tid and r.get('kind')=='pulls' and r.get('phase')=='seek' and
                        (r.get('per_page')==1 and r.get('page')==(left-1)*100 or r.get('per_page')==100 and r.get('page')==left-1)]
            if candidates:
                probe=max(candidates,key=lambda r:(utc(r['observed_at']),r['raw_path']));u=urlsplit(probe['url']);q=parse_qs(u.query)
                expected={'state':'all','sort':'created','direction':'asc','per_page':str(probe['per_page']),'page':str(probe['page'])}
                if u.scheme!='https' or u.netloc!='api.github.com' or u.path.lower()!='/repos/'+tools[tid]['github_repo'].lower()+'/pulls' or q!={k:[v] for k,v in expected.items()}:raise ValueError('Core PR boundary probe scope differs')
                body=raw_body(root,probe,cohort,max_body_bytes)
                if not isinstance(body,list) or len(body)!=probe['per_page'] or probe.get('record_count')!=len(body):raise ValueError('Core PR boundary probe shape differs')
                items=[identity(row,tools[tid],probe,True) for row in body];boundary_receipt=probe['raw_path'];verified.append(probe)
                actual=items[-1]['created_at'];boundary_ok=utc(actual)<start and boundary is not None and utc(actual)==utc(boundary)
        first_last=source.get('boundary_first_last_created');actual_first=core_bounds[tid].get(left,{}).get('last_created')
        if actual_first is not None:boundary_ok=boundary_ok and utc(actual_first)>=start
        if first_last is not None:boundary_ok=boundary_ok and actual_first is not None and utc(actual_first)==utc(first_last)
        count=len(core_ids[tid]);expected=census.get(tid);sizes=core_sizes[tid]
        terminal_ok=positive(right) and right in sizes and sizes[right]<100
        intermediate_ok=page_complete and all(sizes[p]==100 for p in pages if p!=right)
        duplicate_numbers=sorted(n for n,c in Counter(core_numbers[tid]).items() if c>1)
        complete=bool(page_complete and boundary_ok and terminal_ok and intermediate_ok and not duplicate_numbers)
        core_quality[tid]={'pages_complete':bool(page_complete),'boundary_complete':boundary_ok,
                           'expected_created_pr_count':expected,'observed_unique_created_pr_count':count,
                           'created_census_complete':complete,'created_visible_offset_crawl_complete':complete,
                           'terminal_short_page_verified':bool(terminal_ok),'intermediate_page_sizes_verified':bool(intermediate_ok),
                           'duplicate_core_pr_numbers':duplicate_numbers,'boundary_raw_path':boundary_receipt,
                           'search_index_count_agreement':expected is not None and count==expected,
                           'created_pr_numbers':sorted(k[2] for k in core_ids[tid]),
                           'pages_observed':len(pages),'start_page':left,'last_page':right}
    history_rows,history_quality,history_diag=evaluate_pr_history(root,state,registry,identity,utc,raw_body,max_body_bytes)
    for item in history_rows:merge_record(records,{**item,'source_view':'core_pr_history'},max_records)
    verified.extend(history_diag['raw_requests_verified']);visible_diag['proof_file_hashes'].update(history_diag['proof_file_hashes'])
    # Original current pages are independently checked by both acquisition
    # profiles. Count verified physical bodies once while retaining profile QA.
    verified=list({r['raw_path']:r for r in verified}.values())
    for tid,q in visible_quality.items():
        rows=[r for r in visible_rows if r['tool_id']==tid and (r['source_view']=='issues_visible_v3' if tid in v3_quality else r['source_view']=='issues_visible')]
        visible_pr={r['number'] for r in rows if r['kind']=='pull' and start<=utc(records[(tid,r['repo_id'],r['number'])]['pr_created_at'] or r['created_at'])<end}
        direct_pr={k[2] for k in core_ids[tid]};comparable=q['visible_list_pagination_complete'] and core_quality[tid]['created_visible_offset_crawl_complete']
        q['core_pr_views_comparable']=bool(comparable);q['core_pr_created_id_agreement']=visible_pr==direct_pr if comparable else None
        q['only_visible_created_pr_numbers']=sorted(visible_pr-direct_pr) if comparable else []
        q['only_core_created_pr_numbers']=sorted(direct_pr-visible_pr) if comparable else []
        search_issues={key[2] for key,r in records.items() if r['tool_id']==tid and r['kind']=='issue' and 'search_index' in r['_view_records'] and start<=utc(r['created_at'])<end}
        visible_issues={r['number'] for r in rows if r['kind']=='issue' and start<=utc(r['created_at'])<end}
        index_complete=covers(intervals[(tid,'issues_created')],start,end)
        q['search_index_comparable']=bool(index_complete and q['visible_list_pagination_complete'])
        q['search_index_created_issue_id_agreement']=visible_issues==search_issues if q['search_index_comparable'] else None
        q['visible_issue_ids_absent_from_search']=sorted(visible_issues-search_issues) if q['search_index_comparable'] else []
        q['search_issue_ids_absent_from_visible']=sorted(search_issues-visible_issues) if q['search_index_comparable'] else []
        conflicts=[]
        for record in records.values():
            views=record['_view_records'];a=views.get('core_pr');b=views.get('issues_visible_v3') or views.get('issues_visible')
            if record['tool_id']!=tid or not a or not b or not a['merged_at_field_present'] or not b['merged_at_field_present']:continue
            if a['merged_at']!=b['merged_at'] and (a['merged_at'] and b['merged_at'] or a['merged_at'] and utc(b['observed_at'])>=utc(a['merged_at']) or b['merged_at'] and utc(a['observed_at'])>=utc(b['merged_at'])):conflicts.append(record['number'])
        q['merge_timestamp_conflict_numbers']=sorted(conflicts)
        q['issue_vs_pr_creation_timestamp_differences']=[{'number':r['number'],'issue_created_at':r['issue_created_at'],'pr_created_at':r['pr_created_at']} for r in records.values() if r['tool_id']==tid and r['kind']=='pull' and r['issue_created_at'] and r['pr_created_at'] and r['issue_created_at']!=r['pr_created_at']]
    verified_paths={r['raw_path'] for r in verified}
    return records,intervals,core_quality,{'native_pr_history_quality':history_quality,'native_pr_history':{k:v for k,v in history_diag.items() if k!='raw_requests_verified'},'v3_transport_label_diagnostics_applied':v3_diag['transport_label_diagnostics_applied'],'visible_list_quality':visible_quality,'cursor_method_disqualification':visible_diag['cursor_method_disqualification'],'proof_file_hashes':visible_diag['proof_file_hashes'],'explicit_proof_reference_corrections_applied':visible_diag['explicit_proof_reference_corrections_applied'],'partition_plan':partition,'leaf_checks':leaf_checks,'raw_requests_verified':verified,
        'rejected_query_probe_count':len(rejected),'ignored_nonfeature_request_count':sum(r['raw_path'] not in verified_paths for r in requests),
        'errors':errors,'missing_merge_details':missing_merge}


def weekly_features(records,intervals,core_quality,registry,start,end,include_partial=False,visible_quality=None,native_pr_history_quality=None):
    counts=defaultdict(Counter);authors=defaultdict(lambda:{'issue':set(),'pull':set(),'all':set()});unknown=defaultdict(Counter)
    for row in records.values():
        tid=row['tool_id'];created=row['created_at'];kind=row['kind'];author=row['author_account_id']
        if utc(start)<=utc(created)<utc(end):
            key=(tid,week(created));measure='github_issue_opened_count' if kind=='issue' else 'github_pr_opened_count'
            counts[key][measure]+=1
            if author is None:unknown[key][kind]+=1
            else:authors[key][kind].add(author);authors[key]['all'].add(author)
        for field,measure in [('merged_at','github_pr_merged_count'),('last_reported_closed_at','github_issue_last_reported_closed_count' if kind=='issue' else 'github_pr_last_reported_closed_count')]:
            value=row[field]
            if value is not None and utc(start)<=utc(value)<utc(end):counts[(tid,week(value))][measure]+=1
    # Backward-compatible readers can inspect the old Search-index series.
    # Production build always passes explicit visible quality and strict masks.
    strict_pools={}
    if visible_quality is not None:
        for view in ['issues_visible','core_pr']:
            pool={}
            for k,v in records.items():
                actual='issues_visible_v3' if view=='issues_visible' and visible_quality[k[0]].get('pagination_method')=='actual_provider_jsonp_links_v3' else view
                if actual in v['_view_records']:pool[k]=dict(v['_view_records'][actual])
            if view=='issues_visible':
                for key,row in pool.items():
                    if row['kind']=='pull' and records[key]['pr_created_at']:row['created_at']=records[key]['pr_created_at']
            strict_pools[view]=weekly_features(pool,intervals,core_quality,registry,start,end,include_partial)
    if visible_quality is not None and native_pr_history_quality:
        pool={}
        for key,record in records.items():
            candidates=[v for name,v in record['_view_records'].items() if name in {'core_pr','core_pr_history'}]
            if candidates:pool[key]=dict(max(candidates,key=lambda v:(utc(v['observed_at']),v['source_path'])))
        strict_pools['native_pr_history']=weekly_features(pool,intervals,core_quality,registry,start,end,include_partial)
    output=[]
    periods=list(monday_intervals(start,end))
    if include_partial:
        left=utc(start).replace(hour=0,minute=0,second=0,microsecond=0)
        left-=timedelta(days=left.weekday());periods=[]
        while left<utc(end):periods.append((left,left+timedelta(days=7)));left+=timedelta(days=7)
    for calendar_left,calendar_right in periods:
        left,right=max(calendar_left,utc(start)),min(calendar_right,utc(end))
        for tool in sorted(registry['tools'],key=lambda t:t['tool_id']):
            tid=tool['tool_id'];key=(tid,calendar_left.strftime('%Y-%m-%d'));core_complete=core_quality[tid]['created_census_complete']
            issue_complete=covers(intervals[(tid,'issues_created')],left,right)
            flags={'github_issue_opened_count':issue_complete,'github_pr_opened_count':core_complete,
                   'github_pr_merged_count':core_complete and covers(intervals[(tid,'pulls_old_merged')],left,right),
                   'github_issue_last_reported_closed_count':covers(intervals[(tid,'issues_created')],utc(start),right) and covers(intervals[(tid,'issues_old_closed')],left,right),
                   'github_pr_last_reported_closed_count':core_complete and covers(intervals[(tid,'pulls_old_closed')],left,right),
                   'github_issue_creation_authors':issue_complete and not unknown[key]['issue'],
                   'github_pr_creation_authors':core_complete and not unknown[key]['pull'],
                   'github_creation_authors':issue_complete and core_complete and not sum(unknown[key].values())}
            strict_values=None
            if visible_quality is not None:
                q=visible_quality[tid];visible_complete=q['visible_list_pagination_complete'];agreement=q['core_pr_created_id_agreement']
                pr_complete=visible_complete and agreement is not False
                pr_creation_complete=core_quality[tid]['created_visible_offset_crawl_complete']
                # Existing complete core-created cohorts remain separately useful
                # while the new exhaustive issue crawl is still in progress.
                core_only=not visible_complete and core_complete
                flags={'github_issue_opened_count':visible_complete,'github_pr_opened_count':pr_creation_complete or core_only,
                       'github_pr_merged_count':pr_complete and not q['missing_merged_at_numbers'] and not q['merge_timestamp_conflict_numbers'],
                       'github_issue_last_reported_closed_count':visible_complete and not q['missing_closed_at_numbers'],
                       'github_pr_last_reported_closed_count':pr_complete and not q['missing_closed_at_numbers']}
                index=len(output);visible_row=strict_pools['issues_visible'][index];core_row=strict_pools['core_pr'][index]
                pr_row=core_row
                strict_values={m:visible_row['observed_partial_'+m] for m in MEASURES}
                for m in ['github_pr_opened_count','github_pr_creation_authors']:strict_values[m]=pr_row['observed_partial_'+m]
                native=(native_pr_history_quality or {}).get(tid,{})
                native_row=strict_pools['native_pr_history'][index] if 'native_pr_history' in strict_pools else None
                if native.get('merge_count_eligible'):
                    flags['github_pr_merged_count']=True;strict_values['github_pr_merged_count']=native_row['observed_partial_github_pr_merged_count']
                if native.get('last_reported_closed_count_eligible'):
                    flags['github_pr_last_reported_closed_count']=True;strict_values['github_pr_last_reported_closed_count']=native_row['observed_partial_github_pr_last_reported_closed_count']
                flags['github_issue_creation_authors']=visible_complete and not visible_row['github_issue_unknown_creation_author_count']
                flags['github_pr_creation_authors']=(pr_creation_complete or core_only) and not pr_row['github_pr_unknown_creation_author_count']
                flags['github_creation_authors']=visible_complete and agreement is not False and pr_creation_complete and not (visible_row['github_issue_unknown_creation_author_count']+visible_row['github_pr_unknown_creation_author_count'])
            row={'tool_id':tid,'week_start':key[1],'event_window_end_exclusive':calendar_right.strftime('%Y-%m-%d'),
                 'feature_cutoff':(calendar_right+timedelta(days=2)).strftime('%Y-%m-%d'),
                 'effective_window_start':left.isoformat().replace('+00:00','Z'),
                 'effective_window_end_exclusive':right.isoformat().replace('+00:00','Z'),
                 'week_complete':left==calendar_left and right==calendar_right,
                 'availability_lag_days':2,'availability_lag_is_assumption':True,
                 'historical_visibility_complete':False,'primary_eligible':False,
                 'coverage_complete':all(flags.values()),'retrieval_complete':all(flags.values())}
            for measure in MEASURES:
                value=(len(authors[key]['issue']) if measure=='github_issue_creation_authors' else
                       len(authors[key]['pull']) if measure=='github_pr_creation_authors' else
                       len(authors[key]['all']) if measure=='github_creation_authors' else counts[key][measure])
                row[measure]=(strict_values[measure] if strict_values is not None else value) if flags[measure] else None
                row[measure+'_complete']=bool(flags[measure]);row['observed_partial_'+measure]=value
            if visible_quality is not None:
                row['visible_list_pagination_complete']=visible_complete
                row['core_pr_created_id_agreement']=agreement
                row['search_index_created_issue_id_agreement']=q['search_index_created_issue_id_agreement']
                row['pr_creation_measurement_view']='core_pr_created_cohort'
                row['native_pr_history_capture_complete']=bool(native.get('capture_complete'))
                row['pr_merge_measurement_view']='native_pr_all_history' if native.get('merge_count_eligible') else 'hybrid_current_core_and_issue_resultset'
                row['pr_last_closed_measurement_view']='native_pr_all_history' if native.get('last_reported_closed_count_eligible') else 'hybrid_current_core_and_issue_resultset'
            row['github_issue_unknown_creation_author_count']=visible_row['github_issue_unknown_creation_author_count'] if visible_quality is not None else unknown[key]['issue']
            row['github_pr_unknown_creation_author_count']=pr_row['github_pr_unknown_creation_author_count'] if visible_quality is not None else unknown[key]['pull']
            output.append(row)
    return output


def build(args):
    if args.output.exists():raise FileExistsError('Use a fresh immutable output directory')
    state_bytes=args.state_snapshot.read_bytes();state=json.loads(state_bytes);registry=json.loads(args.registry.read_text())
    if state.get('source')!=SOURCE or state.get('registry_sha256')!=sha(args.registry):raise ValueError('Frozen source type/cohort differs')
    if utc(state['end_exclusive'])>datetime.now(timezone.utc):raise ValueError('Use a completed source endpoint')
    records,intervals,core,diagnostics=evaluate(args.source_root,state,registry,args.max_body_mib*1024**2,args.max_records)
    all_weeks=weekly_features(records,intervals,core,registry,state['start'],state['end_exclusive'],True,diagnostics['visible_list_quality'],diagnostics['native_pr_history_quality'])
    weekly=[row for row in all_weeks if row['week_complete']]
    partial_weeks=[row for row in all_weeks if not row['week_complete']]
    complete=all(q['visible_list_pagination_complete'] for q in diagnostics['visible_list_quality'].values()) and all(q['created_visible_offset_crawl_complete'] for q in core.values())
    if diagnostics['native_pr_history']['configured']:complete=complete and all(q['capture_complete'] for q in diagnostics['native_pr_history_quality'].values())
    review_needed=any(not r['coverage_complete'] for r in all_weeks) or any(q.get('event_time_anomalies') for q in diagnostics['native_pr_history_quality'].values())
    status='needs_review' if complete and review_needed else 'passed' if complete else 'partial'
    args.output.parent.mkdir(parents=True,exist_ok=True);staging=Path(tempfile.mkdtemp(prefix='.github-weekly-',dir=args.output.parent))
    try:
        with gzip.GzipFile(filename=str(staging/'normalized_records.jsonl.gz'),mode='wb',mtime=0) as stream:
            for key in sorted(records):stream.write((encoded({**{k:v for k,v in records[key].items() if not k.startswith('_')},'source_view_observations':records[key]['_view_records']})+'\n').encode())
        with (staging/'github_activity_weekly.csv').open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(weekly[0]) if weekly else ['tool_id','week_start']);writer.writeheader();writer.writerows(weekly)
        with (staging/'github_activity_partial_weeks.csv').open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(all_weeks[0]) if all_weeks else ['tool_id','week_start']);writer.writeheader();writer.writerows(partial_weeks)
        verified=diagnostics.pop('raw_requests_verified')
        quality={'source':SOURCE,'status':status,'registry_sha256':sha(args.registry),
            'history_start':state['start'],'history_end_exclusive':state['end_exclusive'],
            'observed_at':stamp(max([state.get('updated_at') or state.get('started_at'),*[r['observed_at'] for r in state['requests']]],key=utc)),
            'built_at':datetime.now(timezone.utc).isoformat(),
            'collection_complete':complete,'historical_visibility_complete':False,'primary_eligible':False,
            'weekly_rows':len(weekly),'coverage_complete_rows':sum(r['coverage_complete'] for r in weekly),
            'partial_calendar_week_rows':len(partial_weeks),
            'normalized_records':len(records),'verified_request_count':len(verified),'core_pr_quality':core,
            'state_snapshot_sha256':hashlib.sha256(state_bytes).hexdigest(),'diagnostics':diagnostics,
            'raw_file_hashes':{r['raw_path']:{'compressed_sha256':r['compressed_sha256'],'raw_sha256':r['raw_sha256']} for r in verified},
            'code_sha256':{Path(__file__).name:sha(Path(__file__)), **{name:sha(Path(__file__).with_name(name)) for name in ['search_plan.py','github_visible_lists.py','github_visible_cursor.py','github_issues_cursor.py','github_jsonp_lists.py','github_pr_history.py']}},
            **transport_summary(verified),
            'limits':['Search agreement describes retrieved index results only; demonstrated omissions prevent using it as a true-issue census.',
                      'Visible-list pagination is a changing retrospective crawl, not an atomic snapshot or proof of historical public availability.',
                      'Core PR-created counts and source-account counts use their independently verified PR-object creation cohort; completing a conflicting issue-list view does not erase them.',
                      'Independent complete core-PR and visible-list created-ID disagreement masks hybrid merge/closure and union-author features; all views and differences remain archived.',
                      'Issue-object and pull-request-object created_at can differ; both are retained, and PR-opened counts use direct PR-object timestamps.',
                      'Complete native all-history PR captures supply merge/last-reported-close counts independently of issue-wrapper profile disagreements; incomplete native extensions remain diagnostic.',
                      'Closure counts describe the last reported closed_at of the newest retained observation, not every close/reopen event.',
                      'Public account IDs identify accounts, not certified people; missing IDs leave exact author counts unknown.',
                      'Confirmed shared deleted-user placeholders remain unknown for exact creator counts; reported IDs are retained for audit.',
                      'Current records were retrospectively collected; all historical primary eligibility remains false.',
                      'The two-day feature availability lag is an assumption, not a historical publication receipt.',
                      'Ineligible or missing query pages remain unknown; observed_partial columns are diagnostics, not zero-filled observations.']}
        payload=json.dumps(quality,indent=2)+'\n'
        for name in ['github_activity_quality.json','source_quality.json']:(staging/name).write_text(payload)
        (staging/'collection_state.snapshot.json').write_bytes(state_bytes);(staging/'tools.study.json').write_bytes(args.registry.read_bytes())
        manifest={'source':SOURCE,'status':quality['status'],'registry_sha256':sha(args.registry),
            'history_start':state['start'],'history_end_exclusive':state['end_exclusive'],'observed_at':quality['observed_at'],
            'historical_visibility_complete':False,'primary_eligible':False,'collection_complete':complete,
            'files':{p.name:sha(p) for p in staging.iterdir() if p.is_file()}}
        (staging/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');staging.rename(args.output)
    except BaseException:shutil.rmtree(staging,ignore_errors=True);raise
    print(json.dumps({'status':quality['status'],'normalized_records':len(records),'weekly_rows':len(weekly),
        'complete_weekly_rows':quality['coverage_complete_rows'],'output':str(args.output)}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['source-root','state-snapshot','registry','output']:parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--max-body-mib',type=int,default=64);parser.add_argument('--max-records',type=int,default=1000000)
    args=parser.parse_args()
    if not 1<=args.max_body_mib<=128 or not 1<=args.max_records<=2000000:parser.error('Use bounded response and normalized-record limits')
    build(args)


if __name__=='__main__':main()
