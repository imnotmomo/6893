#!/usr/bin/env python3
"""Repository-wide release features; published_at is the event date.

GitHub created_at is the date of the commit used for a release, not its draft
or publication time. Current visible pages do not reconstruct deleted history.
"""
from datetime import date,datetime,timedelta,timezone
import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from urllib.parse import urlparse

SOURCE='github_repository_releases'
DOCS='https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release'
COUNTS=['github_repo_release_published_count','github_repo_release_prerelease_count','github_repo_release_stable_count']
UTC=timezone.utc
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text())
def stamp(value):
    result=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Release timestamps require a timezone')
    return result.astimezone(UTC)
def positive(value):return isinstance(value,int) and not isinstance(value,bool) and value>0


def decode(root,request):
    root=Path(root).resolve()
    path=(root/request['raw_path']).resolve()
    if not path.is_relative_to(root) or not path.is_file() or sha(path)!=request['compressed_sha256']:
        raise ValueError('Release compressed raw hash/path mismatch')
    raw=gzip.decompress(path.read_bytes())
    if len(raw)>32*1024**2 or hashlib.sha256(raw).hexdigest()!=request['raw_sha256']:
        raise ValueError('Release uncompressed raw hash/size mismatch')
    return None if request.get('phase')=='error' else json.loads(raw)


def inspect_tool(tool,requests,root,observed):
    tid=tool['tool_id'];errors=[];identities=[];pages={};anchors=[];objects=[]
    selected_attempt=max((item.get('attempt',1) for item in requests if item.get('tool_id')==tid),default=1)
    sequence={id(item):n for n,item in enumerate(requests)}
    for item in requests:
        if item.get('tool_id')!=tid:continue
        body=decode(root,item)
        if item.get('attempt',1)!=selected_attempt:continue
        if stamp(item['observed_at'])>observed:raise ValueError('Raw release response observed after build time')
        if item.get('phase')=='identity':identities.append(body);continue
        if item.get('phase') not in {'list','anchor'}:continue
        size=item.get('per_page');page=item.get('page')
        if not positive(size) or size>100 or not positive(page) or not isinstance(body,list) or len(body)>size:
            errors.append('invalid_page_shape');continue
        if item.get('http_status',200)!=200:errors.append('failed_page_status');continue
        if any(not isinstance(row,dict) or not positive(row.get('id')) for row in body):
            errors.append('invalid_release_identity');continue
        if item['phase']=='anchor':anchors.append((body,item));continue
        if page in pages:errors.append('duplicate_page_number')
        else:pages[page]=(body,item)
    names={tool['github_repo'].casefold(),*(value.casefold() for value in tool.get('github_repo_aliases',[]))}
    identity=bool(identities) and all(isinstance(v,dict) and v.get('id')==tool['github_repo_id']
        and str(v.get('full_name','')).casefold() in names for v in identities)
    sizes={entry[1]['per_page'] for entry in pages.values()}
    numbered=sorted(pages);terminal=[n for n,(rows,item) in pages.items() if len(rows)<item['per_page']]
    sequential=bool(numbered) and numbered==list(range(1,max(numbered)+1))
    terminal_valid=len(terminal)==1 and terminal[0]==max(numbered,default=0)
    head=[row['id'] for row in pages.get(1,([],{}))[0]]
    last_page_sequence=max((sequence[id(item)] for rows,item in pages.values()),default=-1)
    last_page_observed=max((stamp(item['observed_at']) for rows,item in pages.values()),default=observed)
    anchored=bool(anchors) and all(item['page']==1 and [r['id'] for r in rows]==head
        and item['per_page'] in sizes and sequence[id(item)]>last_page_sequence
        and stamp(item['observed_at'])>=last_page_observed for rows,item in anchors)
    ids=[]
    for page in numbered:
        rows,item=pages[page]
        for row in rows:objects.append((row,item));ids.append(row['id'])
    duplicates=len(ids)!=len(set(ids))
    if duplicates:errors.append('duplicate_release_ids_across_list_pages')
    retrieval=identity and len(sizes)==1 and sequential and terminal_valid and anchored and not errors
    normalized=[];known=[];time_complete=True;seen=set()
    for row,item in objects:
        if row['id'] in seen:continue
        seen.add(row['id'])
        url=urlparse(str(row.get('url','')))
        paths={'/repos/'+repo+'/releases/'+str(row['id']) for repo in [tool['github_repo'],*tool.get('github_repo_aliases',[])]}
        url_valid=url.scheme=='https' and url.netloc=='api.github.com' and url.path.casefold() in {p.casefold() for p in paths}
        draft=row.get('draft');prerelease=row.get('prerelease');published=None;date_error=None
        if not isinstance(draft,bool) or not isinstance(prerelease,bool) or not url_valid:
            date_error='invalid_release_flag_or_repository_url'
        elif not draft:
            try:
                published=stamp(row.get('published_at'))
                if published>stamp(item['observed_at']):raise ValueError('Publication timestamp after observation')
            except (ValueError,TypeError):date_error='missing_or_invalid_published_at';published=None
        if date_error:time_complete=False
        if published is not None and not date_error:known.append((published,prerelease))
        normalized.append({'tool_id':tid,'repository':tool['github_repo'],'repository_id':tool['github_repo_id'],
            'release_id':row['id'],'tag_name':row.get('tag_name'),'name':row.get('name'),
            'published_at':row.get('published_at'),'created_at':row.get('created_at'),
            'created_at_semantics':'Provider commit date, not release drafting or publication.',
            'event_time_field':'published_at','event_time_valid':published is not None,'date_error':date_error,
            'draft':draft,'prerelease':prerelease,'target_commitish':row.get('target_commitish'),
            'notes':row.get('body'),'assets':row.get('assets'),'author':row.get('author'),
            'raw_release':row,'metadata_observed_at':item['observed_at'],'raw_path':item['raw_path'],
            'selected_pypi_package':tool['pypi_package'],'pypi_release_correspondence_verified':False,
            'historical_primary_eligible':False})
    summary={'selected_attempt':selected_attempt,'identity_verified':identity,'pages':len(pages),'terminal_page':terminal[0] if terminal_valid else None,
        'sequential_pages':sequential,'head_anchor_ids_match':anchored,'duplicate_release_ids':duplicates,
        'retrieval_complete':bool(retrieval),'published_time_complete':time_complete,
        'features_complete':bool(retrieval and time_complete),'visible_release_objects':len(seen),
        'known_published_release_objects':len(known),'draft_objects':sum(v['draft'] is True for v in normalized),
        'errors':errors,'historical_visibility_complete':False,'historical_primary_eligible':False,
        'pypi_release_correspondence_verified':False}
    return normalized,known,summary,bool(identity and pages)


def weekly_rows(tid,known,quality,observed_any,start,end,partial=False):
    first=start-timedelta(days=start.weekday());rows=[];cursor=first
    while cursor<end:
        right=cursor+timedelta(days=7);left=max(cursor,start);effective_end=min(right,end)
        full=left==cursor and effective_end==right
        if full==partial:cursor=right;continue
        events=[prerelease for published,prerelease in known if left<=published.date()<effective_end]
        values=[len(events),sum(events),len(events)-sum(events)]
        row={'tool_id':tid,'week_start':cursor.isoformat(),'event_window_end_exclusive':right.isoformat(),
             'feature_cutoff':(cursor+timedelta(days=9)).isoformat(),'effective_window_start':left.isoformat(),
             'effective_window_end_exclusive':effective_end.isoformat(),'week_complete':full,
             'retrieval_complete':quality['retrieval_complete'],'published_time_complete':quality['published_time_complete'],
             'historical_primary_eligible':False,'pypi_release_correspondence_verified':False,
             'measurement':'Currently visible repository releases grouped by published_at; not selected-distribution releases.'}
        for name,count in zip(COUNTS,values):
            row[name]=count if quality['features_complete'] else None
            row[name+'_complete']=quality['features_complete']
            row['observed_partial_'+name]=count if observed_any else None
        rows.append(row);cursor=right
    return rows


def write_csv(path,rows):
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]) if rows else ['tool_id','week_start'])
        writer.writeheader();writer.writerows(rows)


def build(source_root,registry,output):
    source_root=Path(source_root).resolve();registry=Path(registry);output=Path(output)
    if output.exists():raise FileExistsError('Use a fresh immutable release-feature output')
    source=read(source_root/'collection_manifest.json');source_sha=sha(source_root/'collection_manifest.json')
    cohort=sha(registry);tools=read(registry)['tools'];ids={t['tool_id'] for t in tools}
    if len(ids)!=30 or len(tools)!=30 or source.get('source')!=SOURCE or source.get('registry_sha256')!=cohort:
        raise ValueError('Release source does not use the frozen thirty-tool cohort')
    if sha(source_root/'tools.study.json')!=cohort:raise ValueError('Release source registry copy changed')
    start=date.fromisoformat(source['history_start']);end=date.fromisoformat(source['history_end_exclusive']);observed=datetime.now(UTC)
    if not start<end<=min(observed.date(),date(2027,1,1)):raise ValueError('Invalid completed release history endpoint')
    normalized=[];weekly=[];edge=[];quality={}
    for tool in tools:
        records,known,summary,observed_any=inspect_tool(tool,source['requests'],source_root,observed)
        normalized.extend(records);quality[tool['tool_id']]=summary
        weekly.extend(weekly_rows(tool['tool_id'],known,summary,observed_any,start,end))
        edge.extend(weekly_rows(tool['tool_id'],known,summary,observed_any,start,end,partial=True))
    if sha(source_root/'collection_manifest.json')!=source_sha:raise ValueError('Release source checkpoint changed while building')
    output.parent.mkdir(parents=True,exist_ok=True);temporary=Path(tempfile.mkdtemp(prefix='.github-releases-',dir=output.parent))
    try:
        with (temporary/'github_releases.jsonl.gz').open('wb') as raw:
            with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as stream:
                for item in normalized:stream.write((json.dumps(item,sort_keys=True)+'\n').encode())
        write_csv(temporary/'github_releases_weekly.csv',weekly);write_csv(temporary/'github_releases_partial_weeks.csv',edge)
        (temporary/'tools.study.json').write_bytes(registry.read_bytes())
        (temporary/'source_collection_manifest.json').write_bytes((source_root/'collection_manifest.json').read_bytes())
        complete=sum(v['features_complete'] for v in quality.values());status='complete' if complete==len(ids) else 'partial'
        report={'source':SOURCE,'status':status,'registry_sha256':cohort,'history_start':str(start),
            'history_end_exclusive':str(end),'observed_at':observed.isoformat(),'source_manifest_sha256':source_sha,
            'tools_requested':len(ids),'tools_feature_complete':complete,'tools':quality,'weekly_rows':len(weekly),
            'partial_week_rows':len(edge),'normalized_release_objects':len(normalized),
            'event_time_field':'published_at','created_at_semantics_documentation':DOCS,
            'historical_visibility_complete':False,'historical_primary_eligible':False,
            'pypi_release_correspondence_verified':False,'builder_sha256':sha(Path(__file__)),
            'limits':['Counts describe currently visible repository releases, not deleted-history or point-in-time completeness.',
                      'A stable first-page identity anchor does not make a paginated collection an atomic snapshot.',
                      'created_at is a provider commit date, not release publication; only published_at enters weekly counts.',
                      'Repository tags/releases may cover multiple packages and are not certified releases of the selected PyPI distribution.',
                      'Embedded asset download counts and mutable notes are current observations, not historical adoption features.']}
        (temporary/'github_release_quality.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
        manifest={'source':SOURCE,'status':status,'registry_sha256':cohort,'history_start':str(start),
            'history_end_exclusive':str(end),'observed_at':observed.isoformat(),'historical_primary_eligible':False,
            'files':{path.name:sha(path) for path in temporary.iterdir() if path.is_file()}}
        (temporary/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n');temporary.rename(output)
    except BaseException:shutil.rmtree(temporary,ignore_errors=True);raise
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--source-root',type=Path,required=True)
    parser.add_argument('--registry',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();report=build(args.source_root,args.registry,args.output)
    print(json.dumps({key:report[key] for key in ['status','tools_requested','tools_feature_complete','weekly_rows','normalized_release_objects']},indent=2))


if __name__=='__main__':main()
