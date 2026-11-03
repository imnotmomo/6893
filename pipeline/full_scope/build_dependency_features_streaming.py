"""Stream full immutable metadata history through the reference event-time sweep.

DuckDB externally sorts decoded source records by canonical distribution. Only
one distribution's artifacts and parsed declarations occupy Python memory; its
weekly contributions are merged into a small frozen-cohort grid. This makes the
multi-million-row export usable without loading the whole history into RAM.
"""
import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack
from datetime import datetime, timezone
from functools import lru_cache
import gzip
import importlib.metadata
import itertools
import json
import os
from pathlib import Path
import shutil
import tempfile

import build_dependency_features as reference
from build_download_dimension_features import DiskGuard, sql_string
from dependency_sources import FIELDS, SOURCE, canonical, targets, utc


INBOUND=['dep_ever_declaring_packages','dep_new_declaring_packages','dep_declaring_release_count',
         'dep_latest_uploaded_release_declaring_packages','dep_unknown_target_artifacts']
OUTBOUND=['dep_outbound_unique_dependencies','dep_outbound_unconditional_dependencies',
          'dep_outbound_conditional_dependencies','dep_outbound_requirement_variants',
          'dep_latest_uploaded_target_version','dep_outbound_event_time_eligible']


def reconstruct_requirements(artifact,edges,errors):
    """Losslessly rebuild ordered derived requirements from relational exports."""
    lookup={};raw=artifact['requires_dist']
    def add(row,removed):
        if row.get('artifact_id')!=artifact['artifact_id']:return
        index=row.get('requirement_index')
        if isinstance(index,bool) or not isinstance(index,int) or not 0<=index<len(raw):raise ValueError('Invalid requirement index')
        if index in lookup:raise ValueError('Duplicate requirement index')
        value={key:value for key,value in row.items() if key not in removed}
        if value.get('raw_requirement')!=raw[index]:raise ValueError('Requirement text differs from artifact metadata')
        lookup[index]=value
    for row in edges:add(row,{'artifact_id','source_package','source_version','provider_upload_time','observed_at','requirement_index'})
    for row in errors:
        if row.get('kind')=='invalid_requirement':add(row,{'kind','package','version','artifact_id','requirement_index'})
    if set(lookup)!=set(range(len(raw))):raise ValueError('Parsed requirement export is incomplete')
    return [lookup[index] for index in range(len(raw))]


def merge_contributions(result, contributions, source_package):
    for row in contributions:
        target=result[(row['tool_id'],row['week_start'])]
        for key in INBOUND:
            if target[key] is not None and row[key] is not None:target[key]+=row[key]
        target['dep_inbound_event_time_eligible'] &= row['dep_inbound_event_time_eligible']
        if row['package']==source_package:
            for key in OUTBOUND:target[key]=row[key]


def finish_momentum(rows):
    previous=defaultdict(list)
    for row in rows:
        history=previous[row['tool_id']]
        for name in ['dep_new_declaring_packages','dep_declaring_release_count']:
            values=[v[name] for v in history[-3:]]+[row[name]]
            row[name+'_4w']=sum(values) if len(values)==4 and all(v is not None for v in values) else None
        for name in ['dep_ever_declaring_packages','dep_latest_uploaded_release_declaring_packages']:
            row[name+'_change_4w']=(row[name]-history[-4][name] if len(history)>=4 and row[name] is not None and history[-4][name] is not None else None)
        history.append(row)
    return rows


def relevant_registry(artifacts, registry):
    mapping=targets(registry);relevant={a['package'] for a in artifacts if a['package'] in mapping}
    for artifact in artifacts:
        for req in artifact['parsed_requirements']:
            dependency=req.get('dependency_package') or req.get('name_candidate')
            if dependency in mapping:relevant.add(dependency)
            elif dependency is None and not req['valid']:relevant.update(mapping)
    return {'tools':[tool for tool in registry['tools'] if canonical(tool['pypi_package']) in relevant]}


def compact_for_features(artifacts,registry):
    """Keep the complete export while sweeping only counters used by this cohort."""
    mapping=targets(registry);output=[]
    keys=['package','version','provider_upload_time','artifact_id','identity_valid','event_time_available']
    for artifact in artifacts:
        requirements=artifact['parsed_requirements']
        if artifact['package'] not in mapping:
            requirements=[req for req in requirements if req.get('dependency_package') in mapping
                or not req['valid'] and (req.get('name_candidate') is None or req.get('name_candidate') in mapping)]
        output.append({**{key:artifact[key] for key in keys},'parsed_requirements':requirements})
    return output


def grouped_features(groups, registry, observed, start, end, complete, artifacts_sink, edges_sink, errors_sink, guard=None, progress=None):
    base=reference.weekly_features([],registry,start,end,complete)
    result={(row['tool_id'],row['week_start']):row for row in base}
    totals=Counter();errors=Counter();samples=[];present=set()
    original_requirement=reference.requirement
    cached=lru_cache(maxsize=32768)(original_requirement)
    reference.requirement=lambda value:cached(value) if isinstance(value,str) else original_requirement(value)
    try:
        for package,rows in groups:
            if guard:guard.check()
            if progress:progress({**dict(totals),'active_package':package,'phase':'normalizing_group'},dict(errors))
            artifacts,edges,audit=reference.normalize_artifacts(rows,observed,end,edge_sink=edges_sink)
            totals['package_groups']+=1;totals['input_rows']+=audit['input_rows']
            totals['unique_artifacts']+=audit['unique_artifacts'];totals['parsed_edges']+=audit['parsed_edges']
            totals['exact_duplicate_rows_removed']+=audit['exact_duplicate_rows_removed']
            totals['largest_package_rows']=max(totals['largest_package_rows'],len(rows))
            present.update(a['package'] for a in artifacts if a['identity_valid'])
            for artifact in artifacts:
                # Derived attributes already live losslessly in the edge/error
                # tables. Avoid repeatedly serializing them inside each file.
                artifacts_sink({key:value for key,value in artifact.items() if key!='parsed_requirements'})
            for error in audit['errors']:
                errors_sink(error);errors[error['kind']]+=1
                if len(samples)<100:samples.append(error)
            feature_artifacts=compact_for_features(artifacts,registry)
            subset=relevant_registry(feature_artifacts,registry);contribution=None
            if subset['tools']:
                contribution=reference.weekly_features(feature_artifacts,subset,start,end,complete)
                merge_contributions(result,contribution,package)
            if progress:progress(dict(totals),dict(errors))
            # Release the previous group's large raw/parsed objects before the
            # sorted input generator allocates the following package.
            del artifacts,edges,feature_artifacts,rows,subset,contribution
    finally:reference.requirement=original_requirement
    missing=sorted(set(targets(registry))-present)
    if missing:
        error={'kind':'missing_target_artifacts','packages':missing};errors_sink(error);errors[error['kind']]+=1
        if len(samples)<100:samples.append(error)
    return finish_momentum(base),{**dict(totals),'error_counts':dict(errors),'error_count':sum(errors.values()),
                                 'errors':samples,'errors_sample_limit':100,'all_errors_file':'parse_errors.jsonl.gz'}


def sorted_groups(connection, input_path, max_package_rows):
    arrays={'requires_dist','classifiers','project_urls'}
    columns={name:'VARCHAR[]' if name in arrays else 'BIGINT' if name=='size' else 'VARCHAR' for name in FIELDS}
    fields='{'+','.join(sql_string(k)+':'+sql_string(v) for k,v in columns.items())+'}'
    query=("SELECT regexp_replace(lower(trim(name)),'[-_.]+','-','g') AS package_group,to_json(source) AS payload "
           'FROM read_json('+sql_string(input_path)+",format='newline_delimited',compression='gzip',columns="+fields+") source ORDER BY package_group NULLS FIRST")
    cursor=connection.execute(query)
    def records():
        while True:
            batch=cursor.fetchmany(256)
            if not batch:break
            for key,payload in batch:yield key,json.loads(payload)
    for key,group in itertools.groupby(records(),key=lambda item:item[0]):
        rows=[]
        for _,row in group:
            if len(rows)>=max_package_rows:
                raise ValueError('One distribution exceeds the configured per-package memory bound: '+str(key))
            rows.append(row)
        yield key,rows


def jsonl_writer(stack,path):
    raw=stack.enter_context(Path(path).open('xb'))
    stream=stack.enter_context(gzip.GzipFile(fileobj=raw,mode='wb',mtime=0,compresslevel=3))
    def append(value):stream.write((reference.encode(value)+'\n').encode())
    return append


def build(args):
    import duckdb
    if args.output.exists():raise FileExistsError('Use a fresh metadata feature directory')
    registry=json.loads(args.registry.read_text());mapping=targets(registry);plan=json.loads(args.query_plan.read_text())
    receipt=json.loads(args.query_receipt.read_text())
    fingerprint=__import__('hashlib').sha256(json.dumps({'query':plan['query'],'parameters':plan['query_parameters']},sort_keys=True).encode()).hexdigest()
    if (plan.get('source')!=SOURCE or plan.get('end_exclusive')!=args.end_exclusive
            or plan.get('target_package_to_tool')!=mapping or plan.get('query_sha256')!=fingerprint):
        raise ValueError('Metadata query plan/cohort/endpoint differs')
    if (receipt.get('query_sha256')!=fingerprint or receipt.get('job_complete') is not True
            or receipt.get('all_pages_fetched') is not True or not isinstance(receipt.get('total_rows'),int)
            or isinstance(receipt.get('total_rows'),bool) or receipt['total_rows']<0
            or receipt.get('output_sha256')!=reference.sha(args.input)):
        raise ValueError('Metadata requires a complete, hash-bound source receipt')
    observed=args.observed_at or receipt.get('observed_at')
    if utc(observed) is None:raise ValueError('A source collection observation is required')
    complete=plan.get('mode')=='candidate_history'
    args.output.parent.mkdir(parents=True,exist_ok=True)
    staging=Path(tempfile.mkdtemp(prefix='.dependency-features-',dir=args.output.parent))
    guard=DiskGuard(staging,args.minimum_free_gib);connection=duckdb.connect();guard.connection=connection
    try:
        connection.execute('SET memory_limit='+sql_string(args.memory_limit));connection.execute('SET threads=1')
        connection.execute('SET preserve_insertion_order=false')
        connection.execute('SET temp_directory='+sql_string(staging/'spill'))
        connection.execute('SET max_temp_directory_size='+sql_string(str(args.max_temp_gib)+'GiB'))
        def progress(totals,errors):
            if 'active_package' in totals or totals.get('package_groups',0)%100==0:
                temporary=staging/'.build_progress.json.tmp'
                temporary.write_text(json.dumps({'totals':totals,'error_counts':errors,'updated_at':datetime.now(timezone.utc).isoformat()})+'\n')
                os.replace(temporary,staging/'build_progress.json')
        with ExitStack() as stack:
            artifacts_sink=jsonl_writer(stack,staging/'artifacts.jsonl.gz')
            edges_sink=jsonl_writer(stack,staging/'declared_dependency_edges.jsonl.gz')
            errors_sink=jsonl_writer(stack,staging/'parse_errors.jsonl.gz')
            weekly,audit=grouped_features(sorted_groups(connection,args.input,args.max_package_rows),registry,
                observed,args.start,args.end_exclusive,complete,artifacts_sink,edges_sink,errors_sink,guard,progress)
        if audit['input_rows']!=receipt['total_rows']:raise ValueError('Decoded metadata rows differ from completion receipt')
        connection.close();guard.connection=None
        import pandas as pd
        frame=pd.DataFrame(weekly);frame.to_csv(staging/'weekly_dependency_features.csv',index=False)
        frame.to_parquet(staging/'weekly_dependency_features.parquet',index=False)
        quality={'source':SOURCE,'format_version':2,
            'artifact_layout':'Original provider fields plus identity/event/provenance; ordered derived requirements join edges/errors on artifact_id and requirement_index.',
            'status':'passed' if not audit['error_count'] and complete else 'needs_review',
            'observed_at':utc(observed).isoformat(),'history_start':args.start,'history_end_exclusive':args.end_exclusive,
            'registry_sha256':reference.sha(args.registry),'input_metadata_sha256':reference.sha(args.input),
            'query_plan_sha256':reference.sha(args.query_plan),'query_receipt_sha256':reference.sha(args.query_receipt),
            'query_sha256':fingerprint,'inbound_history_complete':complete,'weekly_rows':len(weekly),'audit':audit,
            'historical_retrieval_vintage':False,'packaging_version':importlib.metadata.version('packaging'),
            'code_sha256':{Path(path).name:reference.sha(path) for path in [Path(__file__),Path(reference.__file__)]},
            'streaming':{'engine':'DuckDB external sort plus exact per-package reference event-time sweep',
                         'memory_limit':args.memory_limit,'threads':1,'preserve_insertion_order':False,
                         'output_gzip_compresslevel':3,'parsed_edges_streamed':True,
                         'max_temp_gib':args.max_temp_gib,'max_package_rows':args.max_package_rows},
            'limitations':['Dependency declarations count published requirements, not installed users or resolved historical environments.',
                           'All file variants are retained; a later uploaded file cannot backdate its declarations.',
                           'New releases that remove a target requirement are retained in the latest-uploaded-release proxy.',
                           'Undated/malformed records remain in output and disable affected feature eligibility.',
                           'Upload-event time is retrospective provider metadata; historical retrieval vintages are unavailable.']}
        (staging/'dependency_quality.json').write_text(json.dumps(quality,indent=2)+'\n')
        for src,name in [(args.registry,'tools.study.json'),(args.query_plan,'query_plan.json'),(args.query_receipt,'query_receipt.json')]:
            (staging/name).write_bytes(src.read_bytes())
        manifest={'status':quality['status'],'source':SOURCE,'format_version':2,'registry_sha256':reference.sha(args.registry),
            'history_end_exclusive':args.end_exclusive,'observed_at':quality['observed_at'],
            'files':{p.name:reference.sha(p) for p in staging.iterdir() if p.is_file()}}
        (staging/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        shutil.rmtree(staging/'spill',ignore_errors=True);guard.check();guard.close();staging.rename(args.output)
    except BaseException:
        guard.connection=None;connection.close();guard.close();shutil.rmtree(staging,ignore_errors=True);raise
    print(json.dumps({'status':quality['status'],'artifacts':audit['unique_artifacts'],'edges':audit['parsed_edges'],
                      'weekly_rows':len(weekly),'errors':audit['error_count'],'output':str(args.output)}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['input','registry','query-plan','query-receipt','output']:parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--start',default='2023-01-01');parser.add_argument('--end-exclusive',required=True)
    parser.add_argument('--observed-at');parser.add_argument('--memory-limit',default='3GB')
    parser.add_argument('--max-temp-gib',type=int,default=10);parser.add_argument('--minimum-free-gib',type=float,default=15)
    parser.add_argument('--max-package-rows',type=int,default=100000)
    args=parser.parse_args()
    if args.minimum_free_gib<15 or not 1<=args.max_temp_gib<=20 or not 1<=args.max_package_rows<=200000:
        parser.error('Keep 15 GiB free, spill within 1–20 GiB, and at most 200,000 artifacts per package')
    build(args)


if __name__=='__main__':main()
