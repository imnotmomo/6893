"""Validate retained rich download queries and build version/environment features.

This reads completed query exports only. It never queries BigQuery, changes the
frozen primary dataset, or promotes a model. DuckDB streams the large rich input;
the pandas backend is a bounded fixture/pilot reference implementation.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import platform
import shutil
import sys
import tempfile
import threading

import pandas as pd
from download_split_coverage import SUPERSEDED, classify_split_parents


SOURCE = 'pypi_download_dimensions'
COUNTS = ['logged_objects', 'downloads', 'excluded_requests']
DIMENSIONS = {'package_version':'version', 'python_major_minor':'python',
              'python_implementation':'implementation', 'operating_system':'os',
              'installer':'installer', 'country_code':'country', 'artifact_type':'artifact',
              'ci_report':'ci', 'http_status':'http'}
TEXT = [name for name in DIMENSIONS if name != 'http_status']
KEY = ['tool_id','date', *DIMENSIONS]
RAW_FIELDS = ['package','date', *DIMENSIONS, *COUNTS]
UNKNOWN = None


def progress(phase, event, query_index=0, total=None):
    print(json.dumps({'event':'full_scope_materialization_progress','phase':phase,'state':event,
        'query_index':query_index,'total':total,'utc':datetime.now(timezone.utc).isoformat()}),file=sys.stderr,flush=True)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''): digest.update(block)
    return digest.hexdigest()


def canonical(name):
    import re
    value = re.sub(r'[-_.]+', '-', str(name).strip().lower())
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', value): raise ValueError('Invalid distribution name')
    return value


def read(path):
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict): raise ValueError('Expected JSON object: ' + str(path))
    return value


def utc(value):
    result = pd.Timestamp(value)
    if pd.isna(result): raise ValueError('Missing timestamp')
    return result.tz_localize('UTC') if result.tzinfo is None else result.tz_convert('UTC')


def days(start, end):
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    return [start + timedelta(days=n) for n in range((end-start).days)]


def registry_mapping(registry):
    tools = registry['tools']
    mapping = {canonical(t['pypi_package']):t['tool_id'] for t in tools}
    if not mapping or len(mapping) != len(tools) or len(set(mapping.values())) != len(tools):
        raise ValueError('Duplicate or missing canonical tool mapping')
    return mapping


def validate_plan_parameters(plan, mapping):
    parameters=plan['query_parameters']
    by_name={p['name']:p for p in parameters}
    if len(by_name)!=len(parameters) or set(by_name)!={'start_date','end_date','packages'}:
        raise ValueError('Rich source query has an unexpected parameter contract')
    for name,value in [('start_date',plan['start']),('end_date',plan['end_exclusive'])]:
        parameter=by_name[name]
        if parameter.get('parameterType')!={'type':'DATE'} or parameter.get('parameterValue')!={'value':value}:
            raise ValueError('Query date parameters differ from the declared frozen interval')
    packages=by_name['packages']
    if packages.get('parameterType')!={'type':'ARRAY','arrayType':{'type':'STRING'}}:
        raise ValueError('Unexpected package parameter type')
    values=packages.get('parameterValue',{}).get('arrayValues',[])
    names=[canonical(v['value']) for v in values]
    if len(names)!=len(set(names)) or set(names)!=set(mapping):
        raise ValueError('Query package parameters differ from the frozen cohort')


def collect_inputs(root, mapping, start, end):
    """Choose entire latest successful date snapshots, including empty results."""
    root = Path(root).resolve(); wanted = set(days(start,end)); queries, pending = [], []
    for path in sorted(root.rglob('query_plan.json')):
        plan = read(path)
        if plan.get('source') != SOURCE: continue
        left, right = date.fromisoformat(plan['start']), date.fromisoformat(plan['end_exclusive'])
        if not left < right or not wanted.intersection(days(left,right)): continue
        actual = {canonical(k):v for k,v in plan.get('package_to_tool',{}).items()}
        if actual != mapping: raise ValueError('Rich query cohort differs from frozen registry: '+str(path))
        validate_plan_parameters(plan,mapping)
        fingerprint = hashlib.sha256(json.dumps({'query':plan['query'],'parameters':plan['query_parameters']},sort_keys=True).encode()).hexdigest()
        if fingerprint != plan.get('query_sha256'): raise ValueError('Rich query plan fingerprint changed')
        folder = path.parent; receipt_path = folder/'completion_receipt.json'; records = folder/'decoded_records.jsonl.gz'
        if not receipt_path.exists():
            pending.append({'path':str(folder),'start':str(left),'end_exclusive':str(right),
                            'query_sha256':fingerprint,'state':'query_not_complete'})
            continue
        receipt = read(receipt_path)
        if (receipt.get('query_sha256') != fingerprint or receipt.get('job_complete') is not True
                or receipt.get('all_pages_fetched') is not True
                or not isinstance(receipt.get('total_rows'),int) or isinstance(receipt.get('total_rows'),bool)
                or receipt['total_rows'] < 0 or not records.is_file()
                or sha(records) != receipt.get('output_sha256')):
            raise ValueError('Incomplete or changed rich query receipt: '+str(folder))
        observed = utc(receipt.get('observed_at'))
        if (folder/'manifest.json').exists():
            manifest = read(folder/'manifest.json')
            if (manifest.get('status') != 'complete' or manifest.get('query_sha256') != fingerprint
                    or manifest.get('rows_fetched') != receipt['total_rows']
                    or manifest.get('total_rows') != receipt['total_rows']
                    or manifest.get('job_complete') is not True or manifest.get('all_pages_fetched') is not True):
                raise ValueError('Rich source manifest is not complete and consistent')
        queries.append({'path':str(folder),'input_path':str(records.resolve()),'start':str(left),'end_exclusive':str(right),
                        'query_sha256':fingerprint,'receipt_sha256':sha(receipt_path),'input_sha256':receipt['output_sha256'],
                        'total_rows':receipt['total_rows'],'observed_at':observed.isoformat(),
                        'cloud_job_id':receipt.get('cloud_job_id')})
    winners = {}
    for query in queries:
        rank = (utc(query['observed_at']), query['query_sha256'],query['path'])
        for day in wanted.intersection(days(query['start'],query['end_exclusive'])):
            if day not in winners or rank > winners[day][0]: winners[day] = (rank,query['input_path'])
    winning_inputs = {day:item[1] for day,item in winners.items()}
    return queries, winning_inputs, classify_split_parents(pending, queries, winning_inputs)


def validate_record(row, mapping, left, right):
    if not isinstance(row,dict) or not set(RAW_FIELDS)<=set(row): raise ValueError('Rich download row has missing fields')
    if not isinstance(row['date'],str) or len(row['date']) != 10:
        raise ValueError('Rich dates must be ISO UTC dates')
    day = date.fromisoformat(row['date'])
    if not date.fromisoformat(left) <= day < date.fromisoformat(right): raise ValueError('Row outside frozen query window')
    package = canonical(row['package'])
    if package not in mapping: raise ValueError('Unmapped package in rich query')
    for column in COUNTS:
        value = row[column]
        if isinstance(value,bool) or not isinstance(value,int) or value<0: raise ValueError('Counts must be nonnegative integers')
    if row['downloads']+row['excluded_requests'] != row['logged_objects']:
        raise ValueError('Artifact/excluded/logged count arithmetic differs')
    if row['ci_report'] not in {'reported_true','reported_false','unknown'}: raise ValueError('Unknown reported-CI category')
    for column in TEXT:
        if row[column] is not None and not isinstance(row[column],str): raise ValueError('Categorical value must be text or unknown')
    if row['http_status'] is not None and (isinstance(row['http_status'],bool) or not isinstance(row['http_status'],int)):
        raise ValueError('HTTP status must be integer or unknown')
    return {**row,'package':package,'tool_id':mapping[package]}


def dimension_value(dimension, value):
    if value is None or pd.isna(value): return None
    if dimension=='http_status': return str(int(value))
    value = str(value).strip()
    return None if not value or dimension == 'ci_report' and value == 'unknown' else value


def pandas_inputs(queries, winners, mapping, max_rows):
    if sum(v['total_rows'] for v in queries)>max_rows:
        raise ValueError('Pandas input bound exceeded; use the streaming DuckDB backend')
    selected=[]; validated=0
    for query in queries:
        unique=set();count=0
        with gzip.open(query['input_path'],'rt') as stream:
            for line in stream:
                if not line.strip(): continue
                row=validate_record(json.loads(line),mapping,query['start'],query['end_exclusive']);count+=1
                key=tuple(row.get(k) for k in KEY)
                if key in unique: raise ValueError('Duplicate source dimension key')
                unique.add(key)
                if winners.get(date.fromisoformat(row['date']))==query['input_path']: selected.append(row)
        if count!=query['total_rows']: raise ValueError('Decoded row count differs from completion receipt')
        validated+=count
    return pd.DataFrame(selected,columns=['tool_id',*RAW_FIELDS]),validated


def aggregate_daily(frame):
    if frame.empty: return pd.DataFrame(columns=['tool_id','date',*COUNTS,'dimension_rows'])
    groups=frame.groupby(['tool_id','date'],dropna=False)
    output=groups[COUNTS].sum().reset_index();output['dimension_rows']=groups.size().to_numpy()
    return output


def daily_grid(aggregate, mapping, winners, first_artifact, reference, start, end):
    lookup={(r.tool_id,str(r.date)):r for r in aggregate.itertuples(index=False)}
    positive={str(day) for day,g in aggregate.groupby('date') if g.downloads.sum()>0} if not aggregate.empty else set()
    output=[]
    for day in days(start,end):
        for tool in sorted(mapping.values()):
            item=lookup.get((tool,str(day)));pre=day<first_artifact[tool]
            retrieved=day in winners;source_observed=str(day) in positive
            raw={key:int(getattr(item,key)) if item is not None else 0 if retrieved and source_observed else None for key in COUNTS}
            valid=bool(retrieved and source_observed and not pre)
            downloads=raw['downloads'] if valid else None
            frozen=reference.get((tool,str(day)))
            if frozen is not None and pd.isna(frozen): frozen=None
            delta=downloads-frozen if downloads is not None and frozen is not None else None
            status=('pre_visible_artifact' if pre else 'query_not_complete' if not retrieved else
                    'cohort_day_not_observed' if not source_observed else 'retained_dimension_rows' if item is not None else 'captured_zero_after_completed_query')
            output.append({'tool_id':tool,'date':str(day),'downloads':downloads,**{'raw_'+k:v for k,v in raw.items()},
                           'dimension_rows':int(getattr(item,'dimension_rows',0)),'query_retrieved':retrieved,
                           'cohort_day_observed':source_observed,'pre_visible_artifact':pre,'observation_status':status,
                           'frozen_primary_downloads':frozen,'difference_from_frozen_primary':delta,
                           'frozen_reference_found':(tool,str(day)) in reference})
    return pd.DataFrame(output)


def pandas_compositions(frame, grid):
    valid={(r.tool_id,r.date) for r in grid.itertuples() if not pd.isna(r.downloads)}
    totals=Counter()
    for row in frame.to_dict('records'):
        if (row['tool_id'],row['date']) not in valid or row['downloads']==0: continue
        day=date.fromisoformat(row['date']);week=str(day-timedelta(days=day.weekday()))
        for dimension in DIMENSIONS:
            totals[(row['tool_id'],week,dimension,dimension_value(dimension,row[dimension]))]+=int(row['downloads'])
    records=[{'tool_id':tool,'week_start':week,'dimension':dimension,'dimension_value':value,
              'dimension_unknown':value is None,'downloads':count}
             for (tool,week,dimension,value),count in totals.items()]
    return pd.DataFrame(records,columns=['tool_id','week_start','dimension','dimension_value','dimension_unknown','downloads'])


def composition_summary(frame):
    output=[]
    for (tool,week,dimension),group in frame.groupby(['tool_id','week_start','dimension'],dropna=False):
        total=int(group.downloads.sum());known=group[~group.dimension_unknown]
        top=known.sort_values(['downloads','dimension_value'],ascending=[False,True]).iloc[0] if not known.empty else None
        unknown=int(group.loc[group.dimension_unknown,'downloads'].sum())
        entropy=-sum((v/total)*math.log(v/total) for v in group.downloads if v>0) if total else None
        output.append({'tool_id':tool,'week_start':str(week),'dimension':dimension,'total_downloads':total,
                       'distinct_known':len(known),'top_known_value':top.dimension_value if top is not None else None,
                       'top_known_share':int(top.downloads)/total if top is not None and total else None,
                       'unknown_share':unknown/total if total else None,'entropy':entropy,
                       'reported_true_share':int(group.loc[group.dimension_value.eq('reported_true'),'downloads'].sum())/total if total else None,
                       'reported_false_share':int(group.loc[group.dimension_value.eq('reported_false'),'downloads'].sum())/total if total else None})
    return output


def week_features(grid, summaries, start, end):
    summary={(r['tool_id'],str(r['week_start']),r['dimension']):r for r in summaries}
    frame=grid.copy();frame['week_start']=pd.to_datetime(frame.date).dt.to_period('W-SUN').dt.start_time.dt.strftime('%Y-%m-%d')
    output=[]
    for (tool,week),group in frame.groupby(['tool_id','week_start']):
        beginning=date.fromisoformat(week)
        if beginning<date.fromisoformat(start) or beginning+timedelta(days=7)>date.fromisoformat(end):continue
        complete=len(group)==7 and group.date.nunique()==7 and group.downloads.notna().all()
        total=int(group.downloads.sum()) if complete else None
        frozen=int(group.frozen_primary_downloads.sum()) if len(group)==7 and group.frozen_primary_downloads.notna().all() else None
        row={'tool_id':tool,'week_start':week,'feature_cutoff':str(beginning+timedelta(days=9)),
             'rich_dl_downloads':total,'frozen_primary_downloads':frozen,
             'difference_from_frozen_primary':total-frozen if total is not None and frozen is not None else None,
             'download_coverage_days':int(group.downloads.notna().sum()),'query_coverage_days':int(group.query_retrieved.sum()),
             'rich_week_complete':bool(complete),'availability_lag_days':2,'availability_lag_is_assumption':True,
             'primary_model_promoted':False}
        for dimension,prefix in DIMENSIONS.items():
            entry=summary.get((tool,week,dimension))
            for field in ['distinct_known','top_known_value','top_known_share','unknown_share','entropy']:
                row['dl_'+prefix+'_'+field]=entry.get(field) if complete and entry else (0 if complete and total==0 and field=='distinct_known' else None)
            if dimension=='ci_report':
                row['dl_ci_reported_true_share']=entry.get('reported_true_share') if complete and entry else None
                row['dl_ci_reported_false_share']=entry.get('reported_false_share') if complete and entry else None
                row['dl_ci_reported_unknown_share']=entry.get('unknown_share') if complete and entry else None
        logged=int(group.raw_logged_objects.sum()) if complete else None
        excluded=int(group.raw_excluded_requests.sum()) if complete else None
        row['rich_logged_objects']=logged;row['rich_excluded_requests']=excluded
        row['dl_excluded_request_share']=excluded/logged if logged else None
        # Release-age features are unknown until verified artifact dates exist.
        row.update(dl_release_date_known_share=None,dl_recent_version_28d_share_lower_bound=None,
                   dl_release_after_download_requests=None,release_dates_available=False)
        output.append(row)
    return pd.DataFrame(output)


def add_release_features(weekly, frame, grid, release_dates):
    valid={(r.tool_id,r.date) for r in grid.itertuples() if not pd.isna(r.downloads)}
    mapping={r.tool_id:r.package for r in frame[['tool_id','package']].drop_duplicates().itertuples()}
    counts=defaultdict(Counter)
    for row in frame.to_dict('records'):
        if (row['tool_id'],row['date']) not in valid:continue
        day=date.fromisoformat(row['date']);week=str(day-timedelta(days=day.weekday()));key=(row['tool_id'],week)
        published=release_dates.get((row['package'],row['package_version']))
        counts[key]['total']+=int(row['downloads'])
        if published is None:continue
        age=(day-published).days
        if age<0:counts[key]['after']+=int(row['downloads']);continue
        counts[key]['known']+=int(row['downloads'])
        if age<28:counts[key]['recent']+=int(row['downloads'])
    for index,row in weekly.iterrows():
        if not row.rich_week_complete:continue
        item=counts[(row.tool_id,row.week_start)];total=item['total']
        weekly.loc[index,'release_dates_available']=True
        weekly.loc[index,'dl_release_date_known_share']=item['known']/total if total else None
        weekly.loc[index,'dl_recent_version_28d_share_lower_bound']=item['recent']/total if total else None
        weekly.loc[index,'dl_release_after_download_requests']=item['after']
    return weekly


def sql_string(value):return "'"+str(value).replace("'","''")+"'"


class DiskGuard:
    """Interrupt a large local query if its filesystem approaches the reserve."""
    def __init__(self, path, minimum_gib, interval=1):
        self.path=Path(path);self.minimum=int(minimum_gib*1024**3)
        self.interval=interval;self.stopped=threading.Event();self.failed=None;self.connection=None
        self.check()
        self.thread=threading.Thread(target=self.watch,name='download-feature-storage-guard',daemon=True)
        self.thread.start()

    def check(self):
        if self.failed is not None:raise RuntimeError(self.failed)
        if shutil.disk_usage(self.path).free<self.minimum:
            self.failed='Download feature storage reserve reached; partial output was not published'
            raise RuntimeError(self.failed)

    def watch(self):
        while not self.stopped.wait(self.interval):
            try:self.check()
            except RuntimeError:
                if self.connection is not None:self.connection.interrupt()
                return

    def close(self):
        self.stopped.set();self.thread.join()


class DuckBackend:
    def __init__(self, queries, winners, mapping, temp, memory_limit='1GB', max_temp_gib=10, disk_guard=None, threads=1):
        import duckdb
        if not isinstance(threads,int) or isinstance(threads,bool) or not 1<=threads<=2:raise ValueError('DuckDB threads must be1or2')
        self.con=duckdb.connect();con=self.con
        if disk_guard is not None:disk_guard.connection=con
        con.execute("SET TimeZone='UTC'")
        con.execute('SET memory_limit='+sql_string(memory_limit));con.execute('SET threads='+str(threads))
        con.execute('SET preserve_insertion_order=false')
        con.execute('SET temp_directory='+sql_string(temp/'spill'))
        con.execute('SET max_temp_directory_size='+sql_string(str(max_temp_gib)+'GiB'))
        paths='['+','.join(sql_string(v['input_path']) for v in queries)+']'
        columns={**{k:'VARCHAR' for k in ['package','date',*TEXT]},'http_status':'JSON',**{k:'JSON' for k in COUNTS}}
        fields='{'+','.join(sql_string(k)+':'+sql_string(v) for k,v in columns.items())+'}'
        if not queries:
            self.rows_validated=0;self.empty=True;return
        self.empty=False
        # Keep each provider JSON scalar as JSON, including quoted numbers and
        # booleans. This temporary Parquet is a parser cache, never a source or
        # published artifact; source_path remains the original receipt path.
        self.raw_cache=temp/'.raw-cache';self.raw_cache.mkdir()
        cached=[]
        progress('raw_parser_cache','start',0,len(queries))
        for index,query in enumerate(queries,1):
            if disk_guard is not None:disk_guard.check()
            target=self.raw_cache/f'query-{index:06d}.parquet';cached.append(target)
            con.execute(f"COPY (SELECT * FROM read_json({sql_string(query['input_path'])},format='newline_delimited',compression='gzip',columns={fields},filename='source_path')) TO {sql_string(target)} (FORMAT PARQUET,COMPRESSION UNCOMPRESSED)")
            if index%10==0 or index==len(queries):progress('raw_parser_cache','checkpoint',index,len(queries))
        progress('raw_parser_cache','end',len(queries),len(queries))
        cache_paths='['+','.join(sql_string(path) for path in cached)+']'
        raw_projection=','.join([*['"'+name+'"' for name in ['package','date',*TEXT]],
            *['CAST('+name+' AS JSON) AS '+name for name in ['http_status',*COUNTS]],'source_path'])
        con.execute(f'CREATE VIEW raw_input AS SELECT {raw_projection} FROM read_parquet({cache_paths},union_by_name=true)')
        con.register('query_windows',pd.DataFrame([{'source_path':q['input_path'],'left_date':q['start'],'right_date':q['end_exclusive'],'expected_rows':q['total_rows']} for q in queries]))
        con.register('registry_map',pd.DataFrame([{'package_key':k,'tool_id':v} for k,v in mapping.items()]))
        canon="regexp_replace(lower(trim(package)),'[-_.]+','-','g')"
        invalid=' OR '.join(f"{k} IS NULL OR json_type({k}) NOT IN ('BIGINT','UBIGINT') OR TRY_CAST({k} AS BIGINT) IS NULL OR TRY_CAST({k} AS BIGINT)<0" for k in COUNTS)
        bad_values="ci_report IS NULL OR ci_report NOT IN ('reported_true','reported_false','unknown') OR NOT regexp_full_match(date,'[0-9]{4}-[0-9]{2}-[0-9]{2}') OR TRY_CAST(date AS DATE) IS NULL OR (http_status IS NOT NULL AND json_type(http_status)<>'NULL' AND json_type(http_status) NOT IN ('BIGINT','UBIGINT'))"
        progress('global_raw_validation','start',0,len(queries))
        checks=con.execute(f"""SELECT source_path,COUNT(*) AS rows,
 COUNT_IF({invalid}) AS bad_counts,
 COUNT_IF(TRY_CAST(downloads AS BIGINT)+TRY_CAST(excluded_requests AS BIGINT)<>TRY_CAST(logged_objects AS BIGINT)) AS bad_arithmetic,
 COUNT_IF({bad_values}) AS bad_values,
 COUNT_IF(TRY_CAST(date AS DATE)<CAST(left_date AS DATE) OR TRY_CAST(date AS DATE)>=CAST(right_date AS DATE)) AS bad_bounds,
 COUNT_IF(tool_id IS NULL) AS bad_package
 FROM raw_input JOIN query_windows USING(source_path) LEFT JOIN registry_map ON {canon}=package_key GROUP BY source_path""").fetchall()
        counts={item[0]:item[1] for item in checks}
        if any(counts.get(q['input_path'],0)!=q['total_rows'] for q in queries):raise ValueError('Decoded row count differs from completion receipt')
        messages=['Counts must be nonnegative integer JSON values','Artifact/excluded/logged count arithmetic differs',
                  'Invalid date, CI label or HTTP status','Row outside frozen query window','Unmapped package in rich query']
        for item in checks:
            for count,message in zip(item[2:],messages):
                if count:raise ValueError(message)
        self.rows_validated=sum(counts.values())
        progress('global_raw_validation','end',len(queries),len(queries))
        dims=', '.join(DIMENSIONS)
        # Validate uniqueness one retained query at a time so a multi-year input
        # does not require a hash table for every rich row at once.
        progress('query_uniqueness','start',0,len(queries))
        for index,query in enumerate(queries,1):
            source=f"read_parquet({sql_string(cached[index-1])})"
            if con.execute(f'SELECT COUNT(*) FROM (SELECT package,date,{dims},COUNT(*) AS n FROM {source} GROUP BY ALL HAVING n>1)').fetchall()[0][0]:raise ValueError('Duplicate source dimension key')
            if index%10==0 or index==len(queries):progress('query_uniqueness','checkpoint',index,len(queries))
        progress('query_uniqueness','end',len(queries),len(queries))
        con.register('date_winners',pd.DataFrame([{'winner_date':str(day),'source_path':path} for day,path in winners.items()]))
        projection=', '.join('CAST('+name+' AS BIGINT) AS '+name for name in COUNTS)
        dimension_projection=', '.join('CAST(http_status AS BIGINT) AS http_status' if name=='http_status' else name for name in DIMENSIONS)
        self.rich_path=temp/'rich_download_dimensions.parquet';self.materialized=False
        con.execute(f'CREATE VIEW rich AS SELECT tool_id,{canon} AS package,date,{dimension_projection},{projection} FROM raw_input JOIN registry_map ON {canon}=package_key JOIN date_winners USING(source_path) WHERE date=winner_date')

    def materialize(self):
        if self.empty or self.materialized:return
        progress('rich_parquet','start')
        self.con.execute('COPY rich TO '+sql_string(self.rich_path)+" (FORMAT PARQUET,COMPRESSION ZSTD)")
        self.con.execute('CREATE OR REPLACE VIEW rich AS SELECT * FROM read_parquet('+sql_string(self.rich_path)+')')
        self.con.execute('DROP VIEW raw_input')
        shutil.rmtree(self.raw_cache)
        self.materialized=True
        progress('rich_parquet','end')

    def daily(self):
        if self.empty:return pd.DataFrame(columns=['tool_id','date',*COUNTS,'dimension_rows'])
        self.materialize()
        progress('daily_aggregation','start')
        result=self.con.execute('SELECT tool_id,date,SUM(logged_objects)::BIGINT AS logged_objects,SUM(downloads)::BIGINT AS downloads,SUM(excluded_requests)::BIGINT AS excluded_requests,COUNT(*) AS dimension_rows FROM rich GROUP BY tool_id,date ORDER BY tool_id,date').df()
        progress('daily_aggregation','end');return result

    def write_and_summarize(self, output, grid):
        con=self.con
        if self.empty:
            return [],0
        self.materialize()
        if output/'rich_download_dimensions.parquet' != self.rich_path:
            shutil.copyfile(self.rich_path,output/'rich_download_dimensions.parquet')
        valid=grid.loc[grid.downloads.notna(),['tool_id','date']];con.register('valid_dates',valid)
        selections=[]
        for dimension in DIMENSIONS:
            value=("CASE WHEN ci_report='unknown' THEN NULL ELSE NULLIF(TRIM(ci_report),'') END" if dimension=='ci_report' else f"NULLIF(TRIM(CAST({dimension} AS VARCHAR)),'')")
            selections.append(f"SELECT tool_id,strftime(date_trunc('week',CAST(date AS DATE)),'%Y-%m-%d') AS week_start,{sql_string(dimension)} AS dimension,{value} AS dimension_value,SUM(downloads)::BIGINT AS downloads FROM rich JOIN valid_dates USING(tool_id,date) WHERE downloads>0 GROUP BY ALL")
        progress('weekly_composition_parquet','start')
        con.execute('CREATE TEMP VIEW composition AS '+' UNION ALL '.join(selections))
        con.execute('COPY (SELECT *,dimension_value IS NULL AS dimension_unknown FROM composition) TO '+sql_string(output/'weekly_dimension_composition.parquet')+' (FORMAT PARQUET,COMPRESSION ZSTD)')
        # The compact composition has already been written. Reuse those bytes
        # for summaries/counts instead of expanding every dimension over the
        # full multi-year rich dataset again for each CTE and final count.
        con.execute('CREATE OR REPLACE TEMP VIEW composition AS SELECT * FROM read_parquet('+sql_string(output/'weekly_dimension_composition.parquet')+')')
        progress('weekly_composition_parquet','end');progress('composition_summary','start')
        summary=con.execute("""WITH totals AS (
 SELECT tool_id,week_start,dimension,SUM(downloads)::BIGINT AS total_downloads,
 COUNT(dimension_value)::BIGINT AS distinct_known,
 SUM(CASE WHEN dimension_value IS NULL THEN downloads ELSE 0 END)::DOUBLE/SUM(downloads) AS unknown_share,
 SUM(CASE WHEN dimension_value='reported_true' THEN downloads ELSE 0 END)::DOUBLE/SUM(downloads) AS reported_true_share,
 SUM(CASE WHEN dimension_value='reported_false' THEN downloads ELSE 0 END)::DOUBLE/SUM(downloads) AS reported_false_share
 FROM composition GROUP BY ALL
), known AS (
 SELECT *,ROW_NUMBER() OVER(PARTITION BY tool_id,week_start,dimension ORDER BY downloads DESC,dimension_value) AS rank
 FROM composition WHERE dimension_value IS NOT NULL
), entropy AS (
 SELECT c.tool_id,c.week_start,c.dimension,-SUM((c.downloads::DOUBLE/t.total_downloads)*LN(c.downloads::DOUBLE/t.total_downloads)) AS entropy
 FROM composition c JOIN totals t USING(tool_id,week_start,dimension) GROUP BY ALL
)
SELECT t.*,k.dimension_value AS top_known_value,k.downloads::DOUBLE/t.total_downloads AS top_known_share,e.entropy
FROM totals t LEFT JOIN known k ON t.tool_id=k.tool_id AND t.week_start=k.week_start AND t.dimension=k.dimension AND k.rank=1
LEFT JOIN entropy e ON t.tool_id=e.tool_id AND t.week_start=e.week_start AND t.dimension=e.dimension""").df().to_dict('records')
        count=con.execute('SELECT COUNT(*) FROM composition').fetchall()[0][0]
        progress('composition_summary','end');return summary,count

    def release_summaries(self, dependency_history, registry_sha):
        progress('release_date_join','start')
        root=Path(dependency_history).resolve();manifest=read(root/'manifest.json');artifact=root/'artifacts.jsonl.gz'
        if (manifest.get('source')!='pypi_immutable_distribution_metadata' or manifest.get('registry_sha256')!=registry_sha
                or manifest.get('files',{}).get('artifacts.jsonl.gz')!=sha(artifact)):raise ValueError('Release artifact history is unverified')
        self.con.execute("CREATE TEMP VIEW release_dates AS SELECT package,version,CASE WHEN BOOL_AND(TRY_CAST(provider_upload_time AS TIMESTAMPTZ) IS NOT NULL) THEN MIN(TRY_CAST(provider_upload_time AS TIMESTAMPTZ)) END AS published FROM read_json("+sql_string(artifact)+",format='newline_delimited',compression='gzip',columns={'package':'VARCHAR','version':'VARCHAR','provider_upload_time':'VARCHAR','identity_valid':'BOOLEAN'}) WHERE identity_valid IS TRUE AND package IS NOT NULL AND version IS NOT NULL GROUP BY package,version")
        result=self.con.execute("""SELECT tool_id,strftime(date_trunc('week',CAST(date AS DATE)),'%Y-%m-%d') AS week_start,
 SUM(downloads)::BIGINT AS total,
 SUM(CASE WHEN published IS NOT NULL AND CAST(published AS DATE)<=CAST(date AS DATE) THEN downloads ELSE 0 END)::BIGINT AS known,
 SUM(CASE WHEN date_diff('day',CAST(published AS DATE),CAST(date AS DATE)) BETWEEN 0 AND 27 THEN downloads ELSE 0 END)::BIGINT AS recent,
 SUM(CASE WHEN CAST(published AS DATE)>CAST(date AS DATE) THEN downloads ELSE 0 END)::BIGINT AS after_requests
 FROM rich JOIN valid_dates USING(tool_id,date) LEFT JOIN release_dates ON rich.package=release_dates.package AND rich.package_version=release_dates.version
 GROUP BY tool_id,week_start""").df()
        binding={'manifest_sha256':sha(root/'manifest.json'),'artifacts_sha256':sha(artifact),'status':manifest.get('status')}
        progress('release_date_join','end');return result,binding


def primary_inputs(root, registry_path, mapping):
    root=Path(root).resolve();manifest=read(root/'study_manifest.json')
    if manifest.get('status')!='validated_primary':raise ValueError('Frozen primary reference is not a validated snapshot')
    if sha(root/'tools.study.json')!=sha(registry_path):raise ValueError('Frozen primary reference uses a different cohort')
    for name in ['daily_normalized.parquet','coverage.csv']:
        if manifest.get('files',{}).get(name)!=sha(root/name):raise ValueError('Frozen primary input bytes differ: '+name)
    coverage=pd.read_csv(root/'coverage.csv')
    first={r.tool_id:utc(r.first_visible_artifact).date() for r in coverage.itertuples()}
    if set(first)!=set(mapping.values()):raise ValueError('First-artifact coverage differs from registry')
    daily=pd.read_parquet(root/'daily_normalized.parquet',columns=['tool_id','date','dl_downloads'])
    if daily.duplicated(['tool_id','date']).any():raise ValueError('Duplicate frozen primary daily keys')
    reference={(r.tool_id,str(r.date)):r.dl_downloads for r in daily.itertuples()}
    return first,reference,sha(root/'study_manifest.json')


def build(args):
    if args.output.exists():raise FileExistsError('Use a fresh output directory; source vintages remain immutable')
    if not date.fromisoformat(args.start)<date.fromisoformat(args.end_exclusive)<=datetime.now(timezone.utc).date():raise ValueError('Use a completed nonempty UTC date range')
    registry=read(args.registry);mapping=registry_mapping(registry)
    progress('input_receipt_validation','start')
    queries,winners,pending=collect_inputs(args.query_root,mapping,args.start,args.end_exclusive)
    progress('input_receipt_validation','end',len(queries),len(queries))
    first,reference,primary_sha=primary_inputs(args.primary_snapshot,args.registry,mapping)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(args.output.parent).free<args.minimum_free_gib*1024**3:raise ValueError('At least 15 GiB free is required')
    staging=Path(tempfile.mkdtemp(prefix='.download-features-',dir=args.output.parent))
    guard=DiskGuard(staging,args.minimum_free_gib);backend=None
    try:
        if args.backend=='duckdb':
            backend=DuckBackend(queries,winners,mapping,staging,args.memory_limit,args.max_temp_gib,guard,getattr(args,'threads',1))
            guard.check()
            aggregated=backend.daily();rows_validated=backend.rows_validated
        else:
            frame,rows_validated=pandas_inputs(queries,winners,mapping,args.pandas_max_rows);aggregated=aggregate_daily(frame)
        grid=daily_grid(aggregated,mapping,winners,first,reference,args.start,args.end_exclusive)
        release_binding=None
        if args.backend=='duckdb':
            summaries,composition_rows=backend.write_and_summarize(staging,grid)
            weekly=week_features(grid,summaries,args.start,args.end_exclusive)
            if args.dependency_history and not backend.empty:
                release,release_binding=backend.release_summaries(args.dependency_history,sha(args.registry))
                lookup={(r.tool_id,r.week_start):r for r in release.itertuples()}
                for index,row in weekly.iterrows():
                    item=lookup.get((row.tool_id,row.week_start))
                    if not row.rich_week_complete or item is None:continue
                    weekly.loc[index,'release_dates_available']=True
                    weekly.loc[index,'dl_release_date_known_share']=item.known/item.total if item.total else None
                    weekly.loc[index,'dl_recent_version_28d_share_lower_bound']=item.recent/item.total if item.total else None
                    weekly.loc[index,'dl_release_after_download_requests']=item.after_requests
            guard.connection=None;backend.con.close();backend=None
        else:
            composition=pandas_compositions(frame,grid);summaries=composition_summary(composition);composition_rows=len(composition)
            weekly=week_features(grid,summaries,args.start,args.end_exclusive)
            frame.to_csv(staging/'rich_download_dimensions.csv.gz',index=False,compression='gzip')
            composition.to_csv(staging/'weekly_dimension_composition.csv.gz',index=False,compression='gzip')
            if args.dependency_history:raise ValueError('Release history on large real inputs requires DuckDB; unit helpers accept a supplied date map')
        progress('final_quality','start')
        grid.to_csv(staging/'daily_totals.csv',index=False);weekly.to_csv(staging/'weekly_download_dimension_features.csv',index=False)
        try:
            grid.to_parquet(staging/'daily_totals.parquet',index=False);weekly.to_parquet(staging/'weekly_download_dimension_features.parquet',index=False)
        except ImportError:pass
        differences=grid[grid.difference_from_frozen_primary.notna() & grid.difference_from_frozen_primary.ne(0)]
        differences.to_csv(staging/'frozen_primary_differences.csv',index=False)
        query_days=len(winners);expected_days=len(days(args.start,args.end_exclusive))
        errors=int((~grid.pre_visible_artifact & grid.query_retrieved & ~grid.cohort_day_observed).sum())
        reference_missing=int((~grid.frozen_reference_found).sum())
        missing_query_days=sorted(str(day) for day in set(days(args.start,args.end_exclusive))-set(winners))
        pre_artifact_requests=int(grid.loc[grid.pre_visible_artifact,'raw_downloads'].sum())
        full=query_days==expected_days and errors==0 and reference_missing==0 and pre_artifact_requests==0
        status='passed' if full and differences.empty else 'passed_with_reference_differences' if full else 'partial'
        quality={'source':SOURCE,'validation_status':status,'built_at':datetime.now(timezone.utc).isoformat(),
                 'start':args.start,'end_exclusive':args.end_exclusive,'tool_count':len(mapping),'daily_rows':len(grid),
                 'weekly_rows':len(weekly),'complete_weekly_rows':int(weekly.rich_week_complete.sum()) if not weekly.empty else 0,
                 'requested_days':expected_days,'retrieved_query_days':query_days,'missing_query_days':missing_query_days,
                 'unobserved_cohort_tool_days_after_artifact':errors,'pre_artifact_tool_days':int(grid.pre_visible_artifact.sum()),
                 'pre_artifact_observed_artifact_requests':pre_artifact_requests,
                 'source_rows_validated':rows_validated,'composition_rows':composition_rows,'query_count':len(queries),
                 'pending_query_intervals':[v for v in pending if v['state'] != SUPERSEDED],
                 'superseded_split_parent_intervals':[v for v in pending if v['state'] == SUPERSEDED],
                 'winning_input_by_date':{str(k):v for k,v in sorted(winners.items())},
                 'queries':queries,'registry_sha256':sha(args.registry),'frozen_primary_manifest_sha256':primary_sha,
                 'code_sha256':{Path(__file__).name:sha(Path(__file__)),
                                'download_split_coverage.py':sha(Path(__file__).with_name('download_split_coverage.py'))},
                 'runtime':{'python':platform.python_version(),'pandas':pd.__version__,'backend':args.backend,
                     'duckdb_resource_profile':{'memory_limit':args.memory_limit,'threads':getattr(args,'threads',1),
                         'max_temp_gib':args.max_temp_gib,'preserve_insertion_order':False,
                         'composition_summary_source':'materialized_parquet'} if args.backend=='duckdb' else None},
                 'frozen_primary_difference_tool_days':len(differences),'primary_dataset_modified':False,
                 'frozen_reference_missing_tool_days':reference_missing,
                 'release_history_binding':release_binding,'primary_model_promoted':False,
                 'availability_lag_days':2,'availability_lag_is_assumption':True,
                 'limits':['Artifact requests are not users, installs, or retention.',
                           'Reported CI true/false/unknown values are provider metadata, not a reliable human/automation detector.',
                           'Null and blank dimension values remain explicit unknown categories.',
                           'Version-release ages use provider upload dates only where available; recent-version share is a lower bound when dates are missing.',
                           'Differences from the frozen daily target are reported separately; the old data and predictions are not overwritten.',
                           'A complete query interval does not establish that the platform captured every real download.']}
        (staging/'download_dimension_quality.json').write_text(json.dumps(quality,indent=2)+'\n')
        (staging/'tools.study.json').write_bytes(args.registry.read_bytes())
        manifest={'status':status,'source':SOURCE,'registry_sha256':sha(args.registry),'primary_model_promoted':False,
                  'files':{p.name:sha(p) for p in staging.iterdir() if p.is_file()}}
        (staging/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        progress('final_quality','end');progress('publication','start')
        shutil.rmtree(staging/'spill',ignore_errors=True)
        guard.check();guard.close()
        staging.rename(args.output)
        progress('publication','end')
    except BaseException:
        guard.connection=None
        if backend is not None:backend.con.close()
        guard.close();shutil.rmtree(staging,ignore_errors=True);raise
    print(json.dumps({'status':status,'query_days':query_days,'requested_days':expected_days,
                      'weekly_rows':len(weekly),'reference_differences':len(differences),'output':str(args.output)}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--query-root',type=Path,required=True);parser.add_argument('--registry',type=Path,required=True)
    parser.add_argument('--primary-snapshot',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--start',default='2023-01-01');parser.add_argument('--end-exclusive',required=True)
    parser.add_argument('--dependency-history',type=Path)
    parser.add_argument('--backend',choices=['duckdb','pandas'],default='duckdb')
    parser.add_argument('--pandas-max-rows',type=int,default=200000)
    parser.add_argument('--threads',type=int,choices=[1,2],default=1)
    parser.add_argument('--memory-limit',default='1GB');parser.add_argument('--max-temp-gib',type=int,default=10)
    parser.add_argument('--minimum-free-gib',type=float,default=15)
    args=parser.parse_args()
    if args.minimum_free_gib<15 or not 1<=args.max_temp_gib<=20:parser.error('Keep15 GiB free and bound temporary spill1–20 GiB')
    build(args)


if __name__=='__main__':main()
