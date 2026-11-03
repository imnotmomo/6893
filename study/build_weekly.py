#!/usr/bin/env python3
"""Reconstruct a deduplicated retrospective weekly panel; publish no synthetic history."""
import argparse
from datetime import datetime, timezone
import fcntl, gzip, hashlib, json, re, sys
from pathlib import Path
import numpy as np
import pandas as pd
from source_quality import GH_COUNTS, GH_NUMERIC, POLICY_PATH, annotate_daily_quality, load_policy, validate_panel, raw_spot_checks
ROOT = Path.home()/'6893'
sys.path.insert(0,str(ROOT/'duckdb'))

def connect(**kwargs):
    # Pure feature/quality tests do not need a local DuckDB installation.
    from query import connect as database_connect
    return database_connect(**kwargs)

def stable(obj):
    return json.dumps(obj,sort_keys=True,ensure_ascii=False,default=str)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def payload(value):
    return json.loads(value) if isinstance(value,str) else value

def utc_naive(value):
    stamp=pd.Timestamp(value)
    return stamp.tz_localize('UTC').tz_convert(None) if stamp.tzinfo is None else stamp.tz_convert('UTC').tz_localize(None)

def compatible_mapping(current,previous,source):
    if not previous:return False
    package=lambda t:re.sub(r'[-_.]+','-',str(t.get('pypi_package','')).lower())
    if source in ('pypi','pypi_releases'):
        return bool(package(current)) and package(current)==package(previous)
    if source=='gharchive':
        left,right=current.get('github_repo_id'),previous.get('github_repo_id')
        if left is not None and right is not None:return str(left)==str(right)
        left,right=current.get('github_repo','').lower(),previous.get('github_repo','').lower()
        return bool(left) and left==right
    if source=='arxiv':
        return package(current)==package(previous) and ' '.join(current.get('arxiv_query','').split())==' '.join(previous.get('arxiv_query','').split())
    return False

def select_snapshots(jobs,tools,registries):
    """Choose complete compatible snapshots before inspecting their returned rows.

    A newer successful empty response supersedes older positive rows. Per-type
    row deduplication alone cannot express that replacement.
    """
    coverage={s:{t:set() for t in tools} for s in ['pypi','gharchive','pypi_releases']}
    winners={};eligible={}
    for job in jobs:
        old=registries.get(job['plan_sha'],{})
        selected=list(old) if job['tool_id']=='__all__' else [job['tool_id']]
        permitted={tid for tid in selected if tid in tools and compatible_mapping(tools[tid],old.get(tid),job['source'])}
        if not permitted:continue
        eligible[job['job_id']]={**job,'tools':permitted}
        if job['source'] not in coverage:continue
        dates=[x.date() for x in pd.date_range(job['start_date'],pd.Timestamp(job['end_date'])-pd.Timedelta(days=1))]
        rank=(utc_naive(job['updated_at']),job['job_id'])
        for tid in permitted:
            coverage[job['source']][tid].update(dates)
            if job['source'] not in ('pypi','gharchive'):continue
            for day in dates:
                key=(job['source'],tid,day)
                if key not in winners or rank>winners[key][0]:winners[key]=(rank,job['job_id'])
    return coverage,{key:value[1] for key,value in winners.items()},eligible

def latest_observations(records,eligible,winners):
    latest={};candidates={};seen=set();duplicate_count=0;superseded=0
    for item in records:
        source,tid=item['source'],item['tool_id'];job=eligible.get(item['job_id'])
        if not job or tid not in job['tools'] or source!=job['source']:continue
        r=payload(item['payload']);observed=item['observed_at']
        if source=='arxiv':
            key=(tid,re.sub(r'v\d+$','',r.get('id','')))
            if key not in candidates or observed>candidates[key][0]:candidates[key]=(observed,r)
            continue
        if source=='pypi':key=(source,tid,r.get('date'))
        elif source=='gharchive':
            if r.get('event_type')=='__all_types__':
                if (r.get('action')!='__window_unique_actors__' or job['start_date'].weekday()!=0
                        or (job['end_date']-job['start_date']).days!=7
                        or pd.Timestamp(r.get('date')).date()!=job['start_date']):continue
            key=(source,tid,r.get('date'),r.get('event_type'),r.get('action',''))
        elif source=='pypi_releases':key=(source,tid,r.get('version'),r.get('filename'),r.get('artifact_sha256'))
        else:continue
        if key in seen:duplicate_count+=1
        seen.add(key)
        marker=source=='gharchive' and r.get('event_type')=='__all_types__'
        if source in ('pypi','gharchive') and not marker:
            day=pd.Timestamp(r.get('date')).date()
            if winners.get((source,tid,day))!=item['job_id']:
                superseded+=1;continue
        if key not in latest or observed>latest[key][0]:latest[key]=(observed,r,item['job_id'])
    return latest,candidates,duplicate_count,superseded

def weekly_rows(daily, first_upload, tool_id, asof):
    """Pure transformation; gaps remain gaps and lagged outcomes start after issue date."""
    d=annotate_daily_quality(daily,first_upload)
    d.index=pd.to_datetime(d.index)
    groups=d.resample('W-MON',label='left',closed='left')
    w=pd.DataFrame(index=groups.size().index)
    w.index.name='week_start'
    for c in ['dl_downloads',*GH_COUNTS,*['observed_'+x for x in GH_COUNTS]]:
        w[c]=groups[c].sum(min_count=7)
    w['dl_coverage_days']=groups.dl_downloads.count()
    w['gh_coverage_days']=groups.observed_gh_event_count.count()
    w['gh_feature_coverage_days']=groups.gh_event_count.count()
    w['gh_retrieval_complete']=w.gh_coverage_days.eq(7)
    w['release_count']=groups.release_count.sum(min_count=7)
    # Distinct actors cannot be summed or taken from a shorter partial window.
    w['gh_active_actors']=d.gh_active_actors.reindex(w.index)
    w['observed_gh_active_actors']=d.observed_gh_active_actors.reindex(w.index)
    w.loc[w.gh_coverage_days!=7,'observed_gh_active_actors']=np.nan
    w.loc[w.gh_feature_coverage_days!=7,'gh_active_actors']=np.nan
    boundary=pd.Timestamp(load_policy()['sources']['gharchive']['known_degradation_start'])
    w['gh_capture_quality']=np.where(w.index+pd.Timedelta(days=7)>boundary,'known_capture_degradation','unvalidated_capture')
    w['gh_capture_validated']=False
    w['gh_primary_eligible']=False
    w=w[(w.index>=d.index.min().normalize()) & (w.index+pd.Timedelta(days=7)<=asof.normalize())]
    for lag in range(8):w[f'dl_lag_{lag}']=w.dl_downloads.shift(lag)
    w['dl_log_downloads']=np.log1p(w.dl_downloads)
    for lag in [1,4,8]:w[f'dl_log_change_{lag}w']=w.dl_log_downloads-w.dl_log_downloads.shift(lag)
    w['dl_log_median_4w']=np.log1p(w.dl_downloads.rolling(4,min_periods=4).median())
    w['gh_log_events_4w']=np.log1p(w.gh_event_count.rolling(4,min_periods=4).sum())
    w['observed_gh_log_events_4w']=np.log1p(w.observed_gh_event_count.rolling(4,min_periods=4).sum())
    w['release_count_4w']=w.release_count.rolling(4,min_periods=4).sum()
    w['gh_missing']=w.gh_event_count.isna().astype(int)
    w['tool_age_weeks']=np.maximum(0,(w.index-first_upload).days/7) if first_upload is not None else np.nan
    base=w.dl_downloads.rolling(8,min_periods=8).median()
    future=pd.concat([w.dl_downloads.shift(-k) for k in range(2,10)],axis=1)
    full=future.notna().all(axis=1)
    med=future.median(axis=1).where(full)
    w['baseline_median_downloads']=base
    w['future_median_downloads']=med
    w['target_log_growth']=np.log1p(med)-np.log1p(base)
    w['target_sustained']=((future.gt(1.1*base,axis=0)).sum(axis=1)>=6).astype(float).where(full & base.notna())
    w['feature_cutoff']=w.index+pd.Timedelta(days=9)
    w['outcome_start']=w.index+pd.Timedelta(days=14)
    w['outcome_end_exclusive']=w.index+pd.Timedelta(days=70)
    w['label_available_at']=w.index+pd.Timedelta(days=72)
    w['prediction_eligible']=base.notna() & (w.feature_cutoff<=asof)
    w['coverage_complete']=w.prediction_eligible & w.target_log_growth.notna() & (w.label_available_at<=asof)
    w['download_label_eligible']=w.coverage_complete
    w['tool_id']=tool_id
    return w.reset_index()

def build(args):
    data=args.data_dir.resolve(); out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    registry=json.loads(args.registry.read_text());tools={t['tool_id']:t for t in registry['tools']}
    asof=utc_naive(args.as_of or datetime.now(timezone.utc).isoformat())
    with (data/'.pilot-collection.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        with connect(read_only=True,data_dir=data) as con:
            result=con.execute("SELECT r.job_id,r.source,r.tool_id,r.observed_at,r.payload FROM backfill.records r JOIN backfill.jobs j USING(job_id) WHERE j.state='done'")
            columns=[col[0] for col in result.description];records=[dict(zip(columns,row)) for row in result.fetchall()]
            result=con.execute("SELECT job_id,plan_sha,source,tool_id,start_date,end_date,updated_at FROM backfill.jobs WHERE state='done'")
            columns=[col[0] for col in result.description];jobs=[dict(zip(columns,row)) for row in result.fetchall()]
            result=con.execute("SELECT j.job_id,j.tool_id,o.source,o.raw_path,o.sha256,o.observed_at FROM backfill.objects o JOIN backfill.jobs j USING(job_id) WHERE o.source IN ('pypi','gharchive','pypi_releases') AND j.state='done'")
            columns=[col[0] for col in result.description];objects=[dict(zip(columns,row)) for row in result.fetchall()]
            budget=int(con.execute('SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget').fetchone()[0])
    mapping={}
    for plan in {job['plan_sha'] for job in jobs}:
        p=data/'raw/backfill/registries'/plan/'tools.json'
        mapping[plan]={t['tool_id']:t for t in json.loads(p.read_text())['tools']} if p.exists() else {}
    coverage,winners,eligible=select_snapshots(jobs,tools,mapping)
    latest,candidates,duplicate_count,superseded_count=latest_observations(records,eligible,winners)
    first={};release_dates={t:{} for t in tools};checked=set()
    for item in objects:
        if item['source']!='pypi_releases':continue
        job_id,tid,path,sha=(item[k] for k in ['job_id','tool_id','raw_path','sha256'])
        if job_id not in eligible or tid not in eligible[job_id]['tools'] or (tid,sha) in checked:continue
        checked.add((tid,sha)); raw=gzip.decompress((data/path).read_bytes())
        if hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('Raw checksum mismatch: '+path)
        obj=json.loads(raw)
        for version,files in obj.get('releases',{}).items():
            for entry in files:
                d=entry.get('upload_time_iso_8601') or entry.get('upload_time')
                if not d:continue
                dt=pd.Timestamp(d).tz_localize(None)
                first[tid]=min(first.get(tid,dt),dt)
                release_dates[tid][version]=min(release_dates[tid].get(version,dt),dt)
    start=pd.Timestamp('2023-01-01');end=asof.normalize()-pd.Timedelta(days=1)
    positive_dates={(job_id,r.get('date')) for (s,*_),(_,r,job_id) in latest.items() if s=='pypi' and int(r.get('downloads') or 0)>0}
    frames=[];daily_frames=[];coverage_report=[]
    for tid in tools:
        d=pd.DataFrame(index=pd.date_range(start,end,freq='D'))
        for c in ['dl_downloads','gh_event_count','gh_push_events','gh_issue_events','gh_pr_events','gh_watch_events','gh_active_actors','release_count']:d[c]=np.nan
        for stamp in d.index:
            day=stamp.date()
            # Zero means a completed, globally nonempty download query after visible package availability.
            if (day in coverage['pypi'][tid] and (winners.get(('pypi',tid,day)),str(day)) in positive_dates
                    and tid in first and stamp>=first[tid].normalize()):d.loc[stamp,'dl_downloads']=0
            if day in coverage['gharchive'][tid]:d.loc[stamp,['gh_event_count','gh_push_events','gh_issue_events','gh_pr_events','gh_watch_events']]=0
            if day in coverage['pypi_releases'][tid] and tid in first and stamp>=first[tid].normalize():d.loc[stamp,'release_count']=0
        gh={};markers={};bad_gh_dates=set()
        for (source,tool,*_),(_,r,job_id) in latest.items():
            if tool!=tid or source=='pypi_releases':continue
            stamp=pd.Timestamp(r.get('date'))
            if stamp not in d.index:continue
            if (source=='pypi' and tid in first and stamp>=first[tid].normalize()
                    and stamp.date() in coverage['pypi'][tid]):d.loc[stamp,'dl_downloads']=r['downloads']
            if source=='gharchive':
                typ=r.get('event_type');act=r.get('action','')
                if typ=='__all_types__' and act=='__window_unique_actors__':
                    # Only take calendar-aligned seven-day window markers, not arbitrary refresh windows.
                    if (not r.get('missing_event_id_rows') and stamp.weekday()==0
                            and all((stamp+pd.Timedelta(days=k)).date() in coverage['gharchive'][tid] for k in range(7))):
                        markers[stamp]=r.get('active_actors')
                    continue
                if r.get('missing_event_id_rows'):bad_gh_dates.add(stamp)
                gh.setdefault(stamp,{});gh[stamp][typ]=gh[stamp].get(typ,0)+int(r.get('event_count') or 0)
        for stamp,counts in gh.items():
            if pd.isna(d.loc[stamp,'gh_event_count']):continue
            d.loc[stamp,'gh_event_count']=sum(counts.values())
            for typ,col in [('PushEvent','gh_push_events'),('IssuesEvent','gh_issue_events'),('PullRequestEvent','gh_pr_events'),('WatchEvent','gh_watch_events')]:d.loc[stamp,col]=counts.get(typ,0)
        for stamp in bad_gh_dates:d.loc[stamp,['gh_event_count','gh_push_events','gh_issue_events','gh_pr_events','gh_watch_events']]=np.nan
        for stamp,count in markers.items():d.loc[stamp,'gh_active_actors']=count
        for stamp in release_dates[tid].values():
            day=stamp.normalize()
            if day in d.index and pd.notna(d.loc[day,'release_count']):d.loc[day,'release_count']+=1
        d=annotate_daily_quality(d,first.get(tid))
        frames.append(weekly_rows(d,first.get(tid),tid,asof))
        normalized=d.copy()
        normalized.insert(0,'date',normalized.index.strftime('%Y-%m-%d'));normalized.insert(0,'tool_id',tid)
        daily_frames.append(normalized.reset_index(drop=True))
        coverage_report.append({'tool_id':tid,'first_visible_artifact':str(first.get(tid)),'download_days':int(d.dl_downloads.notna().sum()),'github_days':int(d.observed_gh_event_count.notna().sum()),'github_capture_validated_days':0,'github_primary_eligible_days':0,'expected_days':len(d)})
    panel=pd.concat(frames,ignore_index=True).sort_values(['week_start','tool_id'])
    assert not panel.duplicated(['tool_id','week_start']).any()
    assert (panel.loc[panel.prediction_eligible,'outcome_start']>panel.loc[panel.prediction_eligible,'feature_cutoff']).all()
    normalized=pd.concat(daily_frames,ignore_index=True)
    policy=load_policy()
    checks=validate_panel(normalized,panel,tools,asof,first,policy)
    raw_checks=raw_spot_checks(data,objects,tools,eligible,winners,latest,normalized,panel)
    panel.to_parquet(out/'weekly_features.parquet',index=False)
    normalized.to_parquet(out/'daily_normalized.parquet',index=False)
    reference=panel[['tool_id','week_start','dl_downloads','observed_gh_event_count','gh_event_count','release_count']].copy()
    reference['week_start']=reference.week_start.dt.strftime('%Y-%m-%d')
    reference.to_csv(out/'weekly_reference.csv',index=False)
    pd.DataFrame(coverage_report).to_csv(out/'coverage.csv',index=False)
    audit=[]
    for (tid,_),(observed,r) in candidates.items():
        text=(r.get('title','')+' '+r.get('summary','')); matches=[]
        for alias in tools[tid].get('research_exact_aliases',[]):
            if re.search(r'(?<!\w)'+re.escape(alias)+r'(?!\w)',text,re.I):matches.append(alias)
        audit.append({'tool_id':tid,'id':r.get('id'),'title':r.get('title'),'published':r.get('published'),'updated':r.get('updated'),'observed_at':str(observed),'alias_matches':'; '.join(matches),'manual_review_required':True,'historical_feature_eligible':False})
    pd.DataFrame(audit,columns=['tool_id','id','title','published','updated','observed_at','alias_matches','manual_review_required','historical_feature_eligible']).to_csv(out/'research_candidates.csv',index=False)
    config=json.loads((ROOT/'config/backfill.json').read_text())
    status={'as_of':asof.isoformat()+'Z','cohort_size':len(tools),'registry_sha256':digest(args.registry),'weekly_rows':len(panel),'mature_rows':int(panel.coverage_complete.sum()),'prediction_rows':int(panel.prediction_eligible.sum()),'semantic_duplicates_removed':duplicate_count,'budget_reserved_bytes':budget,'conservative_query_cost_usd':round(budget/1024**4*config.get('query_price_usd_per_TiB',6.25),4),'query_budget_usd':config['query_budget_usd'],'raw_source_coverage':coverage_report,'research_candidates':len(audit),'research_historical_features_enabled':False,'target_definition':'log1p(median weekly downloads in t+2..t+9) - log1p(median t-7..t); first full week after 2-day lag; 8 full outcome weeks','source_vintage':'Retrospective event-time reconstruction, not a real-time vintage; currently visible release files and current cohort selection may omit deleted history.','download_measurement_break':'2026-08-24 PyPI metadata counting change; artifact-only filtering applied consistently, sensitivity required.','download_proxy':'Artifact requests, not unique users or retention.','gh_semantics':'Event counts, active event actors; PushEvents are not commit counts.','research_status':'Current revised metadata/candidate matches, unreviewed and ineligible for historical forecasts.','files':{'weekly_features.parquet':digest(out/'weekly_features.parquet')}}
    status['superseded_observation_rows']=superseded_count
    quality_passed=checks['status']=='passed' and raw_checks['status']=='passed'
    (out/'source_quality.json').write_bytes(POLICY_PATH.read_bytes())
    quality={'source_policy':policy['source_policy'],'profile':policy['profile'],'validation_status':'passed' if quality_passed else 'failed','as_of':status['as_of'],'cohort_sha256':digest(args.registry),'source_policy_sha256':digest(out/'source_quality.json'),'enabled_feature_groups':policy['enabled_feature_groups'],'excluded_feature_groups':policy['excluded_feature_groups'],'sources':policy['sources'],'content_checks':checks,'raw_spot_checks':raw_checks,'input_hashes':{name:digest(out/name) for name in ['daily_normalized.parquet','weekly_features.parquet','source_quality.json']},'scope':'Pass applies to enabled primary sources. Retrieval completeness does not certify upstream GH capture. Research and all GH predictors remain excluded.'}
    (out/'data_quality.json').write_text(json.dumps(quality,indent=2,default=str)+'\n')
    status.update({'primary_analysis_ready':quality_passed,'source_quality_profile':policy['profile'],'source_quality_policy':policy['source_policy'],'gh_primary_features_enabled':False,'gh_capture_validated':False,'gh_semantics':policy['sources']['gharchive']['measurement'],'source_quality_warning':policy['sources']['gharchive']['policy']})
    status['files'].update({name:digest(out/name) for name in ['daily_normalized.parquet','weekly_reference.csv']})
    status['files'].update({name:digest(out/name) for name in ['data_quality.json','source_quality.json']})
    (out/'study_status.json').write_text(json.dumps(status,indent=2,default=str)+'\n')
    (out/'feature_schema.json').write_text(stable({c:str(t) for c,t in panel.dtypes.items()})+'\n')
    print(json.dumps({k:status[k] for k in ['cohort_size','weekly_rows','mature_rows','semantic_duplicates_removed','conservative_query_cost_usd','query_budget_usd']},indent=2))
    if not quality_passed:raise ValueError('Primary source quality gate failed; see data_quality.json')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--data-dir',type=Path,default=ROOT/'data');p.add_argument('--registry',type=Path,default=ROOT/'config/tools.study.json');p.add_argument('--output',type=Path,required=True);p.add_argument('--as-of');build(p.parse_args())
