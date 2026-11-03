#!/usr/bin/env python3
"""Join the original project's sources without promoting unreviewed predictors.

The immutable validated primary panel defines the tool/week grid and target.
Optional source backfills may be partial. Missing information remains NULL;
source retrieval, event-time assumptions, relevance and eligibility are separate.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd

PROFILE = 'full_scope_data_spine'
SOURCE_SPECS = {
    'releases': {'source':'github_repository_releases','role':'Visible repository release publications',
        'files':['github_releases_weekly.csv'],'manifest':['manifest.json'],'quality':['github_release_quality.json'],'week':'week_start',
        'defaults':['github_repo_release_published_count','github_repo_release_prerelease_count','github_repo_release_stable_count'],
        'limits':['Exhaustive visible pages and a stable head anchor do not reconstruct deleted releases or an atomic historical snapshot.',
                  'Events use published_at; GitHub created_at can refer to the tag-commit date.',
                  'Repository releases are not verified releases of the selected PyPI distribution.']},
    'research': {'source':'arxiv_first_version_metadata','role':'Research attention candidates',
        'files':['research_v1_weekly.csv'],'manifest':['collection_manifest.json'],
        'quality':['research_v1_quality.json'], 'week':'week_start',
        'defaults':['strict_v1_candidate_count','ambiguous_v1_candidate_count','strict_candidates_available_by_assumed_cutoff'],
        'limits':['Only retained discovered candidates are enriched; historical discovery recall is unvalidated.',
                  'First-version lexical mentions require relevance review and do not prove library use.']},
    'git': {'source':'github_git_contributors','role':'Development activity and contributor pseudonyms',
        'files':['weekly_git_history.csv'],'manifest':['manifest.json'],'quality':[], 'week':'week_start_utc',
        'defaults':['git_reachable_commit_count','git_reachable_author_count','git_reachable_new_author_count','git_reachable_author_growth_4w'],
        'limits':['Current surviving default-branch graph; commit dates do not certify historical public availability.',
                  'Pseudonyms and bot heuristics do not identify unique humans.']},
    'dependency': {'source':'pypi_immutable_distribution_metadata','role':'Published dependency declarations and downstream adoption proxies',
        'files':['weekly_dependency_features.csv'],'manifest':['manifest.json'],'quality':['dependency_quality.json'],'week':'week_start',
        'defaults':['dep_ever_declaring_packages','dep_new_declaring_packages','dep_latest_uploaded_release_declaring_packages','dep_outbound_unique_dependencies'],
        'limits':['Declarations are not installations or users; latest-uploaded-release counts are an explicit proxy.',
                  'Conditional/extras requirements and malformed/undated records retain separate eligibility flags.']},
    'downloads': {'source':'pypi_download_dimensions','role':'Version and environment composition of package demand',
        'files':['weekly_download_dimension_features.csv','weekly_download_features.csv','download_dimensions_weekly.csv'],
        'manifest':['manifest.json'],'quality':['download_dimension_quality.json','download_quality.json'],'week':'week_start',
        'defaults':['rich_dl_downloads','dl_version_distinct_known','dl_installer_distinct_known','dl_python_version_distinct_known'],
        'limits':['Artifact requests are affected by automation, caches and packaging changes; they are not unique users.',
                  'Dimension coverage must be complete for a week before an exact composition is assumed.']},
    'github_api': {'source':'github_direct_issues_pulls','role':'Issue and pull-request creation and recorded merge activity',
        'files':['github_activity_weekly.csv','weekly_github_activity.csv','weekly_issue_pr_features.csv'],
        'manifest':['manifest.json','collection_manifest.json','collection_state.json'],
        'quality':['github_activity_quality.json','source_quality.json'],'week':'week_start',
        'defaults':['github_issue_opened_count','github_pr_opened_count','github_pr_merged_count'],
        'limits':['Currently retained visible objects cannot certify deleted-object or historical visibility completeness.',
                  'Seek/seed pages are not a complete event stream and never become zero-activity weeks.',
                  'Closure/merge counts restricted to a created-date cohort do not cover older objects closed or merged later.']},
}
SUPPLEMENT_SPECS = {
    'stars': {'source':'github_direct_stars','manifest':'collection_manifest.json',
        'csv':'github_direct_stars_weekly.csv','csv_sha':'weekly_csv_sha256',
        'quality':'github_direct_stars_quality.json','output':'supplementary_star_api_calendar.csv',
        'role':'Retrospective surviving star records in provider calendar buckets',
        'limits':['API calendar boundaries are not certified UTC; no star counts are joined to the Monday UTC panel.',
                  'Surviving stars are not historical net stars, point-in-time totals, commits or unique users.']},
    'depsdev': {'source':'depsdev_current_version_requirements_and_resolution','manifest':'manifest.json',
        'csv':'current_dependency_observations.csv','csv_sha':'csv_sha256',
        'output':'supplementary_depsdev_current.csv','role':'Current version declaration and resolved graph observations',
        'limits':['A current resolved dependency graph is not historical resolution or downstream installation data.',
                  'Provider gaps remain unknown; historical immutable distribution declarations are a separate source.']},
}


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def read(path):
    value=json.loads(Path(path).read_text())
    if not isinstance(value,dict):raise ValueError('Expected JSON object: '+str(path))
    return value


def utc(value):
    result=pd.Timestamp(value)
    if pd.isna(result):raise ValueError('Missing required timestamp')
    return result.tz_localize('UTC') if result.tzinfo is None else result.tz_convert('UTC')


def verify_files(root,files):
    root=Path(root).resolve()
    if not isinstance(files,dict) or not files:raise ValueError('Source requires a nonempty file hash manifest')
    for relative,expected in files.items():
        path=(root/relative).resolve()
        if not path.is_relative_to(root) or not path.is_file() or sha(path)!=expected:
            raise ValueError('Source artifact hash/path mismatch: '+str(relative))


def booleans(series,name):
    def value(item):
        if pd.isna(item):return False
        if item in (True,False,0,1):return bool(item)
        if isinstance(item,str) and item.lower() in ('true','false'):return item.lower()=='true'
        raise ValueError('Invalid boolean '+name)
    return series.map(value).astype(bool)


def keyed(frame,ids,week='week_start'):
    frame=frame.copy()
    if not {'tool_id',week}<=set(frame):raise ValueError('Source lacks tool/week keys')
    frame['tool_id']=frame.tool_id.astype(str)
    frame['week_start']=pd.to_datetime(frame[week],utc=True,errors='raise')
    if frame.week_start.isna().any() or not frame.week_start.eq(frame.week_start.dt.normalize()).all() or not frame.week_start.dt.day_of_week.eq(0).all():
        raise ValueError('Every feature week must be midnight Monday UTC')
    if not set(frame.tool_id)<=set(ids) or frame.duplicated(['tool_id','week_start']).any():
        raise ValueError('Unexpected tool identity or duplicate source week')
    return frame.drop(columns=[week]) if week!='week_start' else frame


def validate_temporal_boundaries(frame):
    for column,days in [('feature_cutoff',9),('feature_cutoff_utc',9),('event_window_end_exclusive',7)]:
        if column in frame:
            actual=pd.to_datetime(frame[column],utc=True,errors='raise')
            if not actual.eq(frame.week_start+pd.Timedelta(days=days)).all():
                raise ValueError('Source event/feature boundary differs: '+column)


def validate_values(frame):
    for column in frame:
        if column in {'tool_id','week_start'}:continue
        count=(column.endswith(('_count','_packages','_dependencies')) or column in {'rich_dl_downloads','dl_downloads'}
               or (column.startswith('github_') and column.endswith('_authors')))
        if not count and not pd.api.types.is_numeric_dtype(frame[column]):continue
        values=pd.to_numeric(frame[column],errors='raise').dropna().to_numpy(dtype=float)
        if not np.isfinite(values).all():raise ValueError('Nonfinite source measure: '+column)
        if count and not any(token in column for token in ['change','growth','momentum','difference']):
            if (values<0).any() or (values!=np.floor(values)).any():raise ValueError('Invalid source count: '+column)


def mask_observations(frame,valid,columns,prefix):
    """Keep measured partials separately, assembling wide blocks in one operation."""
    if not columns:return frame
    masked=pd.DataFrame({name:frame[name].where(valid) for name in columns},index=frame.index)
    new_observed=[name for name in columns if prefix+name not in frame]
    observed=frame[new_observed].rename(columns={name:prefix+name for name in new_observed})
    combined=pd.concat([frame.drop(columns=columns),masked,observed],axis=1)
    return combined[list(frame.columns)+list(observed.columns)]


def primary_panel(root,expected_tools=30):
    root=Path(root).resolve();manifest=read(root/'study_manifest.json');before=sha(root/'study_manifest.json')
    if manifest.get('status')!='validated_primary' or manifest.get('profile')!='validated_primary':
        raise ValueError('A validated immutable primary snapshot is required')
    verify_files(root,manifest.get('files'))
    quality=read(root/'data_quality.json');status=read(root/'study_status.json');registry=read(root/'tools.study.json')
    cohort=sha(root/'tools.study.json');ids={item['tool_id'] for item in registry['tools']}
    if (len(ids)!=expected_tools or len(registry['tools'])!=expected_tools or quality.get('cohort_sha256')!=cohort
            or manifest.get('registry_sha256')!=cohort):
        raise ValueError('Frozen primary cohort differs')
    if quality.get('validation_status')!='passed' or quality.get('content_checks',{}).get('status')!='passed':
        raise ValueError('Primary source quality did not pass')
    verify_files(root,quality.get('input_hashes',{}))
    if quality.get('input_hashes',{}).get('weekly_features.parquet')!=sha(root/'weekly_features.parquet'):
        raise ValueError('Primary weekly feature artifact is not quality-bound')
    try:frame=pd.read_parquet(root/'weekly_features.parquet')
    except ImportError:
        if 'weekly_features.csv' not in manifest['files']:raise
        frame=pd.read_csv(root/'weekly_features.csv')
    frame=keyed(frame,ids);asof=utc(status['as_of'])
    start=utc('2023-01-02');weeks=pd.date_range(start,asof.normalize()-pd.Timedelta(days=7),freq='W-MON')
    expected=pd.MultiIndex.from_product([sorted(ids),weeks],names=['tool_id','week_start'])
    actual=pd.MultiIndex.from_frame(frame[['tool_id','week_start']])
    if len(actual)!=len(expected) or len(expected.difference(actual)) or len(actual.difference(expected)):
        raise ValueError('Primary calendar grid is incomplete')
    validate_temporal_boundaries(frame)
    config=read(root/'model_config.json')
    if config.get('feature_groups',{}).get('gh') or config.get('feature_groups',{}).get('research'):
        raise ValueError('Unexpected primary source policy')
    allowed={name for group in config.get('feature_groups',{}).values() for name in group}
    catalog={'source':'validated_primary_download_release_panel','role':'Validated package-demand features and eight-week target',
        'path':str(root),'manifest_sha256':before,'registry_sha256':cohort,'retrieval_status':'validated_primary',
        'collection_complete':True,'historical_primary_eligible':True,'observed_at':status.get('published_at'),
        'history_start':str(start.date()),'history_end_exclusive':str(asof.date()),'rows':len(frame),
        'limits':['Artifact requests are not unique users or durable adoption.','Current cohort and retained release metadata are retrospective.']}
    return frame,registry,cohort,catalog,allowed


def _find(root,names):
    return next((root/name for name in names if (root/name).is_file()),None)


def inspect_supplement(kind,root,cohort,ids):
    """Archive incompatible-calendar/current snapshots without a historical join."""
    spec=SUPPLEMENT_SPECS[kind]
    catalog={'source':spec['source'],'role':spec['role'],'path':str(root) if root else None,
        'retrieval_status':'missing','collection_complete':False,'historical_primary_eligible':False,
        'joined_to_utc_panel':False,'rows_available':0,'limits':spec['limits']}
    if root is None or not Path(root).exists():return catalog,{},[]
    root=Path(root).resolve();manifest_path=root/spec['manifest'];csv_path=root/spec['csv']
    if not manifest_path.exists():catalog['retrieval_status']='awaiting_manifest';return catalog,{},[]
    body=read(manifest_path);fingerprint=sha(manifest_path)
    if body.get('source')!=spec['source'] or body.get('registry_sha256')!=cohort:
        raise ValueError(kind+' supplemental source/cohort differs')
    if sha(root/'tools.study.json')!=cohort:raise ValueError(kind+' supplemental registry changed')
    if set(body.get('tool_ids',[]))!=ids:raise ValueError(kind+' supplemental requested cohort differs')
    expected=body.get(spec['csv_sha'])
    if not expected or sha(csv_path)!=expected:raise ValueError(kind+' supplemental CSV is not hash bound')
    # Original collectors bind every immutable provider response explicitly.
    requests=body.get('requests')
    if not isinstance(requests,list) or not requests:raise ValueError(kind+' lacks raw response evidence')
    for item in requests:
        raw=(root/item['raw_path']).resolve()
        if not raw.is_relative_to(root) or sha(raw)!=(item.get('compressed_sha256') or item.get('gzip_sha256')):
            raise ValueError(kind+' supplemental compressed response changed')
        if hashlib.sha256(gzip.decompress(raw.read_bytes())).hexdigest()!=item['raw_sha256']:
            raise ValueError(kind+' supplemental raw response changed')
    frame=pd.read_csv(csv_path)
    if 'tool_id' not in frame or not set(frame.tool_id.astype(str))<=ids:
        raise ValueError(kind+' supplemental feature identities differ')
    catalog.update(manifest_sha256=fingerprint,registry_sha256=cohort,provider_status=body.get('status'),
        observed_at=body.get('updated_at') or body.get('as_of') or body.get('started_at'),
        rows_available=len(frame),csv_sha256=expected,raw_responses_verified=len(requests),
        tool_status=body.get('tools',{}),output_file=spec['output'])
    payload={spec['output']:(csv_path,expected),kind+'_source_manifest.json':(manifest_path,fingerprint)}
    checkpoints=[(manifest_path,fingerprint),(csv_path,expected)]
    if kind=='stars':
        if any(body.get(name) is not False for name in ['primary_eligible','historical_vintage','point_in_time_snapshot']):
            raise ValueError('Star source semantics cannot certify point-in-time history')
        required={'api_week_start_epoch','api_week_end_epoch','stars','primary_eligible','historical_vintage'}
        if not required<=set(frame) or frame.duplicated(['tool_id','api_week_start_epoch']).any():
            raise ValueError('Star API calendar lacks unique verified buckets')
        for column in ['stars']+['day_'+str(i) for i in range(7)]:
            if column not in frame:raise ValueError('Star calendar day arithmetic is missing')
            values=pd.to_numeric(frame[column],errors='raise')
            if values.isna().any() or (values<0).any() or (values!=np.floor(values)).any():raise ValueError('Invalid star bucket count')
        if not frame.stars.eq(frame[['day_'+str(i) for i in range(7)]].sum(axis=1)).all():
            raise ValueError('Star calendar total differs from its daily counts')
        if not (frame.api_week_end_epoch-frame.api_week_start_epoch).eq(604800).all():
            raise ValueError('Star API calendar bucket does not span seven days')
        for column in ['primary_eligible','historical_vintage']:
            if booleans(frame[column],column).any():raise ValueError('Retrospective stars cannot certify historical eligibility')
        quality_path=root/spec['quality'];quality=read(quality_path);quality_sha=sha(quality_path)
        if (quality.get('registry_sha256')!=cohort or quality.get('weekly_csv_sha256')!=expected
                or quality.get('boundary_alignment_certified_utc') is not False):
            raise ValueError('Star quality/calendar boundary contract changed')
        passed={tid for tid,value in body.get('tools',{}).items() if value.get('validation_status')=='passed'}
        if set(frame.tool_id)!=passed:raise ValueError('Star calendar tools differ from passed tool manifests')
        catalog.update(tools_requested=len(ids),tools_complete=len(passed),history_start=body.get('requested_start'),
            boundary_alignment_certified_utc=False,historical_public_availability='retrospective_surviving_star_records',
            collection_complete=len(passed)==len(ids) and body.get('status') in {'complete','passed'},
            quality_file_sha256=quality_sha)
        payload['stars_source_quality.json']=(quality_path,quality_sha);checkpoints.append((quality_path,quality_sha))
    else:
        if any(body.get(name) is not False for name in ['primary_historical_eligible','historical_resolution_vintage']):
            raise ValueError('Current dependency source semantics cannot certify historical resolution')
        if frame.tool_id.duplicated().any() or set(frame.tool_id)!=ids:
            raise ValueError('Current dependency snapshot must retain every requested tool, including gaps')
        for column in ['primary_historical_eligible','historical_resolution_vintage']:
            if column not in frame or booleans(frame[column],column).any():
                raise ValueError('Current dependency graphs cannot be backdated into history')
        valid=frame.status.eq('complete')
        for column in ['direct_requirement_count','resolved_nodes','resolved_edges']:
            if column not in frame:raise ValueError('Current dependency graph counts are missing')
            values=pd.to_numeric(frame.loc[valid,column],errors='raise')
            if values.isna().any() or (values<0).any() or (values!=np.floor(values)).any():
                raise ValueError('Invalid observed current dependency graph count')
        catalog.update(tools_requested=len(ids),tools_complete=int(valid.sum()),
            source_gap_tools=sorted(frame.loc[~valid,'tool_id'].tolist()),
            retrieval_attempts_complete=body.get('status') in {'complete','complete_with_source_gaps'},
            collection_complete=bool(valid.all()) and body.get('status')=='complete',
            historical_public_availability='current_observed_resolution_only')
    catalog['retrieval_status']='complete_with_source_limits' if catalog['collection_complete'] else 'partial_or_source_gaps'
    for path,expected in checkpoints:
        if sha(path)!=expected:raise ValueError(kind+' supplemental checkpoint changed while reading')
    return catalog,payload,checkpoints


def inspect_source(kind,root,cohort,ids):
    spec=SOURCE_SPECS[kind]
    catalog={'source':spec['source'],'role':spec['role'],'path':str(root) if root else None,
        'retrieval_status':'missing','collection_complete':False,'historical_primary_eligible':False,
        'rows_available':0,'limits':spec['limits'],'feature_columns':[]}
    if root is None or not Path(root).exists():return None,catalog,None
    root=Path(root).resolve();path=_find(root,spec['manifest'])
    if path is None:
        catalog['retrieval_status']='awaiting_manifest';return None,catalog,None
    fingerprint=sha(path);manifest=read(path)
    if manifest.get('source')!=spec['source']:raise ValueError(kind+' source type differs from its adapter')
    actual_cohort=manifest.get('registry_sha256') or manifest.get('cohort_sha256')
    if actual_cohort!=cohort:raise ValueError(kind+' source cohort hash differs')
    catalog.update(manifest_file=str(path),manifest_sha256=fingerprint,registry_sha256=actual_cohort,
        observed_at=manifest.get('updated_at') or manifest.get('observed_at') or manifest.get('created_at') or manifest.get('as_of'),
        history_start=manifest.get('history_start') or manifest.get('start'),
        history_end_exclusive=manifest.get('history_end_exclusive') or manifest.get('end_exclusive'),
        provider_status=manifest.get('status'))
    files=manifest.get('files')
    if files:
        verify_files(root,files);catalog['files_verified']=len(files)
    elif kind=='github_api' and 'requests' in manifest:
        # Connector archives expose one immutable checksum record per response.
        for request in manifest['requests']:
            raw=(root/request['raw_path']).resolve()
            if not raw.is_relative_to(root) or sha(raw)!=request['compressed_sha256']:
                raise ValueError('GitHub API raw response hash/path mismatch')
        catalog['files_verified']=len(manifest['requests'])
    else:raise ValueError(kind+' source lacks bound source files')
    quality_path=_find(root,spec['quality']);quality=read(quality_path) if quality_path else {}
    if quality_path:
        relative=str(quality_path.relative_to(root))
        if files and relative not in files:raise ValueError(kind+' quality report is not hash bound')
        catalog['quality_file_sha256']=sha(quality_path)
        if quality.get('registry_sha256',cohort)!=cohort:raise ValueError(kind+' quality cohort differs')
        catalog['quality_status']=quality.get('status') or quality.get('validation_status')
        for field,candidates in [('observed_at',['observed_at','built_at','created_at']),
                ('history_start',['history_start','start']),
                ('history_end_exclusive',['history_end_exclusive','end_exclusive'])]:
            if not catalog.get(field):catalog[field]=next((quality[name] for name in candidates if quality.get(name)),None)
        if quality.get('frozen_primary_manifest_sha256'):
            catalog['referenced_primary_manifest_sha256']=quality['frozen_primary_manifest_sha256']
    feature=_find(root,spec['files'])
    capture=manifest.get('collection_complete')
    if capture is None:capture=quality.get('collection_complete')
    if capture is not None and not isinstance(capture,bool):raise ValueError(kind+' collection completeness must be an explicit boolean')
    catalog['collection_complete']=capture if capture is not None else manifest.get('status') in {'complete','passed','passed_with_reference_differences'}
    catalog['collection_completeness_basis']='verified_capture_flag' if capture is not None else 'terminal_provider_status'
    catalog['retrieval_status']='complete' if catalog['collection_complete'] else 'partial_or_needs_review'
    if feature is None:
        catalog['raw_retrieval_complete']=catalog['collection_complete']
        catalog['collection_complete']=False
        catalog['retrieval_status']='awaiting_weekly_features';return None,catalog,(path,fingerprint)
    if files and str(feature.relative_to(root)) not in files:raise ValueError(kind+' weekly data is not hash bound')
    if not files:raise ValueError(kind+' weekly data requires a completed hash-bound feature manifest')
    frame=keyed(pd.read_csv(feature),ids,spec['week']);validate_temporal_boundaries(frame);validate_values(frame)
    if not catalog.get('history_start') or not catalog.get('history_end_exclusive'):
        raise ValueError(kind+' feature source requires a declared UTC history interval')
    if utc(catalog['history_start'])>=utc(catalog['history_end_exclusive']):raise ValueError(kind+' source history interval is reversed')
    if catalog.get('observed_at') and utc(catalog['observed_at'])<utc(catalog['history_end_exclusive']):
        raise ValueError(kind+' observation predates its completed source endpoint')
    if not frame.empty:
        start=utc(catalog['history_start']).normalize()
        earliest=start-pd.Timedelta(days=start.day_of_week)
        if (frame.week_start<earliest).any():raise ValueError(kind+' weekly keys precede the declared source interval')
    if catalog.get('history_end_exclusive') and not frame.empty:
        if (frame.week_start>=utc(catalog['history_end_exclusive'])).any():
            raise ValueError(kind+' weekly keys exceed the declared completed-date source interval')
    catalog.update(feature_file=str(feature),feature_sha256=sha(feature),rows_available=len(frame))
    if kind=='research':
        if not {'seed_v1_retrieval_complete','historical_feature_eligible','strict_v1_candidate_count'}<=set(frame):
            raise ValueError('Research weekly quality columns are missing')
        if booleans(frame.historical_feature_eligible,'research eligibility').any():raise ValueError('Unreviewed research cannot be primary enabled')
        valid=booleans(frame.seed_v1_retrieval_complete,'research seed retrieval')
        columns=[name for name in frame if name.endswith('_count') or name=='strict_candidates_available_by_assumed_cutoff']
        frame=mask_observations(frame,valid,columns,'observed_partial_')
        catalog.update(relevance_status='machine_candidate_matches_require_review',historical_public_availability='assumed_lag_not_certified')
    elif kind=='git':
        required={'complete_utc_week','reachable_coverage_days','first_parent_coverage_days','historical_public_availability','primary_eligible'}
        if not required<=set(frame):raise ValueError('Contributor source lacks coverage/availability flags')
        if booleans(frame.primary_eligible,'Git primary eligibility').any() or not frame.historical_public_availability.eq('unknown').all():
            raise ValueError('Contributor source eligibility semantics changed')
        complete=booleans(frame.complete_utc_week,'Git calendar week')
        if {'window_start_utc','window_end_exclusive_utc'}<=set(frame):
            window_start=pd.to_datetime(frame.window_start_utc,utc=True,errors='raise')
            window_end=pd.to_datetime(frame.window_end_exclusive_utc,utc=True,errors='raise')
            if (complete & (~window_start.eq(frame.week_start) | ~window_end.eq(frame.week_start+pd.Timedelta(days=7)))).any():
                raise ValueError('Contributor full-week flag contradicts actual event window')
        valid=complete & frame.reachable_coverage_days.eq(7) & frame.first_parent_coverage_days.eq(7)
        catalog['excluded_partial_calendar_rows']=int((~valid).sum())
        frame=frame.loc[valid].copy()
        catalog.update(historical_public_availability='unknown',identity_policy=manifest.get('identity_policy'))
    elif kind=='dependency':
        if not {'dep_inbound_event_time_eligible','dep_outbound_event_time_eligible'}<=set(frame):
            raise ValueError('Dependency source lacks independent event-time eligibility flags')
        inbound=booleans(frame.dep_inbound_event_time_eligible,'dependency inbound')
        outbound=booleans(frame.dep_outbound_event_time_eligible,'dependency outbound')
        inbound_columns=[name for name in frame if name.startswith(('dep_ever_','dep_new_','dep_declaring_','dep_latest_uploaded_release_declaring_'))]
        outbound_columns=[name for name in frame if name.startswith('dep_outbound_') and not name.endswith('_eligible')]
        frame=mask_observations(frame,inbound,inbound_columns,'observed_')
        frame=mask_observations(frame,outbound,outbound_columns,'observed_')
        catalog.update(historical_public_availability='immutable_provider_upload_dates_retrospectively_collected',
                       historical_retrieval_vintage=False,measurement='Published requirements; no installed-user census')
    elif kind=='downloads':
        # Rich composition must never use incomplete daily/source windows.
        flag=next((name for name in ['rich_week_complete','coverage_complete','download_coverage_complete','complete_week','source_complete'] if name in frame),None)
        if flag is None:raise ValueError('Download dimensions lack a weekly coverage flag')
        valid=booleans(frame[flag],'download dimension coverage')
        for column in ['download_coverage_days','query_coverage_days']:
            if column in frame and (valid & ~frame[column].eq(7)).any():
                raise ValueError('Complete rich-download week lacks seven retrieved days')
        columns=[name for name in frame if pd.api.types.is_numeric_dtype(frame[name])
            and not pd.api.types.is_bool_dtype(frame[name]) and name not in {flag,'feature_cutoff'}
            and not name.endswith(('_eligible','_complete'))]
        frame=mask_observations(frame,valid,columns,'observed_')
        catalog['historical_public_availability']='provider_event_dates_retrospectively_collected'
    elif kind=='releases':
        required={'retrieval_complete','published_time_complete','historical_primary_eligible','pypi_release_correspondence_verified','week_complete'}
        if not required<=set(frame):raise ValueError('Repository releases lack date, coverage or correspondence flags')
        if booleans(frame.historical_primary_eligible,'release eligibility').any() or booleans(frame.pypi_release_correspondence_verified,'release package correspondence').any():
            raise ValueError('Repository releases cannot certify historical or selected-distribution correspondence')
        if not booleans(frame.week_complete,'release full UTC week').all():raise ValueError('Partial release weeks belong in their separate source table')
        base_valid=booleans(frame.retrieval_complete,'release retrieval') & booleans(frame.published_time_complete,'release publication time')
        for column in SOURCE_SPECS[kind]['defaults']:
            if column not in frame:raise ValueError('Repository release feature is missing')
            flag=column+'_complete'
            valid=base_valid & (booleans(frame[flag],flag) if flag in frame else True)
            if (valid & frame[column].isna()).any():raise ValueError('Release completeness contradicts a missing count')
            frame=mask_observations(frame,valid,[column],'observed_partial_')
        catalog.update(event_time_field='published_at',created_at_semantics='Provider commit date, not release publication',
            historical_public_availability='retrospective_current_visible_release_objects',pypi_release_correspondence_verified=False)
    elif kind=='github_api':
        if 'historical_primary_eligible' in frame and booleans(frame.historical_primary_eligible,'GitHub API eligibility').any():
            raise ValueError('Direct API visibility cannot silently enable primary history')
        if 'historical_visibility_complete' in frame and booleans(frame.historical_visibility_complete,'GitHub API historical visibility').any():
            raise ValueError('Current API records cannot certify complete historical visibility')
        flag=next((name for name in ['coverage_complete','source_complete','retrieval_complete'] if name in frame),None)
        if flag is None:raise ValueError('GitHub API weekly features lack explicit retrieval coverage')
        valid=booleans(frame[flag],'GitHub API coverage')
        columns=[name for name in frame if name.startswith('github_') and pd.api.types.is_numeric_dtype(frame[name])
            and not pd.api.types.is_bool_dtype(frame[name]) and not name.endswith('_complete')]
        feature_masks={name:name+'_complete' if name+'_complete' in frame else flag for name in columns}
        for name,mask in feature_masks.items():
            feature_valid=booleans(frame[mask],'GitHub per-feature coverage')
            if mask!=flag and (feature_valid & frame[name].isna()).any():
                raise ValueError('GitHub feature completeness contradicts a missing count: '+name)
            frame=mask_observations(frame,feature_valid,[name],'observed_partial_')
        catalog['per_feature_retrieval_masks']=feature_masks
        catalog['historical_public_availability']='current_visible_objects_not_full_historical_vintage'
    frame['record_present']=True
    frame['source_collection_complete']=bool(catalog['collection_complete'])
    frame['source_observed_at']=catalog.get('observed_at')
    frame['source_manifest_sha256']=fingerprint
    frame['source_history_start']=catalog.get('history_start')
    frame['source_history_end_exclusive']=catalog.get('history_end_exclusive')
    frame['historical_primary_eligible']=False
    if sha(path)!=fingerprint:raise ValueError(kind+' source changed while reading; retry after collection checkpoint')
    catalog['feature_columns']=[name for name in frame if name not in {'tool_id','week_start'}]
    return frame,catalog,(path,fingerprint)


def join_source(base,frame,kind,catalog):
    keys=['tool_id','week_start'];before=base[keys].copy()
    if frame is None:
        values={kind+'__'+column:np.nan for column in SOURCE_SPECS[kind]['defaults']}
        values.update({kind+'__record_present':False,kind+'__source_collection_complete':False,
            kind+'__historical_primary_eligible':False,kind+'__source_observed_at':catalog.get('observed_at'),
            kind+'__source_manifest_sha256':catalog.get('manifest_sha256'),kind+'__source_history_start':catalog.get('history_start'),
            kind+'__source_history_end_exclusive':catalog.get('history_end_exclusive')})
        base=pd.concat([base,pd.DataFrame(values,index=base.index)],axis=1)
        catalog['matched_grid_rows']=0
        catalog['missing_grid_rows']=len(base)
        return base
    payload=[name for name in frame if name not in keys]
    renamed=frame.rename(columns={name:kind+'__'+name for name in payload})
    out=base.merge(renamed,on=keys,how='left',validate='one_to_one',sort=False)
    if len(out)!=len(base) or not out[keys].equals(before):raise ValueError('Source join changed the frozen primary grid')
    for field in ['record_present','source_collection_complete','historical_primary_eligible']:
        out[kind+'__'+field]=out[kind+'__'+field].astype('boolean').fillna(False).astype(bool)
    catalog['matched_grid_rows']=int(out[kind+'__record_present'].sum())
    catalog['missing_grid_rows']=len(out)-catalog['matched_grid_rows']
    return out


def dictionary(frame,primary_allowed):
    result={}
    for column in frame:
        source,_,name=column.partition('__')
        if not name:
            result[column]={'source':'join_key','role':'identity_or_time','dtype':str(frame[column].dtype)};continue
        role='quality_or_provenance' if any(token in name for token in ['eligible','complete','missing','observed_at','manifest_sha','coverage','status','available_at']) else 'observed_feature'
        if source=='primary' and any(token in name for token in ['target_','future_','outcome_','label_available']):role='target_or_future_label_not_predictor'
        if name=='attention_adoption_gap':role='deferred_exploratory_score'
        result[column]={'source':source,'source_column':name,'role':role,'dtype':str(frame[column].dtype),
            'nonmissing_rows':int(frame[column].notna().sum()),
            'allowed_in_existing_primary_models':source=='primary' and name in primary_allowed,
            'null_meaning':'Unknown, ineligible, unavailable source, or required warmup; never imputed to zero by the assembler.'}
    return result


def assemble(primary,output,sources,expected_tools=30):
    primary,output=Path(primary).resolve(),Path(output).resolve()
    if output.exists():raise FileExistsError('Use a fresh full-scope output directory')
    source_paths=[primary]+[Path(path).resolve() for path in sources.values() if path]
    if any(output.is_relative_to(path) for path in source_paths):raise ValueError('Output must be outside every immutable source directory')
    frame,registry,cohort,primary_catalog,allowed=primary_panel(primary,expected_tools)
    keys=['tool_id','week_start'];ids=set(frame.tool_id)
    target=frame.rename(columns={name:'primary__'+name for name in frame if name not in keys})
    target['feature_cutoff']=pd.to_datetime(frame.feature_cutoff,utc=True)
    catalog={'primary':primary_catalog};checkpoints=[];supplemental_files={}
    for kind in SOURCE_SPECS:
        extra,info,checkpoint=inspect_source(kind,sources.get(kind),cohort,ids)
        if info.get('referenced_primary_manifest_sha256',primary_catalog['manifest_sha256'])!=primary_catalog['manifest_sha256']:
            raise ValueError(kind+' frozen-primary reference differs from the selected primary snapshot')
        target=join_source(target,extra,kind,info);catalog[kind]=info
        if checkpoint:checkpoints.append(checkpoint)
    for kind in SUPPLEMENT_SPECS:
        info,payload,pins=inspect_supplement(kind,sources.get(kind),cohort,ids)
        catalog[kind]=info;supplemental_files.update(payload);checkpoints.extend(pins)
    target['exploratory__attention_adoption_gap']=np.nan
    target['exploratory__gap_status']='deferred_pending_research_relevance_and_historical_availability_review'
    target=target.sort_values(keys).reset_index(drop=True)
    for path,expected in checkpoints:
        if sha(path)!=expected:raise ValueError('Source checkpoint changed during assembly; retry')
    if sha(primary/'study_manifest.json')!=primary_catalog['manifest_sha256']:
        raise ValueError('Primary snapshot changed during assembly')
    catalog_complete=all(value.get('collection_complete',False) for value in catalog.values())
    coverage={'profile':PROFILE,'status':'assembled_with_source_limits' if catalog_complete else 'assembled_partial',
        'created_at':datetime.now(timezone.utc).isoformat(),'registry_sha256':cohort,
        'tools':len(ids),'rows':len(target),'first_week':target.week_start.min().isoformat(),
        'last_week':target.week_start.max().isoformat(),'source_catalog':catalog,
        'all_seed_sources_retrieval_complete':catalog_complete,'original_goal_fully_validated':False,
        'research_relevance_manually_validated':False,'historical_point_in_time_complete':False,
        'primary_snapshot_changed':False,'primary_model_promoted':False,'gap_score_enabled':False,
        'gap_score_reason':'A research/development/adoption gap would overstate unreviewed and retrospectively observed sources.',
        'join_policy':'Exact frozen tool_id and MondayUTC keys; missing source values remain NULL; no forward fill or all-source zero imputation.'}
    output.parent.mkdir(parents=True,exist_ok=True)
    temporary=Path(tempfile.mkdtemp(prefix='.full-scope-spine-',dir=output.parent))
    try:
        target.to_csv(temporary/'full_scope_weekly.csv',index=False)
        target.to_parquet(temporary/'full_scope_weekly.parquet',index=False)
        (temporary/'tools.study.json').write_bytes((primary/'tools.study.json').read_bytes())
        for name,(path,expected) in supplemental_files.items():
            raw=path.read_bytes()
            if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('Supplemental source changed before snapshot copy')
            (temporary/name).write_bytes(raw)
        definitions=dictionary(target,allowed)
        (temporary/'data_dictionary.json').write_text(json.dumps(definitions,indent=2,sort_keys=True)+'\n')
        pd.DataFrame([{'column':name,**value} for name,value in definitions.items()]).to_csv(temporary/'data_dictionary.csv',index=False)
        (temporary/'source_coverage.json').write_text(json.dumps(coverage,indent=2,sort_keys=True,default=str,allow_nan=False)+'\n')
        manifest={'profile':PROFILE,'status':coverage['status'],'created_at':coverage['created_at'],
            'registry_sha256':cohort,'primary_manifest_sha256':primary_catalog['manifest_sha256'],
            'assembler_sha256':sha(Path(__file__)),'rows':len(target),'columns':len(target.columns),
            'all_seed_sources_retrieval_complete':catalog_complete,'original_goal_fully_validated':False,
            'primary_snapshot_changed':False,'primary_model_promoted':False,
            'files':{path.name:sha(path) for path in temporary.iterdir() if path.is_file()}}
        (temporary/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
        temporary.rename(output)
    except BaseException:
        shutil.rmtree(temporary,ignore_errors=True);raise
    return manifest


def main():
    root=Path.home()/'6893/data'
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--primary',type=Path,default=root/'study/latest')
    parser.add_argument('--research',type=Path,default=root/'raw/research_v1/backfill')
    parser.add_argument('--contributors',type=Path,default=root/'raw/github_git_contributors/latest')
    parser.add_argument('--dependencies',type=Path,default=root/'full_scope/dependency_history')
    parser.add_argument('--downloads',type=Path,default=root/'full_scope/download_dimensions')
    parser.add_argument('--github-api',type=Path)
    parser.add_argument('--releases',type=Path,default=root/'full_scope/github_releases')
    parser.add_argument('--stars',type=Path,default=root/'raw/github_direct_stars/backfill')
    parser.add_argument('--depsdev',type=Path,default=root/'raw/depsdev/full-scope-initial')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=assemble(args.primary,args.output,{'research':args.research,'git':args.contributors,
        'dependency':args.dependencies,'downloads':args.downloads,'github_api':args.github_api,
        'stars':args.stars,'depsdev':args.depsdev,'releases':args.releases})
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
