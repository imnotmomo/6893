#!/usr/bin/env python3
"""Build, validate and atomically publish a real c0 study snapshot.

No paid queries are submitted here. The only optional collection work finishes
up to forty due public PyPI metadata jobs under the existing collection lock.
Research candidates remain explicitly ineligible for historical forecasting.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import uuid

import numpy as np
import pandas as pd


ROOT=Path.home()/'6893'
SCRIPTS=Path(__file__).resolve().parent
ALLOWED_STATES={'done','split','reused','superseded'}
UI_MODEL_FILES=('predictions.parquet','latest_forecasts.parquet','future_predictions.parquet',
                'metrics.json','metrics.csv','per_week_metrics.csv','per_tool_errors.csv',
                'measurement_boundary_metrics.json','measurement_boundary_metrics.csv',
                'block_bootstrap_vs_persistence.json','block_bootstrap_vs_persistence.csv',
                'source_ablation_metrics.json','source_ablation_metrics.csv','source_ablation_per_tool.csv',
                'folds.json','model_config.json','feature_groups.json','final_models.json')
PRIMARY_MODELS={'persistence','training_median','trend8','ridge_dl','ridge_dl_release','histgb_dl','histgb_dl_release'}
PRIMARY_FEATURES={'dl_downloads','dl_log_downloads','dl_log_change_1w','dl_log_change_4w','dl_log_change_8w',
                  'dl_log_median_4w','dl_missing',*(f'dl_lag_{lag}' for lag in range(8)),
                  'release_count','release_count_4w','release_missing','tool_age_weeks'}


class NotReady(RuntimeError):
    pass


def utc(value=None):
    stamp=pd.Timestamp(value or datetime.now(timezone.utc))
    if pd.isna(stamp):raise ValueError('Missing UTC timestamp')
    return stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')


def encode(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,default=str,allow_nan=False)


def read_json(path):
    result=json.loads(path.read_text())
    if not isinstance(result,dict):raise ValueError('Expected JSON object: '+str(path))
    return result


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('w') as stream:
            stream.write(encode(value)+'\n');stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)


def sha256(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def guard_space(path,minimum):
    free=shutil.disk_usage(path).free
    if free<minimum*1024**3:
        raise RuntimeError(f'Free disk {free/1024**3:.2f} GiB is below {minimum} GiB guard')


@contextmanager
def file_lock(path,deadline=None,wait=False):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as stream:
        while True:
            try:
                fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB);break
            except BlockingIOError:
                if not wait:raise RuntimeError('Another process holds '+str(path))
                if deadline is not None and time.monotonic()>=deadline:
                    raise TimeoutError('Timed out waiting for '+str(path))
                time.sleep(1)
        try:yield
        finally:fcntl.flock(stream,fcntl.LOCK_UN)


def load_backfill():
    sys.path.insert(0,str(SCRIPTS))
    sys.path.insert(0,str(ROOT/'duckdb'))
    sys.path.insert(0,str(ROOT/'pipeline'))
    import backfill
    return backfill


def core_ready(status,plan_sha,asof):
    reasons=[]
    if status.get('plan_sha256')!=plan_sha:reasons.append('collector plan differs from frozen cohort')
    if status.get('tool_count')!=30:reasons.append('collector cohort is not thirty tools')
    try:
        if utc(status['history_end_exclusive']).date()<asof.date():reasons.append('history endpoint is behind the study date')
    except (KeyError,ValueError,TypeError):reasons.append('missing valid history endpoint')
    for source in ('pypi','gharchive'):
        states=[row for row in status.get('states',[]) if row.get('source')==source and int(row.get('jobs',0))>0]
        if not states:reasons.append(source+' jobs absent')
        elif any(row.get('state') not in ALLOWED_STATES for row in states):reasons.append(source+' still has unfinished jobs')
    return not reasons,reasons


def wait_for_core(data,plan_sha,asof,deadline,wait,poll_seconds):
    previous=None
    while True:
        try:status=read_json(data/'backfill-status.json')
        except (OSError,ValueError) as error:status={};reason=[type(error).__name__+': '+str(error)]
        else:_,reason=core_ready(status,plan_sha,asof)
        ready,_=core_ready(status,plan_sha,asof)
        if ready:return status
        signature=encode({'reasons':reason,'states':status.get('states',[])})
        if signature!=previous:
            print(encode({'phase':'waiting_for_core','reasons':reason,'states':status.get('states',[])}),flush=True);previous=signature
        if not wait:raise NotReady('; '.join(reason))
        if time.monotonic()>=deadline:raise TimeoutError('Core collection did not become ready within the wait limit')
        time.sleep(min(poll_seconds,max(0,deadline-time.monotonic())))


def finish_due_releases(b,data,config,registry,plan_sha,deadline,minimum,remaining_requests):
    """Free metadata only; release the collection lock before feature building."""
    tools={tool['tool_id']:tool for tool in registry['tools']}
    with file_lock(data/'.pilot-collection.lock',deadline=deadline,wait=True):
        with b.connect(data_dir=data) as con:
            before=int(con.execute('SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget').fetchone()[0])
        while remaining_requests>0:
            if time.monotonic()>=deadline:raise TimeoutError('Release metadata preparation exceeded the wait limit')
            guard_space(data,minimum)
            job=b.next_job(data,plan_sha,'pypi_releases')
            if job is None:break
            remaining_requests-=1
            result=b.releases(tools[job['tool_id']],job['start_date'],job['end_date'])
            if result.get('ok') and result.get('status','ok')=='ok':
                count=b.save(data,job,result)
                print(encode({'phase':'release_metadata','tool_id':job['tool_id'],'rows':count}),flush=True)
            else:
                b.defer(data,job,'retry',result.get('error') or {'message':'PyPI metadata fetch failed'},delay=180)
                print(encode({'phase':'release_metadata','tool_id':job['tool_id'],'status':'retry','error':result.get('error')}),flush=True)
            time.sleep(0.2)
        with b.connect(data_dir=data) as con:
            after=int(con.execute('SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget').fetchone()[0])
        if after!=before:raise RuntimeError('Public metadata preparation unexpectedly changed the query ledger')
        status=b.export_status(data,plan_sha,config)
    return status,remaining_requests


def run_command(command,log_path,timeout_seconds,env):
    print(encode({'phase':'running','command':command,'log':str(log_path)}),flush=True)
    log_path.parent.mkdir(parents=True,exist_ok=True)
    with log_path.open('ab') as log:
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
        try:code=process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(timeout=20)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
            raise TimeoutError('Step exceeded its runtime limit; see '+str(log_path))
    if code:raise RuntimeError(f'Step exited {code}; see {log_path}')


def checked_file(root,relative):
    path=(root/relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError('Missing or out-of-snapshot manifest path: '+str(relative))
    return path


def verify_files(root,files):
    if not isinstance(files,dict) or not files:raise ValueError('Empty artifact hash manifest')
    for relative,expected in files.items():
        if sha256(checked_file(root,relative))!=expected:raise ValueError('Artifact hash mismatch: '+relative)


def validate_coverage(run,registry,asof):
    status=read_json(run/'study_status.json')
    rows=status.get('raw_source_coverage',[]);wanted={tool['tool_id'] for tool in registry['tools']}
    if len(wanted)!=30 or status.get('cohort_size')!=30 or len(rows)!=30 or {row.get('tool_id') for row in rows}!=wanted:
        raise NotReady('Feature coverage does not contain the complete thirty-tool cohort')
    expected_start=utc('2023-01-01').date();expected_days=(asof.date()-expected_start).days
    daily=pd.read_parquet(run/'daily_normalized.parquet')
    required={'tool_id','date','dl_downloads','observed_gh_event_count','gh_event_count','release_count'}
    if not required<=set(daily):raise ValueError('Normalized daily dataset is missing required columns')
    if daily.duplicated(['tool_id','date']).any():raise ValueError('Duplicate normalized tool/day keys')
    if len(daily)!=30*expected_days or set(daily.tool_id)!=wanted:raise NotReady('Normalized daily row coverage is incomplete')
    parsed=pd.to_datetime(daily.date,utc=True,errors='raise')
    if not daily.date.map(lambda value:isinstance(value,str)).all():raise ValueError('Normalized dates must be strings for Spark')
    if parsed.min().date()!=expected_start or parsed.max().date()!=asof.date()-timedelta(days=1):
        raise NotReady('Normalized daily date bounds do not match the frozen study endpoint')
    reasons=[]
    for row in rows:
        tid=row['tool_id'];first=row.get('first_visible_artifact')
        try:first_date=utc(first).date() if first not in (None,'None','NaT','') else None
        except (ValueError,TypeError):first_date=None
        if first_date is None:reasons.append(tid+': first visible artifact is missing');continue
        before=max(0,min(expected_days,(first_date-expected_start).days));expected_downloads=expected_days-before
        frame=daily[daily.tool_id==tid]
        if int(row.get('expected_days',-1))!=expected_days or len(frame)!=expected_days:reasons.append(tid+': calendar coverage differs')
        if int(row.get('github_days',-1))!=expected_days or int(frame.observed_gh_event_count.notna().sum())!=expected_days:
            reasons.append(tid+': archived GitHub response coverage is incomplete; this checks retrieval only')
        if int(row.get('download_days',-1))!=expected_downloads or int(frame.dl_downloads.notna().sum())!=expected_downloads:
            reasons.append(tid+': PyPI daily coverage is incomplete')
        if int(frame.release_count.notna().sum())!=expected_downloads:reasons.append(tid+': release metadata history is incomplete')
        for column in ['dl_downloads','observed_gh_event_count','gh_event_count','release_count']:
            values=pd.to_numeric(frame[column],errors='raise').dropna().to_numpy(dtype=float)
            if not np.isfinite(values).all() or (values<0).any():raise ValueError(tid+': invalid '+column)
    if reasons:raise NotReady('; '.join(reasons))
    verify_files(run,status.get('files'))
    return status


def validate_source_quality(run,asof):
    """Fail closed on semantics separately from retrieval/calendar completeness."""
    quality=read_json(run/'data_quality.json')
    if quality.get('profile')!='validated_primary' or quality.get('validation_status')!='passed':
        raise ValueError('Enabled primary sources did not pass semantic source-quality checks')
    if any(quality.get(name,{}).get('status')!='passed' for name in ('content_checks','raw_spot_checks')):
        raise ValueError('Content checks and raw-source comparisons must both pass')
    if set(quality.get('enabled_feature_groups',[]))!={'dl','release','controls'}:
        raise ValueError('Source-quality policy enables a different primary source set')
    if quality.get('source_policy')!='capture_quality_v1' or utc(quality.get('as_of'))!=asof:
        raise ValueError('Source-quality policy or frozen endpoint differs')
    if quality.get('cohort_sha256')!=sha256(run/'tools.study.json'):
        raise ValueError('Source-quality checks used a different frozen cohort')
    for name in ('daily_normalized.parquet','weekly_features.parquet','source_quality.json'):
        if quality.get('input_hashes',{}).get(name)!=sha256(checked_file(run,name)):
            raise ValueError('Source-quality checks used a different '+name)
    if quality.get('source_policy_sha256')!=sha256(run/'source_quality.json'):
        raise ValueError('Source-quality report does not bind its policy artifact')
    sources=quality.get('sources',{})
    if any(sources.get(name,{}).get('status')!='primary_enabled' for name in ('pypi','pypi_releases')):
        raise ValueError('The primary download/release sources are not enabled')
    if sources.get('gharchive',{}).get('status')!='capture_limited' or sources.get('gharchive',{}).get('primary_eligible') is not False or sources.get('gharchive',{}).get('capture_validated') is not False:
        raise ValueError('Capture-limited GitHub data must remain excluded')
    if sources.get('arxiv',{}).get('status')!='quarantined' or not {'gh','research'}<=set(quality.get('excluded_feature_groups',[])):
        raise ValueError('Unreviewed research must remain quarantined')
    return quality


def validate_models(run,asof):
    model=run/'model_results';manifest=read_json(model/'study_manifest.json')
    if manifest.get('status')!='complete':raise ValueError('Model manifest is incomplete')
    verify_files(model,manifest.get('files'))
    config=read_json(model/'model_config.json');metrics=read_json(model/'metrics.json')
    for name,value in [('configuration',config),('metrics',metrics),('manifest',manifest)]:
        if value.get('profile')!='validated_primary' or value.get('analysis_type')!='post_audit_corrective_sensitivity':
            raise ValueError(name+' is not a corrective primary-source study')
        if value.get('previously_examined_2026') is not True:
            raise ValueError(name+' must disclose that 2026 outcomes were previously examined')
        if not {'gh','research'}<=set(value.get('excluded_feature_groups',[])):
            raise ValueError(name+' did not exclude capture-limited or unreviewed predictors')
        gate=value.get('quality_gate',{})
        if gate.get('validation_status')!='passed':raise ValueError(name+' did not pass the source-quality gate')
        for key,path in [('quality_report_sha256',run/'data_quality.json'),('source_policy_sha256',run/'source_quality.json'),
                         ('weekly_features_sha256',run/'weekly_features.parquet'),('cohort_sha256',run/'tools.study.json')]:
            if gate.get(key)!=sha256(path):raise ValueError(name+' is bound to a different '+key)
    if config.get('source_quality_policy')!='capture_quality_v1':
        raise ValueError('Models do not declare the required source-quality policy')
    groups=config.get('feature_groups',{})
    if groups.get('gh') or groups.get('research'):
        raise ValueError('Primary model feature groups include excluded sources')
    columns=[column for group in groups.values() for column in group]
    if not columns or any(str(column).removeprefix('feature_') not in PRIMARY_FEATURES for column in columns):
        raise ValueError('Primary feature groups contain a column outside the exact safe allowlist')
    recipes=config.get('recipes',{})
    if not isinstance(recipes,dict) or set(recipes)!={'ridge_dl','ridge_dl_release','histgb_dl','histgb_dl_release'}:
        raise ValueError('The required four fixed primary algorithm recipes are absent')
    for recipe in recipes.values():
        columns=recipe.get('features',[]) if isinstance(recipe,dict) else []
        if not columns or any(str(column).removeprefix('feature_') not in PRIMARY_FEATURES for column in columns):
            raise ValueError('A primary model recipe includes an excluded or unknown predictor')
    feature=run/'weekly_features.parquet';fingerprint=hashlib.sha256();fingerprint.update(feature.name.encode())
    with feature.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):fingerprint.update(chunk)
    if config.get('input_sha256')!=fingerprint.hexdigest():raise ValueError('Models were fitted on a different feature artifact')
    if config.get('include_research'):raise ValueError('Unreviewed research cannot be enabled by the publisher')
    if utc(config['as_of'])!=asof:raise ValueError('Model as-of differs from the frozen feature cutoff')
    predictions=pd.read_parquet(model/'predictions.parquet');future=pd.read_parquet(model/'latest_forecasts.parquet')
    if set(predictions.model)!=PRIMARY_MODELS or (not future.empty and set(future.model)!=PRIMARY_MODELS):
        raise ValueError('Primary publication does not contain the required seven safe recipes')
    if predictions.empty or predictions.duplicated(['model','tool_id','week_start']).any():raise ValueError('Empty or duplicate backtest predictions')
    if manifest.get('prediction_rows')!=len(predictions) or manifest.get('future_prediction_rows')!=len(future):
        raise ValueError('Model manifest row counts differ from its Parquet files')
    if not pd.read_parquet(model/'future_predictions.parquet').equals(future):
        raise ValueError('The two published future-forecast aliases differ')
    for name,frame in [('backtest',predictions),('future',future)]:
        for column in ['prediction_log_growth','predicted_median_downloads']:
            values=pd.to_numeric(frame[column],errors='raise').to_numpy(dtype=float)
            if not np.isfinite(values).all():raise ValueError(name+' has nonfinite '+column)
        if (frame.predicted_median_downloads<0).any():raise ValueError(name+' predicts negative downloads')
    for column in ['target_log_growth','target_sustained']:
        if not np.isfinite(pd.to_numeric(predictions[column],errors='raise').to_numpy(dtype=float)).all():raise ValueError('Incomplete historical '+column)
    feature_cut=pd.to_datetime(predictions.feature_cutoff,utc=True);train_cut=pd.to_datetime(predictions.train_cutoff,utc=True)
    label_at=pd.to_datetime(predictions.label_available_at,utc=True)
    target_start=pd.to_datetime(predictions.target_window_start,utc=True);target_end=pd.to_datetime(predictions.target_window_end_exclusive,utc=True)
    if (train_cut>feature_cut).any() or (label_at>asof).any() or (target_start<=feature_cut).any() or (target_end-target_start!=pd.Timedelta(days=56)).any():
        raise ValueError('Historical prediction cutoff/outcome timing is invalid')
    if not future.empty:
        if future.duplicated(['model','tool_id','week_start']).any():raise ValueError('Duplicate future forecasts')
        for column in ['target_log_growth','target_sustained','observed_future_median_downloads']:
            if future[column].notna().any():raise ValueError('Future forecast contains an observed target: '+column)
        if (pd.to_datetime(future.target_window_end_exclusive,utc=True)<=asof).any() or (pd.to_datetime(future.label_available_at,utc=True)<=asof).any():
            raise ValueError('Future forecast outcome is already mature')
        if (pd.to_datetime(future.train_cutoff,utc=True)>asof).any():raise ValueError('Future model training occurs after as-of')
    folds=read_json(model/'folds.json')
    for fold in folds.get('folds',[]):
        if fold.get('status')=='evaluated' and utc(fold['maximum_training_label_available_at'])>utc(fold['train_cutoff']):
            raise ValueError('A training fold used unavailable labels')
    final=read_json(model/'final_models.json')
    if utc(final['train_cutoff'])!=asof or utc(final['maximum_training_label_available_at'])>asof:
        raise ValueError('Final models used labels unavailable at the frozen as-of')
    selection=metrics.get('selection',{})
    if selection.get('status')!='selected_from_frozen_validation' or not metrics.get('best_model'):
        raise ValueError('No frozen validation model selection is available')
    if selection.get('best_model')!=metrics['best_model'] or metrics['best_model'] not in set(predictions.model):
        raise ValueError('Published model selection is inconsistent')
    if utc(selection['maximum_label_available_at'])>utc(selection['label_availability_cutoff']):
        raise ValueError('Model selection used labels after its frozen cutoff')
    heldout=predictions[predictions.split=='holdout_2026']
    if heldout.empty or utc(selection['label_availability_cutoff'])>pd.to_datetime(heldout.feature_cutoff,utc=True).min():
        raise ValueError('Frozen selection must precede a nonempty 2026 holdout')
    return metrics,manifest


def validate_spark(run,expected_master):
    report=read_json(run/'spark_validation.json')
    if report.get('ok') is not True or report.get('status')!='matched' or report.get('mismatch_rows')!=0:
        raise ValueError('Spark weekly correctness validation did not match')
    if not report.get('application_id') or report.get('master')!=expected_master or report.get('spark_stop_error'):
        raise ValueError('Spark did not finish successfully on the expected standalone master')
    driver=report.get('cpu_resources',{}).get('driver_host')
    if not driver or set(report.get('executor_hosts',[]))!={driver}:
        raise ValueError('Spark placement does not match the c0-only local-input policy')
    for name,path in [('daily',run/'daily_normalized.parquet'),('reference',run/'weekly_reference.csv')]:
        if report.get('inputs',{}).get(name,{}).get('sha256')!=sha256(path):raise ValueError('Spark compared different '+name+' input')
    return report


def atomic_latest(study_root,run):
    latest=study_root/'latest'
    if os.path.lexists(latest) and not latest.is_symlink():raise RuntimeError('Refusing to replace a non-symlink latest directory')
    relative=os.path.relpath(run,study_root);temporary=study_root/('.latest.'+uuid.uuid4().hex)
    try:temporary.symlink_to(relative,target_is_directory=True);os.replace(temporary,latest)
    finally:temporary.unlink(missing_ok=True)
    try:
        descriptor=os.open(study_root,os.O_RDONLY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    except OSError as error:
        print(encode({'status':'published','durability_warning':type(error).__name__+': '+str(error)}),flush=True)


def previous_snapshot(study_root):
    latest=study_root/'latest'
    if not os.path.lexists(latest):return None
    if not latest.is_symlink():raise ValueError('The previous latest must be an immutable snapshot symlink')
    previous=latest.resolve(strict=True)
    if not previous.is_relative_to((study_root/'runs').resolve()):
        raise ValueError('The previous latest points outside the study run archive')
    manifest=previous/'study_manifest.json'
    return {'snapshot_id':previous.name,'path':os.path.relpath(previous,study_root),
            'manifest_sha256':sha256(manifest) if manifest.is_file() else None,
            'reason':'Source-quality correction: all GH Archive predictors are excluded; unreviewed research remains quarantined.',
            'prior_findings_status':'superseded_primary_claims_not_source_validated'}


def publish(args):
    config=read_json(args.config.expanduser().resolve())
    data=Path(config['data_dir']).expanduser().resolve();data.mkdir(parents=True,exist_ok=True)
    registry_path=(args.registry or ROOT/config['tool_registry']).expanduser().resolve()
    registry=read_json(registry_path)
    if len(registry.get('tools',[]))!=30 or len({tool['tool_id'] for tool in registry['tools']})!=30:
        raise ValueError('Publication requires a frozen thirty-tool registry')
    config['tool_count']=30
    study_root=(args.study_root or data/'study').expanduser().resolve();study_root.mkdir(parents=True,exist_ok=True)
    minimum=max(15,float(config.get('minimum_free_GiB',15)))
    guard_space(data,minimum);asof=utc(args.as_of);deadline=time.monotonic()+args.wait_timeout_seconds
    b=load_backfill();plan_sha=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+b.encode(registry).encode()).hexdigest()
    env=os.environ.copy();env.update(OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2',MKL_NUM_THREADS='2',NUMEXPR_NUM_THREADS='2')
    run=None
    with file_lock(study_root/'.publish.lock'):
        supersedes=previous_snapshot(study_root)
        identifier=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        run=study_root/'runs'/identifier;run.mkdir(parents=True,exist_ok=False)
        try:
            write_json(run/'publication_state.json',{'state':'preparing','snapshot_id':identifier,'as_of':asof})
            shutil.copyfile(registry_path,run/'tools.study.json')
            collection=wait_for_core(data,plan_sha,asof,deadline,args.wait_for_core,args.poll_seconds)
            remaining_requests=40
            while True:
                collection,remaining_requests=finish_due_releases(b,data,config,registry,plan_sha,deadline,minimum,remaining_requests)
                write_json(run/'collection_status.json',collection)
                guard_space(data,minimum)
                run_command([sys.executable,str(SCRIPTS/'build_weekly.py'),'--data-dir',str(data),'--registry',str(run/'tools.study.json'),'--output',str(run),'--as-of',asof.isoformat()],run/'logs/features.log',1200,env)
                try:
                    status=validate_coverage(run,registry,asof)
                    quality=validate_source_quality(run,asof)
                    break
                except NotReady as error:
                    if not args.wait_for_core or time.monotonic()+180>=deadline:raise
                    print(encode({'phase':'waiting_for_coverage','reason':str(error),'release_requests_remaining':remaining_requests}),flush=True)
                    time.sleep(min(180,max(0,deadline-time.monotonic())))
                    collection=wait_for_core(data,plan_sha,asof,deadline,True,args.poll_seconds)
            guard_space(data,minimum)
            run_command([sys.executable,str(SCRIPTS/'model_study.py'),'--input',str(run/'weekly_features.parquet'),'--output-dir',str(run/'model_results'),'--as-of',asof.isoformat(),'--n-jobs','2','--boosted-model',args.boosted_model],run/'logs/models.log',1800,env)
            metrics,model_manifest=validate_models(run,asof)
            for name in UI_MODEL_FILES:shutil.copyfile(checked_file(run/'model_results',name),run/name)
            shutil.copyfile(run/'model_results/study_manifest.json',run/'model_study_manifest.json')
            plot_script=SCRIPTS/'plot_study.py'
            if plot_script.is_file():
                run_command([sys.executable,str(plot_script),'--results-dir',str(run)],run/'logs/figures.log',300,env)
                verify_files(run,read_json(run/'figures/figure_manifest.json')['outputs'])
            guard_space(data,minimum)
            if args.skip_spark:
                write_json(run/'publication_state.json',{'state':'test_only_unpublished','snapshot_id':identifier,'reason':'Spark validation explicitly skipped'})
                print(encode({'status':'test_only_unpublished','run':str(run),'latest_unchanged':True}),flush=True)
                return 0
            wrapper=shutil.which('6893-spark-submit')
            if not wrapper:raise FileNotFoundError('6893-spark-submit is not installed on PATH')
            run_command([wrapper,str(SCRIPTS/'spark_weekly_check.py'),'--data-dir',str(run),'--output',str(run/'spark_validation.json')],run/'logs/spark.log',1200,env)
            spark=validate_spark(run,args.spark_master)
            verify_files(run,status['files']);verify_files(run/'model_results',model_manifest['files'])
            validate_source_quality(run,asof)
            reserved=int(status['budget_reserved_bytes']);limit=int(config['max_total_query_bytes'])
            if reserved>limit:raise ValueError('Cumulative query ledger exceeds its configured cap')
            status.update(snapshot_id=identifier,published_at=utc().isoformat(),publication_status='validated_primary',
                primary_analysis_ready=True,profile='validated_primary',analysis_type='post_audit_corrective_sensitivity',
                previously_examined_2026=True,excluded_feature_groups=['gh','research'],
                retrieval_validation={'status':'passed','meaning':'Requested source responses and normalized panel coverage; not a claim of complete platform activity.'},
                source_validation={'status':'passed_for_enabled_primary_sources','report':'data_quality.json','sha256':sha256(run/'data_quality.json'),'enabled_sources':['pypi','pypi_releases'],'excluded_sources':['gharchive','arxiv']},
                analysis_readiness={'status':'validated_primary','enabled_sources':['pypi','pypi_releases'],'gharchive':'capture_limited_excluded','arxiv':'quarantined_excluded','spark_correctness':'passed'},
                supersedes=supersedes,
                canonical_source_coverage=status['raw_source_coverage'],source_coverage=status['raw_source_coverage'],
                cohort=[{key:tool.get(key) for key in ['tool_id','name','pypi_package','github_repo','category','research_ambiguous']} for tool in registry['tools']],
                collection_states=collection.get('states',[]),collection_status_as_of=collection.get('updated_at'),
                model_selection=metrics['selection'],spark_validation={'ok':True,'application_id':spark['application_id'],'master':spark['master'],'executor_hosts':spark['executor_hosts'],'mismatch_rows':0},
                budget={'limit_usd':config['query_budget_usd'],'reserved_and_known_billed_bytes':reserved,'conservative_query_cost_usd':status['conservative_query_cost_usd'],'scan_byte_cap':limit,'is_invoice':False,'meaning':'Known saved-job billed bytes plus worst-case outstanding reservations; excludes unrelated project charges; not a billing invoice'},
                limitations=[status['source_vintage'],status['download_proxy'],status['download_measurement_break'],
                    'The frozen, present-day thirty-tool cohort is conditional and subject to selection/survivorship bias; results do not establish performance on unseen tools.',
                    'Research text matches remain candidates requiring relevance and version-availability review; historical research predictors are disabled. Research collection may continue after this snapshot.',
                    'Historical dependency features are not enabled without verified date-aligned snapshots.',
                    'GH Archive is capture-limited. All GitHub predictors are excluded from the primary study, including earlier periods without independently established capture completeness. Archived zero counts do not establish inactivity.',
                    'The previous GitHub-based performance claim is superseded. This corrective run uses already-examined 2026 outcomes and is a sensitivity analysis, not an untouched confirmatory test.',
                    'Spark validates actual c0 execution and weekly totals; this is not a multi-host speedup benchmark.',
                    'Latest forecasts are generated at this snapshot time; some outcome windows may already have started. See the recorded window dates.'])
            status['research_provenance']={'historical_features_enabled':False,'status':'candidate_metadata_only','collection_states':[row for row in collection.get('states',[]) if row.get('source')=='arxiv'],'source_evidence':'research_candidates.csv plus retained raw/backfill manifests; candidate matches are not validated library citations'}
            write_json(run/'study_status.json',status)
            write_json(run/'correction_record.json',{'snapshot_id':identifier,'supersedes':supersedes,'analysis_type':'post_audit_corrective_sensitivity','previously_examined_2026':True,'primary_sources':['pypi','pypi_releases'],'excluded_sources':['gharchive','arxiv'],'prior_29_percent_claim':'superseded_not_validated_primary_finding','previous_snapshot_modified':False})
            write_json(run/'publication_state.json',{'state':'validated_primary','snapshot_id':identifier,'as_of':asof,'spark_required':True,'source_quality_required':True})
            guard_space(data,minimum)
            files={str(path.relative_to(run)):sha256(path) for path in sorted(run.rglob('*')) if path.is_file() and path.name!='study_manifest.json'}
            # Preserve the model's original manifest under model_results; include it too.
            files['model_results/study_manifest.json']=sha256(run/'model_results/study_manifest.json')
            write_json(run/'study_manifest.json',{'schema_version':2,'status':'validated_primary','snapshot_id':identifier,'as_of':asof,'plan_sha256':plan_sha,'registry_sha256':sha256(run/'tools.study.json'),'profile':'validated_primary','analysis_type':'post_audit_corrective_sensitivity','previously_examined_2026':True,'excluded_feature_groups':['gh','research'],'source_validation':status['source_validation'],'supersedes':supersedes,'model_selection':metrics['selection'],'spark_application_id':spark['application_id'],'files':files})
            verify_files(run,files)
            atomic_latest(study_root,run)
            print(encode({'status':'published','snapshot_id':identifier,'run':str(run),'latest':str(study_root/'latest'),'spark_application_id':spark['application_id']}),flush=True)
            return 0
        except BaseException as error:
            latest=study_root/'latest'
            already_published=latest.is_symlink() and latest.resolve()==run.resolve()
            if not already_published:
                write_json(run/'publication_failure.json',{'status':'unpublished','error':type(error).__name__+': '+str(error),'at':utc().isoformat()})
            print(encode({'status':'published' if already_published else 'unpublished','run':str(run),'error':type(error).__name__+': '+str(error)}),flush=True)
            raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/backfill.json')
    parser.add_argument('--registry',type=Path)
    parser.add_argument('--study-root',type=Path)
    parser.add_argument('--as-of',help='Frozen UTC feature/evaluation endpoint; defaults to launch time')
    parser.add_argument('--wait-for-core',action='store_true')
    parser.add_argument('--wait-timeout-seconds',type=int,default=5400)
    parser.add_argument('--poll-seconds',type=int,default=20)
    parser.add_argument('--boosted-model',choices=['histgb','xgboost'],default='histgb')
    parser.add_argument('--spark-master',default='spark://10.0.0.132:7077')
    parser.add_argument('--skip-spark',action='store_true',help='Test preparation only: always leaves the run unpublished and latest unchanged')
    args=parser.parse_args()
    if not 1<=args.poll_seconds<=60 or not 1<=args.wait_timeout_seconds<=5400:parser.error('Use poll seconds 1–60 and wait limit 1–5400 seconds')
    try:return publish(args)
    except Exception as error:
        print(encode({'status':'failed','error':type(error).__name__+': '+str(error)}),file=sys.stderr,flush=True)
        return 1


if __name__=='__main__':raise SystemExit(main())
