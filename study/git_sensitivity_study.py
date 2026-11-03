#!/usr/bin/env python3
"""Separate posthoc Git-history sensitivity study; never publishes primary results.

Surviving current-default-branch commits grouped by committer date do not prove
historical public availability and do not reconstruct GitHub PushEvents.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import model_study as core

PROFILE = 'retrospective_git_sensitivity'
ANALYSIS = 'posthoc_retrospective_git_sensitivity'
COUNTS = ['git_reachable_commit_count', 'git_first_parent_commit_count']
GIT_FEATURES = COUNTS + [name + suffix for name in COUNTS for suffix in ('_log1p', '_sum_4w', '_log1p_sum_4w')]
NOTE = ('Posthoc sensitivity analysis of previously examined 2026 outcomes. Current surviving default-branch '
        'history and committer dates do not certify historical public availability, recover deleted/rewritten '
        'history, or restore GitHub PushEvents. No primary model selection or promotion is performed.')


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError('Expected JSON object: ' + str(path))
    return value


def verify_files(root, manifest):
    root = Path(root).resolve()
    if not manifest.get('files'):
        raise ValueError('A nonempty file hash manifest is required')
    for relative, expected in manifest['files'].items():
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file() or sha(path) != expected:
            raise ValueError('Missing or changed manifest artifact: ' + relative)


def validate_primary(root):
    root = Path(root).resolve()
    manifest = read(root / 'study_manifest.json')
    if (manifest.get('status') != 'validated_primary' or manifest.get('profile') != 'validated_primary'
            or manifest.get('analysis_type') != 'post_audit_corrective_sensitivity'):
        raise ValueError('A corrected published primary snapshot is required')
    verify_files(root, manifest)
    config, metrics = read(root / 'model_config.json'), read(root / 'metrics.json')
    asof = core.utc_timestamp(metrics['as_of'])
    quality, policy, binding = core.validate_quality_gate(root / 'weekly_features.parquet', asof)
    if sha(root / 'tools.study.json') != binding['cohort_sha256']:
        raise ValueError('Primary cohort registry hash differs')
    if config.get('feature_groups', {}).get('gh') or config.get('feature_groups', {}).get('research'):
        raise ValueError('Primary snapshot includes excluded predictors')
    for artifact in (config, metrics):
        if artifact.get('profile') != 'validated_primary' or artifact.get('quality_gate') != {'validation_status':'passed', **binding}:
            raise ValueError('Primary model quality binding differs')
    return manifest, config, metrics, read(root / 'tools.study.json'), asof


def validate_git(root, expected_sha, registry_sha, ids):
    root = Path(root).resolve()
    if sha(root / 'manifest.json') != expected_sha:
        raise ValueError('Git manifest differs from the explicitly frozen SHA256')
    manifest = read(root / 'manifest.json')
    if (manifest.get('status') != 'complete' or manifest.get('source') != 'github_git_default_branch'
            or manifest.get('profile') != 'retrospective_current_default_branch' or manifest.get('requested_tools') != 30
            or manifest.get('completed_tools') != 30 or len(ids) != 30
            or manifest.get('registry_sha256') != registry_sha
            or manifest.get('primary_eligible') is not False
            or manifest.get('historical_public_availability') != 'unknown'
            or manifest.get('retrospective') is not True):
        raise ValueError('Git source must be complete for the bound thirty-tool retrospective registry')
    verify_files(root, manifest)
    tools = manifest.get('tools', [])
    if len(tools) != 30 or {row['tool_id'] for row in tools} != set(ids):
        raise ValueError('Git tool identities differ from the primary cohort')
    for tool in tools:
        if (tool.get('status') != 'complete' or tool.get('shallow') is not False
                or tool.get('registry_sha256') != registry_sha
                or tool.get('historical_public_availability') != 'unknown'
                or tool.get('primary_eligible') is not False
                or not isinstance(tool.get('date_anomalies'), dict)
                or 'committer' not in tool.get('date_basis', '')):
            raise ValueError('Git tool history, timestamp evidence or availability is incomplete')
        metadata = (root / tool['metadata_path']).resolve()
        if not metadata.is_relative_to(root) or sha(metadata) != tool['metadata_sha256']:
            raise ValueError('Git commit metadata hash differs')
    return manifest


def git_features(frame):
    frame = frame.copy()
    required = {'tool_id', 'week_start_utc', 'window_start_utc', 'window_end_exclusive_utc',
                'reachable_coverage_days', 'first_parent_coverage_days', 'complete_utc_week',
                'historical_public_availability', 'primary_eligible', *COUNTS}
    if not required <= set(frame):
        raise ValueError('Git weekly schema is incomplete')
    for old, new in [('week_start_utc','week_start'), ('window_start_utc','window_start'),
                     ('window_end_exclusive_utc','window_end')]:
        frame[new] = pd.to_datetime(frame[old], utc=True, errors='raise')
    if frame.duplicated(['tool_id','week_start']).any() or frame.week_start.isna().any():
        raise ValueError('Duplicate or missing Git tool/week keys')
    if (not frame.week_start.dt.dayofweek.eq(0).all()
            or not frame.week_start.eq(frame.week_start.dt.normalize()).all()
            or not frame.historical_public_availability.eq('unknown').all()
            or not frame.primary_eligible.eq(False).all()):
        raise ValueError('Git dates or historical availability semantics are invalid')
    complete = (frame.window_start.eq(frame.week_start)
                & frame.window_end.eq(frame.week_start + pd.Timedelta(days=7))
                & frame.reachable_coverage_days.eq(7) & frame.first_parent_coverage_days.eq(7))
    flag = core._flag(frame.complete_utc_week, 'complete_utc_week')
    if not flag.eq(complete).all():
        raise ValueError('Git complete-week flag differs from UTC coverage bounds')
    for column in COUNTS:
        values = pd.to_numeric(frame[column], errors='raise')
        valid = values.dropna()
        if not np.isfinite(valid).all() or (valid < 0).any() or (valid != np.floor(valid)).any():
            raise ValueError('Git counts must be finite nonnegative integers or unknown')
        if values[complete].isna().any():
            raise ValueError('A complete Git week has missing counts')
        frame[column] = values.where(complete)
    if (frame[COUNTS[1]] > frame[COUNTS[0]]).any():
        raise ValueError('First-parent count exceeds reachable commits')
    frame = frame.sort_values(['tool_id','week_start']).reset_index(drop=True)
    for _, group in frame.groupby('tool_id', sort=False):
        if not group.week_start.diff().dropna().eq(pd.Timedelta(days=7)).all():
            raise ValueError('Git weekly grid has gaps; rolling windows cannot bridge gaps')
    for column in COUNTS:
        frame[column + '_log1p'] = np.log1p(frame[column])
        frame[column + '_sum_4w'] = frame.groupby('tool_id', sort=False)[column].transform(
            lambda values: values.rolling(4, min_periods=4).sum())
        frame[column + '_log1p_sum_4w'] = np.log1p(frame[column + '_sum_4w'])
    frame['git_history_eligible'] = np.isfinite(frame[GIT_FEATURES].to_numpy(dtype=float)).all(axis=1)
    return frame[['tool_id','week_start', 'git_history_eligible', *GIT_FEATURES]]


def recipes(primary_config):
    result = {}
    for algorithm in ('ridge','histgb'):
        baseline = primary_config['recipes'][algorithm + '_dl_release']
        columns = list(baseline['features'])
        if baseline['kind'] != algorithm or any(column.removeprefix('feature_') not in core.CORE_FEATURE_ALLOWLIST for column in columns):
            raise ValueError('Unsafe or mismatched primary reference recipe')
        result[algorithm + '_dl_release_common'] = {'kind':algorithm, 'features':columns}
        result[algorithm + '_dl_release_git'] = {'kind':algorithm, 'features':columns + GIT_FEATURES}
    return result


def references_for_keys(primary_predictions, keys):
    result = primary_predictions.merge(keys[['tool_id','week_start']], on=['tool_id','week_start'], how='inner', validate='many_to_one')
    if set(result.model) != set(core.MODEL_IDS) or len(result) != len(keys) * len(core.MODEL_IDS):
        raise ValueError('Published primary references do not cover identical common evaluation keys')
    result['primary_reference_model'] = result.model
    result['model'] = 'published_' + result.model
    result['prediction_origin'] = 'unchanged_published_primary_reference'
    return result


def classify(frame):
    return frame.assign(profile=PROFILE, analysis_type=ANALYSIS, previously_examined_2026=True,
                        primary_eligible=False, historical_public_availability='unknown')


def run(primary, git_source, output, git_manifest_sha256, n_jobs=2):
    primary, git_source, output = [Path(value).resolve() for value in (primary,git_source,output)]
    if output.is_relative_to(primary) or output.is_relative_to(git_source) or output.exists():
        raise ValueError('Use a new separate output directory outside both immutable source snapshots')
    _, config, primary_metrics, registry, asof = validate_primary(primary)
    git_manifest = validate_git(git_source, git_manifest_sha256, sha(primary/'tools.study.json'), [row['tool_id'] for row in registry['tools']])
    frame = core.normalize_frame(pd.read_parquet(primary/'weekly_features.parquet'))
    frame = frame.merge(git_features(pd.read_csv(git_source/'weekly_git_history.csv')),
                        on=['tool_id','week_start'], how='left', validate='one_to_one')
    eligible = frame.git_history_eligible.fillna(False).astype(bool)
    original = pd.read_parquet(primary/'predictions.parquet')
    original['week_start'] = pd.to_datetime(original.week_start,utc=True)
    original_keys = original[['tool_id','week_start']].drop_duplicates()
    recipe = recipes(config)
    parts, fold_records = [], []
    folds = read(primary/'folds.json')['folds']
    evaluated = [fold for fold in folds if fold['status']=='evaluated']
    if len(evaluated) != 20 or config['protocol']['min_train_weeks'] != 52:
        raise ValueError('Expected the frozen twenty monthly primary folds with 52-week minimum history')
    for fold in evaluated:
        cutoff = core.utc_timestamp(fold['train_cutoff'])
        first = min(core.utc_timestamp(value) for value in fold['evaluation_weeks'])
        train = core.select_train_rows(frame, cutoff, first)
        train = train.loc[train.git_history_eligible.fillna(False).astype(bool)]
        if train.week_start.nunique() < 52 or len(train) < 20:
            raise ValueError('Insufficient common Git training history in ' + fold['fold_id'])
        test = frame.merge(original_keys, on=['tool_id','week_start'], how='inner', validate='one_to_one')
        test = test.loc[test.week_start.isin(pd.to_datetime(fold['evaluation_weeks'],utc=True))
                        & test.git_history_eligible.fillna(False).astype(bool)].copy()
        if test.empty:
            raise ValueError('No common evaluation rows in ' + fold['fold_id'])
        fitted = core._fit_recipes(train, recipe, n_jobs)
        with core.limited_threads(n_jobs):
            predicted = {name:fitted[name].predict(core._numeric_features(test, item['features'])) for name,item in recipe.items()}
        lower = -np.log1p(np.median(test[core.lag_columns(test)].to_numpy(dtype=float),axis=1))
        predicted = {name:np.maximum(values,lower) for name,values in predicted.items()}
        new = core.prediction_records(test,predicted,cutoff,train,datetime.now(timezone.utc))
        new['prediction_origin'] = 'retrospective_git_common_training_refit'
        refs = references_for_keys(original,test)
        parts.extend([classify(new),classify(refs)])
        fold_records.append({**fold,'original_train_row_count':fold['train_row_count'],
                             'common_train_row_count':len(train),'common_train_week_count':train.week_start.nunique(),
                             'common_maximum_training_label_available_at':train.label_available_at.max(),
                             'common_evaluation_row_count':len(test)})
        print('Evaluated retrospective Git sensitivity '+fold['fold_id'],flush=True)
    predictions = core.add_prediction_ranks(pd.concat(parts,ignore_index=True))
    core.assert_same_evaluation_rows(predictions)
    results, weekly, tools = core.calculate_metrics(predictions)
    ablations = []
    for algorithm in ('ridge','histgb'):
        baseline, augmented = algorithm+'_dl_release_common',algorithm+'_dl_release_git'
        intervals = core.paired_block_bootstrap(weekly,reference=baseline)
        for split in sorted(predictions.split.unique()):
            first = next(row for row in results if row['split']==split and row['model']==baseline)
            second = next(row for row in results if row['split']==split and row['model']==augmented)
            ci = next(row for row in intervals['results'] if row['split']==split and row['model']==augmented)
            ablations.append({'algorithm':algorithm,'split':split,'baseline_model':baseline,'augmented_model':augmented,
                              'baseline_mae':first['mae'],'augmented_mae':second['mae'],
                              'mae_difference':second['mae']-first['mae'],'n_predictions':first['n_predictions'],
                              'paired_block_interval':ci})
    frozen = predictions.loc[predictions.split.eq('validation_2025') & (predictions.label_available_at<=core.SELECTION_CUTOFF)]
    frozen_metrics = core.calculate_metrics(frozen)[0]
    provenance = {'profile':PROFILE,'analysis_type':ANALYSIS,'primary_eligible':False,
                  'historical_public_availability':'unknown','previously_examined_2026':True,'note':NOTE,
                  'generated_at':datetime.now(timezone.utc),'as_of':asof,'primary_snapshot':str(primary),
                  'primary_manifest_sha256':sha(primary/'study_manifest.json'),'primary_quality_report_sha256':sha(primary/'data_quality.json'),
                  'registry_sha256':sha(primary/'tools.study.json'),'git_manifest_sha256':git_manifest_sha256,
                  'git_weekly_sha256':sha(git_source/'weekly_git_history.csv'),'recipes':recipe,
                  'git_feature_columns':GIT_FEATURES,'date_basis':'UTC committer date; anomaly records retained without filtering or invented corrections',
                  'git_tool_date_anomalies':{row['tool_id']:row['date_anomalies'] for row in git_manifest['tools']},
                  'git_tool_tips':{row['tool_id']:row['tip_sha'] for row in git_manifest['tools']},
                  'model_selection':'none; no winner promotion; original primary selection is unchanged',
                  'primary_selected_model_unchanged':primary_metrics['best_model'],
                  'row_audit':{'primary_panel_rows':len(frame),'git_history_eligible_rows':int(eligible.sum()),
                               'prediction_rows':len(predictions),'models':sorted(predictions.model.unique()),'monthly_folds':len(fold_records)},
                  'source_limits':git_manifest['limitations'],'primary_mutated':False}
    output.parent.mkdir(parents=True,exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.git-sensitivity-',dir=output.parent))
    try:
        predictions.to_parquet(staging/'predictions.parquet',index=False)
        predictions.to_csv(staging/'predictions.csv',index=False)
        pd.DataFrame(results).to_csv(staging/'metrics.csv',index=False)
        weekly.to_csv(staging/'per_week_metrics.csv',index=False)
        tools.to_csv(staging/'per_tool_errors.csv',index=False)
        frame[['tool_id','week_start','git_history_eligible',*GIT_FEATURES]].to_parquet(staging/'git_features.parquet',index=False)
        core.write_json(staging/'metrics.json',{'profile':PROFILE,'analysis_type':ANALYSIS,'note':NOTE,'results':results,'model_selection':'none'})
        core.write_json(staging/'frozen_2025_metrics.json',{'label_availability_cutoff':core.SELECTION_CUTOFF,'model_selection':'none','results':frozen_metrics})
        core.write_json(staging/'ablations.json',{'profile':PROFILE,'note':NOTE,'results':ablations})
        pd.DataFrame([{k:v for k,v in row.items() if k!='paired_block_interval'} for row in ablations]).to_csv(staging/'ablations.csv',index=False)
        for reference in ('published_persistence','published_training_median'):
            core.write_json(staging/('block_ci_vs_'+reference+'.json'),core.paired_block_bootstrap(weekly,reference=reference))
        core.write_json(staging/'folds.json',{'folds':fold_records,'training_protocol':'original monthly cutoffs; all models refit on same Git-eligible training rows; labels available by cutoff only'})
        core.write_json(staging/'provenance_quality.json',provenance)
        manifest = {'status':'complete','profile':PROFILE,'analysis_type':ANALYSIS,'primary_eligible':False,
                    'historical_public_availability':'unknown','previously_examined_2026':True,'note':NOTE,
                    'files':{str(path.relative_to(staging)):sha(path) for path in sorted(staging.iterdir()) if path.is_file()}}
        core.write_json(staging/'manifest.json',manifest)
        staging.rename(output)
    except BaseException:
        shutil.rmtree(staging,ignore_errors=True)
        raise
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--primary-snapshot',type=Path,required=True)
    parser.add_argument('--git-source',type=Path,required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--git-manifest-sha256',required=True)
    parser.add_argument('--n-jobs',type=int,default=2)
    args=parser.parse_args()
    try: result=run(args.primary_snapshot,args.git_source,args.output_dir,args.git_manifest_sha256,args.n_jobs)
    except Exception as error:
        print(json.dumps({'status':'failed','error':type(error).__name__+': '+str(error)}));return 1
    print(json.dumps(result,indent=2));return 0

if __name__=='__main__': raise SystemExit(main())
