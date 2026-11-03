"""Source validity is separate from retrieving a complete set of query windows."""
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

GH_COUNTS = ['gh_event_count', 'gh_push_events', 'gh_issue_events', 'gh_pr_events', 'gh_watch_events']
GH_NUMERIC = GH_COUNTS + ['gh_active_actors']
POLICY_PATH = Path(__file__).with_name('source_quality.json')

def load_policy(path=None):
    path = Path(path or POLICY_PATH)
    policy = json.loads(path.read_text())
    if (policy.get('source_policy') != 'capture_quality_v1' or policy.get('profile') != 'validated_primary'
            or set(policy.get('enabled_feature_groups', [])) != {'dl', 'release', 'controls'}
            or not {'gh', 'research'}.issubset(policy.get('excluded_feature_groups', []))
            or policy['sources']['gharchive'].get('primary_eligible') is not False
            or policy['sources']['gharchive'].get('capture_validated') is not False
            or policy['sources']['gharchive'].get('known_degradation_start') != '2025-05-24'
            or policy['sources']['arxiv'].get('primary_eligible') is not False
            or any(policy['sources'][source].get('status') != 'primary_enabled'
                   or policy['sources'][source].get('primary_eligible') is not True
                   for source in ['pypi', 'pypi_releases'])):
        raise ValueError('Unrecognized or unsafe source quality policy')
    return policy

def annotate_daily_quality(daily, first_upload=None, policy=None):
    """Retain captured values, quarantine known bad periods, certify no GH dates.

    Query completion permits a *captured* zero only. It never changes the two
    eligibility booleans below, even if every global table is nonempty.
    """
    policy = policy or load_policy()
    d = daily.copy()
    d.index = pd.to_datetime(d.index)
    boundary = pd.Timestamp(policy['sources']['gharchive']['known_degradation_start'])
    for col in GH_NUMERIC:
        observed = 'observed_' + col
        if observed not in d:
            d[observed] = d[col]
    d['gh_capture_quality'] = np.where(d.index >= boundary, 'known_capture_degradation', 'unvalidated_capture')
    d['gh_capture_validated'] = False
    d['gh_primary_eligible'] = False
    d['gh_retrieval_complete'] = d.observed_gh_event_count.notna()
    d.loc[d.index >= boundary, GH_NUMERIC] = np.nan
    pre = d.index < pd.Timestamp(first_upload).normalize() if first_upload is not None else np.ones(len(d), dtype=bool)
    for col, label in [('dl_downloads', 'dl'), ('release_count', 'release')]:
        d[label + '_observation_status'] = np.where(pre, 'pre_visible_artifact', np.where(d[col].notna(), 'retrieved', 'missing_retrieval'))
    return d

def validate_panel(daily, weekly, tools, asof, first_upload, policy=None):
    """Validate all keys/counts and distinguish permitted missingness from gaps."""
    policy = policy or load_policy()
    ids = set(tools)
    dates = pd.date_range('2023-01-01', pd.Timestamp(asof).normalize() - pd.Timedelta(days=1))
    weeks = pd.date_range('2023-01-02', pd.Timestamp(asof).normalize() - pd.Timedelta(days=7), freq='W-MON')
    errors = []
    observed_days = pd.to_datetime(daily.date)
    observed_weeks = pd.to_datetime(weekly.week_start)
    expected_daily = pd.MultiIndex.from_product([sorted(ids), dates], names=['tool_id', 'date'])
    expected_weekly = pd.MultiIndex.from_product([sorted(ids), weeks], names=['tool_id', 'week_start'])
    for label, frame, stamps, expected in [('daily', daily, observed_days, expected_daily), ('weekly', weekly, observed_weeks, expected_weekly)]:
        actual = pd.MultiIndex.from_arrays([frame.tool_id, stamps], names=expected.names)
        if actual.has_duplicates or len(actual) != len(expected) or len(expected.difference(actual)) or len(actual.difference(expected)):
            errors.append(label + '_tool_time_grid_mismatch')
        count_columns = [c for c in frame if c in ['dl_downloads', 'release_count', *GH_NUMERIC] or c in ['observed_' + x for x in GH_NUMERIC]]
        for col in count_columns:
            values = pd.to_numeric(frame[col], errors='coerce').dropna().to_numpy()
            if not np.isfinite(values).all() or np.any(values < 0) or np.any(values != np.floor(values)):
                errors.append(label + '_invalid_count:' + col)
            if frame[col].notna().sum() != len(values):
                errors.append(label + '_nonnumeric_count:' + col)
        if frame.gh_primary_eligible.any() or frame.gh_capture_validated.any():
            errors.append(label + '_uncertified_gh_enabled')
        boundary = pd.Timestamp(policy['sources']['gharchive']['known_degradation_start'])
        compromised = stamps >= boundary if label == 'daily' else stamps + pd.Timedelta(days=7) > boundary
        if frame.loc[compromised, GH_NUMERIC].notna().any().any():
            errors.append(label + '_compromised_gh_not_masked')
        # Daily actor markers cover a full week, not the marker's Monday alone.
        for sub in GH_COUNTS[1:] + (['gh_active_actors'] if label == 'weekly' else []):
            if (frame['observed_' + sub] > frame.observed_gh_event_count).any():
                errors.append(label + '_observed_subtype_or_actor_exceeds_events:' + sub)
    missing = {'daily': {}, 'weekly': {}}
    unexpected = {}
    expected_pre = {tid: (dates < pd.Timestamp(first_upload[tid]).normalize()).sum() if tid in first_upload else len(dates) for tid in ids}
    daily_pre = pd.Series([tid not in first_upload or stamp < pd.Timestamp(first_upload[tid]).normalize()
                           for tid, stamp in zip(daily.tool_id, observed_days)], index=daily.index)
    weekly_pre = pd.Series([tid not in first_upload or stamp < pd.Timestamp(first_upload[tid]).normalize()
                            for tid, stamp in zip(weekly.tool_id, observed_weeks)], index=weekly.index)
    for col, label in [('dl_downloads', 'dl'), ('release_count', 'release')]:
        before = daily_pre
        if not before.eq(daily[label + '_observation_status'].eq('pre_visible_artifact')).all():
            errors.append('incorrect_pre_artifact_status:' + col)
        expected = sum(expected_pre.values())
        actual = int(daily[col].isna().sum())
        bad = int((daily[col].isna() & ~before).sum())
        unexpected[col] = bad
        if bad or actual != expected or daily.loc[before, col].notna().any():
            errors.append('unexpected_primary_daily_missingness:' + col)
        weekly_expected = 0
        for tid in ids:
            weekly_expected += int((weeks < pd.Timestamp(first_upload[tid]).normalize()).sum()) if tid in first_upload else len(weeks)
        if int(weekly[col].isna().sum()) != weekly_expected or not weekly[col].isna().eq(weekly_pre).all():
            errors.append('unexpected_primary_weekly_missingness:' + col)
        missing['daily'][col] = actual
        missing['weekly'][col] = int(weekly[col].isna().sum())
    frozen = policy['missingness']['frozen_snapshot_check']
    if pd.Timestamp(asof).date().isoformat() == frozen['as_of_utc_date'] and len(ids) == frozen['tools']:
        if len(daily) != frozen['daily_rows'] or len(weekly) != frozen['weekly_rows']:
            errors.append('frozen_snapshot_grid_changed')
        for col in ['dl_downloads', 'release_count']:
            if missing['daily'][col] != frozen['daily_pre_artifact_missing_per_primary_count'] or missing['weekly'][col] != frozen['weekly_pre_artifact_missing_per_primary_count']:
                errors.append('frozen_snapshot_missingness_changed:' + col)
    return {'status': 'passed' if not errors else 'failed', 'errors': errors,
            'tools': len(ids), 'daily_rows': len(daily), 'weekly_rows': len(weekly),
            'daily_days_per_tool': len(dates), 'weekly_weeks_per_tool': len(weeks),
            'primary_missing_counts': missing, 'unexpected_primary_missing_counts': unexpected,
            'expected_pre_artifact_days_by_tool': {t: int(n) for t, n in expected_pre.items()},
            'gh_capture_quality_daily_counts': daily.gh_capture_quality.value_counts().to_dict(),
            'gh_capture_quality_weekly_counts': weekly.gh_capture_quality.value_counts().to_dict(),
            'retrieval_complete_does_not_imply_capture_validity': True,
            'gh_primary_eligible_rows': int(weekly.gh_primary_eligible.sum()),
            'expected_missingness': policy['missingness']}

def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def raw_spot_checks(data, objects, tools, eligible, winners, latest, daily, weekly):
    """Read retained HTTP bodies independently of backfill.records/normalization.

    Check first/last weeks, annual midyear weeks and documented source-boundary
    weeks for every tool, including captured zeros. Check every eligible raw
    PyPI row's artifact/exclusion arithmetic and last-week distinct releases.
    This is a bounded consistency check, never a GH capture-completeness claim.
    """
    import collections
    import gzip
    import re
    chosen = {}
    for obj in objects:
        if obj['job_id'] not in eligible:
            continue
        rank = (str(obj['observed_at']), obj['raw_path'])
        if obj['job_id'] not in chosen or rank > chosen[obj['job_id']][0]:
            chosen[obj['job_id']] = (rank, obj)
    bodies = {}
    errors = []
    def body(job_id):
        if job_id not in bodies:
            obj = chosen.get(job_id, (None, None))[1]
            if obj is None:
                raise ValueError('Missing raw object for sampled job ' + str(job_id))
            raw = gzip.decompress((Path(data) / obj['raw_path']).read_bytes())
            if hashlib.sha256(raw).hexdigest() != obj['sha256']:
                raise ValueError('Raw checksum mismatch: ' + obj['raw_path'])
            bodies[job_id] = json.loads(raw)
        return bodies[job_id]
    decoded = {}
    def rows(job_id):
        if job_id not in decoded:
            output = []
            for response in body(job_id).get('responses', []):
                response = response.get('response', {})
                fields = response.get('schema', {}).get('fields', [])
                for item in response.get('rows', []):
                    if len(fields) != len(item.get('f', [])):
                        raise ValueError('Raw BigQuery schema width mismatch')
                    row = {}
                    for field, cell in zip(fields, item['f']):
                        value = cell.get('v')
                        if value is not None and field['type'] in ('INTEGER', 'INT64'):
                            value = int(value)
                        row[field['name']] = value
                    output.append(row)
            decoded[job_id] = output
        return decoded[job_id]
    week_values = sorted(pd.to_datetime(weekly.week_start).unique())
    if not week_values:
        return {'status': 'failed', 'errors': ['No full week for raw spot checks']}
    available_weeks = {pd.Timestamp(value) for value in week_values}
    boundary_weeks = {'2025-05-19', '2025-05-26', '2025-10-06', '2025-10-13',
                      '2026-03-02', '2026-08-17', '2026-08-24', '2026-10-19', '2026-10-26'}
    sample_weeks = {pd.Timestamp(week_values[0]), pd.Timestamp(week_values[-1])}
    sample_weeks.update(pd.Timestamp(value) for value in boundary_weeks)
    for year in sorted({value.year for value in available_weeks}):
        july = pd.Timestamp(year=year, month=7, day=1)
        sample_weeks.add(july - pd.Timedelta(days=july.weekday()))
    sample_weeks = sorted(sample_weeks.intersection(available_weeks))
    sample_days = {str((week + pd.Timedelta(days=k)).date()) for week in sample_weeks for k in range(7)}
    sample = daily[daily.date.isin(sample_days)]
    checked = collections.Counter()
    for job_id, (_, obj) in chosen.items():
        if obj['source'] != 'pypi':
            continue
        for value in rows(job_id):
            if value.get('tool_id') not in eligible[job_id]['tools']:
                continue
            counts = [value.get(name) for name in ['downloads', 'excluded_requests', 'logged_objects']]
            if (any(v is None or not isinstance(v, int) or v < 0 for v in counts)
                    or counts[0] + counts[1] != counts[2]):
                errors.append('Raw PyPI artifact/exclusion arithmetic:' + job_id + ':' + str(value.get('tool_id')) + ':' + str(value.get('date')))
            checked['pypi_eligible_raw_rows_arithmetic'] += 1
    for _, entry in sample.iterrows():
        day = pd.Timestamp(entry.date).date()
        for source in ['pypi', 'gharchive']:
            job_id = winners.get((source, entry.tool_id, day))
            if not job_id:
                errors.append('Missing sampled winning job:' + source + ':' + entry.tool_id + ':' + str(day))
                continue
            values = [r for r in rows(job_id) if r.get('tool_id') == entry.tool_id and r.get('date') == entry.date]
            if source == 'pypi':
                if len(values) > 1:
                    errors.append('Duplicate raw PyPI tool-day:' + entry.tool_id + ':' + str(day))
                for value in values:
                    if (value.get('downloads') is None or value.get('excluded_requests') is None
                            or value.get('logged_objects') is None
                            or value['downloads'] + value['excluded_requests'] != value['logged_objects']):
                        errors.append('Raw PyPI artifact/exclusion arithmetic:' + entry.tool_id + ':' + str(day))
                if entry.dl_observation_status == 'pre_visible_artifact':
                    expected = np.nan
                elif values:
                    expected = values[0]['downloads']
                else:
                    globally_positive = any(r.get('date') == entry.date and (r.get('downloads') or 0) > 0 for r in rows(job_id))
                    expected = 0 if globally_positive else np.nan
                actual = entry.dl_downloads
            else:
                values = [r for r in values if r.get('event_type') != '__all_types__']
                expected = np.nan if any(r.get('missing_event_id_rows') for r in values) else sum(r.get('event_count') or 0 for r in values)
                actual = entry.observed_gh_event_count
            if not ((pd.isna(actual) and pd.isna(expected)) or actual == expected):
                errors.append('Raw/normalized mismatch:' + source + ':' + entry.tool_id + ':' + str(day))
            checked[source + '_tool_days'] += 1
    last_week = sample_weeks[-1]
    weekly_index = weekly.set_index(['tool_id', 'week_start'])
    canonical = lambda name: re.sub(r'[-_.]+', '-', str(name).lower())
    for tid, tool in tools.items():
        available = [v[1] for v in chosen.values() if v[1]['source'] == 'pypi_releases'
                     and v[1]['tool_id'] == tid and tid in eligible[v[1]['job_id']]['tools']]
        if not available:
            errors.append('Missing raw package metadata:' + tid)
            continue
        obj = max(available, key=lambda v: (str(v['observed_at']), v['raw_path']))
        project = body(obj['job_id'])
        if canonical(project.get('info', {}).get('name')) != canonical(tool['pypi_package']):
            errors.append('Raw package identity mismatch:' + tid)
        expected = 0
        for version, artifacts in project.get('releases', {}).items():
            times = [pd.Timestamp(a.get('upload_time_iso_8601') or a['upload_time']).tz_localize(None)
                     for a in artifacts if a.get('upload_time_iso_8601') or a.get('upload_time')]
            if times:
                checked['visible_release_versions'] += 1
                expected += int(last_week <= min(times) < last_week + pd.Timedelta(days=7))
            checked['visible_artifact_files'] += len(artifacts)
        actual = weekly_index.loc[(tid, last_week), 'release_count']
        if pd.notna(actual) and actual != expected:
            errors.append('Raw distinct release-version mismatch:' + tid)
        checked['release_tool_weeks'] += 1
        marker = latest.get(('gharchive', tid, str(last_week.date()), '__all_types__', '__window_unique_actors__'))
        if marker:
            raw_markers = [r for r in rows(marker[2]) if r.get('tool_id') == tid and r.get('date') == str(last_week.date())
                           and r.get('event_type') == '__all_types__' and r.get('action') == '__window_unique_actors__']
            if len(raw_markers) != 1 or weekly_index.loc[(tid, last_week), 'observed_gh_active_actors'] != raw_markers[0].get('active_actors'):
                errors.append('Raw weekly distinct captured actors mismatch:' + tid)
            checked['gh_actor_tool_weeks'] += 1
    return {'status': 'passed' if not errors else 'failed', 'errors': errors,
            'sample_week_starts': [str(v.date()) for v in sample_weeks],
            'checked': dict(checked), 'raw_objects_read': len(bodies),
            'upstream_capture_validated': False,
            'scope': 'Independent retained HTTP-body consistency, not proof of source completeness.'}
