"""Resolve metadata-variant uncertainty without rebuilding the declaration graph.

All provider rows and parsed edges remain immutable.  Only cohort/week feature
eligibility is narrowed, using the complete artifact context for packages whose
conflicting variants disagree about a cohort target's declared membership.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil

from audit_dependency_conflicts import audit
from build_dependency_features import encode, requirement, sha, week_start
from dependency_sources import SOURCE, canonical, targets

POLICY_VERSION = 'conflict-context-v1'
INBOUND = ['dep_ever_declaring_packages', 'dep_new_declaring_packages',
           'dep_declaring_release_count', 'dep_latest_uploaded_release_declaring_packages',
           'dep_new_declaring_packages_4w', 'dep_declaring_release_count_4w',
           'dep_ever_declaring_packages_change_4w',
           'dep_latest_uploaded_release_declaring_packages_change_4w']
PACKAGE = re.compile(rb'"package"\s*:\s*"([A-Za-z0-9._-]+)"')
NAME = re.compile(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]*)')


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


@lru_cache(maxsize=32768)
def parsed(value):
    return requirement(value)


def membership(row, cohort):
    """Only parse candidate target names; markers/specifiers never erase edges."""
    if '_cohort_membership' in row:
        return row['_cohort_membership']
    result = set()
    for value in row['requires_dist'] or []:
        if not isinstance(value, str):
            continue
        name = NAME.match(value)
        if name is None:
            continue
        try:
            candidate = canonical(name.group(1))
        except ValueError:
            # Invalid names remain in the parser's existing unknown-edge
            # quarantine. They cannot be a valid cohort membership claim.
            continue
        if candidate not in cohort:
            continue
        req = parsed(value)
        if req['valid'] and req['dependency_package'] != row['package']:
            result.add(req['dependency_package'])
    return result


def context_events(rows, groups, cohort):
    """Collapse alternative claims about one file to certain/possible membership."""
    variants = {artifact: index for index, group in enumerate(groups)
                for artifact in group['artifact_ids']}
    grouped = defaultdict(list)
    result = []
    for row in rows:
        if not row.get('identity_valid') or not row.get('provider_upload_time'):
            # Existing parser eligibility already represents invalid/undated rows.
            continue
        members = membership(row, cohort)
        event = {'version': row['version'], 'at': timestamp(row['provider_upload_time']),
                 'lower': members, 'upper': members,
                 'optional_id': row['artifact_id']}
        index = variants.get(row['artifact_id'])
        if index is None:
            result.append(event)
        else:
            grouped[index].append((row['artifact_id'], event))
    for index, group in enumerate(groups):
        records = grouped[index]
        if {key for key, _ in records} != set(group['artifact_ids']):
            raise ValueError('Complete conflicting artifact context is required')
        events = [value for _, value in records]
        if len({(value['version'], value['at']) for value in events}) != 1:
            raise ValueError('One conflicting file identity changed version/upload timestamp')
        lower = set.intersection(*(value['lower'] for value in events))
        upper = set.union(*(value['upper'] for value in events))
        result.append({**events[0], 'lower': lower, 'upper': upper,
                       'optional_id': min(group['artifact_ids'])})
    return result


def first_options(events, target):
    certain = [event['at'] for event in events if target in event['lower']]
    first_certain = min(certain) if certain else None
    options = {event['at'] for event in events if target in event['upper']
               and (first_certain is None or event['at'] <= first_certain)}
    if first_certain is not None:
        options.add(first_certain)
    else:
        options.add(None)
    return options


def variable(options, predicate):
    return len({predicate(option) for option in options}) > 1


def latest_state(events, target, boundary):
    available = [event for event in events if event['at'] < boundary]
    if not available:
        return (False, frozenset())
    first = {}
    for event in available:
        first[event['version']] = min(first.get(event['version'], event['at']), event['at'])
    selected = max(first, key=lambda version: (first[version], version))
    selected_events = [event for event in available if event['version'] == selected]
    if any(target in event['lower'] for event in selected_events):
        return (True, frozenset())
    return (False, frozenset(event['optional_id'] for event in selected_events
                             if target in event['upper']))


def latest_difference_variable(current, previous):
    # An uncertain but identical OR-expression at both boundaries cancels.
    if current == previous:
        return False
    return bool(current[1] or previous[1])


def package_uncertainty(events, candidates, weeks):
    """Exact counter uncertainty for independent file-variant alternatives.

    Per-target membership is a binary claim. Requirement text differences that
    retain this claim cannot alter these all-condition declaration counters.
    """
    result = defaultdict(set)
    for target in candidates:
        package_options = first_options(events, target)
        by_version = defaultdict(list)
        for event in events:
            by_version[event['version']].append(event)
        release_options = [first_options(values, target) for values in by_version.values()]
        release_options = [options for options in release_options if len(options) > 1]
        states = []
        for index, monday in enumerate(weeks):
            boundary = datetime.combine(monday + timedelta(days=7), datetime.min.time(), timezone.utc)
            state = latest_state(events, target, boundary)
            states.append(state)
            metric = result[(target, monday.isoformat())]
            if variable(package_options, lambda value: value is not None and value < boundary):
                metric.add('dep_ever_declaring_packages')
            if variable(package_options, lambda value: value is not None and week_start(value.date()) == monday):
                metric.add('dep_new_declaring_packages')
            if any(variable(options, lambda value: value is not None and week_start(value.date()) == monday)
                   for options in release_options):
                metric.add('dep_declaring_release_count')
            if state[1]:
                metric.add('dep_latest_uploaded_release_declaring_packages')
            if index >= 3:
                start = monday - timedelta(days=21)
                predicate = lambda value: value is not None and start <= week_start(value.date()) <= monday
                if variable(package_options, predicate):
                    metric.add('dep_new_declaring_packages_4w')
                if any(variable(options, predicate) for options in release_options):
                    metric.add('dep_declaring_release_count_4w')
            if index >= 4:
                start = monday - timedelta(days=21)
                if variable(package_options, lambda value: value is not None and start <= week_start(value.date()) <= monday):
                    metric.add('dep_ever_declaring_packages_change_4w')
                if latest_difference_variable(state, states[index - 4]):
                    metric.add('dep_latest_uploaded_release_declaring_packages_change_4w')
    return {key: sorted(values) for key, values in result.items() if values}


def resolve_context(source, registry, conflict_audit):
    source = Path(source)
    cohort = set(targets(json.loads(Path(registry).read_text())))
    by_package = defaultdict(list)
    for group in conflict_audit['groups']:
        if group['candidate_outbound_selected_weeks']:
            raise ValueError('Outbound conflict review is not supported by this inbound-only policy')
        if group['candidate_inbound_targets']:
            by_package[group['package']].append(group)
    with (source / 'weekly_dependency_features.csv').open() as stream:
        weekly = list(csv.DictReader(stream))
    weeks = sorted({date.fromisoformat(row['week_start']) for row in weekly})
    rows = defaultdict(list)
    scanned = selected = 0
    with gzip.open(source / 'artifacts.jsonl.gz', 'rb') as stream:
        for line in stream:
            scanned += 1
            match = PACKAGE.search(line)
            if match is None or match.group(1).decode() not in by_package:
                continue
            row = json.loads(line)
            if row['package'] in by_package:
                # Keep only binary cohort membership, never millions of nested
                # foreign requirements or the full rich provider payload.
                rows[row['package']].append({**{key: row[key] for key in
                    ['package', 'version', 'provider_upload_time', 'identity_valid', 'artifact_id']},
                    '_cohort_membership': membership(row, cohort)})
                selected += 1
    if scanned != conflict_audit['artifacts_scanned']:
        raise ValueError('Artifact context no longer matches complete conflict audit')
    masks = defaultdict(lambda: {'packages': set(), 'metrics': set()})
    package_details = []
    for package, groups in sorted(by_package.items()):
        events = context_events(rows[package], groups, cohort)
        candidates = set().union(*(set(group['candidate_inbound_targets']) for group in groups))
        affected = package_uncertainty(events, candidates, weeks)
        for key, metrics in affected.items():
            masks[key]['packages'].add(package)
            masks[key]['metrics'].update(metrics)
        package_details.append({'package': package, 'artifact_rows': len(rows[package]),
                                'conflict_groups': len(groups), 'uncertain_target_weeks': len(affected)})
    details = [{'package': key[0], 'week_start': key[1],
                'source_packages': sorted(value['packages']), 'metrics': sorted(value['metrics'])}
               for key, value in sorted(masks.items())]
    lookup = {(row['package'], row['week_start']): row for row in weekly}
    for detail in details:
        current = lookup[(detail['package'], detail['week_start'])]
        detail['previous_inbound_eligible'] = current['dep_inbound_event_time_eligible'] == 'True'
    return {'policy_version': POLICY_VERSION, 'artifacts_scanned': scanned,
            'affected_package_context_rows': selected, 'candidate_source_packages': len(by_package),
            'uncertain_target_weeks': len(details),
            'newly_masked_target_weeks': sum(row['previous_inbound_eligible'] for row in details),
            'metric_uncertainty_counts': dict(Counter(metric for row in details for metric in row['metrics'])),
            'packages': package_details, 'feature_masks': details,
            'policy': 'Only feature uncertainty across immutable-file metadata alternatives is masked; unchanged numeric observations remain diagnostic. All conditional declarations count. Historical retrieval vintages are unavailable.'}


def verify_completion(source, registry):
    """Retrieval completeness is independent of provider-metadata usability."""
    manifest = json.loads((source / 'manifest.json').read_text())
    quality = json.loads((source / 'dependency_quality.json').read_text())
    receipt = json.loads((source / 'query_receipt.json').read_text())
    plan = json.loads((source / 'query_plan.json').read_text())
    if manifest.get('source') != SOURCE or manifest.get('registry_sha256') != sha(registry):
        raise ValueError('Review cohort/source mismatch')
    for name, digest in manifest['files'].items():
        if Path(name).name != name or sha(source / name) != digest:
            raise ValueError('Review source file hash mismatch: ' + name)
    counts = quality['audit']
    if (receipt.get('job_complete') is not True or receipt.get('all_pages_fetched') is not True
            or receipt.get('query_sha256') != plan.get('query_sha256')
            or quality.get('query_sha256') != plan.get('query_sha256')
            or quality.get('input_metadata_sha256') != receipt.get('output_sha256')
            or quality.get('query_plan_sha256') != sha(source / 'query_plan.json')
            or quality.get('query_receipt_sha256') != sha(source / 'query_receipt.json')
            or counts['input_rows'] != receipt.get('total_rows')
            or counts['unique_artifacts'] + counts['exact_duplicate_rows_removed'] != counts['input_rows']
            or plan.get('target_package_to_tool') != targets(json.loads(Path(registry).read_text()))):
        raise ValueError('Complete query receipt/row arithmetic/cohort binding is required')
    return manifest, quality


def reviewed_source(source, registry, output_root=None, audit_path=None):
    source = Path(source).resolve()
    registry = Path(registry)
    fingerprint = hashlib.sha256((sha(source / 'manifest.json') + sha(registry) + sha(Path(__file__))).encode()).hexdigest()
    output = (Path(output_root) if output_root else source.parent) / (source.name + '-review-' + fingerprint[:16])
    if output.exists():
        review = json.loads((output / 'conflict_feature_review.json').read_text())
        if (review.get('source_manifest_sha256') != sha(source / 'manifest.json')
                or review.get('review_code_sha256') != sha(Path(__file__))):
            raise ValueError('Existing review is bound to different source/code')
        verify_completion(output, registry)
        return output
    manifest, quality = verify_completion(source, registry)
    if audit_path:
        evidence = json.loads(Path(audit_path).read_text())
    else:
        evidence = audit(source, registry)
    if (evidence.get('source_manifest_sha256') != sha(source / 'manifest.json')
            or evidence.get('registry_sha256') != sha(registry)
            or evidence.get('all_provider_hashes_verified') is not True):
        raise ValueError('Complete hash-bound conflict audit is required')
    review = resolve_context(source, registry, evidence)
    review.update(source_manifest_sha256=sha(source / 'manifest.json'), registry_sha256=sha(registry),
                  review_code_sha256=sha(Path(__file__)), audit_code_sha256=sha(Path(__file__).with_name('audit_dependency_conflicts.py')))
    if review['artifacts_scanned'] != quality['audit']['unique_artifacts']:
        raise ValueError('Review did not scan every retained artifact')
    staging = output.with_name('.' + output.name + '.staging')
    if staging.exists():
        raise FileExistsError('Previous review staging exists; inspect it before retrying')
    staging.mkdir(parents=True)
    try:
        changed = {'weekly_dependency_features.csv', 'weekly_dependency_features.parquet',
                   'dependency_quality.json', 'manifest.json'}
        for item in source.iterdir():
            if item.is_file() and item.name not in changed:
                os.link(item, staging / item.name)
        masks = {(row['package'], row['week_start']) for row in review['feature_masks']}
        with (source / 'weekly_dependency_features.csv').open() as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames
            rows = list(reader)
        for row in rows:
            if (row['package'], row['week_start']) in masks:
                row['dep_inbound_event_time_eligible'] = 'False'
        with (staging / 'weekly_dependency_features.csv').open('x', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader(); writer.writerows(rows)
        if (source / 'weekly_dependency_features.parquet').exists():
            import pandas as pd
            frame = pd.read_parquet(source / 'weekly_dependency_features.parquet')
            selected = [(package, week) in masks for package, week in zip(frame['package'], frame['week_start'])]
            frame.loc[selected, 'dep_inbound_event_time_eligible'] = False
            frame.to_parquet(staging / 'weekly_dependency_features.parquet', index=False)
        quality.update(collection_complete=True, input_query_complete=True, quality_status=quality['status'],
                       conflict_review={'policy_version': POLICY_VERSION, 'complete': True,
                                        'conflicting_artifact_groups': evidence['conflict_groups'],
                                        'classifications': evidence['classifications'],
                                        'uncertain_target_weeks': review['uncertain_target_weeks'],
                                        'newly_masked_target_weeks': review['newly_masked_target_weeks'],
                                        'selected_cohort_outbound_conflicts': 0,
                                        'all_provider_hashes_verified': True},
                       review_parent_manifest_sha256=sha(source / 'manifest.json'))
        (staging / 'conflict_feature_review.json').write_text(json.dumps(review, indent=2) + '\n')
        (staging / 'conflict_metadata_audit.json').write_text(json.dumps(evidence, indent=2) + '\n')
        (staging / 'dependency_quality.json').write_text(json.dumps(quality, indent=2) + '\n')
        new_manifest = {**manifest, 'collection_complete': True, 'input_query_complete': True,
                        'quality_status': quality['status'], 'review_parent_manifest_sha256': sha(source / 'manifest.json'),
                        'files': {path.name: sha(path) for path in sorted(staging.iterdir()) if path.is_file()}}
        (staging / 'manifest.json').write_text(json.dumps(new_manifest, indent=2) + '\n')
        verify_completion(staging, registry)
        os.replace(staging, output)
    except BaseException:
        shutil.rmtree(staging)
        raise
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--output-root', type=Path)
    parser.add_argument('--audit', type=Path)
    args = parser.parse_args()
    output = reviewed_source(args.source, args.registry, args.output_root, args.audit)
    review = json.loads((output / 'conflict_feature_review.json').read_text())
    print(json.dumps({'output': str(output), **{key: value for key, value in review.items()
                      if key not in {'packages', 'feature_masks'}}}))


if __name__ == '__main__':
    main()
