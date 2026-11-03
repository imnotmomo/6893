"""Build upload-dated declaration edges and weekly adoption proxies.

The counters describe published dependency declarations. They do not count
installed users or historically resolved environments. Current deps.dev
resolution snapshots must never be passed to this historical builder.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import csv
import gzip
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re

from packaging.requirements import Requirement, InvalidRequirement
from dependency_sources import SOURCE, canonical, targets, utc


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def requirement(value):
    if not isinstance(value, str):
        return {'raw_requirement': value, 'valid': False, 'error': 'Requirement is not text', 'name_candidate': None}
    try:
        parsed = Requirement(value)
        marker = str(parsed.marker) if parsed.marker else None
        return {'raw_requirement': value, 'valid': True, 'dependency_package': canonical(parsed.name),
                'specifier': str(parsed.specifier), 'extras': sorted({canonical(v) for v in parsed.extras}),
                'environment_marker': marker, 'has_extra_marker': bool(marker and re.search(r'\bextra\b', marker)),
                'conditional_declaration': marker is not None, 'direct_url': parsed.url}
    except (InvalidRequirement, ValueError) as error:
        candidate = re.match(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]*)', value)
        return {'raw_requirement': value, 'valid': False, 'error': type(error).__name__,
                'name_candidate': canonical(candidate.group(1)) if candidate else None}


def normalize_artifacts(rows, observed_at, end_exclusive, edge_sink=None):
    observed = utc(observed_at)
    end = datetime.combine(date.fromisoformat(str(end_exclusive)), datetime.min.time(), timezone.utc)
    if observed is None or end > observed:
        raise ValueError('Collection observation must follow the completed UTC endpoint')
    artifacts, edges, errors = [], [], []
    seen, identities = set(), {}
    duplicates = 0
    edge_count = 0
    for index, original in enumerate(rows):
        if not isinstance(original, dict):
            raise ValueError('Metadata record must be an object')
        for name in ('name', 'version', 'requires_dist', 'upload_time', 'filename', 'path'):
            if name not in original:
                raise ValueError('Metadata source omitted required schema field ' + name)
        raw_hash = hashlib.sha256(encode(original).encode()).hexdigest()
        if raw_hash in seen:
            duplicates += 1
            continue
        seen.add(raw_hash)
        try:
            package = canonical(original['name'])
        except ValueError:
            package = None
            errors.append({'kind': 'invalid_package_identity', 'raw_row_sha256': raw_hash})
        version = original['version']
        if not isinstance(version, str) or not version.strip():
            version = None
            errors.append({'kind': 'invalid_version_identity', 'raw_row_sha256': raw_hash})
        try:
            uploaded = utc(original['upload_time'])
        except (ValueError, TypeError, OverflowError):
            uploaded = None
            errors.append({'kind': 'invalid_upload_timestamp', 'raw_row_sha256': raw_hash})
        if uploaded is not None and uploaded >= end:
            raise ValueError('Metadata includes an upload outside the completed query endpoint')
        if uploaded is None:
            errors.append({'kind': 'undated_artifact', 'package': package, 'version': version, 'raw_row_sha256': raw_hash})
        raw_requirements = original['requires_dist']
        if raw_requirements is None:
            raw_requirements = []
        if not isinstance(raw_requirements, list):
            raise ValueError('requires_dist must be a source array, not a flattened string')
        parsed = [requirement(value) for value in raw_requirements]
        identity = encode([package if package is not None else original['name'], original['version'], original['filename'], original['path'],
                           uploaded.isoformat() if uploaded else None, original.get('sha256_digest')])
        if identity in identities and identities[identity] != raw_hash:
            errors.append({'kind': 'conflicting_artifact_metadata', 'package': package, 'version': version,
                           'raw_row_sha256': raw_hash, 'other_raw_row_sha256': identities[identity]})
        identities[identity] = raw_hash
        item = {**original, 'package': package, 'provider_upload_time': uploaded.isoformat() if uploaded else None,
                'observed_at': observed.isoformat(), 'source': SOURCE, 'artifact_id': raw_hash,
                'requires_dist': raw_requirements, 'parsed_requirements': parsed,
                'event_time_available': uploaded is not None, 'identity_valid': package is not None and version is not None,
                'historical_retrieval_vintage': False}
        artifacts.append(item)
        for position, parsed_requirement in enumerate(parsed):
            if not parsed_requirement['valid']:
                errors.append({'kind': 'invalid_requirement', 'package': package, 'version': version,
                               'artifact_id': raw_hash, 'requirement_index': position, **parsed_requirement})
                continue
            edge = {'artifact_id': raw_hash, 'source_package': package, 'source_version': version,
                    'provider_upload_time': item['provider_upload_time'], 'observed_at': observed.isoformat(),
                    'requirement_index': position, **parsed_requirement}
            edge_count += 1
            if edge_sink is None:edges.append(edge)
            else:edge_sink(edge)
    return artifacts, edges, {'input_rows': len(rows), 'unique_artifacts': len(artifacts),
                             'exact_duplicate_rows_removed': duplicates, 'parsed_edges': edge_count,
                             'errors': errors, 'error_counts': dict(Counter(v['kind'] for v in errors))}


def week_start(day):
    return day - timedelta(days=day.weekday())


def weekly_features(artifacts, registry, start, end_exclusive, inbound_complete):
    """Sweep upload events; a later file can add an edge but never backdate it."""
    mapping = targets(registry)
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end_exclusive))
    monday = start + timedelta(days=(-start.weekday()) % 7)
    events = sorted((item for item in artifacts if item['provider_upload_time']),
                    key=lambda item: (utc(item['provider_upload_time']), item['package'] or '', item['version'] or '', item['artifact_id']))
    # Known undated records and unparsed target requirements affect eligibility;
    # they are retained and never silently converted to absent declarations.
    unknown = {target: set() for target in mapping}
    undated_outbound = set()
    for item in artifacts:
        uncertain = not item['event_time_available']
        if not uncertain:
            continue
        for req in item['parsed_requirements']:
            if not req['valid']:
                candidate = req.get('name_candidate')
                if candidate in mapping:
                    unknown[candidate].add(item['artifact_id'])
                elif candidate is None:
                    for target in mapping: unknown[target].add(item['artifact_id'])
        if uncertain:
            for req in item['parsed_requirements']:
                if req.get('dependency_package') in mapping:
                    unknown[req['dependency_package']].add(item['artifact_id'])
            if item['package'] in mapping:
                undated_outbound.add(item['package'])
    releases = {}
    latest = {}
    latest_declaring = {target: set() for target in mapping}
    ever = {target: set() for target in mapping}
    first_pair, first_release_pair = {}, {}
    new_packages, new_releases = Counter(), Counter()
    current = 0
    output = []
    while monday + timedelta(days=7) <= end:
        boundary = datetime.combine(monday + timedelta(days=7), datetime.min.time(), timezone.utc)
        while current < len(events) and utc(events[current]['provider_upload_time']) < boundary:
            item = events[current]; current += 1
            package, version = item['package'], item['version']
            stamp = utc(item['provider_upload_time'])
            if not item.get('identity_valid', True):
                for req in item['parsed_requirements']:
                    dependency = req.get('dependency_package') or req.get('name_candidate')
                    if dependency in mapping: unknown[dependency].add(item['artifact_id'])
                    elif dependency is None:
                        for target in mapping: unknown[target].add(item['artifact_id'])
                continue
            key = (package, version)
            previous_target_set = set()
            if package in latest:
                previous_target_set = releases[latest[package]]['targets'].copy()
            if key not in releases:
                releases[key] = {'first_upload': stamp, 'dependencies': set(), 'unconditional': set(),
                                 'conditional': set(), 'requirements': set(), 'targets': set(), 'invalid': False}
            release = releases[key]
            for req in item['parsed_requirements']:
                if not req['valid']:
                    release['invalid'] = True
                    candidate = req.get('name_candidate')
                    if candidate in mapping:
                        unknown[candidate].add(item['artifact_id'])
                    elif candidate is None:
                        for target in mapping: unknown[target].add(item['artifact_id'])
                    continue
                dependency = req['dependency_package']
                release['dependencies'].add(dependency)
                release['requirements'].add(encode({k: req[k] for k in ('dependency_package','specifier','extras','environment_marker','direct_url')}))
                release['conditional' if req['conditional_declaration'] else 'unconditional'].add(dependency)
                if dependency in mapping and dependency != package:
                    release['targets'].add(dependency)
                    pair = (package, dependency)
                    if pair not in first_pair:
                        first_pair[pair] = stamp
                        ever[dependency].add(package)
                        new_packages[(dependency, week_start(stamp.date()))] += 1
                    release_pair = (package, version, dependency)
                    if release_pair not in first_release_pair:
                        first_release_pair[release_pair] = stamp
                        new_releases[(dependency, week_start(stamp.date()))] += 1
            if (package not in latest or
                    (release['first_upload'], version) > (releases[latest[package]]['first_upload'], latest[package][1])):
                latest[package] = key
            updated_targets = releases[latest[package]]['targets']
            for target in previous_target_set - updated_targets: latest_declaring[target].discard(package)
            for target in updated_targets - previous_target_set: latest_declaring[target].add(package)
        for package, tool in sorted(mapping.items(), key=lambda v: v[1]):
            selected = releases[latest[package]] if package in latest else None
            inbound_valid = bool(inbound_complete and not unknown[package])
            # All-declaration counts include environment/extra conditions. They
            # are never called installed users or an active downstream census.
            output.append({'tool_id': tool, 'package': package, 'week_start': monday.isoformat(),
                           'event_window_end_exclusive': (monday + timedelta(days=7)).isoformat(),
                           'feature_cutoff': (monday + timedelta(days=9)).isoformat(),
                           'dep_ever_declaring_packages': len(ever[package]) if inbound_complete else None,
                           'dep_new_declaring_packages': new_packages[(package, monday)] if inbound_complete else None,
                           'dep_declaring_release_count': new_releases[(package, monday)] if inbound_complete else None,
                           'dep_latest_uploaded_release_declaring_packages': len(latest_declaring[package]) if inbound_complete else None,
                           'dep_outbound_unique_dependencies': len(selected['dependencies']) if selected else None,
                           'dep_outbound_unconditional_dependencies': len(selected['unconditional']) if selected else None,
                           'dep_outbound_conditional_dependencies': len(selected['conditional']) if selected else None,
                           'dep_outbound_requirement_variants': len(selected['requirements']) if selected else None,
                           'dep_latest_uploaded_target_version': latest[package][1] if selected else None,
                           'dep_inbound_history_complete': bool(inbound_complete),
                           'dep_inbound_event_time_eligible': inbound_valid,
                           'dep_outbound_event_time_eligible': bool(selected and not selected['invalid'] and package not in undated_outbound),
                           'dep_unknown_target_artifacts': len(unknown[package]),
                           'historical_retrieval_vintage': False,
                           'measurement': 'Published dependency declarations; no installed-user or historical-resolution claim'})
        monday += timedelta(days=7)
    previous = defaultdict(list)
    for row in output:
        history = previous[row['tool_id']]
        for name in ['dep_new_declaring_packages', 'dep_declaring_release_count']:
            values = [v[name] for v in history[-3:]] + [row[name]]
            row[name + '_4w'] = sum(values) if len(values) == 4 and all(v is not None for v in values) else None
        for name in ['dep_ever_declaring_packages', 'dep_latest_uploaded_release_declaring_packages']:
            row[name + '_change_4w'] = (row[name] - history[-4][name]
                                      if len(history) >= 4 and row[name] is not None and history[-4][name] is not None else None)
        history.append(row)
    return output


def read_rows(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt') as stream:
        # JSON Lines permits bounded query-result pagination/export upstream.
        if str(path).endswith(('.jsonl', '.jsonl.gz')):
            return [json.loads(line) for line in stream if line.strip()]
        value = json.load(stream)
        rows = value.get('rows') if isinstance(value, dict) else value
        if not isinstance(rows, list):
            raise ValueError('Input must be decoded artifact rows or JSON Lines')
        return rows


def write_jsonl(path, rows):
    with Path(path).open('xb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0) as stream:
            for row in rows:
                stream.write((encode(row) + '\n').encode())


def build(args):
    if args.output.exists():
        raise FileExistsError('Use a fresh output directory to preserve metadata vintages')
    registry = json.loads(args.registry.read_text())
    plan = json.loads(args.query_plan.read_text())
    mapping = targets(registry)
    if (plan.get('source') != SOURCE or plan.get('end_exclusive') != args.end_exclusive
            or plan.get('target_package_to_tool') != mapping):
        raise ValueError('Query plan, endpoint or registry does not match')
    rows = read_rows(args.input)
    receipt = json.loads(args.query_receipt.read_text())
    if (receipt.get('query_sha256') != plan['query_sha256']
            or receipt.get('job_complete') is not True or receipt.get('all_pages_fetched') is not True
            or receipt.get('total_rows') != len(rows) or receipt.get('output_sha256') != sha(args.input)):
        raise ValueError('A complete, hash-bound query-result receipt is required; truncated pages are not full dependency history')
    artifacts, edges, audit = normalize_artifacts(rows, args.observed_at, args.end_exclusive)
    present = {v['package'] for v in artifacts if v['identity_valid']}
    missing_targets = sorted(set(mapping) - present)
    if missing_targets:
        audit['errors'].append({'kind': 'missing_target_artifacts', 'packages': missing_targets})
    complete = plan.get('mode') == 'candidate_history'
    weekly = weekly_features(artifacts, registry, args.start, args.end_exclusive, complete)
    args.output.mkdir(parents=True)
    write_jsonl(args.output / 'artifacts.jsonl.gz', artifacts)
    write_jsonl(args.output / 'declared_dependency_edges.jsonl.gz', edges)
    write_jsonl(args.output / 'parse_errors.jsonl.gz', audit['errors'])
    with (args.output / 'weekly_dependency_features.csv').open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(weekly[0]) if weekly else ['tool_id','week_start'])
        writer.writeheader(); writer.writerows(weekly)
    try:
        import pandas as pd
        pd.DataFrame(weekly).to_parquet(args.output / 'weekly_dependency_features.parquet', index=False)
    except ImportError:
        pass
    quality = {'source': SOURCE, 'status': 'passed' if not audit['errors'] and complete else 'needs_review',
               'observed_at': utc(args.observed_at).isoformat(), 'history_start': args.start,
               'history_end_exclusive': args.end_exclusive, 'registry_sha256': sha(args.registry),
               'input_metadata_sha256': sha(args.input), 'query_plan_sha256': sha(args.query_plan),
               'query_receipt_sha256': sha(args.query_receipt),
               'query_sha256': plan['query_sha256'], 'inbound_history_complete': complete,
               'code_sha256': {Path(__file__).name: sha(Path(__file__)),
                               'dependency_sources.py': sha(Path(__file__).with_name('dependency_sources.py'))},
               'packaging_version': importlib.metadata.version('packaging'),
               'historical_retrieval_vintage': False, 'weekly_rows': len(weekly), 'audit': audit,
               'limitations': ['Declared package requirements are not installed users or runtime use.',
                              'Conditional and extra requirements are retained without assuming an installation environment.',
                              'All artifact variants are retained; weekly release requirements union only files uploaded by the event boundary.',
                              'Latest uploaded release is an explicit declaration proxy, not the version chosen by pip or an active-user census.',
                              'Provider immutable upload dates support retrospective event-time reconstruction; original retrieval vintages are unavailable.',
                              'Malformed or undated metadata remain recorded and disable affected eligibility rather than becoming zero.']}
    (args.output / 'dependency_quality.json').write_text(json.dumps(quality, indent=2) + '\n')
    (args.output / 'query_plan.json').write_bytes(args.query_plan.read_bytes())
    (args.output / 'query_receipt.json').write_bytes(args.query_receipt.read_bytes())
    (args.output / 'tools.study.json').write_bytes(args.registry.read_bytes())
    manifest = {'status': quality['status'], 'source': SOURCE, 'registry_sha256': sha(args.registry),
                'history_end_exclusive': args.end_exclusive, 'observed_at': quality['observed_at'],
                'files': {p.name: sha(p) for p in sorted(args.output.iterdir()) if p.is_file()}}
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'status': quality['status'], 'artifacts': len(artifacts), 'edges': len(edges),
                      'weekly_rows': len(weekly), 'errors': len(audit['errors']), 'output': str(args.output)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--query-plan', type=Path, required=True)
    parser.add_argument('--query-receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', default='2023-01-01')
    parser.add_argument('--end-exclusive', required=True)
    parser.add_argument('--observed-at', required=True)
    build(parser.parse_args())


if __name__ == '__main__':
    main()
