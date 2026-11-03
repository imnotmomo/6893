#!/usr/bin/env python3
"""Retain GitHub's aggregate star-history API without collecting user lists.

This is a new supplementary source, not a repair or replacement of GH Archive.
The API reconstructs surviving star records at collection time. It does not
provide historical point-in-time star totals, unstars, or general activity.
"""
import argparse
import csv
import fcntl
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.error
import urllib.request

SOURCE = 'github_direct_stars'
VERSION = '2026-03-10'
WEEK = 604800
DOCUMENTATION = 'https://docs.github.com/en/rest/activity/starring?apiVersion=2026-03-10#get-repository-star-history'
SEMANTIC_EVIDENCE = 'https://github.blog/engineering/architecture-optimization/from-2500-requests-to-24-rebuilding-star-history-without-the-stargazers/'
SEMANTICS = ('Retrospective counts of currently surviving star records grouped by creation time; '
             'not historical net stars, historical point-in-time totals, WatchEvents, commits, or users.')
SAFE_HEADERS = {'date', 'etag', 'last-modified', 'link', 'retry-after', 'x-ratelimit-limit',
                'x-ratelimit-remaining', 'x-ratelimit-reset', 'x-ratelimit-resource', 'x-github-request-id'}

def now():
    return datetime.now(timezone.utc)

def stamp(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)

def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)

def valid_repo(value):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', str(value)):
        raise ValueError('Invalid repository name')
    return str(value)

def parse_gh_response(raw):
    """Parse gh --include output, including a possible followed redirect."""
    status, headers = None, {}
    while raw.startswith(b'HTTP/'):
        separator = b'\r\n\r\n' if b'\r\n\r\n' in raw else b'\n\n'
        if separator not in raw:
            raise ValueError('Missing gh response header boundary')
        block, raw = raw.split(separator, 1)
        lines = block.decode('utf-8', 'replace').splitlines()
        match = re.match(r'HTTP/[\d.]+\s+(\d{3})', lines[0])
        if not match:
            raise ValueError('Unrecognized gh HTTP status')
        status = int(match.group(1)); headers = {}
        for line in lines[1:]:
            if ':' in line:
                key, value = line.split(':', 1)
                if key.lower() in SAFE_HEADERS:
                    headers[key.lower()] = value.strip()
        raw = raw.lstrip(b'\r\n')
    if status is None:
        raise ValueError('gh did not return HTTP headers; check gh authentication')
    return status, headers, raw

def request(endpoint, transport):
    if not endpoint.startswith('/repos/') or '\n' in endpoint:
        raise ValueError('Only repository metadata and aggregate history endpoints are supported')
    started = now()
    if transport == 'gh':
        command = ['gh', 'api', '--hostname', 'github.com', '--method', 'GET', '--include',
                   '-H', 'Accept: application/vnd.github+json', '-H', 'X-GitHub-Api-Version: ' + VERSION,
                   endpoint]
        result = subprocess.run(command, capture_output=True, timeout=55)
        # Authentication is handled inside gh. Never capture/print its token or stderr.
        status, headers, body = parse_gh_response(result.stdout)
    else:
        req = urllib.request.Request('https://api.github.com' + endpoint, headers={
            'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': VERSION,
            'User-Agent': 'EECS6893-AITechnologyRadar-aggregate-star-history'})
        try:
            with urllib.request.urlopen(req, timeout=45) as response:
                status, headers, body = response.status, dict(response.headers), response.read(8 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as error:
            status, headers, body = error.code, dict(error.headers), error.read(8 * 1024 * 1024 + 1)
        headers = {k.lower(): str(v) for k, v in headers.items() if k.lower() in SAFE_HEADERS}
    if len(body) > 8 * 1024 * 1024:
        raise ValueError('Aggregate response exceeds 8 MiB limit')
    return {'status': status, 'headers': headers, 'body': body,
            'requested_at': iso(started), 'observed_at': iso(now())}

def retry_at(status, headers, observed):
    """Return an explicit future retry time; callers exit instead of busy retrying."""
    current = stamp(observed)
    candidates = [current + timedelta(seconds=300 if status == 403 else 60)]
    retry = headers.get('retry-after')
    if retry:
        try:
            candidates.append(current + timedelta(seconds=max(0, int(retry))))
        except ValueError:
            try:
                candidates.append(parsedate_to_datetime(retry).astimezone(timezone.utc))
            except (TypeError, ValueError):
                pass
    if headers.get('x-ratelimit-remaining') == '0' and headers.get('x-ratelimit-reset'):
        try:
            candidates.append(datetime.fromtimestamp(int(headers['x-ratelimit-reset']), timezone.utc) + timedelta(seconds=5))
        except ValueError:
            pass
    return iso(max(candidates))

class Deferred(Exception):
    pass

class Collector:
    def __init__(self, output, registry, start, as_of=None, transport='gh', selected=None,
                 recent_weeks=0, alignment_delay_days=3, max_requests=500, fetch=request):
        self.root = Path(output)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / 'collection_manifest.json'
        self.registry_bytes = Path(registry).read_bytes()
        registry_value = json.loads(self.registry_bytes)
        self.tools = [t for t in registry_value['tools'] if not selected or t['tool_id'] in set(selected)]
        if not self.tools or len({t['tool_id'] for t in self.tools}) != len(self.tools):
            raise ValueError('Empty or duplicate selected tool cohort')
        if selected and set(selected) != {t['tool_id'] for t in self.tools}:
            raise ValueError('Unknown selected tool')
        for tool in self.tools:
            if not re.fullmatch(r'[A-Za-z0-9_.-]+', str(tool['tool_id'])):
                raise ValueError('Invalid tool identifier')
            valid_repo(tool['github_repo'])
            if not isinstance(tool.get('github_repo_id'), int) or isinstance(tool.get('github_repo_id'), bool):
                raise ValueError('Frozen numeric GitHub repository identity is required')
        self.transport, self.fetch = transport, fetch
        self.collector_sha256 = sha(Path(__file__).read_bytes())
        self.request_count, self.max_requests = 0, max_requests
        if self.path.exists():
            self.manifest = json.loads(self.path.read_text())
            if (self.manifest.get('source') != SOURCE or self.manifest.get('registry_sha256') != sha(self.registry_bytes)
                    or self.manifest.get('requested_start') != start
                    or self.manifest.get('tool_ids') != [t['tool_id'] for t in self.tools]
                    or self.manifest.get('recent_weeks') != recent_weeks
                    or self.manifest.get('alignment_delay_days') != alignment_delay_days
                    or (as_of and stamp(self.manifest['as_of']) != stamp(as_of))):
                raise ValueError('Resume arguments differ from frozen collection manifest')
            if sha((self.root / 'tools.study.json').read_bytes()) != sha(self.registry_bytes):
                raise ValueError('Frozen registry copy checksum mismatch')
        else:
            self.manifest = {'source': SOURCE, 'format_version': 1, 'api_version': VERSION,
                'registry_sha256': sha(self.registry_bytes), 'requested_start': start,
                'as_of': iso(stamp(as_of) if as_of else now()), 'started_at': iso(now()),
                'tool_ids': [t['tool_id'] for t in self.tools], 'recent_weeks': recent_weeks,
                'alignment_delay_days': alignment_delay_days, 'alignment_delay_is_assumption': True,
                'historical_vintage': False, 'retrospective': True, 'primary_eligible': False,
                'measurement': SEMANTICS, 'documentation': DOCUMENTATION, 'semantic_evidence': SEMANTIC_EVIDENCE,
                'point_in_time_snapshot': False,
                'collector_sha256': self.collector_sha256,
                'bucket_boundary': 'API-defined calendar weeks and Sunday-first days; UTC alignment is not guaranteed.',
                'requests': [], 'tools': {}, 'status': 'in_progress'}
            (self.root / 'tools.study.json').write_bytes(self.registry_bytes)
            self.save()
        self.as_of = stamp(self.manifest['as_of'])
        self.start = stamp(start)
        if recent_weeks:
            self.start = max(self.start, self.as_of - timedelta(weeks=recent_weeks))

    def save(self):
        self.manifest['updated_at'] = iso(now())
        atomic_json(self.path, self.manifest)

    def get(self, tool_id, kind, endpoint, page=None):
        key = tool_id + ':' + kind + ':' + str(page or 0)
        previous = [r for r in self.manifest['requests'] if r['key'] == key]
        successful = [r for r in previous if r['status'] == 200]
        if successful:
            record = successful[-1]
            raw = gzip.decompress((self.root / record['raw_path']).read_bytes())
            if sha(raw) != record['raw_sha256']:
                raise ValueError('Retained raw checksum mismatch: ' + key)
            return json.loads(raw)
        if previous:
            last = previous[-1]
            if last.get('retry_not_before') and now() < stamp(last['retry_not_before']):
                raise Deferred('Retry deferred until ' + last['retry_not_before'])
            if last['status'] == 404:
                raise ValueError('Repository or endpoint unavailable (404); retained response preserved')
        if self.manifest.get('api_retry_not_before') and now() < stamp(self.manifest['api_retry_not_before']):
            raise Deferred('API retry deferred until ' + self.manifest['api_retry_not_before'])
        if self.request_count >= self.max_requests:
            raise Deferred('Per-invocation request limit reached; rerun the same command to resume')
        self.request_count += 1
        try:
            result = self.fetch(endpoint, self.transport)
        except (OSError, subprocess.SubprocessError, urllib.error.URLError) as error:
            # Avoid storing exception text which could contain local credential paths.
            result = {'status': 0, 'headers': {}, 'body': b'', 'requested_at': iso(now()), 'observed_at': iso(now()),
                      'transport_error': type(error).__name__}
        raw = result.pop('body')
        digest = sha(raw)
        relative = Path('raw') / tool_id / (f'{len(previous)+1:02d}-{kind}-{page or 0:03d}-' + digest[:16] + '.json.gz')
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        compressed = gzip.compress(raw, mtime=0)
        if path.exists() and gzip.decompress(path.read_bytes()) != raw:
            raise ValueError('Immutable raw path collision')
        if not path.exists():
            with path.open('xb') as stream:
                stream.write(compressed)
        record = {**result, 'key': key, 'endpoint': endpoint, 'transport': self.transport,
                  'collector_sha256': self.collector_sha256,
                  'raw_path': str(relative), 'raw_sha256': digest, 'compressed_sha256': sha(path.read_bytes()),
                  'raw_bytes': len(raw), 'compressed_bytes': path.stat().st_size, 'page': page}
        if record['status'] in (0, 403, 408, 429, 500, 502, 503, 504):
            record['retry_not_before'] = retry_at(record['status'], record['headers'], record['observed_at'])
        if record['status'] == 429 or (record['status'] == 403 and record['headers'].get('x-ratelimit-remaining') == '0'):
            self.manifest['api_retry_not_before'] = record['retry_not_before']
        self.manifest['requests'].append(record)
        self.save()
        if record['status'] != 200:
            if record.get('retry_not_before'):
                raise Deferred(f"HTTP {record['status']}; retry deferred until " + record['retry_not_before'])
            raise ValueError(f"HTTP {record['status']}; raw response retained")
        try:
            return json.loads(raw)
        except ValueError as error:
            raise ValueError('Successful response is not JSON') from error

    def collect_tool(self, tool):
        tid = tool['tool_id']
        metadata = self.get(tid, 'repository', '/repos/' + valid_repo(tool['github_repo']))
        if not isinstance(metadata, dict) or metadata.get('id') != tool['github_repo_id']:
            raise ValueError('Repository numeric identity does not match frozen registry')
        repo = valid_repo(metadata['full_name'])
        created = stamp(metadata['created_at'])
        required_begin = max(self.start.timestamp(), created.timestamp())
        entries = []
        pages = 0
        for page in range(1, 101):
            batch = self.get(tid, 'history', '/repos/' + repo + '/stargazers/history?per_page=30&page=' + str(page), page)
            validate_rows(batch)
            if len(batch) > 30:
                raise ValueError('History page exceeds the documented 30-week maximum')
            if not batch:
                raise ValueError('History ended before required coverage was reached')
            entries.extend(batch)
            validate_rows(entries)
            pages = page
            if entries[-1]['week'] <= required_begin:
                break
            if len(batch) < 30:
                raise ValueError('Truncated page before required coverage was reached')
        else:
            raise ValueError('100-page API limit reached before required coverage')
        summary, rows = validate_history(entries, created, self.start, self.as_of,
                                         self.manifest['alignment_delay_days'])
        observations = [r['observed_at'] for r in self.manifest['requests'] if r['key'].startswith(tid + ':') and r['status'] == 200]
        observed_min, observed_max = min(observations), max(observations)
        for row in rows:
            row.update({'tool_id': tid, 'repository_id': metadata['id'], 'repository': repo,
                        'source': SOURCE, 'retrospective': True, 'historical_vintage': False,
                        'primary_eligible': False, 'collection_as_of': self.manifest['as_of'],
                        'observed_at': observed_max})
        summary.update({'status': 'passed', 'repository_id': metadata['id'], 'repository': repo,
                        'repository_created_at': iso(created), 'pages': pages,
                        'observed_at_min': observed_min, 'observed_at_max': observed_max})
        return summary, rows

    def run(self):
        all_rows = []
        for tool in self.tools:
            tid = tool['tool_id']
            try:
                summary, rows = self.collect_tool(tool)
                all_rows.extend(rows)
                self.manifest['tools'][tid] = summary
            except Deferred as error:
                self.manifest['tools'][tid] = {'status': 'deferred', 'reason': str(error)}
            except (KeyError, TypeError, ValueError) as error:
                self.manifest['tools'][tid] = {'status': 'failed', 'reason': str(error)}
            self.save()
        fields = ['source', 'tool_id', 'repository_id', 'repository', 'api_week_start_epoch',
                  'api_week_start_timestamp_rendered_utc', 'api_week_end_epoch', 'stars',
                  *['day_' + str(n) for n in range(7)], 'creation_partial_week',
                  'conservative_alignment_cutoff_utc', 'alignment_eligible_as_of',
                  'retrospective', 'historical_vintage', 'primary_eligible', 'collection_as_of', 'observed_at']
        path = self.root / 'github_direct_stars_weekly.csv'
        temporary = path.with_suffix('.csv.tmp')
        with temporary.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted(all_rows, key=lambda r: (r['tool_id'], r['api_week_start_epoch'])))
        temporary.replace(path)
        passed = sum(value['status'] == 'passed' for value in self.manifest['tools'].values())
        self.manifest.update({'status': 'passed' if passed == len(self.tools) else 'partial',
                              'tools_passed': passed, 'weekly_rows': len(all_rows),
                              'weekly_csv_sha256': sha(path.read_bytes()), 'finished_at': iso(now())})
        quality = {'source': SOURCE, 'validation_status': self.manifest['status'],
                   'validation_scope': 'Identity, retained response integrity, count arithmetic and requested API-bucket coverage only.',
                   'primary_eligible': False, 'historical_vintage': False, 'retrospective': True,
                   'measurement': SEMANTICS, 'boundary_alignment_certified_utc': False,
                   'alignment_delay_days': self.manifest['alignment_delay_days'], 'alignment_delay_is_assumption': True,
                   'as_of': self.manifest['as_of'], 'registry_sha256': self.manifest['registry_sha256'],
                   'weekly_csv_sha256': self.manifest['weekly_csv_sha256'], 'tools': self.manifest['tools'],
                   'documentation': DOCUMENTATION, 'semantic_evidence': SEMANTIC_EVIDENCE}
        atomic_json(self.root / 'github_direct_stars_quality.json', quality)
        self.save()
        return self.manifest

def validate_rows(entries):
    if not isinstance(entries, list):
        raise ValueError('History response must be an array')
    previous = None
    for row in entries:
        if not isinstance(row, dict) or not isinstance(row.get('week'), int) or isinstance(row.get('week'), bool):
            raise ValueError('Week anchor must be integer Unix seconds')
        days, total = row.get('days'), row.get('total')
        if (not isinstance(days, list) or len(days) != 7
                or any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in days)
                or not isinstance(total, int) or isinstance(total, bool) or total < 0 or total != sum(days)):
            raise ValueError('Weekly total must equal seven nonnegative integer daily counts')
        if previous is not None and previous - row['week'] != WEEK:
            raise ValueError('Duplicate, unordered or non-604800-second history anchors; inspect source/DST, do not invent missing weeks')
        previous = row['week']

def validate_history(entries, created, start, as_of, alignment_delay_days):
    validate_rows(entries)
    if not entries:
        raise ValueError('Empty history')
    if entries[0]['week'] > as_of.timestamp() or as_of.timestamp() - entries[0]['week'] >= 2 * WEEK:
        raise ValueError('Newest API bucket is future-dated or too stale for requested as-of')
    if entries[-1]['week'] > max(start.timestamp(), created.timestamp()):
        raise ValueError('Required study start/creation bucket is missing')
    if any(row['week'] + WEEK <= created.timestamp() for row in entries):
        raise ValueError('History contains a full bucket before repository creation')
    rows = []
    partial = 0
    for entry in entries:
        begin, end = entry['week'], entry['week'] + WEEK
        if end > as_of.timestamp():
            partial += 1
            continue
        if end <= start.timestamp():
            continue
        aligned = datetime.fromtimestamp(end, timezone.utc) + timedelta(days=alignment_delay_days)
        rows.append({'api_week_start_epoch': begin,
                     'api_week_start_timestamp_rendered_utc': iso(datetime.fromtimestamp(begin, timezone.utc)),
                     'api_week_end_epoch': end, 'stars': entry['total'],
                     **{'day_' + str(n): value for n, value in enumerate(entry['days'])},
                     'creation_partial_week': begin < created.timestamp(),
                     'conservative_alignment_cutoff_utc': iso(aligned), 'alignment_eligible_as_of': aligned <= as_of})
    if not rows:
        raise ValueError('No completed API weeks in requested interval')
    return {'validation_status': 'passed', 'continuous_week_spacing_seconds': WEEK,
            'duplicate_weeks': 0, 'raw_api_weeks': len(entries), 'completed_api_weeks': len(rows),
            'excluded_current_partial_weeks': partial,
            'earliest_completed_api_week_epoch': min(r['api_week_start_epoch'] for r in rows),
            'latest_completed_api_week_epoch': max(r['api_week_start_epoch'] for r in rows),
            'required_start_covered': True, 'identity_verified': True,
            'historical_vintage': False, 'primary_eligible': False,
            'semantic_quality': 'supplementary_retrospective_surviving_stars',
            'boundary_alignment_certified_utc': False}, rows

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, default=Path.home() / '6893/config/tools.study.json')
    parser.add_argument('--output', type=Path, required=True, help='New or resumable collection directory; retained raw responses are immutable')
    parser.add_argument('--start', default='2023-01-01')
    parser.add_argument('--as-of')
    parser.add_argument('--transport', choices=['gh', 'urllib'], default='gh')
    parser.add_argument('--tool', action='append')
    parser.add_argument('--recent-weeks', type=int, default=0, help='Optional bounded incremental window; zero collects the full requested history')
    parser.add_argument('--alignment-delay-days', type=int, default=3, help='Conservative alignment assumption, not a historical availability guarantee')
    parser.add_argument('--max-requests', type=int, default=500)
    args = parser.parse_args()
    if args.recent_weeks < 0 or args.alignment_delay_days < 1 or args.max_requests < 1:
        parser.error('Invalid positive collection limits')
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / '.collection.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('Collection is already running for this output directory.', file=sys.stderr)
            return 1
        result = Collector(args.output, args.registry, args.start, args.as_of, args.transport, args.tool,
                           args.recent_weeks, args.alignment_delay_days, args.max_requests).run()
    print(json.dumps({k: result[k] for k in ['source', 'status', 'tools_passed', 'weekly_rows', 'historical_vintage', 'primary_eligible']}, indent=2))
    return 0 if result['status'] == 'passed' else 1

if __name__ == '__main__':
    sys.exit(main())
