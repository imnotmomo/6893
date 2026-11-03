"""Retain current deps.dev package requirements and resolved graph snapshots.

These observations are never a historical dependency-resolution time series.
For historical declared-dependency events, use immutable upload-dated PyPI
metadata and build_dependency_features.py instead.
"""
import argparse
import csv
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request

from packaging.version import Version, InvalidVersion
from dependency_sources import canonical, targets, utc


SOURCE = 'depsdev_current_version_requirements_and_resolution'
API = 'https://api.deps.dev/v3'
DOCUMENTATION = 'https://docs.deps.dev/api/v3/index.html'
MAX_BODY = 8 * 1024 * 1024
HEADERS = {'date', 'etag', 'last-modified', 'retry-after', 'content-type'}


def now(): return datetime.now(timezone.utc)
def iso(value): return value.astimezone(timezone.utc).isoformat()
def sha(raw): return hashlib.sha256(raw).hexdigest()


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def fetch(endpoint):
    if not endpoint.startswith('/systems/pypi/packages/') or '\n' in endpoint:
        raise ValueError('Only public PyPI package metadata endpoints are supported')
    request = urllib.request.Request(API + endpoint, headers={
        'Accept': 'application/json', 'User-Agent': 'EECS6893-AITechnologyRadar-dependency-observations'})
    started = now()
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            status, headers, body = response.status, dict(response.headers), response.read(MAX_BODY + 1)
    except urllib.error.HTTPError as error:
        status, headers, body = error.code, dict(error.headers), error.read(MAX_BODY + 1)
    if len(body) > MAX_BODY:
        raise ValueError('Response exceeds the bounded 8 MiB observation limit')
    return {'status': status, 'headers': {k.lower(): v for k, v in headers.items() if k.lower() in HEADERS},
            'requested_at': iso(started), 'observed_at': iso(now()), 'body': body}


def retry_time(headers, observed):
    stamp = utc(observed)
    delay = headers.get('retry-after')
    candidate = stamp + timedelta(seconds=300)
    if delay:
        try: candidate = max(candidate, stamp + timedelta(seconds=max(0, int(delay))))
        except ValueError:
            try: candidate = max(candidate, parsedate_to_datetime(delay).astimezone(timezone.utc))
            except (ValueError, TypeError): pass
    return iso(candidate)


def selected_version(response, expected_package):
    identity = response.get('packageKey', {})
    if identity.get('system') != 'PYPI' or canonical(identity.get('name', '')) != canonical(expected_package):
        raise ValueError('deps.dev package identity differs from frozen registry')
    versions = response.get('versions', [])
    if not isinstance(versions, list): raise ValueError('Version list is not an array')
    defaults = [v for v in versions if v.get('isDefault') is True]
    if len(defaults) > 1: raise ValueError('Multiple default versions returned')
    if defaults: chosen, rule = defaults[0], 'provider_current_default'
    else:
        parsed = []
        for item in versions:
            try: version = Version(item['versionKey']['version'])
            except (KeyError, InvalidVersion, TypeError): continue
            parsed.append((version, item))
        stable = [v for v in parsed if not v[0].is_prerelease and not v[0].is_devrelease]
        if not parsed: raise ValueError('No default or parseable version available')
        chosen = max(stable or parsed, key=lambda v: v[0])[1]
        rule = 'highest_pep440_stable_version_observed_now' if stable else 'highest_pep440_version_observed_now'
    key = chosen.get('versionKey', {})
    if (key.get('system') != 'PYPI' or canonical(key.get('name', '')) != canonical(expected_package)
            or not isinstance(key.get('version'), str) or not key['version']):
        raise ValueError('Chosen version identity differs from frozen registry')
    return key['version'], rule


def graph_counts(value):
    nodes, edges = value.get('nodes'), value.get('edges', [])
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ValueError('Resolved graph must contain node and edge arrays')
    if not nodes:
        raise ValueError('Resolved graph has no root package node')
    for edge in edges:
        for key in ['fromNode', 'toNode']:
            # Protobuf JSON may omit an integer whose value is the default0.
            index = edge.get(key, 0)
            if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(nodes):
                raise ValueError('Graph edge refers to an absent node')
    return len(nodes), len(edges)


class Deferred(Exception): pass


class Collector:
    def __init__(self, registry, output, max_requests=150, interval=1.0, selected=None, fetcher=fetch):
        self.registry = Path(registry)
        self.raw_registry = self.registry.read_bytes()
        registry_value = json.loads(self.raw_registry)
        targets(registry_value)
        self.tools = [t for t in registry_value['tools'] if not selected or t['tool_id'] in set(selected)]
        if not self.tools or (selected and {t['tool_id'] for t in self.tools} != set(selected)):
            raise ValueError('Unknown or empty tool selection')
        self.output = Path(output); self.output.mkdir(parents=True, exist_ok=True)
        self.path = self.output / 'manifest.json'
        self.limit, self.count, self.interval, self.fetcher = max_requests, 0, interval, fetcher
        if self.path.exists():
            self.manifest = json.loads(self.path.read_text())
            if (self.manifest.get('source') != SOURCE or self.manifest.get('registry_sha256') != sha(self.raw_registry)
                    or self.manifest.get('tool_ids') != [t['tool_id'] for t in self.tools]):
                raise ValueError('Resume differs from frozen registry/source')
            if sha((self.output/'tools.study.json').read_bytes()) != sha(self.raw_registry):
                raise ValueError('Frozen registry copy differs')
        else:
            (self.output/'tools.study.json').write_bytes(self.raw_registry)
            self.manifest = {'source': SOURCE, 'started_at': iso(now()), 'status': 'in_progress',
                             'tool_ids': [t['tool_id'] for t in self.tools], 'registry_sha256': sha(self.raw_registry),
                             'primary_historical_eligible': False, 'historical_resolution_vintage': False,
                             'measurement': 'Current package/version declaration and resolved dependency observations',
                             'documentation': DOCUMENTATION, 'requests': [], 'tools': {}}
            self.save()

    def save(self):
        self.manifest['updated_at'] = iso(now())
        atomic_json(self.path, self.manifest)

    def get(self, tool, kind, endpoint):
        key = tool + ':' + kind
        previous = [r for r in self.manifest['requests'] if r['key'] == key]
        success = [r for r in previous if r['status'] == 200]
        if success:
            item = success[-1]; compressed = (self.output/item['raw_path']).read_bytes()
            if sha(compressed) != item['gzip_sha256']:
                raise ValueError('Retained current observation checksum differs')
            raw = gzip.decompress(compressed)
            if sha(raw) != item['raw_sha256']:
                raise ValueError('Retained current observation checksum differs')
            return json.loads(raw), item
        if previous and previous[-1]['status'] in (400, 404):
            return None, previous[-1]
        until = self.manifest.get('retry_not_before')
        if until and now() < utc(until): raise Deferred('Provider cooldown until ' + until)
        if self.count >= self.limit: raise Deferred('Per-invocation request limit reached')
        if shutil.disk_usage(self.output).free < 15 * 1024**3:
            raise Deferred('Storage guard requires at least15 GiB free')
        last = self.manifest.get('last_request_started_at')
        if last:
            remaining = self.interval - (now() - utc(last)).total_seconds()
            if remaining > 0: time.sleep(remaining)
        self.count += 1
        self.manifest['last_request_started_at'] = iso(now()); self.save()
        try: result = self.fetcher(endpoint)
        except (OSError, urllib.error.URLError) as error:
            result = {'status': 0, 'headers': {}, 'body': b'', 'requested_at': iso(now()), 'observed_at': iso(now()),
                      'transport_error': type(error).__name__}
        raw = result.pop('body')
        filename = f'{len(previous)+1:02d}-{kind}-{sha(raw)[:16]}.json.gz'
        relative = Path('raw')/tool/filename; path = self.output/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        compressed = gzip.compress(raw, mtime=0)
        if path.exists() and path.read_bytes() != compressed: raise ValueError('Immutable response collision')
        if not path.exists(): path.write_bytes(compressed)
        item = {**result, 'key': key, 'endpoint': endpoint, 'raw_path': str(relative),
                'raw_sha256': sha(raw), 'gzip_sha256': sha(compressed), 'raw_bytes': len(raw),
                'gzip_bytes': len(compressed)}
        self.manifest['requests'].append(item)
        if item['status'] in (0, 403, 408, 429, 500, 502, 503, 504):
            self.manifest['retry_not_before'] = retry_time(item['headers'], item['observed_at'])
        self.save()
        if item['status'] != 200:
            if item['status'] in (400, 404): return None, item
            raise Deferred('Provider request deferred with HTTP ' + str(item['status']))
        return json.loads(raw), item

    def collect_tool(self, tool):
        tid, package = tool['tool_id'], canonical(tool['pypi_package'])
        endpoint = '/systems/pypi/packages/' + urllib.parse.quote(package, safe='')
        data, first = self.get(tid, 'package', endpoint)
        if data is None:
            return {'status': 'unavailable', 'package': package, 'http_status': first['status']}
        version, rule = selected_version(data, package)
        base = endpoint + '/versions/' + urllib.parse.quote(version, safe='')
        result = {'status': 'complete', 'package': package, 'version': version, 'selection_rule': rule,
                  'primary_historical_eligible': False, 'historical_resolution_vintage': False,
                  'observed_at_min': first['observed_at'], 'observed_at_max': first['observed_at']}
        for kind, suffix in [('version',''),('requirements',':requirements'),('dependencies',':dependencies')]:
            body, response = self.get(tid, kind, base + suffix)
            result[kind + '_http_status'] = response['status']
            result['observed_at_max'] = max(result['observed_at_max'], response['observed_at'])
            if body is None:
                result['status'] = 'partial_unavailable'
                continue
            if kind == 'version':
                key = body.get('versionKey', {})
                if key.get('version') != version or canonical(key.get('name','')) != package or key.get('system') != 'PYPI':
                    raise ValueError('Version response identity mismatch')
                result['provider_version_published_at'] = body.get('publishedAt')
            elif kind == 'requirements':
                if not isinstance(body.get('pypi'), dict):
                    raise ValueError('Requirements response lacks explicit PyPI system data')
                entries = body['pypi'].get('dependencies', [])
                if not isinstance(entries, list): raise ValueError('PyPI requirements must be an array')
                result['direct_requirement_count'] = len(entries)
            else:
                result['resolved_nodes'], result['resolved_edges'] = graph_counts(body)
        return result

    def run(self):
        for tool in self.tools:
            tid = tool['tool_id']
            try: self.manifest['tools'][tid] = self.collect_tool(tool)
            except Deferred as error: self.manifest['tools'][tid] = {'status': 'deferred', 'reason': str(error)}
            except (ValueError, KeyError, TypeError) as error:
                self.manifest['tools'][tid] = {'status': 'failed', 'reason': str(error)}
            self.save()
        finished = sum(v['status'] == 'complete' for v in self.manifest['tools'].values())
        terminal = all(v['status'] in ('complete','partial_unavailable','unavailable') for v in self.manifest['tools'].values())
        self.manifest.update(status='complete' if finished == len(self.tools) else 'complete_with_source_gaps' if terminal else 'partial',
                             tools_complete=finished, tools_requested=len(self.tools))
        fields = ['tool_id','package','version','status','direct_requirement_count','resolved_nodes','resolved_edges',
                  'provider_version_published_at','observed_at_min','observed_at_max',
                  'primary_historical_eligible','historical_resolution_vintage']
        path = self.output/'current_dependency_observations.csv'
        with path.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore'); writer.writeheader()
            writer.writerows({'tool_id': tid, **v} for tid,v in sorted(self.manifest['tools'].items()))
        self.manifest['csv_sha256'] = sha(path.read_bytes()); self.save()
        return self.manifest


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tool', action='append')
    parser.add_argument('--max-requests', type=int, default=150)
    parser.add_argument('--interval', type=float, default=1.0)
    args = parser.parse_args()
    if not 1 <= args.max_requests <= 150 or not 1 <= args.interval <= 60: parser.error('Bound requests1–150 and intervals1–60seconds')
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output/'.collector.lock').open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: return 0
        result = Collector(args.registry,args.output,args.max_requests,args.interval,args.tool).run()
    print(json.dumps({k: result[k] for k in ('source','status','tools_complete','tools_requested')}))
    return 0 if result['status'] in ('complete','complete_with_source_gaps') else 1


if __name__ == '__main__': raise SystemExit(main())
