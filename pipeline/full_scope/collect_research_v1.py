#!/usr/bin/env python3
"""Enrich retained arXiv candidates with explicit first-version metadata.

This is a retrospective candidate dataset. Retrieving v1 removes revised-text
leakage for that version; it does not certify historical search recall or
manually reviewed tool relevance. No primary-model eligibility is granted.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone, date
from email.utils import parsedate_to_datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

UTC = timezone.utc
ATOM = '{http://www.w3.org/2005/Atom}'
ARXIV = '{http://arxiv.org/schemas/atom}'
SOURCE = 'arxiv_first_version_metadata'
POLICY = 'v1_conservative_lexical_candidates_v1'
API = 'https://export.arxiv.org/api/query'
MAX_BODY = 4 * 1024 * 1024
MIN_INTERVAL = 10.0
NOTE = ('First-version metadata is retrieved retrospectively from a candidate universe '
        'discovered by current-query searches. Lexical matches are machine-classified '
        'candidates, not verified library use, citations, adoption, or manually approved labels.')
SAFE_HEADERS = {'date', 'retry-after', 'content-type', 'etag', 'last-modified'}


def now():
    return datetime.now(UTC)


def stamp(value):
    value = str(value).strip().replace('Z', '+00:00')
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError('Explicit timezone required: ' + value)
    return parsed.astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)


def canonical_id(value):
    value = str(value).strip()
    if value.startswith(('http://', 'https://')):
        url = urllib.parse.urlparse(value)
        if url.hostname not in {'arxiv.org', 'export.arxiv.org', 'www.arxiv.org'}:
            raise ValueError('Unsupported arXiv identifier host')
        value = re.sub(r'^/(?:abs|pdf)/', '', url.path)
    value = re.sub(r'\.pdf$', '', value)
    value = re.sub(r'v[1-9][0-9]*$', '', value)
    if not re.fullmatch(r'(?:[0-9]{4}\.[0-9]{4,5}|[A-Za-z][A-Za-z0-9.-]*/[0-9]{7})', value):
        raise ValueError('Invalid arXiv identifier: ' + value)
    return value


def text(element, name, namespace=ATOM):
    return ' '.join((element.findtext(namespace + name) or '').split())


def parse_v1_feed(raw, requested, observed_at):
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise ValueError('Unsupported XML declarations')
    feed = ET.fromstring(raw)
    if feed.tag != ATOM + 'feed':
        raise ValueError('Response is not an Atom feed')
    expected = set(requested)
    output = {}
    for item in feed.findall(ATOM + 'entry'):
        identifier = text(item, 'id')
        if '/api/errors' in identifier or text(item, 'title').lower() == 'error':
            raise ValueError('arXiv error entry: ' + text(item, 'summary'))
        base = canonical_id(identifier)
        if base not in expected or base in output or not identifier.endswith('v1'):
            raise ValueError('Unexpected, duplicate or non-v1 paper response')
        title, summary = text(item, 'title'), text(item, 'summary')
        published, updated = stamp(text(item, 'published')), stamp(text(item, 'updated'))
        if not title or not summary or not published <= updated <= stamp(observed_at):
            raise ValueError('Missing text or invalid first-version dates')
        authors = []
        for author in item.findall(ATOM + 'author'):
            authors.append({'name': text(author, 'name'),
                            'affiliation': text(author, 'affiliation', ARXIV) or None})
        if not authors or any(not author['name'] for author in authors):
            raise ValueError('Missing first-version author metadata')
        categories = sorted({element.get('term') for element in item.findall(ATOM + 'category') if element.get('term')})
        primary = item.find(ARXIV + 'primary_category')
        output[base] = {'paper_id': base, 'requested_version': 1, 'returned_id': identifier,
                        'title_v1': title, 'abstract_v1': summary, 'authors_v1': authors,
                        'published': iso(published), 'v1_updated': iso(updated),
                        'v1_timestamp_equals_first_submission': published == updated,
                        'categories': categories,
                        'primary_category': primary.get('term') if primary is not None else None,
                        'doi': text(item, 'doi', ARXIV) or None,
                        'journal_ref': text(item, 'journal_ref', ARXIV) or None,
                        'comment': text(item, 'comment', ARXIV) or None,
                        'links': [dict(link.attrib) for link in item.findall(ATOM + 'link')],
                        'metadata_observed_at': observed_at, 'version_verified': True,
                        'historical_feature_eligible': False, 'manual_review_completed': False}
    return output, sorted(expected - set(output))


def literal_matches(value, aliases):
    return [alias for alias in aliases if re.search(r'(?<!\w)' + re.escape(alias) + r'(?!\w)', value, re.I)]


def classify(tool, paper, availability_lag_days=2):
    value = paper['title_v1'] + '\n' + paper['abstract_v1']
    aliases = tool.get('research_exact_aliases') or []
    hits = literal_matches(value, aliases)
    ambiguous = bool(tool.get('research_ambiguous'))
    # Ambiguous tools are accepted as *candidates* only through the registry's
    # explicit contextual phrases. A bare Ray/PEFT/Transformers token is not enough.
    if hits:
        status = 'strict_contextual_v1_candidate' if ambiguous else 'strict_alias_v1_candidate'
    else:
        broad = [tool['tool_id'].replace('-', ' '), tool.get('name', ''), tool.get('pypi_package', '')]
        status = 'ambiguous_v1_review_required' if ambiguous and literal_matches(value, [v for v in broad if v]) else 'no_strict_v1_match'
    published, updated = stamp(paper['published']), stamp(paper['v1_updated'])
    available = max(published, updated) + timedelta(days=availability_lag_days)
    return {'match_policy': POLICY, 'match_status': status, 'matched_aliases': hits,
            'strict_lexical_candidate': bool(hits),
            'match_evidence': 'first_version_title_and_abstract',
            'review_required': True, 'manual_review_completed': False,
            'relevance_precision_validated': False, 'historical_feature_eligible': False,
            'assumed_v1_available_at': iso(available), 'availability_lag_days': availability_lag_days,
            'availability_is_assumption': True, 'historical_public_availability_certified': False,
            'discovery_coverage_certified': False,
            'interpretation': 'A text mention candidate, not proven tool use or package adoption.'}


def load_pairs(path, registry):
    tools = {tool['tool_id']: tool for tool in registry['tools']}
    pairs = {}
    with Path(path).open(newline='') as stream:
        for row in csv.DictReader(stream):
            if row.get('tool_id') not in tools:
                continue
            pid = canonical_id(row.get('id') or row.get('paper_id'))
            key = (row['tool_id'], pid)
            if key not in pairs or str(row.get('observed_at', '')) > str(pairs[key].get('candidate_observed_at', '')):
                pairs[key] = {'tool_id': row['tool_id'], 'paper_id': pid,
                              'candidate_published': row.get('published', ''),
                              'candidate_updated': row.get('updated', ''),
                              'candidate_observed_at': row.get('observed_at', ''),
                              'candidate_query': row.get('candidate_query', ''),
                              'discovery_source': 'retained_current_query_candidates'}
    if not pairs:
        raise ValueError('No valid retained arXiv candidate pairs')
    return [pairs[key] for key in sorted(pairs)]


def export_backfill_candidates(data, target):
    # Call only while holding the same project collection lock as the writer.
    import duckdb
    with duckdb.connect(str(Path(data) / 'radar.duckdb'), read_only=True) as con:
        rows = con.execute("SELECT r.tool_id,r.observed_at,r.payload FROM backfill.records r JOIN backfill.jobs j USING(job_id) WHERE r.source='arxiv' AND j.state='done'").fetchall()
    fields = ['tool_id', 'id', 'published', 'updated', 'observed_at', 'candidate_query']
    with Path(target).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
        for tool_id, observed_at, payload in rows:
            item = json.loads(payload) if isinstance(payload, str) else payload
            writer.writerow({'tool_id': tool_id, 'observed_at': str(observed_at),
                             **{key: item.get(key, '') for key in ['id', 'published', 'updated', 'candidate_query']}})


class Deferred(Exception):
    pass


def request(ids):
    url = API + '?' + urllib.parse.urlencode({'id_list': ','.join(pid + 'v1' for pid in ids),
                                             'start': 0, 'max_results': len(ids)})
    headers = {'User-Agent': 'EECS6893-AITechnologyRadar-first-version-research/1.0',
               'Accept': 'application/atom+xml', 'Accept-Encoding': 'identity'}
    req = urllib.request.Request(url, headers=headers)
    started = now()
    try:
        response = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read(MAX_BODY + 1)
        return {'status': response.getcode(), 'request_url': url,
                'headers': {key.lower(): val for key, val in response.headers.items() if key.lower() in SAFE_HEADERS},
                'requested_at': iso(started), 'observed_at': iso(now()),
                'body_truncated': len(raw) > MAX_BODY, 'body': raw[:MAX_BODY]}


def retry_seconds(headers, current, minimum=120):
    value = headers.get('retry-after')
    if not value:
        return minimum
    try:
        return max(minimum, float(value))
    except ValueError:
        try:
            return max(minimum, (parsedate_to_datetime(value).astimezone(UTC) - current).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return minimum


def preserve_cooldown(data, until, reason):
    path = Path(data) / '.arxiv-cooldown.json'
    if path.exists():
        existing = read_json(path)
        if stamp(existing['until']) >= until:
            return
    atomic_json(path, {'until': iso(until), 'reason': reason})


class Collector:
    def __init__(self, output, registry, candidates, data, start='2023-01-01', end=None,
                 batch_size=100, max_requests=50, max_seconds=480, availability_lag_days=2,
                 fetch=request, sleep=time.sleep, clock=now):
        self.root, self.data = Path(output), Path(data)
        self.root.mkdir(parents=True, exist_ok=True); self.data.mkdir(parents=True, exist_ok=True)
        self.registry_bytes = Path(registry).read_bytes()
        self.code_sha256 = sha(Path(__file__).read_bytes())
        self.registry = json.loads(self.registry_bytes)
        self.tools = {tool['tool_id']: tool for tool in self.registry['tools']}
        if not self.tools or len(self.tools) != len(self.registry['tools']):
            raise ValueError('Missing or duplicate registry tools')
        if not 1 <= batch_size <= 100 or not 0 <= availability_lag_days <= 7:
            raise ValueError('Use batches1..100 and availability lag0..7 days')
        self.fetch, self.sleep, self.clock = fetch, sleep, clock
        self.max_requests, self.max_seconds = max_requests, max_seconds
        self.requests_used = 0; self.deadline = time.monotonic() + max_seconds
        self.last_response_at = None
        self.manifest_path = self.root / 'collection_manifest.json'
        if self.manifest_path.exists():
            self.manifest = read_json(self.manifest_path)
            if (self.manifest['registry_sha256'] != sha(self.registry_bytes)
                    or self.manifest['batch_size'] != batch_size
                    or self.manifest['history_start'] != start
                    or self.manifest['availability_lag_days'] != availability_lag_days
                    or (end and self.manifest['history_end_exclusive'] != end)):
                raise ValueError('Resume arguments differ from frozen research collection')
            frozen = self.root / 'candidates_input.csv'
            if sha(frozen.read_bytes()) != self.manifest['candidate_input_sha256']:
                raise ValueError('Frozen candidate input changed')
            if (self.root / 'tools.study.json').read_bytes() != self.registry_bytes:
                raise ValueError('Frozen research registry copy changed')
            if candidates and sha(Path(candidates).read_bytes()) != self.manifest['candidate_input_sha256']:
                raise ValueError('Use a new output for a changed candidate universe')
        else:
            frozen = self.root / 'candidates_input.csv'
            if candidates:
                frozen.write_bytes(Path(candidates).read_bytes())
            else:
                export_backfill_candidates(self.data, frozen)
            (self.root / 'tools.study.json').write_bytes(self.registry_bytes)
            stop = end or self.clock().date().isoformat()
            if not date.fromisoformat(start) < date.fromisoformat(stop) <= self.clock().date():
                raise ValueError('Use a nonempty completed-date range')
            self.manifest = {'source': SOURCE, 'format_version': 1, 'match_policy': POLICY,
                'created_at': iso(self.clock()), 'as_of': iso(self.clock()),
                'history_start': start, 'history_end_exclusive': stop,
                'registry_sha256': sha(self.registry_bytes), 'candidate_input_sha256': sha(frozen.read_bytes()),
                'collector_sha256': self.code_sha256, 'batch_size': batch_size,
                'availability_lag_days': availability_lag_days,
                'historical_feature_eligible': False, 'manual_review_completed': False,
                'discovery_coverage_certified': False, 'note': NOTE, 'requests': [], 'batches': {},
                'status': 'in_progress'}
        self.pairs = load_pairs(frozen, self.registry)
        self.ids = sorted({pair['paper_id'] for pair in self.pairs})
        self.batches = [self.ids[offset:offset+batch_size] for offset in range(0, len(self.ids), batch_size)]
        self.manifest.update({'candidate_pairs': len(self.pairs), 'unique_candidate_papers': len(self.ids),
                              'requested_batches': len(self.batches),
                              'collector_versions': sorted(set(self.manifest.get('collector_versions', [self.manifest['collector_sha256']])) | {self.code_sha256})})
        self.save()

    def save(self):
        self.manifest['updated_at'] = iso(self.clock())
        atomic_json(self.manifest_path, self.manifest)

    def guard(self):
        if shutil.disk_usage(self.data).free < 15 * 1024**3:
            raise Deferred('Below the project15GiB free-space guard')
        if self.requests_used >= self.max_requests or time.monotonic() >= self.deadline:
            raise Deferred('Per-invocation request/time limit; resume the same output')
        cooldown = self.data / '.arxiv-cooldown.json'
        if cooldown.exists() and stamp(read_json(cooldown)['until']) > self.clock():
            raise Deferred('Project-wide arXiv cooldown is active')

    def get_batch(self, number, ids):
        key = str(number)
        batch = self.manifest['batches'].get(key)
        if batch and batch['status'] in {'complete','incomplete_response'}:
            raw = gzip.decompress((self.root / batch['raw_path']).read_bytes())
            if sha(raw) != batch['raw_sha256']:
                raise ValueError('Cached first-version raw checksum mismatch')
            papers, missing = parse_v1_feed(raw, ids, batch['observed_at'])
            if missing and batch['status'] == 'complete':
                raise ValueError('Cached complete batch lacks requested papers')
            if batch['status'] == 'complete' or (batch.get('retry_not_before') and stamp(batch['retry_not_before']) > self.clock()):
                return papers
        if batch and batch.get('retry_not_before') and stamp(batch['retry_not_before']) > self.clock():
            raise Deferred('Batch retry is deferred until ' + batch['retry_not_before'])
        self.guard()
        spacing = self.data / '.arxiv-request-state.json'
        if self.requests_used == 0:
            remaining = MIN_INTERVAL
        elif spacing.exists():
            last = stamp(read_json(spacing)['last_request_started_at'])
            remaining = MIN_INTERVAL - (self.clock() - last).total_seconds()
        else:
            # Existing collector tracks spacing in-process, so guard the first
            # request after acquiring its shared project lock as well.
            remaining = MIN_INTERVAL
        if remaining > 0:
            self.sleep(remaining)
        self.guard(); started = self.clock()
        atomic_json(spacing, {'last_request_started_at': iso(started), 'source': SOURCE})
        self.requests_used += 1
        try:
            response = self.fetch(ids)
        except (OSError, urllib.error.URLError) as error:
            response = {'status': 0, 'headers': {}, 'request_url': API,
                        'requested_at': iso(started), 'observed_at': iso(self.clock()),
                        'body': b'', 'transport_error': type(error).__name__}
        raw = response.pop('body'); digest = sha(raw)
        self.last_response_at = stamp(response['observed_at'])
        attempt = 1 + sum(row['batch_number'] == number for row in self.manifest['requests'])
        relative = Path('raw') / f'batch-{number:04d}-attempt-{attempt:03d}-{digest[:16]}.xml.gz'
        path = self.root / relative; path.parent.mkdir(exist_ok=True)
        compressed = gzip.compress(raw, mtime=0)
        with path.open('xb') as stream:
            stream.write(compressed)
        record = {**response, 'batch_number': number, 'requested_ids': ids,
                  'collector_sha256': self.code_sha256,
                  'raw_path': str(relative), 'raw_sha256': digest,
                  'compressed_sha256': sha(compressed), 'raw_bytes': len(raw),
                  'compressed_bytes': len(compressed), 'requested_version': 1}
        self.manifest['requests'].append(record)
        state = {'status': 'pending', **record}
        self.manifest['batches'][key] = state
        self.save()
        observed = stamp(response['observed_at'])
        if response['status'] == 429:
            until = observed + timedelta(seconds=retry_seconds(response['headers'], observed))
            preserve_cooldown(self.data, until, 'arXiv v1 HTTP429; shared project cooldown')
            state.update(status='deferred', retry_not_before=iso(until)); self.save()
            raise Deferred('HTTP429; global cooldown persisted, no busy retry')
        if response['status'] != 200:
            state.update(status='deferred', retry_not_before=iso(observed + timedelta(seconds=1800)))
            self.save(); raise Deferred('HTTP' + str(response['status']) + '; raw response retained')
        try:
            if response.get('body_truncated'):
                raise ValueError('Truncated first-version response retained; not a complete body')
            papers, missing = parse_v1_feed(raw, ids, response['observed_at'])
        except (ValueError, ET.ParseError) as error:
            state.update(status='invalid_response', error=str(error), retry_not_before=iso(observed + timedelta(seconds=1800)))
            self.save(); raise Deferred('Invalid v1 response retained; no cursor advancement')
        if missing:
            state.update(status='incomplete_response', missing_ids=missing,
                         retry_not_before=iso(observed + timedelta(seconds=1800)))
            # Keep verified returned papers and continue unrelated batches.
            # Missing IDs remain explicit and this batch never becomes complete.
            self.save(); return papers
        state['status'] = 'complete'; state['paper_count'] = len(papers); self.save()
        return papers

    def finish_spacing(self):
        # Keep the main collector's project lock until ten seconds after the
        # final HTTP response. The main collector tracks request timing only
        # in-process and waits three seconds when it acquires this same lock.
        if self.last_response_at is not None:
            remaining = MIN_INTERVAL - (self.clock() - self.last_response_at).total_seconds()
            if remaining > 0:
                self.sleep(remaining)

    def run(self):
        papers = {}; deferred = None
        for number, ids in enumerate(self.batches):
            try:
                papers.update(self.get_batch(number, ids))
            except Deferred as error:
                deferred = str(error); break
        self.export(papers)
        complete = len(papers) == len(self.ids)
        self.manifest.update({'status': 'complete' if complete else 'partial', 'retrieved_v1_papers': len(papers),
                              'complete_batches': sum(b['status'] == 'complete' for b in self.manifest['batches'].values()),
                              'deferred_reason': deferred, 'requests_this_invocation': self.requests_used})
        self.save()
        return self.manifest

    def export(self, papers):
        with (self.root / 'papers_v1.jsonl').open('w') as stream:
            for pid in sorted(papers):
                stream.write(json.dumps(papers[pid], sort_keys=True) + '\n')
        matched = []
        for pair in self.pairs:
            item = {**pair, 'source': SOURCE, 'historical_feature_eligible': False,
                    'manual_review_completed': False, 'review_required': True}
            if pair['paper_id'] in papers:
                paper = papers[pair['paper_id']]
                item.update({'v1_retrieval_status': 'retrieved', 'title_v1': paper['title_v1'],
                             'abstract_v1': paper['abstract_v1'], 'published': paper['published'],
                             'v1_updated': paper['v1_updated'], 'metadata_observed_at': paper['metadata_observed_at'],
                             **classify(self.tools[pair['tool_id']], paper, self.manifest['availability_lag_days'])})
                if pair['candidate_published']:
                    item['discovery_published_agrees'] = stamp(pair['candidate_published']) == stamp(paper['published'])
                    if not item['discovery_published_agrees']:
                        item['match_status'] = 'date_disagreement_review_required'; item['strict_lexical_candidate'] = False
            else:
                item.update(v1_retrieval_status='unresolved', match_status='unresolved_v1', strict_lexical_candidate=False)
            matched.append(item)
        with (self.root / 'tool_paper_v1.jsonl').open('w') as stream:
            for row in matched:
                stream.write(json.dumps(row, sort_keys=True) + '\n')
        fields = ['tool_id', 'paper_id', 'match_status', 'v1_retrieval_status', 'published', 'v1_updated',
                  'metadata_observed_at', 'matched_aliases', 'title_v1', 'abstract_v1',
                  'assumed_v1_available_at', 'review_required', 'manual_review_completed', 'historical_feature_eligible']
        with (self.root / 'research_review_queue.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore'); writer.writeheader()
            for row in matched:
                writer.writerow({**row, 'matched_aliases': '; '.join(row.get('matched_aliases', []))})
        sample = review_sample(matched)
        with (self.root / 'research_review_sample.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields + ['sample_stratum','human_relevance_label','reviewer','reviewed_at','review_notes'], extrasaction='ignore')
            writer.writeheader()
            for row in sample:
                writer.writerow({**row, 'matched_aliases': '; '.join(row.get('matched_aliases', [])),
                                 'human_relevance_label':'','reviewer':'','reviewed_at':'','review_notes':''})
        weekly = weekly_rows(matched, sorted(self.tools), self.manifest['history_start'], self.manifest['history_end_exclusive'])
        fields = ['tool_id','week_start','strict_v1_candidate_count','ambiguous_v1_candidate_count',
                  'no_strict_v1_match_count','unresolved_v1_seed_papers','seed_v1_retrieval_complete',
                  'strict_candidates_available_by_assumed_cutoff','feature_cutoff_utc',
                  'historical_feature_eligible','relevance_precision_validated','discovery_coverage_certified']
        with (self.root / 'research_v1_weekly.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerows(weekly)
        counts = Counter(row['match_status'] for row in matched)
        quality = {'source': SOURCE, 'match_policy': POLICY, 'status': 'complete' if len(papers)==len(self.ids) else 'partial',
                   'scope': 'First-version identity/text/date integrity and deterministic candidate classification only.',
                   'historical_feature_eligible': False, 'manual_review_completed': False,
                   'relevance_precision_validated': False, 'historical_public_availability_certified': False,
                   'discovery_coverage_certified': False, 'note': NOTE,
                   'as_of': self.manifest['as_of'], 'registry_sha256': self.manifest['registry_sha256'],
                   'exporter_sha256': self.code_sha256,
                   'candidate_pairs': len(matched), 'unique_candidate_papers': len(self.ids),
                   'retrieved_v1_papers': len(papers), 'match_status_counts': dict(counts),
                   'review_required_pairs': len(matched), 'weekly_rows': len(weekly),
                   'review_sample_rows': len(sample),
                   'review_sample_design': 'Deterministic SHA-sorted maximum5 strict and5 other retrieved pairs per tool. Human labels are blank; no precision estimate is claimed.',
                   'limits': ['Only retained discovered candidates are enriched; this is not all arXiv papers or certified historical search recall.',
                              'Strict lexical mention is not manual relevance review or proof of actual library usage.',
                              'Two-day availability lag is an explicit modeling assumption, not an observed first-public timestamp.',
                              'A complete seed response set does not certify zero research attention in unobserved papers.']}
        names = ['candidates_input.csv','tools.study.json','papers_v1.jsonl','tool_paper_v1.jsonl','research_review_queue.csv','research_review_sample.csv','research_v1_weekly.csv']
        names.extend(row['raw_path'] for row in self.manifest['requests'])
        self.manifest['files'] = {name: sha((self.root / name).read_bytes()) for name in names}
        quality['files'] = self.manifest['files']
        atomic_json(self.root / 'research_v1_quality.json', quality)
        self.manifest['files']['research_v1_quality.json'] = sha((self.root / 'research_v1_quality.json').read_bytes())


def review_sample(matched):
    groups = {}
    for row in matched:
        if row['v1_retrieval_status'] != 'retrieved':
            continue
        stratum = 'strict_v1_candidate' if row.get('strict_lexical_candidate') else 'other_retrieved_v1_candidate'
        groups.setdefault((row['tool_id'],stratum),[]).append(row)
    return [{**row,'sample_stratum':stratum}
            for (tid,stratum),rows in sorted(groups.items())
            for row in sorted(rows,key=lambda item:sha((tid+'|'+item['paper_id']+'|6893').encode()))[:5]]


def weekly_rows(matched, tools, start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    first = start + timedelta(days=(-start.weekday()) % 7)
    weeks = []
    cursor = first
    while cursor + timedelta(days=7) <= end:
        weeks.append(cursor); cursor += timedelta(days=7)
    counts = Counter(); unresolved = Counter()
    for row in matched:
        tid = row['tool_id']
        if row['v1_retrieval_status'] != 'retrieved':
            unresolved[tid] += 1; continue
        day = stamp(row['published']).date(); week = day - timedelta(days=day.weekday())
        if not start <= day < end:
            continue
        status = row['match_status']
        label = 'strict' if row.get('strict_lexical_candidate') else 'ambiguous' if status == 'ambiguous_v1_review_required' else 'no_match'
        counts[(tid, week, label)] += 1
        cutoff = datetime.combine(week, datetime.min.time(), UTC) + timedelta(days=9)
        if label == 'strict' and stamp(row['assumed_v1_available_at']) <= cutoff:
            counts[(tid, week, 'available')] += 1
    return [{'tool_id': tid, 'week_start': week.isoformat(),
             'strict_v1_candidate_count': counts[(tid, week, 'strict')],
             'ambiguous_v1_candidate_count': counts[(tid, week, 'ambiguous')],
             'no_strict_v1_match_count': counts[(tid, week, 'no_match')],
             'unresolved_v1_seed_papers': unresolved[tid], 'seed_v1_retrieval_complete': unresolved[tid] == 0,
             'strict_candidates_available_by_assumed_cutoff': counts[(tid, week, 'available')],
             'feature_cutoff_utc': iso(datetime.combine(week, datetime.min.time(), UTC) + timedelta(days=9)),
             'historical_feature_eligible': False, 'relevance_precision_validated': False,
             'discovery_coverage_certified': False}
            for tid in tools for week in weeks]


def main():
    os.umask(0o077)
    root = Path.home() / '6893'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, default=root/'config/tools.study.json')
    parser.add_argument('--candidates', type=Path, help='Freeze this retained candidate CSV; default snapshots arXiv backfill.records')
    parser.add_argument('--data-dir', type=Path, default=root/'data')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', default='2023-01-01')
    parser.add_argument('--end-exclusive')
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--max-requests', type=int, default=50)
    parser.add_argument('--max-seconds', type=int, default=480)
    parser.add_argument('--availability-lag-days', type=int, default=2)
    args = parser.parse_args(); args.data_dir.mkdir(parents=True, exist_ok=True)
    if not 1 <= args.max_requests <= 100 or not 1 <= args.max_seconds <= 600:
        parser.error('Use1..100 requests and1..600 seconds per invocation')
    with (args.data_dir / '.pilot-collection.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'status':'skipped_project_busy','source':SOURCE})); return 0
        collector = Collector(args.output,args.registry,args.candidates,args.data_dir,args.start,args.end_exclusive,
                              args.batch_size,args.max_requests,args.max_seconds,args.availability_lag_days)
        try:
            result = collector.run()
        finally:
            collector.finish_spacing()
    print(json.dumps({key: result.get(key) for key in ['source','status','candidate_pairs','unique_candidate_papers','retrieved_v1_papers','complete_batches','deferred_reason']},indent=2))
    return 0 if result['status']=='complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
