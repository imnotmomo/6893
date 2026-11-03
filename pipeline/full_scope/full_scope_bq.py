"""Stream richer public BigQuery results through the existing global budget.

Raw response pages, decoded records and a completion receipt are retained.
Only the existing backfill.budget ledger authorizes billed execution. A resume
uses the same deterministic cloud job; unknown charges remain reserved.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sys
import time
import re
import urllib.request
from zoneinfo import ZoneInfo

ROOT = Path.home() / '6893'
sys.path.insert(0, str(ROOT / 'pipeline'))
import backfill
import bigquery_sources
from bigquery_sources import _Client, _canonical_parameters, SourceError
from dependency_sources import decode_rows

# This process only: jobs.getQueryResults returns up to 20 MB of internal
# data, whose JSON representation can exceed the old 8 MiB response limit.
# Retain a bounded transport envelope and clear it after every streamed page.
bigquery_sources.MAX_RESPONSE_BYTES = 64 * 1024**2
bigquery_sources.MAX_LEDGER_BYTES = 128 * 1024**2
# Only this executor's ADC request; the extra read scope is used exclusively to
# verify billing remains disabled before a new sandbox job is inserted.
bigquery_sources.SCOPES = ['https://www.googleapis.com/auth/bigquery',
                          'https://www.googleapis.com/auth/cloud-billing.readonly']


def now(): return datetime.now(timezone.utc).isoformat()
def encode(value): return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, default=str)
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def atomic(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w') as stream:
        stream.write(encode(value) + '\n'); stream.flush(); os.fsync(stream.fileno())
    os.replace(temp, path)


@contextmanager
def project_lock(data):
    with (data / '.pilot-collection.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)


def reserve(data, identifier, maximum, limit):
    with project_lock(data):
        with backfill.connect(read_only=False, data_dir=data) as con:
            old = con.execute('SELECT reserved_bytes FROM backfill.budget WHERE job_id=?', [identifier]).fetchone()
            if old:
                if int(old[0]) < maximum:
                    raise ValueError('Existing reservation cannot be enlarged or reused with another cap')
                return int(old[0])
            used = int(con.execute('SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget').fetchone()[0])
            if used + maximum > limit:
                raise ValueError('Global cumulative scan-byte budget would be exceeded')
            con.execute('INSERT INTO backfill.budget VALUES (?,?,?)', [identifier, maximum, datetime.now(timezone.utc)])
    return maximum


def reconcile(data, identifier, billed):
    with project_lock(data):
        with backfill.connect(read_only=False, data_dir=data) as con:
            old = con.execute('SELECT reserved_bytes FROM backfill.budget WHERE job_id=?', [identifier]).fetchone()
            if old is None or billed < 0 or billed > int(old[0]):
                raise ValueError('Actual charge is missing, negative, or exceeds its reservation')
            con.execute('UPDATE backfill.budget SET reserved_bytes=? WHERE job_id=?', [billed, identifier])


def identity(plan, project):
    definition = {'query': plan['query'], 'parameters': plan['query_parameters'], 'project': project}
    return definition, 'fullscope_' + hashlib.sha256(encode(definition).encode()).hexdigest()[:40]


def logical_query_id(plan):
    return hashlib.sha256(encode({'query': plan['query'], 'location': 'US', 'useLegacySql': False,
        'parameters': _canonical_parameters(plan['query_parameters'])}).encode()).hexdigest()


def execution_policy(plan):
    """Separate, opt-in routing; ordinary metadata/core queries keep their default."""
    path = ROOT / 'config/full_scope_execution.json'
    if not path.exists(): return None
    policy = json.loads(path.read_text())
    if not policy.get('enabled') or plan['source'] not in policy.get('sources', []): return None
    if policy.get('format_version') != 1 or not isinstance(policy.get('allow_paid_fallback'), bool):
        raise ValueError('Invalid free-first execution policy')
    return policy


def validate_free_projects(policy):
    if policy is None: return
    month = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m')
    seen = set()
    for item in policy.get('free_projects', []):
        project = item.get('project_id', '')
        if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', project) or project in seen:
            raise ValueError('Free execution project must be valid and unique')
        seen.add(project)
        cap, baseline = item.get('allocation_limit_bytes'), item.get('baseline_usage_bytes')
        if (item.get('billing_month') != month or type(cap) is not int or type(baseline) is not int
                or not 0 <= baseline <= cap <= 1024**4):
            raise ValueError('Free allowance needs a current billing month and bounded verified baseline')
        evidence = item.get('baseline_evidence', {})
        evidence_path = Path(evidence.get('path', ''))
        if not evidence_path.is_file() or sha(evidence_path) != evidence.get('sha256'):
            raise ValueError('Free allowance baseline evidence is missing or changed')
        value = json.loads(evidence_path.read_text())
        if (value.get('project_id') != project or value.get('billingEnabled') is not False
                or value.get('billing_month') != month or value.get('baseline_usage_bytes') != baseline
                or not value.get('observed_at')):
            raise ValueError('Free allowance baseline evidence does not bind project, billing and usage')


def route_book(data):
    path = data / 'full_scope_query_routes.json'
    value = json.loads(path.read_text()) if path.exists() else {'format_version': 1, 'routes': {}}
    if value.get('format_version') != 1 or not isinstance(value.get('routes'), dict):
        raise ValueError('Invalid global query routing registry')
    for logical, route in value['routes'].items():
        if not re.fullmatch(r'[0-9a-f]{64}', logical) or not isinstance(route, dict):
            raise ValueError('Invalid logical query routing entry')
        if route.get('route_kind') == 'unbilled_sandbox':
            allocation = route.get('free_reserved_bytes'); cap = route.get('maximum_bytes_billed')
            if type(allocation) is not int or type(cap) is not int or not 0 <= cap <= allocation <= 1024**4:
                raise ValueError('Invalid retained free-query allowance reservation')
    return path, value


def legacy_route(data, plan, manifest, out, config, policy, book, con):
    """Import retained reservations before any account switch, never reassign them."""
    logical = logical_query_id(plan)
    if logical in book['routes']:
        item = book['routes'][logical]
        definition = item.get('definition', {})
        frozen = {'query': definition.get('query'), 'query_parameters': definition.get('parameters', [])}
        expected, identifier = identity(frozen, item['execution_project'])
        if (logical_query_id(frozen) != logical or definition != expected or item.get('job_id') != identifier
                or item.get('ledger_job_id') != identifier):
            raise ValueError('Global logical query route changed')
        return item
    candidates = [manifest] if manifest else []
    for path in sorted((data / 'raw/full_scope/bigquery').glob('*/manifest.json')):
        if path.resolve() == (out / 'manifest.json').resolve(): continue
        value = json.loads(path.read_text())
        if value.get('definition', {}).get('query') == plan['query']: candidates.append(value)
    pinned = {}
    for value in candidates:
        definition = value.get('definition', {})
        if (definition.get('query') != plan['query']
                or _canonical_parameters(definition.get('parameters', [])) != _canonical_parameters(plan['query_parameters'])):
            continue
        project = definition.get('project')
        expected, identifier = identity({'query': definition['query'], 'query_parameters': definition['parameters']}, project)
        if value.get('job_id') != identifier or value.get('ledger_job_id') != identifier:
            raise ValueError('Retained execution binding does not match deterministic job identity')
        old = con.execute('SELECT reserved_bytes FROM backfill.budget WHERE job_id=?', [identifier]).fetchone()
        started = value.get('status') not in {'planned', 'dry_run_ok'} or value.get('pages') or value.get('job_complete')
        if not old and not started: continue
        cap = value.get('maximum_bytes_billed')
        if type(cap) is not int or cap < 0:
            raise ValueError('Retained execution binding lacks its original query cap')
        binding = {'definition': expected, 'execution_project': project, 'job_id': identifier,
            'ledger_job_id': identifier, 'route_kind': 'paid_legacy', 'maximum_bytes_billed': cap,
            'state': value['status'], 'created_at': value.get('created_at'), 'adopted_at': now()}
        if project in pinned and pinned[project]['job_id'] != identifier:
            raise ValueError('Logical query already has multiple cloud job IDs; manual review required')
        pinned[project] = binding
    # An orphan reservation must block a new route even if its output was moved.
    projects = {config['bigquery_project']} | set((policy or {}).get('legacy_projects', []))
    projects |= {p['project_id'] for p in (policy or {}).get('free_projects', [])}
    for project in projects:
        _, identifier = identity(plan, project)
        old = con.execute('SELECT reserved_bytes FROM backfill.budget WHERE job_id=?', [identifier]).fetchone()
        if old and project not in pinned:
            raise ValueError('Existing global reservation lacks a retained execution binding; refuse rerouting')
    if len(pinned) > 1: raise ValueError('Logical query already has multiple execution projects; manual review required')
    if pinned:
        item = next(iter(pinned.values())); book['routes'][logical] = item
        return item
    return None


def select_route(data, plan, manifest, out, config, policy, cap=None, claim=False):
    """One lock binds allowance, logical identity and the existing global ledger.

    Registry-first persistence is conservative across a crash: an allowance can
    stay held without a ledger insert, but a paid insert is never authorized
    without both durable records. No original ledger row is deleted/reset.
    """
    if claim and (type(cap) is not int or not 10*1024**2 <= cap <= min(50*1024**3, int(config['max_bytes_per_query']))):
        raise ValueError('Invalid pinned per-query cap')
    with project_lock(data):
        path, book = route_book(data)
        with backfill.connect(read_only=False, data_dir=data) as con:
            existing = legacy_route(data, plan, manifest, out, config, policy, book, con)
            if existing:
                atomic(path, book)
                return existing
            validate_free_projects(policy)
            selected = None
            for project in (policy or {}).get('free_projects', []):
                used = sum(int(r.get('free_reserved_bytes', 0)) for r in book['routes'].values()
                    if r.get('route_kind') == 'unbilled_sandbox' and r.get('execution_project') == project['project_id']
                    and r.get('billing_month') == project['billing_month'])
                if project['baseline_usage_bytes'] + used + (cap or 10*1024**2) <= project['allocation_limit_bytes']:
                    selected = project; break
            if selected:
                project = selected['project_id']; kind = 'unbilled_sandbox'
            elif policy and not policy['allow_paid_fallback']:
                raise ValueError('Verified free query allocation exhausted; paid fallback disabled')
            else:
                project = config['bigquery_project']; kind = 'paid'
            definition, identifier = identity(plan, project)
            item = {'definition': definition, 'execution_project': project, 'job_id': identifier,
                'ledger_job_id': identifier, 'route_kind': kind, 'state': 'unsubmitted'}
            if not claim: return item
            if kind != 'unbilled_sandbox' and not config.get('allow_billed_queries'):
                raise ValueError('Billed queries disabled')
            used = int(con.execute('SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget').fetchone()[0])
            if used + cap > int(config['max_total_query_bytes']):
                raise ValueError('Global cumulative scan-byte budget would be exceeded')
            item.update(maximum_bytes_billed=cap, state='reserved', created_at=now(),
                origin_output_path=str(out.resolve()), original_query_sha256=plan['query_sha256'], source=plan['source'])
            if selected:
                item.update(billing_month=selected['billing_month'], free_reserved_bytes=cap,
                    baseline_evidence=selected['baseline_evidence'], baseline_usage_bytes=selected['baseline_usage_bytes'])
            book['routes'][logical_query_id(plan)] = item
            atomic(path, book)
            con.execute('INSERT INTO backfill.budget VALUES (?,?,?)', [identifier, cap, datetime.now(timezone.utc)])
            return item


def update_route(data, plan, route, state):
    with project_lock(data):
        path, book = route_book(data); stored = book['routes'].get(logical_query_id(plan))
        if not stored or stored.get('job_id') != route['job_id']:
            raise ValueError('Execution route disappeared or changed')
        if stored.get('state') not in {'complete', 'cloud_job_failed'}:
            stored.update(state=state, updated_at=now())
        atomic(path, book)


def verify_unbilled(client, project):
    """Fresh provider evidence before any new sandbox insert; never enable billing."""
    url = 'https://cloudbilling.googleapis.com/v1/projects/' + project + '/billingInfo'
    headers = {'Accept': 'application/json'}
    if client.token:
        headers['Authorization'] = 'Bearer ' + client.token
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=20) as response:
            body = response.read(1024**2 + 1); status = response.status
    else:
        response = client.session.request('GET', url, headers=headers, timeout=20)
        body, status = response.content, response.status_code
    if status != 200 or len(body) > 1024**2:
        raise ValueError('Cannot verify sandbox billing is disabled')
    value = json.loads(body)
    if (value.get('projectId') != project or value.get('billingEnabled') is not False
            or value.get('billingAccountName')):
        raise ValueError('Free execution project has billing enabled or ambiguous billing association')
    return {'project_id': project, 'observed_at': now(), 'response': value,
            'response_sha256': hashlib.sha256(body).hexdigest()}


def match_job(job, plan, cap, cloud_id, project):
    cfg = job.get('configuration', {}).get('query', {})
    ref = job.get('jobReference', {})
    if (cfg.get('query') != plan['query'] or int(cfg.get('maximumBytesBilled', -1)) != cap
            or cfg.get('useLegacySql', False) is not False
            or _canonical_parameters(cfg.get('queryParameters', [])) != _canonical_parameters(plan['query_parameters'])
            or ref.get('jobId') != cloud_id or ref.get('projectId') != project
            or ref.get('location', 'US').upper() != 'US'):
        raise ValueError('Cloud job does not match the frozen query, parameters, cap or location')
    statement = job.get('statistics', {}).get('query', {}).get('statementType')
    if statement is not None and statement != 'SELECT':
        raise ValueError('Only SELECT query jobs are permitted')


def checked_parts(manifest, out):
    total = 0
    for part in manifest.get('pages', []):
        for key in ['raw', 'decoded']:
            path = (out / part[key]).resolve()
            if not path.is_relative_to(out.resolve()) or not path.is_file() or sha(path) != part[key + '_sha256']:
                raise ValueError('Retained page changed: ' + part[key])
        total += int(part['rows'])
    if total != manifest.get('rows_fetched', 0):
        raise ValueError('Manifest page counts disagree')


def run(plan_path, out, execute=False, max_seconds=600, page_size=1000):
    out.mkdir(parents=True,exist_ok=True)
    # Claim the output before reading or initializing its manifest. A losing
    # dispatcher must never overwrite a concurrent export's completed state.
    with (out/'.collection.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return _run_locked(plan_path,out,execute,max_seconds,page_size)


def _run_locked(plan_path, out, execute=False, max_seconds=600, page_size=1000):
    plan = json.loads(plan_path.read_text())
    fingerprint = hashlib.sha256(json.dumps({'query':plan['query'],'parameters':plan['query_parameters']},sort_keys=True).encode()).hexdigest()
    if plan.get('query_sha256') != fingerprint:
        raise ValueError('Query plan fingerprint differs from SQL and parameters')
    config = json.loads((ROOT / 'config/backfill.json').read_text())
    data = Path(config['data_dir']); out.mkdir(parents=True, exist_ok=True)
    if not (plan['query'].lstrip().upper().startswith(('SELECT ', 'WITH '))) or ';' in plan['query']:
        raise ValueError('Only one SELECT query is permitted')
    policy = execution_policy(plan)
    manifest_path = out / 'manifest.json'
    manifest = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        definition = manifest.get('definition', {})
        if (definition.get('query') != plan['query'] or definition.get('parameters') != plan['query_parameters']
                or manifest.get('plan_file_sha256') != sha(plan_path)):
            raise ValueError('Resume plan changed')
        checked_parts(manifest, out)
    route = select_route(data, plan, manifest, out, config, policy)

    def adopt(selected):
        nonlocal manifest, route
        route = selected; definition = route['definition']
        changed = manifest and (manifest.get('definition') != definition or manifest.get('job_id') != route['job_id'])
        history = list((manifest or {}).get('execution_route_history', []))
        if changed:
            if manifest.get('pages') or manifest.get('job_complete') or manifest.get('status') not in {'planned', 'dry_run_ok'}:
                raise ValueError('A started output cannot change execution project')
            retained = out / 'lineage'; retained.mkdir(exist_ok=True)
            old_sha = sha(manifest_path)
            old_path = retained / ('manifest-' + old_sha + '.json')
            if not old_path.exists(): old_path.write_bytes(manifest_path.read_bytes())
            record = {'definition': manifest['definition'], 'job_id': manifest['job_id'],
                'manifest_path': str(old_path.relative_to(out)), 'manifest_sha256': old_sha,
                'rerouted_at': now(), 'reason': 'unsubmitted_query_selected_execution_project'}
            if (out / 'dry_run.json').exists():
                dry_sha = sha(out / 'dry_run.json'); dry_path = retained / ('dry-run-' + dry_sha + '.json')
                if not dry_path.exists(): dry_path.write_bytes((out / 'dry_run.json').read_bytes())
                record.update(dry_run_path=str(dry_path.relative_to(out)), dry_run_sha256=dry_sha)
            history.append(record)
        if manifest is None or changed:
            manifest = {'source': plan['source'], 'definition': definition, 'plan_file_sha256': sha(plan_path),
                'query_sha256': plan['query_sha256'], 'job_id': route['job_id'], 'ledger_job_id': route['ledger_job_id'],
                'status': 'planned', 'created_at': now(), 'pages': [], 'rows_fetched': 0,
                'next_page_token': None, 'last_page_fetched': False, 'job_complete': False,
                'all_pages_fetched': False, 'source_plan': plan}
            if history: manifest['execution_route_history'] = history
            (out / 'query_plan.json').write_bytes(plan_path.read_bytes())
        if route['state'] != 'unsubmitted' and manifest['status'] in {'planned', 'dry_run_ok'}:
            # Another output may already have fetched this same job. Its cloud
            # result can be downloaded again, but a missing result is never rerun.
            manifest.update(status=route['state'] if route['state'] != 'complete' else 'fetching',
                maximum_bytes_billed=route['maximum_bytes_billed'])
        if manifest.get('status') != 'complete':
            manifest['execution_route'] = {k: route[k] for k in ['execution_project', 'job_id', 'route_kind']}
            atomic(manifest_path, manifest)

    adopt(route)
    if manifest.get('status') == 'complete':
        receipt = json.loads((out/'completion_receipt.json').read_text())
        if (receipt.get('query_sha256') != plan['query_sha256']
                or receipt.get('job_complete') is not True or receipt.get('all_pages_fetched') is not True
                or manifest.get('job_complete') is not True or manifest.get('all_pages_fetched') is not True
                or manifest.get('last_page_fetched') is not True
                or receipt.get('total_rows') != manifest.get('rows_fetched')
                or receipt.get('total_rows') != manifest.get('total_rows')
                or receipt.get('cloud_job_id') != manifest.get('job_id')
                or receipt.get('execution_project', route['execution_project']) != route['execution_project']
                or receipt.get('logical_query_sha256', logical_query_id(plan)) != logical_query_id(plan)
                or receipt.get('actual_billed_bytes') != manifest.get('actual_billed_bytes')
                or sha(out/'decoded_records.jsonl.gz') != receipt['output_sha256']
                or receipt['output_sha256'] != manifest.get('decoded_records_sha256')):
            raise ValueError('Combined decoded result changed')
        return manifest
    client = _Client(None)
    def request(method, path, **kwargs):
        result = client.request(method, path, **kwargs)
        client.ledger=[]; client.ledger_size=0
        return result
    def dry_run(project, fixed_cap=None):
        dry = request('POST', '/projects/'+project+'/jobs', payload={'configuration':{'dryRun':True,
            'query':{'query':plan['query'], 'useLegacySql':False, 'parameterMode':'NAMED', 'queryParameters':plan['query_parameters']}}})
        atomic(out/'dry_run.json', dry)
        if dry.get('statistics',{}).get('query',{}).get('statementType') != 'SELECT':
            raise ValueError('Dry run did not establish a SELECT statement')
        estimate = int(dry['statistics']['query']['totalBytesProcessed'])
        cap = max(10*1024**2, math.ceil(estimate/(10*1024**2))*(10*1024**2))
        if cap > min(50*1024**3, int(config['max_bytes_per_query'])):
            raise ValueError('Dry-run estimate exceeds per-query cap; split the date interval first')
        if fixed_cap is not None and cap > fixed_cap:
            raise ValueError('Selected-project dry run exceeds the already pinned query cap')
        manifest.update(estimated_scan_bytes=estimate, maximum_bytes_billed=fixed_cap or cap,
            dry_run_project=project, status='dry_run_ok' if fixed_cap is None else 'reserved', updated_at=now())
        atomic(manifest_path, manifest)
    if manifest['status'] == 'planned': dry_run(route['execution_project'])
    if not execute:
        return manifest
    if route['state'] == 'unsubmitted':
        selected = select_route(data, plan, manifest, out, config, policy,
            cap=int(manifest['maximum_bytes_billed']), claim=True)
        dry_project = manifest.get('dry_run_project', manifest['definition']['project'])
        adopt(selected)
        if dry_project != route['execution_project']:
            dry_run(route['execution_project'], route['maximum_bytes_billed'])
        manifest.update(status='reserved', maximum_bytes_billed=route['maximum_bytes_billed'], reserved_at=now())
        atomic(manifest_path, manifest)
    project = route['execution_project']; identifier = route['ledger_job_id']; cloud_id = route['job_id']
    job_path = '/projects/' + project + '/jobs/' + cloud_id
    cap = int(manifest['maximum_bytes_billed'])
    if cap != route['maximum_bytes_billed']: raise ValueError('Output cap differs from pinned execution route')
    deadline = time.monotonic() + max_seconds
    guard = lambda: shutil.disk_usage(data).free >= max(15, config.get('minimum_free_GiB',15))*1024**3
    if not guard(): raise ValueError('Insufficient free space')
    job = request('GET', job_path, params={'location':'US'}, allow_404=True)
    if job is None:
        if (manifest['status'] not in {'reserved', 'submission_started'}
                or route['state'] not in {'reserved', 'submission_started'}):
            raise ValueError('Previously submitted job is missing; refuse a potentially duplicate paid query')
        if route['route_kind'] == 'unbilled_sandbox':
            validate_free_projects(policy)
            if not policy or not any(p['project_id'] == project and p['billing_month'] == route['billing_month']
                    for p in policy['free_projects']):
                raise ValueError('Pinned free route no longer has a verified current-month allocation')
            atomic(out / 'billing_disabled_verification.json', verify_unbilled(client, project))
        elif not config.get('allow_billed_queries') or (policy and not policy['allow_paid_fallback']):
            raise ValueError('New paid submission disabled; existing job remains pinned')
        # A failed/unknown insert is retried with exactly the same cloud ID.
        # Verify the full cap remains reserved before any possible insert.
        reserve(data, identifier, cap, int(config['max_total_query_bytes']))
        manifest.update(status='submission_started',updated_at=now());atomic(manifest_path,manifest)
        update_route(data, plan, route, 'submission_started')
        payload={'jobReference':{'projectId':project,'jobId':cloud_id,'location':'US'},'configuration':{
            'query':{'query':plan['query'],'useLegacySql':False,'parameterMode':'NAMED',
                     'queryParameters':plan['query_parameters'],'maximumBytesBilled':str(cap)},
            'labels':{'project':'6893','source':'full-scope'}}}
        request('POST','/projects/'+project+'/jobs',payload=payload,allow_409=True)
        job=request('GET',job_path,params={'location':'US'})
    while True:
        match_job(job,plan,cap,cloud_id,project)
        atomic(out/'cloud_job.json', job)
        if job.get('status',{}).get('state') == 'DONE':
            stats = job.get('statistics',{}).get('query',{})
            if 'totalBytesBilled' in stats:
                reconcile(data,identifier,int(stats['totalBytesBilled']))
                manifest['actual_billed_bytes']=int(stats['totalBytesBilled'])
            if job.get('status',{}).get('errorResult'):
                manifest.update(status='cloud_job_failed', error=job['status']['errorResult'],updated_at=now());atomic(manifest_path,manifest)
                update_route(data, plan, route, 'cloud_job_failed')
                raise ValueError('Cloud query failed; charge evidence retained')
            manifest.update(status='fetching',job_complete=True,job_finished_at=now());atomic(manifest_path,manifest)
            update_route(data, plan, route, 'fetching')
            break
        if time.monotonic() >= deadline:
            manifest.update(status='query_pending',updated_at=now());atomic(manifest_path,manifest)
            update_route(data, plan, route, 'query_pending');return manifest
        time.sleep(2); job=request('GET',job_path,params={'location':'US'})
    while not manifest['last_page_fetched'] and time.monotonic() < deadline:
        if not guard(): raise ValueError('Free-space guard stopped result download')
        params={'location':'US','maxResults':page_size,'timeoutMs':0}
        if manifest['next_page_token']:params['pageToken']=manifest['next_page_token']
        page=request('GET','/projects/'+project+'/queries/'+cloud_id,params=params)
        def invalid_page(reason):
            body=encode(page).encode();digest=hashlib.sha256(body).hexdigest()
            target=out/'invalid'/('response-'+digest+'.json.gz');target.parent.mkdir(exist_ok=True)
            if not target.exists():target.write_bytes(gzip.compress(body,mtime=0))
            manifest.setdefault('validation_errors',[]).append({'reason':reason,'raw':str(target.relative_to(out)),
                'raw_sha256':sha(target),'observed_at':now()});atomic(manifest_path,manifest)
            raise ValueError(reason)
        if page.get('jobComplete') is not True:raise ValueError('Completed query result is not ready')
        incoming = page.get('schema') or manifest.get('schema')
        if manifest.get('schema') and incoming != manifest['schema']:
            invalid_page('Result schema changed between pages')
        if 'total_rows' in manifest and int(page['totalRows']) != manifest['total_rows']:
            invalid_page('Result total row count changed between pages')
        manifest['schema']=incoming
        rows=decode_rows(manifest['schema'],page.get('rows',[]))
        token=page.get('pageToken')
        if token and token in [p.get('next_page_token') for p in manifest['pages']]:
            invalid_page('Repeated result page token')
        index=len(manifest['pages']); raw=out/'raw'/f'page-{index:06d}.json.gz'; decoded=out/'decoded'/f'part-{index:06d}.jsonl.gz'
        raw.parent.mkdir(exist_ok=True);decoded.parent.mkdir(exist_ok=True)
        raw.write_bytes(gzip.compress(encode(page).encode(),compresslevel=3,mtime=0))
        decoded.write_bytes(gzip.compress((''.join(encode(row)+'\n' for row in rows)).encode(),compresslevel=3,mtime=0))
        manifest['pages'].append({'raw':str(raw.relative_to(out)),'raw_sha256':sha(raw),'decoded':str(decoded.relative_to(out)),
            'decoded_sha256':sha(decoded),'rows':len(rows),'next_page_token':token,'observed_at':now()})
        manifest.update(rows_fetched=manifest['rows_fetched']+len(rows), total_rows=int(page['totalRows']),
                        next_page_token=token,last_page_fetched=not bool(token),updated_at=now())
        atomic(manifest_path,manifest)
        if index%25==0:print(encode({'source':plan['source'],'pages':index+1,'rows':manifest['rows_fetched'],'total_rows':manifest['total_rows']}),flush=True)
    if manifest['last_page_fetched']:
        checked_parts(manifest,out)
        if manifest['rows_fetched'] != manifest['total_rows']:raise ValueError('Result rows are incomplete')
        target=out/'decoded_records.jsonl.gz';temp=target.with_name(target.name+'.tmp')
        with temp.open('wb') as output:
            for page in manifest['pages']:
                with (out/page['decoded']).open('rb') as source:shutil.copyfileobj(source,output)
        os.replace(temp,target)
        receipt={'query_sha256':plan['query_sha256'],'job_complete':True,'all_pages_fetched':True,
                 'total_rows':manifest['total_rows'],'output_sha256':sha(target),'cloud_job_id':cloud_id,
                 'execution_project':project,'logical_query_sha256':logical_query_id(plan),
                 'actual_billed_bytes':manifest.get('actual_billed_bytes'),'observed_at':now()}
        atomic(out/'completion_receipt.json',receipt)
        manifest.update(status='complete',all_pages_fetched=True,decoded_records_sha256=receipt['output_sha256'],finished_at=now())
        update_route(data, plan, route, 'complete')
    atomic(manifest_path,manifest)
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--execute',action='store_true');parser.add_argument('--max-seconds',type=int,default=600)
    parser.add_argument('--page-size',type=int,default=1000)
    args=parser.parse_args();os.umask(0o077)
    if not 1<=args.max_seconds<=3600 or not 1<=args.page_size<=100000:parser.error('Use bounded duration and page size')
    try:
        result=run(args.plan,args.output,args.execute,args.max_seconds,args.page_size)
        print(encode({k:result.get(k) for k in ['source','status','job_id','rows_fetched','total_rows','actual_billed_bytes','maximum_bytes_billed']}))
    except BlockingIOError:
        print(encode({'status':'skipped_collection_busy'}))
    except Exception as error:
        print(encode({'status':'failed','error':type(error).__name__+': '+str(error)}));raise SystemExit(1)
