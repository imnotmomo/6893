"""Offline classification of retained query reservations with bound free proof.

All ledger rows remain intact. A bare free-route flag never removes bytes from
the conservative paid/unknown cap. Local derived records are not cloud jobs.
This module reads retained metadata only and performs no provider requests.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from zoneinfo import ZoneInfo

ALLOCATION = 1024**4
PROFILE = 'provider_free_sandbox_v1'
EXEMPTION = 'verified_billing_disabled_sandbox_monthly_allocation'
MAX_JSON_BYTES = 16 * 1024**2
SHA = re.compile(r'[0-9a-f]{64}')


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, default=str)


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def timestamp(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Free evidence requires timezone-aware timestamps')
    return result.astimezone(timezone.utc)


def integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError('Invalid nonnegative integer in ' + name)
    return value


def read_json(path):
    path = Path(path)
    before = path.stat()
    if not path.is_file() or not 0 < before.st_size <= MAX_JSON_BYTES:
        raise ValueError('Missing or oversized free-budget evidence')
    raw = path.read_bytes()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError('Free-budget evidence changed while reading')
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate JSON key in budget evidence')
            out[key] = value
        return out
    value = json.loads(raw, object_pairs_hook=pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite budget evidence')))
    if not isinstance(value, dict):
        raise ValueError('Free-budget evidence must be an object')
    return value, hashlib.sha256(raw).hexdigest()


def contained_evidence(data, receipt):
    if not isinstance(receipt, dict) or not SHA.fullmatch(str(receipt.get('sha256', ''))):
        raise ValueError('Free quota evidence lacks its file checksum')
    path = Path(receipt.get('path', ''))
    path = (path if path.is_absolute() else data / path).resolve(strict=True)
    if not path.is_relative_to(data):
        raise ValueError('Free quota evidence leaves project data')
    value, checksum = read_json(path)
    if checksum != receipt['sha256']:
        raise ValueError('Retained free quota evidence checksum differs')
    return value, checksum


def verify_observation(observation, project, month):
    if (observation.get('profile') != PROFILE or observation.get('project_id') != project
            or observation.get('billing_month') != month or observation.get('pagination_complete') is not True):
        raise ValueError('Free quota observation is incomplete or has another project/month')
    observed = timestamp(observation.get('observed_at'))
    if observed.astimezone(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m') != month:
        raise ValueError('Free quota observation month differs from its timestamp')
    billing = observation.get('billing_disabled_verification', {})
    response = billing.get('response', {})
    if (billing.get('project_id') != project or response.get('projectId') != project
            or response.get('name') != 'projects/' + project + '/billingInfo'
            or response.get('billingEnabled') is not False or response.get('billingAccountName')):
        raise ValueError('Retained provider billing-disabled proof is missing or changed')
    # New captures retain a canonical checksum as well as their original raw
    # response digest. The canonical legacy shape is accepted only on a match.
    canonical = billing.get('response_canonical_sha256')
    if canonical is None:
        canonical = billing.get('response_sha256')
    if canonical != digest(response):
        raise ValueError('Billing response does not match its canonical checksum')
    checked = timestamp(billing.get('observed_at'))
    if abs((checked - observed).total_seconds()) > 300:
        raise ValueError('Billing and monthly-usage observations are not one bounded check')
    visibility = billing.get('complete_job_visibility', {})
    permissions = visibility.get('response', {})
    if (visibility.get('project_id') != project
            or not {'bigquery.jobs.listAll', 'bigquery.jobs.list'}.issubset(permissions.get('permissions', []))
            or visibility.get('response_canonical_sha256') != digest(permissions)):
        raise ValueError('Monthly provider inventory lacks bound cross-user query visibility')
    if abs((timestamp(visibility.get('observed_at')) - observed).total_seconds()) > 300:
        raise ValueError('Cross-user query visibility is not part of the bounded provider check')
    jobs = observation.get('jobs')
    if not isinstance(jobs, dict) or len(jobs) > 32000:
        raise ValueError('Free quota job inventory is missing or unbounded')
    total = 0
    for identifier, job in jobs.items():
        if not isinstance(identifier, str) or not identifier or not isinstance(job, dict):
            raise ValueError('Invalid provider query identity')
        amount = integer(job.get('accounted_bytes'), 'provider job bytes')
        created = integer(job.get('creation_time_ms'), 'provider creation time')
        instant = datetime.fromtimestamp(created / 1000, timezone.utc)
        if instant.astimezone(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m') != month or instant > observed + timedelta(minutes=5):
            raise ValueError('Provider job is outside its billing month/observation')
        kind = job.get('accounting')
        if kind in {'actual_billed', 'confirmed_cached'}:
            if job.get('state') != 'DONE' or (kind == 'confirmed_cached' and amount != 0):
                raise ValueError('Completed/cached provider charge evidence is inconsistent')
        elif kind != 'unknown_or_inflight_cap':
            raise ValueError('Unknown provider quota accounting method')
        total += amount
    if integer(observation.get('provider_accounted_bytes'), 'provider total') != total:
        raise ValueError('Monthly provider quota arithmetic differs')
    return jobs, total


def verify_route(logical, route, reservation, observation):
    from bigquery_sources import _canonical_parameters
    definition = route.get('definition', {})
    project = route.get('execution_project')
    query, parameters = definition.get('query'), definition.get('parameters')
    if (not isinstance(project, str) or not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', project)
            or definition.get('project') != project or not isinstance(query, str)
            or not query.lstrip().upper().startswith(('SELECT ', 'WITH '))
            or ';' in query or not isinstance(parameters, list)):
        raise ValueError('Invalid free-query definition')
    identifier = 'fullscope_' + digest(definition)[:40]
    expected_logical = digest({'query': query, 'location': 'US', 'useLegacySql': False,
                              'parameters': _canonical_parameters(parameters)})
    if (logical != expected_logical or route.get('job_id') != identifier
            or route.get('ledger_job_id') != identifier):
        raise ValueError('Free route does not bind its deterministic query/job identity')
    original_sha = hashlib.sha256(json.dumps({'query': query, 'parameters': parameters}, sort_keys=True).encode()).hexdigest()
    if route.get('original_query_sha256') != original_sha:
        raise ValueError('Free route query fingerprint differs')
    maximum = integer(route.get('maximum_bytes_billed'), 'query cap')
    held = integer(route.get('free_reserved_bytes'), 'free reservation')
    if not 0 <= reservation <= maximum <= held <= ALLOCATION:
        raise ValueError('Ledger reservation exceeds its verified free-query cap')
    month = route.get('billing_month')
    jobs, total = verify_observation(observation, project, month)
    if route.get('baseline_usage_bytes') != total:
        raise ValueError('Route baseline differs from provider-accounted bytes')
    allowance = route.get('provider_allowance', {})
    allocation = integer(allowance.get('allocation_limit_bytes'), 'monthly allocation')
    additional = integer(allowance.get('additional_bytes'), 'additional quota reservation')
    unseen = allowance.get('unseen_retained_reservations')
    if not isinstance(unseen, dict):
        raise ValueError('Free allowance omits unobserved reservations')
    unseen_bytes = sum(integer(value, 'unseen reservation') for value in unseen.values())
    if any(not isinstance(key, str) or not key for key in unseen):
        raise ValueError('Invalid unobserved free reservation identity')
    used = integer(allowance.get('used_bytes'), 'quota usage')
    if (not 0 < allocation <= ALLOCATION or used != total + unseen_bytes
            or used + additional > allocation or allowance.get('fits') is not True):
        raise ValueError('Free allocation proof is missing or exceeds the monthly allocation')
    job = jobs.get(identifier)
    if additional == 0:
        if (route.get('state') != 'complete' or not isinstance(job, dict)
                or job.get('state') != 'DONE' or job.get('accounting') not in {'actual_billed', 'confirmed_cached'}
                or job['accounted_bytes'] != reservation):
            raise ValueError('A completed adopted capture requires exact provider billed-byte proof')
    elif additional < maximum:
        raise ValueError('Free allowance does not cover the original query reservation')
    if isinstance(job, dict) and job.get('state') == 'DONE' and job.get('accounting') in {'actual_billed', 'confirmed_cached'}:
        if job['accounted_bytes'] > reservation:
            raise ValueError('Provider billed bytes exceed the retained reservation')
    return (project, identifier), {
        'project_id': project, 'cloud_job_id': identifier, 'billing_month': month,
        'reserved_bytes': reservation, 'provider_billed_bytes': job['accounted_bytes']
            if isinstance(job, dict) and job.get('accounting') in {'actual_billed', 'confirmed_cached'} else None,
        'monthly_usage_bytes_at_verification': total, 'monthly_allocation_limit_bytes': allocation}


def verify_complete_capture(data, logical, route, reservation):
    """Bind a completed exemption to its retained actual provider job request.

    The source auditor separately verifies large raw/decoded result files. This
    budget check binds those retained receipt hashes and their query identity.
    """
    from bigquery_sources import _canonical_parameters
    folder = Path(route.get('origin_output_path', ''))
    folder = (folder if folder.is_absolute() else data / folder).resolve(strict=True)
    if not folder.is_relative_to(data) or not folder.is_dir():
        raise ValueError('Completed free capture leaves project data')
    values, hashes = {}, {}
    for name in ('manifest.json', 'completion_receipt.json', 'cloud_job.json', 'billing_disabled_verification.json'):
        values[name], hashes[name] = read_json(folder / name)
    manifest, receipt, job, billing = (values[name] for name in (
        'manifest.json', 'completion_receipt.json', 'cloud_job.json', 'billing_disabled_verification.json'))
    definition, project = route['definition'], route['execution_project']
    identifier, query_sha = route['job_id'], route['original_query_sha256']
    maximum, month = route['maximum_bytes_billed'], route['billing_month']
    # The one-off Mac repair collector retained its API parameter list sorted
    # by name. Production routes normalize the API types/values to a mapping.
    # Verify the exact recorded encoding instead of rewriting that provenance.
    repair_logical = hashlib.sha256(json.dumps({'query': definition['query'], 'location': 'US',
        'useLegacySql': False, 'parameters': sorted(definition['parameters'], key=lambda p: p['name'])}, sort_keys=True).encode()).hexdigest()
    retained_logical = receipt.get('logical_query_sha256')
    if (manifest.get('status') != 'complete' or manifest.get('job_complete') is not True
            or manifest.get('all_pages_fetched') is not True or receipt.get('job_complete') is not True
            or receipt.get('all_pages_fetched') is not True or manifest.get('definition') != definition
            or manifest.get('job_id') != identifier or manifest.get('ledger_job_id') != identifier
            or manifest.get('maximum_bytes_billed') != maximum or manifest.get('query_sha256') != query_sha
            or manifest.get('actual_billed_bytes') != reservation or receipt.get('actual_billed_bytes') != reservation
            or receipt.get('cloud_job_id') != identifier or receipt.get('execution_project') != project
            or receipt.get('query_sha256') != query_sha or retained_logical not in (logical, repair_logical)):
        raise ValueError('Completed free manifest/receipt identity or billed bytes differ')
    execution = manifest.get('execution_route', {})
    if (execution.get('execution_project') != project or execution.get('job_id') != identifier
            or execution.get('route_kind') != 'unbilled_sandbox'):
        raise ValueError('Completed free capture execution route differs')
    plan = manifest.get('source_plan', {})
    if (plan.get('query') != definition['query'] or plan.get('query_parameters') != definition['parameters']
            or plan.get('query_sha256') != query_sha):
        raise ValueError('Completed free source plan differs from query/period identity')
    ref, query = job.get('jobReference', {}), job.get('configuration', {}).get('query', {})
    query_stats = job.get('statistics', {}).get('query', {})
    statement = query_stats.get('statementType')
    uses_cte = definition['query'].lstrip().upper().startswith('WITH ')
    if statement != 'SELECT' and (uses_cte or statement is not None):
        raise ValueError('Completed free provider job must establish a SELECT statement')
    billed = query_stats.get('totalBytesBilled')
    try:
        billed, cap = int(billed), int(query.get('maximumBytesBilled'))
    except (TypeError, ValueError):
        raise ValueError('Completed free provider job lacks billed bytes/cap') from None
    if (job.get('status', {}).get('state') != 'DONE' or job.get('status', {}).get('errorResult')
            or ref.get('projectId') != project or ref.get('jobId') != identifier or ref.get('location') != 'US'
            or query.get('query') != definition['query'] or query.get('useLegacySql') is not False
            or _canonical_parameters(query.get('queryParameters', [])) != _canonical_parameters(definition['parameters'])
            or cap != maximum or billed != reservation):
        raise ValueError('Completed free provider job does not match retained query/charge')
    created = integer(int(job.get('statistics', {}).get('creationTime', -1)), 'capture provider creation time')
    if datetime.fromtimestamp(created / 1000, timezone.utc).astimezone(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m') != month:
        raise ValueError('Completed free provider job is from another billing month')
    response = billing.get('response', {})
    checksum = billing.get('response_canonical_sha256', billing.get('response_sha256'))
    if (response.get('projectId') != project or response.get('name') != 'projects/' + project + '/billingInfo'
            or response.get('billingEnabled') is not False or response.get('billingAccountName')
            or checksum != digest(response)):
        raise ValueError('Completed capture billing-disabled receipt differs')
    if timestamp(billing.get('observed_at')).astimezone(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m') != month:
        raise ValueError('Completed capture billing proof is from another month')
    if (not SHA.fullmatch(str(receipt.get('output_sha256', '')))
            or receipt['output_sha256'] != manifest.get('decoded_records_sha256')):
        raise ValueError('Completed free output receipts are not hash-bound')
    return {'capture_path': str(folder.relative_to(data)),
            'capture_metadata_sha256': hashes, 'output_sha256': receipt['output_sha256'],
            'provider_billed_bytes': reservation,
            'provider_billed_evidence': 'retained_completed_provider_job',
            'receipt_query_identity_encoding': 'canonical_types' if retained_logical == logical else 'named_api_sorted'}


def classify_reservations(data_root, budget_by_job, route_registry=None):
    """Return verified free bytes without deleting or rewriting a ledger row."""
    data = Path(data_root).resolve(strict=True)
    for identifier, value in budget_by_job.items():
        if not isinstance(identifier, str) or not identifier:
            raise ValueError('Invalid retained budget identity')
        integer(value, 'retained budget reservation')
    registry_checksum = None
    if route_registry is None:
        path = data / 'full_scope_query_routes.json'
        route_registry, registry_checksum = read_json(path) if path.exists() else ({'format_version': 1, 'routes': {}}, None)
    if route_registry.get('format_version') != 1 or not isinstance(route_registry.get('routes'), dict):
        raise ValueError('Invalid retained query routing registry')
    free, cloud, receipts, unverified = {}, {}, {}, 0
    observations = {}
    for logical, route in route_registry['routes'].items():
        if not isinstance(route, dict):
            raise ValueError('Invalid retained query route')
        identifier = route.get('ledger_job_id') or route.get('job_id')
        if identifier not in budget_by_job:
            continue
        claimed = route.get('provider_usage_guard') is not None or route.get('paid_cap_exemption') is not None
        if not claimed:
            unverified += route.get('route_kind') == 'unbilled_sandbox'
            continue
        if (route.get('route_kind') != 'unbilled_sandbox' or route.get('provider_usage_guard') != PROFILE
                or route.get('paid_cap_exemption') != EXEMPTION):
            raise ValueError('Unsupported claimed free-budget exemption')
        evidence = route.get('baseline_evidence', {})
        key = (str(evidence.get('path')), str(evidence.get('sha256')))
        if key not in observations:
            observations[key] = contained_evidence(data, evidence)
        observation, checksum = observations[key]
        identity, verified = verify_route(logical, route, budget_by_job[identifier], observation)
        if route.get('state') == 'complete':
            verified.update(verify_complete_capture(data, logical, route, budget_by_job[identifier]))
        if identifier in free or identity in cloud:
            raise ValueError('Duplicate retained route for the same cloud query')
        free[identifier] = budget_by_job[identifier]
        cloud[identity] = verified['provider_billed_bytes']
        receipts[identifier] = {**verified, 'provider_observation_sha256': checksum}
    total = sum(budget_by_job.values())
    return {'total_reserved_bytes_all_rows': total,
            'verified_free_reserved_bytes': sum(free.values()),
            'paid_or_unknown_reserved_bytes': total - sum(free.values()),
            'verified_free_by_job': free,
            'verified_free_cloud_queries': [
                {'project_id': key[0], 'cloud_job_id': key[1], 'provider_billed_bytes': value}
                for key, value in sorted(cloud.items())],
            'verified_free_reservations': receipts,
            'legacy_unverified_free_routes_kept_conservative': unverified,
            'route_registry_sha256': registry_checksum,
            'ledger_rows_preserved': len(budget_by_job)}


def budget_report(config, budget_by_job, known_queries, classification):
    """Apply the unchanged paid cap and deduplicate retained provider queries."""
    if classification.get('total_reserved_bytes_all_rows') != sum(budget_by_job.values()):
        raise ValueError('Budget classification does not bind the retained ledger')
    free = {}
    for row in classification['verified_free_cloud_queries']:
        if row['provider_billed_bytes'] is not None:
            free[(row['project_id'], 'US', row['cloud_job_id'])] = row['provider_billed_bytes']
    combined = {}
    for key, amount in known_queries.items():
        if not isinstance(key, tuple) or len(key) not in (2, 3) or not all(isinstance(value, str) and value for value in key):
            raise ValueError('Known cloud query identity must include project, optional location and job')
        integer(amount, 'known retained query bytes')
        key = key if len(key) == 3 else (key[0], 'US', key[1])
        if key in combined and combined[key] != amount:
            raise ValueError('Same cloud query has inconsistent retained billed bytes')
        combined[key] = amount
    for key, amount in free.items():
        if key in combined and combined[key] != amount:
            raise ValueError('Same cloud query has inconsistent free/provider billed bytes')
        combined[key] = amount
    protected = classification['paid_or_unknown_reserved_bytes']
    cap = integer(config.get('max_total_query_bytes'), 'configured paid/unknown byte cap')
    nonfree_known = sum(amount for key, amount in combined.items() if key not in free)
    if protected > cap:
        raise ValueError('Paid or unverified reservations exceed the configured cumulative byte cap')
    if protected < nonfree_known:
        raise ValueError('Paid/unknown reservations are below known unclassified billed bytes')
    price, dollars = config.get('query_price_usd_per_TiB', 6.25), config.get('query_budget_usd')
    if (type(price) not in (int, float) or not math.isfinite(price) or price <= 0
            or type(dollars) not in (int, float) or not math.isfinite(dollars) or dollars <= 0):
        raise ValueError('Invalid configured query price or currency budget')
    conservative = protected / 1024**4 * price
    if conservative > dollars + 1e-9:
        raise ValueError('Conservative paid/unknown query cost exceeds the approved currency budget')
    return {'limit_usd': dollars, 'configured_scan_byte_cap': cap,
            'global_ledger_entries': len(budget_by_job),
            'reserved_and_known_billed_bytes': classification['total_reserved_bytes_all_rows'],
            'paid_or_unknown_reserved_bytes': protected,
            'provider_verified_free_reserved_bytes': classification['verified_free_reserved_bytes'],
            'known_billed_bytes_unique_retained_queries': sum(combined.values()),
            'known_paid_or_unknown_billed_bytes': nonfree_known,
            'known_provider_verified_free_billed_bytes': sum(free.values()),
            'unique_retained_queries_with_billed_bytes': len(combined),
            'provider_verified_free_queries': len(free),
            'conservative_cost_usd_before_free_tier': conservative,
            'conservative_cost_scope': 'Paid or unclassified reservations only. Verified billing-disabled sandbox reservations are reported separately.',
            'is_invoice': False,
            'history_check': 'Every retained completed query keeps its sufficient ledger row. Free classification requires retained provider billing and monthly quota evidence. Local derived data do not create another cloud query.',
            'free_classification': classification}
