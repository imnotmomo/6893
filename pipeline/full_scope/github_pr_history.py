"""Offline validation of native PR pages extending the frozen current crawl.

The source is a crawl of currently visible native PR objects. Even a fully
captured offset range does not prove historical visibility or an atomic API
snapshot. No HTTP requests or source mutations occur in this module.
"""
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

VERSION = 'native_pr_all_history_v1'


def positive(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def document(root, name, expected):
    if not isinstance(name, str) or Path(name).is_absolute() or '..' in Path(name).parts:
        raise ValueError('PR-history proof path is not a relative archive path')
    path = (root / name).resolve()
    if not path.is_relative_to(root) or not path.is_file() or digest(path) != expected:
        raise ValueError('PR-history proof path/hash differs')
    return json.loads(path.read_text())


def request_url(request, tool):
    url = urlsplit(request['url'])
    expected = {'state': ['all'], 'sort': ['created'], 'direction': ['asc'],
                'per_page': ['100'], 'page': [str(request['page'])]}
    if (url.scheme != 'https' or url.netloc != 'api.github.com' or url.fragment
            or url.path.lower() != '/repos/' + tool['github_repo'].lower() + '/pulls'
            or parse_qs(url.query, keep_blank_values=True) != expected):
        raise ValueError('PR-history URL changes the frozen native PR offset query')
    if (request.get('repo', '').lower() != tool['github_repo'].lower()
            or request.get('kind') != 'pulls' or request.get('per_page') != 100
            or not positive(request.get('page'))):
        raise ValueError('PR-history receipt repository/page metadata differs')


def canonical_identity(row, tool):
    if not isinstance(row, dict) or not positive(row.get('number')):
        raise ValueError('Native PR lacks a canonical repository/number identity')
    base = (row.get('base') or {}).get('repo') or {}
    if not positive(base.get('id')) or base['id'] != tool['github_repo_id']:
        raise ValueError('Native PR base numeric repository identity differs')
    url = urlsplit(row.get('url', ''))
    expected = '/repos/' + tool['github_repo'].lower() + '/pulls/' + str(row['number'])
    if (url.scheme != 'https' or url.netloc != 'api.github.com' or url.fragment
            or url.query or url.path.lower() != expected):
        raise ValueError('Native PR row URL differs from repository/number')
    if base.get('full_name') and base['full_name'].lower() != tool['github_repo'].lower():
        raise ValueError('Native PR base repository name differs')
    return (tool['github_repo_id'], row['number'])


def scope(root, state, registry, utc):
    tools = {tool['tool_id']: tool for tool in registry['tools']}
    if len(tools) != len(registry['tools']) or any(not positive(tool.get('github_repo_id')) for tool in tools.values()):
        raise ValueError('PR-history cohort requires unique tools and numeric repository IDs')
    binding = state['core_pr_history_plan']
    plan = document(root, binding['path'], binding['sha256'])
    if (plan.get('plan_version') != VERSION or plan.get('registry_sha256') != state['registry_sha256']
            or utc(plan['event_window_start']) != utc(state['start'])
            or utc(plan['event_window_end_exclusive']) != utc(state['end_exclusive'])
            or not utc(state['start']) < utc(state['end_exclusive'])
            or plan.get('request_phase') != 'pr_history_collect' or plan.get('request_stream') != 'core_pr_history'
            or plan.get('query') != {'state':'all', 'sort':'created', 'direction':'asc', 'per_page':100}
            or set(plan.get('tools', {})) != set(tools)):
        raise ValueError('PR-history plan version/cohort/date/query scope differs')
    index_binding = plan['reused_current_index']
    index = document(root, index_binding['path'], index_binding['sha256'])
    if (index.get('version') != 'native_pr_current_reuse_index_v1'
            or index.get('registry_sha256') != state['registry_sha256'] or not isinstance(index.get('entries'), list)):
        raise ValueError('PR-history frozen current-page index differs')
    proof_binding = plan['repo_identity_proofs']
    proofs = document(root, proof_binding['path'], proof_binding['sha256'])
    if proofs.get('registry_sha256') != state['registry_sha256'] or set(proofs.get('tools', {})) != set(tools):
        raise ValueError('PR-history repository proof cohort differs')
    for tid, tool in tools.items():
        definition = plan['tools'][tid]
        original = state.get('tools', {}).get(tid, {})
        identity_proof = proofs['tools'][tid]
        if (definition.get('github_repo', '').lower() != tool['github_repo'].lower()
                or definition.get('github_repo_id') != tool['github_repo_id']
                or identity_proof.get('github_repo', '').lower() != tool['github_repo'].lower()
                or identity_proof.get('expected_repo_id') != tool['github_repo_id']
                or identity_proof.get('observed_repo_id') != tool['github_repo_id']
                or not positive(definition.get('current_start_page'))
                or not positive(definition.get('frozen_window_last_page'))
                or definition['current_start_page'] > definition['frozen_window_last_page']
                or original.get('start_page') != definition['current_start_page']
                or original.get('last_page') != definition['frozen_window_last_page']
                or original.get('repo', '').lower() != tool['github_repo'].lower()
                or original.get('repo_id') != tool['github_repo_id']
                or definition.get('required_older_first_page') != 1
                or definition.get('required_older_last_page') != definition['current_start_page'] - 1
                or definition.get('required_older_count') != definition['current_start_page'] - 1
                or definition.get('tail_probe_start_page') != definition['frozen_window_last_page'] + 1
                or type(definition.get('frozen_final_reused_record_count')) is not int
                or not 0 <= definition['frozen_final_reused_record_count'] <= 100):
            raise ValueError('PR-history frozen repository/window/older-page topology differs')
    if (plan.get('required_older_page_count') != sum(value['required_older_count'] for value in plan['tools'].values())
            or plan.get('reused_current_page_count') != len(index['entries'])):
        raise ValueError('PR-history plan page arithmetic differs')
    requests = defaultdict(list)
    for request in state['requests']:
        requests[request['raw_path']].append(request)
    reused = defaultdict(list)
    index_fields = {'tool_id', 'page', 'kind', 'phase', 'per_page', 'url', 'raw_path', 'raw_sha256',
                    'compressed_sha256', 'record_count', 'observed_at'}
    seen = set()
    for entry in index['entries']:
        if not index_fields <= set(entry):
            raise ValueError('PR-history reuse index omitted receipt bindings')
        tid = entry['tool_id']; key = (tid, entry['page'])
        if tid not in tools or key in seen or entry.get('phase') != 'collect':
            raise ValueError('PR-history reuse index has a duplicate/foreign/non-core page')
        matching = [request for request in requests[entry['raw_path']]
                    if all(request.get(field) == value for field, value in entry.items())]
        if not matching:
            raise ValueError('PR-history reused receipt differs from the original state')
        seen.add(key); reused[tid].append(matching[0])
    for tid, definition in plan['tools'].items():
        if {request['page'] for request in reused[tid]} != set(range(definition['current_start_page'], definition['frozen_window_last_page'] + 1)):
            raise ValueError('PR-history reuse index omits part of the frozen current window')
    return tools, plan, reused, {binding['path']:binding['sha256'], index_binding['path']:index_binding['sha256'],
                                proof_binding['path']:proof_binding['sha256']}


def evaluate_pr_history(root, state, registry, normalize, utc, load_body, max_body_bytes, existing_core=None):
    """Return new observations, all-history certainty, and verified provenance.

    Reused pages are independently read and checked. ``existing_core`` is
    reserved for a future exact-bound certificate, and is never trusted here.
    New rows retain duplicates and use the native PR object timestamp semantics.
    """
    if 'core_pr_history_plan' not in state:
        return [], {}, {'raw_requests_verified':[], 'proof_file_hashes':{}, 'configured':False}
    root = Path(root).resolve()
    tools, plan, reused, proofs = scope(root, state, registry, utc)
    binding = state['core_pr_history_plan']; cohort = state['registry_sha256']
    new = defaultdict(list)
    for request in state['requests']:
        if request.get('phase') != 'pr_history_collect':
            continue
        tid = request.get('tool_id')
        if tid not in tools:
            raise ValueError('PR-history request names an unknown tool')
        definition = plan['tools'][tid]
        segment = request.get('history_segment'); page = request.get('page')
        if (request.get('stream') != 'core_pr_history' or request.get('plan_version') != VERSION
                or request.get('core_pr_history_plan_path') != binding['path']
                or request.get('core_pr_history_plan_sha256') != binding['sha256']
                or request.get('github_repo_id') != tools[tid]['github_repo_id']
                or request.get('repo_identity_proof_path') != plan['repo_identity_proofs']['path']
                or request.get('repo_identity_proof_sha256') != plan['repo_identity_proofs']['sha256']
                or not positive(page) or segment not in {'older', 'tail', 'boundary_refresh'}
                or segment == 'older' and not 1 <= page < definition['current_start_page']
                or segment == 'tail' and page < definition['tail_probe_start_page']
                or segment == 'boundary_refresh' and page != definition['frozen_window_last_page']
                or type(request.get('terminal_page')) is not bool
                or not utc(plan['created_at']) <= utc(request['request_started_at']) <= utc(request['observed_at'])):
            raise ValueError('PR-history new receipt plan/segment/time binding differs')
        new[tid].append(request)
    observations = []; quality = {}; verified = []
    for tid, tool in tools.items():
        definition = plan['tools'][tid]; checkpoint = state['tools'][tid].get('core_pr_history', {})
        selected = {}; page_versions = defaultdict(list); new_paths = {request['raw_path'] for request in new[tid]}
        for request in sorted(reused[tid] + new[tid], key=lambda value:(utc(value['observed_at']), value['raw_path'])):
            request_url(request, tool)
            body = load_body(root, request, cohort, max_body_bytes)
            if (not isinstance(body, list) or len(body) > 100 or type(request.get('record_count')) is not int
                    or request['record_count'] != len(body)):
                raise ValueError('PR-history page shape/count differs')
            is_new = request['raw_path'] in new_paths
            if is_new and request['terminal_page'] is not (not body and request['history_segment'] == 'tail'):
                raise ValueError('PR-history terminal flag is not an actual empty tail')
            records = []
            for row in body:
                key = canonical_identity(row, tool)
                item = normalize(row, tool, request, True)
                if item['repo_id'] != key[0] or item['number'] != key[1] or item['kind'] != 'pull':
                    raise ValueError('PR-history normalizer changed canonical identity')
                if is_new:
                    observations.append({**item, 'source_stream':'core_pr_history'})
                records.append({'number':key[1], 'id':row.get('id'), 'created_at':item['created_at'],
                                'merged_present':'merged_at' in row, 'closed_present':'closed_at' in row,
                                'author_known':item.get('author_account_id') is not None,
                                'event_time_anomalies':item.get('event_time_anomalies',[])})
            page = request['page']; page_versions[page].append((request, records))
            selected[page] = (request, records); verified.append(request)
        reasons = []; pages = sorted(selected)
        old = sorted({request['page'] for request in new[tid] if request['history_segment'] == 'older'})
        tail = sorted({request['page'] for request in new[tid] if request['history_segment'] == 'tail'})
        refresh = [request for request in new[tid] if request['history_segment'] == 'boundary_refresh']
        expected_old = list(range(1, definition['current_start_page']))
        terminal = checkpoint.get('terminal_page_number')
        terminal_valid = positive(terminal) and terminal >= definition['tail_probe_start_page']
        page_complete = bool(terminal_valid and pages == list(range(1, terminal + 1)))
        terminal_request, terminal_rows = selected.get(terminal, ({}, [None])) if terminal_valid else ({}, [None])
        terminal_empty = bool(terminal_valid and terminal_request.get('history_segment') == 'tail'
                              and terminal_request.get('terminal_page') is True and not terminal_rows)
        if not page_complete:reasons.append('missing_all_history_offset_pages')
        if old != expected_old:reasons.append('required_older_pages_incomplete')
        if not terminal_empty:reasons.append('actual_empty_tail_missing')
        expected_tail = list(range(definition['tail_probe_start_page'], terminal + 1)) if terminal_valid else []
        if tail != expected_tail:reasons.append('tail_offset_pages_incomplete')
        def checkpoint_pages(name, expected):
            values=checkpoint.get(name)
            return isinstance(values,list) and all(positive(value) for value in values) and sorted(values)==expected
        def checkpoint_time(name, expected):
            try:return utc(checkpoint.get(name))==expected
            except (ValueError,TypeError,KeyError):return False
        new_records=sum(request['record_count'] for request in new[tid])
        chronological=sorted(new[tid],key=lambda request:(utc(request['observed_at']),request['raw_path']))
        last_receipt=next((request for request in chronological if request['raw_path']==checkpoint.get('last_receipt_raw_path')),None)
        collector_errors=[request['raw_path'] for request in new[tid] if request.get('validation_error')]
        checkpoint_ok = bool(checkpoint and checkpoint.get('status') == 'complete' and chronological
            and checkpoint_pages('older_pages_saved',old) and checkpoint_pages('tail_pages_saved',tail)
            and checkpoint.get('tail_next_page') is None and terminal_empty
            and checkpoint.get('terminal_raw_path') == terminal_request.get('raw_path')
            and checkpoint.get('core_pr_history_plan_path') == binding['path']
            and checkpoint.get('core_pr_history_plan_sha256') == binding['sha256']
            and checkpoint.get('plan_version') == VERSION and checkpoint.get('github_repo_id') == tool['github_repo_id']
            and type(checkpoint.get('record_observations')) is int and checkpoint['record_observations']==new_records
            and last_receipt is not None and utc(last_receipt['observed_at'])==utc(chronological[-1]['observed_at'])
            and checkpoint_time('first_requested_at',min(utc(request['request_started_at']) for request in chronological))
            and checkpoint_time('last_observed_at',utc(chronological[-1]['observed_at'])))
        if not checkpoint_ok:reasons.append('history_checkpoint_incomplete_or_differs')
        if collector_errors:reasons.append('collector_reported_validation_error')
        # A newer native boundary observation must fill any short-cache gap.
        nonempty_tail = any(records for page, (_, records) in selected.items() if page >= definition['tail_probe_start_page'])
        refresh_required = nonempty_tail and definition['frozen_final_reused_record_count'] < 100
        boundary_page = definition['frozen_window_last_page']; boundary_request, boundary_rows = selected[boundary_page]
        frozen_last = next(request for request in reused[tid] if request['page'] == boundary_page)
        if frozen_last['record_count'] != definition['frozen_final_reused_record_count']:
            raise ValueError('PR-history cached final-page count differs from its frozen plan')
        refresh_ok = not refresh_required or bool(refresh and boundary_request.get('history_segment') == 'boundary_refresh'
            and len(boundary_rows) == 100 and checkpoint.get('boundary_refresh_page') == boundary_page
            and checkpoint.get('boundary_refresh_raw_path') == boundary_request['raw_path']
            and checkpoint.get('boundary_refresh_record_count') == len(boundary_rows)
            and checkpoint.get('boundary_refresh_needed') is False)
        if not refresh_ok:reasons.append('short_cached_boundary_requires_verified_refresh')
        original_boundary=next(records for request,records in page_versions[boundary_page] if request['raw_path']==frozen_last['raw_path'])
        boundary_prefix_ok=all((old['number'],old['id'],old['created_at'])==(fresh['number'],fresh['id'],fresh['created_at'])
            for old,fresh in zip(original_boundary,boundary_rows)) and len(boundary_rows)>=len(original_boundary)
        if not boundary_prefix_ok:reasons.append('refreshed_boundary_changed_frozen_prefix')
        counts = Counter(); ids = defaultdict(set); numbers_for_id = defaultdict(set)
        missing_merge = []; missing_closed = []; missing_authors = []; unknown_ids = []; last_created = None; order_ok = True
        event_anomalies=[];bad_merge_in_window=[];bad_close_in_window=[]
        nonempty = [page for page in pages if selected[page][1]]
        last_nonempty = max(nonempty, default=0)
        sizes_ok = all(len(selected[page][1]) == 100 for page in pages if page < last_nonempty)
        for page in pages:
            for row in selected[page][1]:
                number = row['number']; counts[number] += 1
                if positive(row['id']):ids[number].add(row['id']); numbers_for_id[row['id']].add(number)
                else:unknown_ids.append(number)
                current = utc(row['created_at'])
                if last_created is not None and current < last_created:order_ok = False
                last_created = current
                if not row['merged_present']:missing_merge.append(number)
                if not row['closed_present']:missing_closed.append(number)
                if not row['author_known']:missing_authors.append(number)
                for anomaly in row['event_time_anomalies']:
                    inside=utc(state['start'])<=utc(anomaly['event_at'])<utc(state['end_exclusive'])
                    event_anomalies.append({**anomaly,'number':number,'in_study_window':inside})
                    if inside:(bad_merge_in_window if anomaly['field']=='merged_at' else bad_close_in_window).append(number)
        duplicates = sorted(number for number, count in counts.items() if count > 1)
        collisions = {str(identifier):sorted(values) for identifier, values in numbers_for_id.items() if len(values) > 1}
        canonical_conflicts = {str(number):sorted(values) for number, values in ids.items() if len(values) > 1}
        changed_pages = sorted(page for page, versions in page_versions.items()
            if len({tuple((row['number'], row['id'] if positive(row['id']) else None, row['created_at']) for row in records) for _, records in versions}) > 1
            and page != boundary_page)
        if duplicates:reasons.append('duplicate_canonical_pr_numbers')
        if collisions or canonical_conflicts or unknown_ids:reasons.append('provider_record_identity_uncertainty')
        if changed_pages:reasons.append('offset_page_membership_changed')
        if not sizes_ok:reasons.append('short_page_before_later_nonempty_page')
        if bad_merge_in_window or bad_close_in_window:reasons.append('native_event_time_uncertainty_in_study_window')
        # Every native offset is read through an actual empty tail. No
        # serialized timestamp is used to select a page or stop the crawl, so
        # body-field order is diagnostic rather than a pagination proof.
        capture = bool(checkpoint_ok and page_complete and terminal_empty and old == expected_old and tail == expected_tail and refresh_ok and not collector_errors)
        pagination = bool(capture and boundary_prefix_ok and sizes_ok and not duplicates and not changed_pages
                          and not collisions and not canonical_conflicts and not unknown_ids)
        quality[tid] = {'capture_complete':capture, 'visible_offset_crawl_complete':pagination,
            'merged_at_fields_complete':not missing_merge, 'closed_at_fields_complete':not missing_closed,
            'author_fields_complete':not missing_authors, 'merge_count_eligible':pagination and not missing_merge and not bad_merge_in_window,
            'last_reported_closed_count_eligible':pagination and not missing_closed and not bad_close_in_window,
            'merged_at_chronology_complete_in_window':not bad_merge_in_window,
            'closed_at_chronology_complete_in_window':not bad_close_in_window,
            'event_time_anomalies':event_anomalies,
            'in_window_merged_at_chronology_uncertainty_numbers':sorted(set(bad_merge_in_window)),
            'in_window_closed_at_chronology_uncertainty_numbers':sorted(set(bad_close_in_window)),
            'pages_complete':page_complete, 'terminal_empty_page_verified':terminal_empty,
            'boundary_refresh_required':refresh_required, 'boundary_refresh_verified':refresh_ok,
            'boundary_prefix_preserved':boundary_prefix_ok,
            'intermediate_page_sizes_verified':sizes_ok, 'created_order_monotone':order_ok,
            'diagnostic_warnings':[] if order_ok else ['serialized_native_created_at_is_not_monotone'],
            'reasons':sorted(set(reasons)), 'pages_observed':len(pages), 'reused_current_pages':len(reused[tid]),
            'older_pages_observed':len(old), 'tail_pages_observed':len(tail),
            'record_observations':sum(len(rows) for _, rows in selected.values()), 'unique_pr_numbers':len(counts),
            'new_record_observations':new_records, 'collector_validation_error_paths':collector_errors,
            'duplicate_pr_numbers':duplicates, 'unknown_provider_id_numbers':sorted(set(unknown_ids)),
            'provider_id_collisions':collisions, 'canonical_provider_id_conflicts':canonical_conflicts,
            'changed_offset_pages':changed_pages, 'missing_merged_at_numbers':sorted(set(missing_merge)),
            'missing_closed_at_numbers':sorted(set(missing_closed)), 'missing_author_numbers':sorted(set(missing_authors)),
            'terminal_page_number':terminal, 'history_start':state['start'], 'history_end_exclusive':state['end_exclusive'],
            'historical_visibility_complete':False, 'atomic_snapshot':False, 'historical_primary_eligible':False,
            'scope':'Currently visible native PR objects across offset pages; no deleted-object or historical public-availability census.'}
    return observations, quality, {'raw_requests_verified':verified, 'proof_file_hashes':proofs,
        'configured':True, 'plan_version':VERSION, 'original_current_boundaries_preserved':True,
        'existing_core_certificate_used':False, 'unrecognized_provider_fields_retained_in_raw':True}
