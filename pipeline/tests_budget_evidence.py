"""Offline adversarial checks for separated free/paid retained accounting."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import budget_evidence as b
from bigquery_sources import _canonical_parameters


class BudgetEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.prepare_capture()
    def prepare_capture(self, query=None, statement=None):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name)
        self.project = 'radar6893-free2-20261103'
        self.at = '2026-11-16T03:00:00+00:00'
        self.created = int(datetime(2026, 11, 16, 2, tzinfo=timezone.utc).timestamp() * 1000)
        self.query = query if query is not None else 'SELECT @start AS start, @end AS end'
        self.params = [
            {'name': 'start', 'parameterType': {'type': 'DATE'}, 'parameterValue': {'value': '2026-11-10'}},
            {'name': 'end', 'parameterType': {'type': 'DATE'}, 'parameterValue': {'value': '2026-11-11'}},
        ]
        self.definition = {'query': self.query, 'parameters': self.params, 'project': self.project}
        self.identifier = 'fullscope_' + b.digest(self.definition)[:40]
        self.logical = b.digest({'query': self.query, 'location': 'US', 'useLegacySql': False,
                                 'parameters': _canonical_parameters(self.params)})
        self.query_sha = hashlib.sha256(json.dumps({'query': self.query, 'parameters': self.params}, sort_keys=True).encode()).hexdigest()
        self.actual = 4 * 1024**3
        self.maximum = 5 * 1024**3
        self.response = {'billingAccountName': '', 'billingEnabled': False,
                         'name': 'projects/' + self.project + '/billingInfo', 'projectId': self.project}
        self.billing = {'project_id': self.project, 'observed_at': self.at, 'response': self.response,
                        'response_sha256': b.digest(self.response), 'response_canonical_sha256': b.digest(self.response)}
        permission_response = {'permissions': ['bigquery.jobs.listAll', 'bigquery.jobs.list']}
        self.billing['complete_job_visibility'] = {'project_id': self.project, 'observed_at': self.at,
                                                  'response': permission_response,
                                                  'response_canonical_sha256': b.digest(permission_response)}
        self.observation = {'profile': b.PROFILE, 'project_id': self.project, 'billing_month': '2026-11',
                            'observed_at': self.at, 'billing_disabled_verification': self.billing,
                            'pagination_complete': True,
                            'jobs': {self.identifier: {'accounted_bytes': self.actual, 'creation_time_ms': self.created,
                                                       'accounting': 'actual_billed', 'state': 'DONE'}},
                            'provider_accounted_bytes': self.actual}
        self.route = {'definition': self.definition, 'execution_project': self.project, 'job_id': self.identifier,
                      'ledger_job_id': self.identifier, 'route_kind': 'unbilled_sandbox', 'state': 'complete',
                      'maximum_bytes_billed': self.maximum, 'free_reserved_bytes': self.maximum,
                      'billing_month': '2026-11', 'original_query_sha256': self.query_sha,
                      'baseline_usage_bytes': self.actual, 'provider_usage_guard': b.PROFILE,
                      'paid_cap_exemption': b.EXEMPTION,
                      'provider_allowance': {'used_bytes': self.actual, 'unseen_retained_reservations': {},
                                             'additional_bytes': 0, 'allocation_limit_bytes': b.ALLOCATION, 'fits': True},
                      'origin_output_path': 'raw/full_scope/bigquery/example'}
        self.budget = {'paid_legacy': 12 * b.ALLOCATION, self.identifier: self.actual}
        self.config = {'query_budget_usd': 80, 'max_total_query_bytes': 12 * b.ALLOCATION, 'query_price_usd_per_TiB': 6.25}
        folder = self.data / self.route['origin_output_path']
        folder.mkdir(parents=True)
        output_sha = b.digest({'pretend': 'source auditor separately checks these bytes'})
        manifest = {'status': 'complete', 'job_complete': True, 'all_pages_fetched': True,
                    'definition': self.definition, 'job_id': self.identifier, 'ledger_job_id': self.identifier,
                    'maximum_bytes_billed': self.maximum, 'query_sha256': self.query_sha,
                    'actual_billed_bytes': self.actual, 'decoded_records_sha256': output_sha,
                    'execution_route': {'execution_project': self.project, 'job_id': self.identifier, 'route_kind': 'unbilled_sandbox'},
                    'source_plan': {'query': self.query, 'query_parameters': self.params, 'query_sha256': self.query_sha}}
        receipt = {'job_complete': True, 'all_pages_fetched': True, 'actual_billed_bytes': self.actual,
                   'cloud_job_id': self.identifier, 'execution_project': self.project,
                   'query_sha256': self.query_sha, 'logical_query_sha256': self.logical, 'output_sha256': output_sha}
        job = {'status': {'state': 'DONE'}, 'jobReference': {'projectId': self.project, 'jobId': self.identifier, 'location': 'US'},
               'configuration': {'query': {'query': self.query, 'queryParameters': self.params, 'useLegacySql': False,
                                           'maximumBytesBilled': str(self.maximum)}},
               'statistics': {'creationTime': str(self.created), 'query': {'totalBytesBilled': str(self.actual)}}}
        if statement is not None:
            job['statistics']['query']['statementType'] = statement
        for name, value in [('manifest.json', manifest), ('completion_receipt.json', receipt),
                            ('cloud_job.json', job), ('billing_disabled_verification.json', self.billing)]:
            self.write(folder / name, value)
        self.persist()

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(b.encode(value) + '\n')

    def persist(self):
        path = self.data / 'quota.json'
        self.write(path, self.observation)
        self.route['baseline_evidence'] = {'path': 'quota.json', 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        self.book = {'format_version': 1, 'routes': {self.logical: self.route}}

    def classify(self):
        return b.classify_reservations(self.data, self.budget, self.book)

    def reject_observation(self, mutate):
        mutate(self.observation)
        self.persist()
        with self.assertRaises(ValueError):
            self.classify()

    def reject_capture(self, filename, mutate):
        path = self.data / self.route['origin_output_path'] / filename
        value = json.loads(path.read_text())
        mutate(value)
        self.write(path, value)
        with self.assertRaises(ValueError):
            self.classify()

    def test_verified_free_is_separate_preserving_all_rows(self):
        before = deepcopy(self.budget)
        result = self.classify()
        report = b.budget_report(self.config, self.budget, {}, result)
        self.assertEqual(self.budget, before)
        self.assertEqual(report['reserved_and_known_billed_bytes'], 12 * b.ALLOCATION + self.actual)
        self.assertEqual(report['paid_or_unknown_reserved_bytes'], 12 * b.ALLOCATION)
        self.assertEqual(report['provider_verified_free_reserved_bytes'], self.actual)
        self.assertEqual(report['limit_usd'], 80)
        self.assertEqual(report['configured_scan_byte_cap'], 12 * b.ALLOCATION)
        self.assertEqual(report['conservative_cost_usd_before_free_tier'], 75)

    def test_bare_free_flag_remains_conservative(self):
        self.route.pop('provider_usage_guard')
        self.route.pop('paid_cap_exemption')
        result = self.classify()
        self.assertEqual(result['verified_free_reserved_bytes'], 0)
        self.assertEqual(result['legacy_unverified_free_routes_kept_conservative'], 1)
        with self.assertRaisesRegex(ValueError, 'cumulative byte cap'):
            b.budget_report(self.config, self.budget, {}, result)

    def test_invalid_claim_cannot_silently_fall_back(self):
        self.route['paid_cap_exemption'] = 'free'
        with self.assertRaises(ValueError): self.classify()

    def test_tampered_observation_file(self):
        (self.data / 'quota.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'checksum'): self.classify()

    def test_evidence_cannot_leave_data(self):
        self.route['baseline_evidence']['path'] = '/etc/passwd'
        with self.assertRaisesRegex(ValueError, 'leaves project'): self.classify()

    def test_billing_enabled_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification']['response'].update(billingEnabled=True))

    def test_billing_project_mismatch_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification'].update(project_id='different-project'))

    def test_billing_response_checksum_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification'].update(response_canonical_sha256='0' * 64))

    def test_incomplete_pagination_rejected(self):
        self.reject_observation(lambda o: o.update(pagination_complete=False))

    def test_month_mismatch_rejected(self):
        self.reject_observation(lambda o: o.update(billing_month='2026-10'))

    def test_provider_usage_sum_rejected(self):
        self.reject_observation(lambda o: o.update(provider_accounted_bytes=self.actual - 1))

    def test_unknown_job_accounting_rejected(self):
        self.reject_observation(lambda o: o['jobs'][self.identifier].update(accounting='free'))

    def test_billing_and_inventory_time_separation_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification'].update(observed_at='2026-11-16T00:00:00+00:00'))

    def test_missing_cross_user_visibility_rejected_even_when_pagination_claims_complete(self):
        self.reject_observation(lambda o: o['billing_disabled_verification'].pop('complete_job_visibility'))

    def test_partial_cross_user_permissions_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification']['complete_job_visibility']['response'].update(permissions=['bigquery.jobs.list']))

    def test_cross_user_permissions_checksum_must_match(self):
        self.reject_observation(lambda o: o['billing_disabled_verification']['complete_job_visibility'].update(response_canonical_sha256='0' * 64))

    def test_cross_user_permissions_from_another_project_rejected(self):
        self.reject_observation(lambda o: o['billing_disabled_verification']['complete_job_visibility'].update(project_id='another-project'))

    def test_allowance_overflow_rejected(self):
        self.route['provider_allowance']['allocation_limit_bytes'] = 1
        with self.assertRaises(ValueError): self.classify()

    def test_adopted_free_job_requires_actual_complete_provider_bytes(self):
        self.reject_observation(lambda o: o['jobs'][self.identifier].update(state='RUNNING', accounting='unknown_or_inflight_cap'))

    def test_wrong_deterministic_job_rejected(self):
        self.route['job_id'] = 'fullscope_wrong'
        with self.assertRaises(ValueError): self.classify()

    def test_wrong_query_period_rejected(self):
        self.reject_capture('cloud_job.json', lambda j: j['configuration']['query']['queryParameters'][0]['parameterValue'].update(value='2026-11-01'))

    def test_changed_capture_receipt_rejected(self):
        self.reject_capture('completion_receipt.json', lambda r: r.update(actual_billed_bytes=self.actual - 1))

    def test_original_mac_repair_receipt_encoding_is_validated_without_rewrite(self):
        path = self.data / self.route['origin_output_path'] / 'completion_receipt.json'
        receipt = json.loads(path.read_text())
        receipt['logical_query_sha256'] = hashlib.sha256(json.dumps({'query': self.query, 'location': 'US',
            'useLegacySql': False, 'parameters': sorted(self.params, key=lambda p: p['name'])}, sort_keys=True).encode()).hexdigest()
        self.write(path, receipt)
        before = path.read_bytes()
        result = self.classify()
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(result['verified_free_reservations'][self.identifier]['receipt_query_identity_encoding'], 'named_api_sorted')

    def test_unknown_receipt_identity_encoding_rejected(self):
        self.reject_capture('completion_receipt.json', lambda r: r.update(logical_query_sha256='0' * 64))

    def test_changed_capture_output_hash_rejected(self):
        self.reject_capture('completion_receipt.json', lambda r: r.update(output_sha256='0' * 64))

    def test_captured_provider_query_mismatch_rejected(self):
        self.reject_capture('cloud_job.json', lambda j: j['configuration']['query'].update(query='SELECT 1'))

    def test_duplicate_logical_routes_cannot_remove_same_ledger_row_twice(self):
        self.book['routes']['0' * 64] = deepcopy(self.route)
        with self.assertRaises(ValueError): self.classify()

    def test_local_derived_parent_and_rich_source_only_count_one_cloud_job(self):
        # The source auditor uses one dictionary key for direct + derived parent.
        known = {(self.project, self.identifier): self.actual}
        known[(self.project, self.identifier)] = self.actual
        report = b.budget_report(self.config, self.budget, known, self.classify())
        self.assertEqual(report['known_billed_bytes_unique_retained_queries'], self.actual)
        self.assertEqual(report['unique_retained_queries_with_billed_bytes'], 1)
        self.assertEqual(report['known_provider_verified_free_billed_bytes'], self.actual)

    def test_conflicting_same_cloud_job_bytes_rejected(self):
        with self.assertRaisesRegex(ValueError, 'inconsistent'):
            b.budget_report(self.config, self.budget, {(self.project, self.identifier): self.actual + 1}, self.classify())

    def test_distinct_cloud_projects_remain_distinct(self):
        report = b.budget_report(self.config, self.budget, {('another-project', self.identifier): self.actual}, self.classify())
        self.assertEqual(report['unique_retained_queries_with_billed_bytes'], 2)
        self.assertEqual(report['known_paid_or_unknown_billed_bytes'], self.actual)

    def test_actual_audit_triple_query_keys_deduplicate_same_parent(self):
        known = {(self.project, 'US', self.identifier): self.actual}
        report = b.budget_report(self.config, self.budget, known, self.classify())
        self.assertEqual(report['unique_retained_queries_with_billed_bytes'], 1)

    def test_negative_or_boolean_ledger_reservations_rejected(self):
        for value in (-1, True):
            self.budget['paid_legacy'] = value
            with self.assertRaises(ValueError): self.classify()

    def test_paid_cap_and_currency_authorization_both_unchanged(self):
        self.config['query_budget_usd'] = 50
        with self.assertRaisesRegex(ValueError, 'currency budget'):
            b.budget_report(self.config, self.budget, {}, self.classify())

    def test_reserved_unsubmitted_route_uses_full_query_cap(self):
        self.route['state'] = 'reserved'
        self.route.pop('origin_output_path')
        self.route['provider_allowance']['additional_bytes'] = self.maximum
        self.budget[self.identifier] = self.maximum
        self.observation['jobs'] = {}
        self.observation['provider_accounted_bytes'] = 0
        self.route['baseline_usage_bytes'] = 0
        self.route['provider_allowance']['used_bytes'] = 0
        self.persist()
        result = self.classify()
        self.assertEqual(result['verified_free_reserved_bytes'], self.maximum)
        self.assertEqual(result['verified_free_cloud_queries'][0]['provider_billed_bytes'], None)

    def test_completed_route_with_preclaim_inventory_uses_actual_retained_provider_charge(self):
        # A normal route's monthly observation was captured before submission.
        # Its eventual cloud_job/receipt supply the actual amount without a new
        # query or a second charge for any locally derived canonical totals.
        self.observation['jobs'] = {}
        self.observation['provider_accounted_bytes'] = 0
        self.route['baseline_usage_bytes'] = 0
        self.route['provider_allowance']['used_bytes'] = 0
        self.route['provider_allowance']['additional_bytes'] = self.maximum
        self.persist()
        report = b.budget_report(self.config, self.budget,
            {(self.project, 'US', self.identifier): self.actual}, self.classify())
        self.assertEqual(report['unique_retained_queries_with_billed_bytes'], 1)
        self.assertEqual(report['known_paid_or_unknown_billed_bytes'], 0)
        self.assertEqual(report['known_provider_verified_free_billed_bytes'], self.actual)

    def test_reserved_route_cannot_claim_less_than_full_original_cap(self):
        self.route['state'] = 'reserved'
        self.route['provider_allowance']['additional_bytes'] = self.maximum - 1
        with self.assertRaises(ValueError): self.classify()


    def test_completed_cte_requires_select_statement(self):
        cte = 'WITH source AS (SELECT @start AS start, @end AS end) SELECT * FROM source'
        self.prepare_capture(cte, 'SELECT')
        self.assertEqual(self.classify()['verified_free_reserved_bytes'], self.actual)
        for statement in (None, 'INSERT', 'SCRIPT'):
            with self.subTest(statement=statement):
                self.prepare_capture(cte, statement)
                with self.assertRaisesRegex(ValueError, 'must establish a SELECT statement'):
                    self.classify()

    def test_write_and_multiple_statements_are_rejected(self):
        for query in ('INSERT INTO target SELECT 1', 'SELECT 1; SELECT 2'):
            with self.subTest(query=query):
                self.prepare_capture(query)
                with self.assertRaisesRegex(ValueError, 'Invalid free-query definition'):
                    self.classify()


if __name__ == '__main__':
    unittest.main()
