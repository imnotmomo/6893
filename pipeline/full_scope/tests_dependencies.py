import copy
import argparse
from datetime import datetime, timezone
import hashlib
import json
import tempfile
from pathlib import Path
import unittest

from dependency_sources import build_metadata_query, decode_rows, canonical, utc
from build_dependency_features import normalize_artifacts, requirement, weekly_features, build


REGISTRY = {'tools': [{'tool_id': 'pytorch', 'pypi_package': 'torch'}]}
OBSERVED = '2023-02-01T00:00:00Z'


def artifact(package, version, uploaded, deps, filename=None):
    filename = filename or package + '-' + version + '.whl'
    return {'name': package, 'version': version, 'requires_dist': deps, 'upload_time': uploaded,
            'filename': filename, 'path': '/files/' + filename, 'sha256_digest': 'a' * 64}


class QueryContract(unittest.TestCase):
    def test_candidate_history_retains_versions_that_drop_dependencies(self):
        plan = build_metadata_query(REGISTRY, '2023-02-01')
        self.assertIn('JOIN candidates', plan['query'])
        self.assertIn('SELECT source.*', plan['query'])
        self.assertNotIn('@start_date', plan['query'])
        self.assertTrue(plan['pre_study_history_required'])
        self.assertNotIn('author_email', plan['query'])
        self.assertEqual(plan['target_package_to_tool'], {'torch': 'pytorch'})

    def test_rest_decoder_preserves_arrays_and_nested_records(self):
        schema = {'fields': [{'name': 'requires_dist', 'type': 'STRING', 'mode': 'REPEATED'},
                             {'name': 'file', 'type': 'RECORD', 'fields': [{'name': 'size', 'type': 'INTEGER'}]}]}
        self.assertEqual(decode_rows(schema, [{'f': [{'v': [{'v': 'torch>=1'}, {'v': 'numpy'}]},
                                                   {'v': {'f': [{'v': '42'}]}}]}]),
                         [{'requires_dist': ['torch>=1', 'numpy'], 'file': {'size': 42}}])
        with self.assertRaises(ValueError): decode_rows(schema, [{'f': []}])

    def test_timestamp_and_name_normalization(self):
        self.assertEqual(canonical('Scikit_Learn'), 'scikit-learn')
        self.assertEqual(utc('1.000001').microsecond, 1)
        self.assertEqual(utc('1.000001E0').microsecond, 1)
        self.assertEqual(utc('1.6725312E9').isoformat(), '2023-01-01T00:00:00+00:00')
        self.assertEqual(utc('2023-01-01T01:00:00+01:00').hour, 0)
        with self.assertRaises(ValueError): utc('2023-01-01T00:00:00')


class Requirements(unittest.TestCase):
    def test_specifiers_markers_extras_and_direct_urls_survive(self):
        value = requirement("Torch[foo_bar]>=1,<3; python_version >= '3.9' and extra == 'train'")
        self.assertTrue(value['valid'])
        self.assertEqual(value['dependency_package'], 'torch')
        self.assertEqual(value['extras'], ['foo-bar'])
        self.assertTrue(value['has_extra_marker'])
        self.assertTrue(value['conditional_declaration'])
        self.assertEqual(requirement('x @ https://example.org/x.whl')['direct_url'], 'https://example.org/x.whl')

    def test_invalid_requirement_is_retained_not_silently_absent(self):
        value = requirement('torch >>> nope')
        self.assertFalse(value['valid'])
        self.assertEqual(value['name_candidate'], 'torch')
        row = artifact('consumer', '1', '2023-01-10T00:00:00Z', ['torch >>> nope'])
        artifacts, edges, audit = normalize_artifacts([row], OBSERVED, '2023-02-01')
        self.assertEqual(edges, [])
        self.assertEqual(audit['error_counts'], {'invalid_requirement': 1})
        weeks = weekly_features(artifacts, REGISTRY, '2023-01-02', '2023-01-23', True)
        self.assertTrue(weeks[0]['dep_inbound_event_time_eligible'])
        self.assertFalse(weeks[1]['dep_inbound_event_time_eligible'])
        self.assertEqual(weeks[1]['dep_unknown_target_artifacts'], 1)

    def test_duplicate_rows_removed_and_conflicting_metadata_flagged(self):
        first = artifact('consumer', '1', '2023-01-01T00:00:00Z', ['torch'])
        second = copy.deepcopy(first); second['requires_dist'] = []
        artifacts, edges, audit = normalize_artifacts([first, first, second], OBSERVED, '2023-02-01')
        self.assertEqual(len(artifacts), 2)
        self.assertEqual(audit['exact_duplicate_rows_removed'], 1)
        self.assertEqual(audit['error_counts'], {'conflicting_artifact_metadata': 1})

    def test_invalid_source_identity_is_preserved_and_quarantined(self):
        bad = artifact('not a package', '1', '2023-01-10T00:00:00Z', ['torch'])
        artifacts, edges, audit = normalize_artifacts([bad], OBSERVED, '2023-02-01')
        self.assertEqual(len(artifacts), 1)
        self.assertFalse(artifacts[0]['identity_valid'])
        self.assertEqual(edges[0]['dependency_package'], 'torch')
        self.assertIsNone(edges[0]['source_package'])
        self.assertEqual(audit['error_counts']['invalid_package_identity'], 1)
        a,b = weekly_features(artifacts, REGISTRY, '2023-01-02','2023-01-16', True)
        self.assertEqual(b['dep_ever_declaring_packages'], 0)
        self.assertTrue(a['dep_inbound_event_time_eligible'])
        self.assertFalse(b['dep_inbound_event_time_eligible'])


class HistoricalDeclarations(unittest.TestCase):
    def test_pre_study_baseline_and_later_dependency_removal(self):
        rows = [artifact('torch', '0', '2022-01-01T00:00:00Z', ['numpy']),
                artifact('consumer', '0', '2022-12-01T00:00:00Z', ['torch>=1']),
                artifact('consumer', '1', '2023-01-04T00:00:00Z', []),
                artifact('consumer', '2', '2023-01-11T00:00:00Z', ["torch; extra == 'train'"])]
        artifacts, _, _ = normalize_artifacts(rows, OBSERVED, '2023-02-01')
        a, b, c = weekly_features(artifacts, REGISTRY, '2023-01-02', '2023-01-23', True)
        self.assertEqual(a['dep_ever_declaring_packages'], 1)
        self.assertEqual(a['dep_new_declaring_packages'], 0)
        self.assertEqual(a['dep_latest_uploaded_release_declaring_packages'], 0)
        self.assertEqual(b['dep_latest_uploaded_release_declaring_packages'], 1)
        self.assertEqual(b['dep_declaring_release_count'], 1)
        self.assertEqual(c['dep_declaring_release_count'], 0)
        self.assertEqual(a['dep_outbound_unique_dependencies'], 1)
        self.assertFalse(a['historical_retrieval_vintage'])

    def test_later_artifact_requirement_is_not_backdated(self):
        rows = [artifact('consumer', '1', '2023-01-02T00:00:00Z', [], 'one.whl'),
                artifact('consumer', '1', '2023-01-10T00:00:00Z', ['torch'], 'two.whl')]
        artifacts, _, _ = normalize_artifacts(rows, OBSERVED, '2023-02-01')
        a, b = weekly_features(artifacts, REGISTRY, '2023-01-02', '2023-01-16', True)
        self.assertEqual(a['dep_ever_declaring_packages'], 0)
        self.assertEqual(b['dep_new_declaring_packages'], 1)
        self.assertEqual(b['dep_declaring_release_count'], 1)

    def test_unqueried_inbound_history_is_unknown_not_zero(self):
        artifacts, _, _ = normalize_artifacts([artifact('torch', '1', '2023-01-02T00:00:00Z', [])], OBSERVED, '2023-02-01')
        row = weekly_features(artifacts, REGISTRY, '2023-01-02', '2023-01-09', False)[0]
        self.assertIsNone(row['dep_ever_declaring_packages'])
        self.assertFalse(row['dep_inbound_history_complete'])
        self.assertFalse(row['dep_inbound_event_time_eligible'])

    def test_query_receipt_rejects_truncation_and_input_tampering(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); registry = root/'registry.json'; registry.write_text(json.dumps(REGISTRY))
            plan = root/'query.json'; plan.write_text(json.dumps(build_metadata_query(REGISTRY, '2023-02-01')))
            source = root/'records.jsonl'; source.write_text(json.dumps(artifact('torch','1','2023-01-01T00:00:00Z',[]))+'\n')
            receipt = {'query_sha256': json.loads(plan.read_text())['query_sha256'], 'job_complete': True,
                       'all_pages_fetched': True, 'total_rows': 1,
                       'output_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
            path = root/'receipt.json'
            args = argparse.Namespace(input=source, registry=registry, query_plan=plan, query_receipt=path,
                                      output=root/'out', start='2023-01-02', end_exclusive='2023-02-01', observed_at=OBSERVED)
            for key,value in [('total_rows',2), ('all_pages_fetched',False), ('output_sha256','wrong')]:
                bad = {**receipt, key:value}; path.write_text(json.dumps(bad))
                with self.assertRaisesRegex(ValueError,'receipt'): build(args)
            path.write_text(json.dumps(receipt)); build(args)
            self.assertEqual(json.loads((args.output/'dependency_quality.json').read_text())['status'], 'passed')

    def test_future_upload_rejected_and_undated_history_quarantined(self):
        with self.assertRaisesRegex(ValueError, 'endpoint'):
            normalize_artifacts([artifact('x', '1', '2023-02-01T00:00:00Z', ['torch'])], OBSERVED, '2023-02-01')
        rows = [artifact('x', '1', None, ['torch'])]
        artifacts, _, audit = normalize_artifacts(rows, OBSERVED, '2023-02-01')
        row = weekly_features(artifacts, REGISTRY, '2023-01-02', '2023-01-09', True)[0]
        self.assertEqual(audit['error_counts']['undated_artifact'], 1)
        self.assertFalse(row['dep_inbound_event_time_eligible'])


if __name__ == '__main__': unittest.main(verbosity=2)
