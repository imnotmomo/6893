import itertools
from datetime import date, timedelta
import csv
import json
from pathlib import Path
import tempfile
import unittest
import random

from review_dependency_conflicts import context_events, package_uncertainty, latest_difference_variable, reviewed_source, membership
from build_dependency_features import normalize_artifacts, weekly_features, write_jsonl, sha
from dependency_sources import FIELDS as PROVIDER_FIELDS, SOURCE, build_metadata_query
from tests_dependencies import artifact

REGISTRY = {'tools': [{'tool_id': 'pytorch', 'pypi_package': 'torch'}]}
WEEKS = [date(2023, 1, 2) + timedelta(days=7 * index) for index in range(10)]
FIELDS = ['dep_ever_declaring_packages', 'dep_new_declaring_packages',
          'dep_declaring_release_count', 'dep_latest_uploaded_release_declaring_packages',
          'dep_new_declaring_packages_4w', 'dep_declaring_release_count_4w',
          'dep_ever_declaring_packages_change_4w',
          'dep_latest_uploaded_release_declaring_packages_change_4w']


class ExactContext(unittest.TestCase):
    def oracle(self, pairs, reliable):
        variants = []
        for choices in itertools.product([0, 1], repeat=len(pairs)):
            rows = reliable + [pair[index] for pair, index in zip(pairs, choices)]
            records, _, _ = normalize_artifacts(rows, '2023-04-01T00:00:00Z', '2023-03-13')
            variants.append(weekly_features(records, REGISTRY, '2023-01-01', '2023-03-13', True))
        expected = {}
        for index, week in enumerate(WEEKS):
            names = [field for field in FIELDS if len({records[index][field] for records in variants}) > 1]
            if names:
                expected[('torch', week.isoformat())] = sorted(names)
        all_records, _, _ = normalize_artifacts(reliable + [value for pair in pairs for value in pair],
                                               '2023-04-01T00:00:00Z', '2023-03-13')
        groups = []
        for pair in pairs:
            normalized, _, _ = normalize_artifacts(pair, '2023-04-01T00:00:00Z', '2023-03-13')
            groups.append({'artifact_ids': [row['artifact_id'] for row in normalized]})
        events = context_events(all_records, groups, {'torch'})
        self.assertEqual(package_uncertainty(events, {'torch'}, WEEKS), expected)
        return expected

    def pair(self, version, at, present='torch'):
        row = artifact('consumer', version, at, [])
        return [row, {**row, 'requires_dist': [present]}]

    def test_first_declared_date_and_rolling_uncertainty_expire_exactly(self):
        expected = self.oracle([self.pair('1', '2023-01-03T00:00:00Z')],
                               [artifact('consumer', '2', '2023-01-17T00:00:00Z', ['torch'])])
        # In week4 the package's first declaration is certainly inside the
        # four-week window. The optional old release still changes release sum.
        self.assertEqual(expected[('torch', '2023-01-23')], ['dep_declaring_release_count_4w'])
        self.assertEqual({week for _, week in expected},
                         {'2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', '2023-01-30', '2023-02-06'})

    def test_intermediate_first_release_date_is_not_missed_by_extreme_bounds(self):
        # Package already counted and version3 remains latest. Only old
        # version1's first declaration date can move between three weeks.
        reliable = [artifact('consumer', '0', '2022-01-01T00:00:00Z', ['torch']),
                    artifact('consumer', '1', '2023-01-17T00:00:00Z', ['torch']),
                    artifact('consumer', '3', '2023-01-02T00:00:00Z', ['torch'])]
        expected = self.oracle([self.pair('1', '2023-01-03T00:00:00Z'),
                                self.pair('1', '2023-01-10T00:00:00Z')], reliable)
        self.assertIn('dep_declaring_release_count', expected[('torch', '2023-01-09')])

    def test_duplicate_reliable_file_removes_membership_ambiguity(self):
        reliable = artifact('consumer', '1', '2023-01-03T00:00:00Z', ['torch'])
        reliable = {**reliable, 'filename': 'different.whl', 'path': 'different/path'}
        self.assertEqual(self.oracle([self.pair('1', '2023-01-03T00:00:00Z')], [reliable]), {})

    def test_redundant_late_file_old_release_cannot_change_latest_version(self):
        self.oracle([self.pair('1', '2023-01-20T00:00:00Z')],
                    [artifact('consumer', '1', '2022-01-01T00:00:00Z', []),
                     artifact('consumer', '2', '2023-01-02T00:00:00Z', [])])

    def test_uncertain_same_latest_expression_cancels_in_four_week_delta(self):
        state = (False, frozenset({'artifact1'}))
        self.assertFalse(latest_difference_variable(state, state))
        self.assertTrue(latest_difference_variable(state, (False, frozenset())))
        self.oracle([self.pair('1', '2022-01-01T00:00:00Z')], [])

    def test_conditional_membership_and_exact_monday_midnight(self):
        self.oracle([self.pair('1', '2023-01-09T00:00:00Z', "torch>=2; extra == 'train'")], [])

    def test_missing_variant_never_becomes_absence(self):
        rows, _, _ = normalize_artifacts(self.pair('1', '2023-01-03T00:00:00Z'),
                                         '2023-04-01T00:00:00Z', '2023-03-13')
        with self.assertRaisesRegex(ValueError, 'Complete conflicting'):
            context_events(rows[:1], [{'artifact_ids': [row['artifact_id'] for row in rows]}], {'torch'})

    def test_invalid_candidate_name_stays_in_existing_parse_quarantine(self):
        row={'package':'consumer','requires_dist':['foreign-', 'torch >>> invalid', 'torch>=1']}
        self.assertEqual(membership(row, {'torch'}), {'torch'})

    def test_seeded_small_histories_match_every_variant_assignment(self):
        rng=random.Random(6893)
        for case in range(16):
            pairs=[]; reliable=[]
            for index in range(3):
                day=date(2022,12,20)+timedelta(days=rng.randrange(60))
                row=artifact('consumer',str(rng.randrange(1,4)),day.isoformat()+'T00:00:00Z',[],filename=f'optional-{index}.whl')
                pairs.append([row,{**row,'requires_dist':['torch']}])
            for index in range(4):
                day=date(2022,12,20)+timedelta(days=rng.randrange(60))
                reliable.append(artifact('consumer',str(rng.randrange(1,4)),day.isoformat()+'T00:00:00Z',
                                         ['torch'] if rng.randrange(2) else [], filename=f'reliable-{index}.whl'))
            with self.subTest(case=case):self.oracle(pairs,reliable)


class Publication(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'base'; self.source.mkdir()
        self.registry = self.root / 'registry.json'
        self.registry.write_text(json.dumps(REGISTRY))
        row = artifact('consumer', '1', '2023-01-03T00:00:00Z', [])
        rows = [row, {**row, 'requires_dist': ['torch']},
                artifact('torch', '1', '2022-01-01T00:00:00Z', ['numpy'])]
        rows = [{field: row.get(field) for field in PROVIDER_FIELDS} for row in rows]
        records, edges, audit = normalize_artifacts(rows, '2023-04-01T00:00:00Z', '2023-03-13')
        write_jsonl(self.source / 'artifacts.jsonl.gz', records)
        write_jsonl(self.source / 'declared_dependency_edges.jsonl.gz', edges)
        write_jsonl(self.source / 'parse_errors.jsonl.gz', audit['errors'])
        weekly = weekly_features(records, REGISTRY, '2023-01-01', '2023-03-13', True)
        with (self.source / 'weekly_dependency_features.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(weekly[0])); writer.writeheader(); writer.writerows(weekly)
        plan = build_metadata_query(REGISTRY, '2023-03-13')
        (self.source / 'query_plan.json').write_text(json.dumps(plan))
        receipt = {'job_complete': True, 'all_pages_fetched': True, 'total_rows': 3,
                   'query_sha256': plan['query_sha256'], 'output_sha256': 'a' * 64}
        (self.source / 'query_receipt.json').write_text(json.dumps(receipt))
        quality = {'source': SOURCE, 'status': 'needs_review', 'audit': audit,
                   'history_start': '2023-01-01', 'history_end_exclusive': '2023-03-13',
                   'input_metadata_sha256': 'a' * 64, 'query_sha256': plan['query_sha256'],
                   'query_plan_sha256': sha(self.source / 'query_plan.json'),
                   'query_receipt_sha256': sha(self.source / 'query_receipt.json')}
        (self.source / 'dependency_quality.json').write_text(json.dumps(quality))
        self.manifest()

    def manifest(self):
        body = {'source': SOURCE, 'status': 'needs_review', 'registry_sha256': sha(self.registry),
                'files': {path.name: sha(path) for path in self.source.iterdir() if path.name != 'manifest.json'}}
        (self.source / 'manifest.json').write_text(json.dumps(body))

    def test_new_review_preserves_all_raw_bytes_and_numeric_observations(self):
        before = {path.name: sha(path) for path in self.source.iterdir()}
        output = reviewed_source(self.source, self.registry)
        self.assertNotEqual(output, self.source)
        for name, digest in before.items():
            self.assertEqual(sha(self.source / name), digest)
        for name in ['artifacts.jsonl.gz', 'declared_dependency_edges.jsonl.gz', 'parse_errors.jsonl.gz']:
            self.assertEqual((output / name).stat().st_ino, (self.source / name).stat().st_ino)
        with (self.source / 'weekly_dependency_features.csv').open() as stream:
            original = list(csv.DictReader(stream))
        with (output / 'weekly_dependency_features.csv').open() as stream:
            reviewed = list(csv.DictReader(stream))
        self.assertTrue(any(row['dep_inbound_event_time_eligible'] == 'False' for row in reviewed))
        for first, second in zip(original, reviewed):
            for field in first:
                if field != 'dep_inbound_event_time_eligible': self.assertEqual(first[field], second[field])
        quality = json.loads((output / 'dependency_quality.json').read_text())
        self.assertTrue(quality['collection_complete']); self.assertTrue(quality['input_query_complete'])
        self.assertEqual(quality['status'], 'needs_review')
        self.assertEqual(reviewed_source(self.source, self.registry), output)

    def test_tampered_source_and_incomplete_receipt_are_rejected(self):
        path = self.source / 'query_receipt.json'
        value = json.loads(path.read_text()); value['all_pages_fetched'] = False
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'file hash mismatch'): reviewed_source(self.source, self.registry)
        quality_path = self.source / 'dependency_quality.json'
        quality = json.loads(quality_path.read_text()); quality['query_receipt_sha256'] = sha(path)
        quality_path.write_text(json.dumps(quality)); self.manifest()
        with self.assertRaisesRegex(ValueError, 'Complete query receipt'): reviewed_source(self.source, self.registry)


if __name__ == '__main__':
    unittest.main()
