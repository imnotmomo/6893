import copy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from download_split_coverage import SUPERSEDED, classify_split_parents


class SplitCoverage(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.row = {'path':str(self.root), 'start':'2026-08-17', 'end_exclusive':'2026-08-24',
                    'query_sha256':'frozen-parent', 'state':'query_not_complete'}
        self.marker = {'query_sha256':'frozen-parent', 'estimated_scan_bytes':60*1024**3,
                       'windows':[['2026-08-17','2026-08-20'],['2026-08-20','2026-08-24']]}
        self.manifest = {'source':'pypi_download_dimensions', 'status':'split',
            'query_sha256':'frozen-parent', 'source_plan':dict(self.row), 'pages':[],
            'rows_fetched':0, 'job_complete':False, 'all_pages_fetched':False}
        self.save()

    def save(self):
        (self.root/'manifest.json').write_text(json.dumps(self.manifest))
        (self.root/'split_children.json').write_text(json.dumps(self.marker))

    def queries(self, windows):
        queries, winners = [], {}
        for index, (left, right) in enumerate(windows):
            path = str(self.root/('child-'+str(index)))
            queries.append({'start':left, 'end_exclusive':right, 'input_path':path,
                            'query_sha256':'query-'+str(index)})
            a, b = date.fromisoformat(left), date.fromisoformat(right)
            winners.update({a+timedelta(days=n):path for n in range((b-a).days)})
        return queries, winners

    def classify(self, queries, winners):
        return classify_split_parents([self.row], queries, winners)[0]

    def test_complete_children_are_superseded_without_mutating_source(self):
        before = {p.name:p.read_bytes() for p in self.root.iterdir()}
        queries, winners = self.queries(self.marker['windows'])
        row = self.classify(queries, winners)
        self.assertEqual(row['state'], SUPERSEDED)
        self.assertEqual(row['missing_child_query_days'], [])
        self.assertEqual(row['split_children_sha256'], hashlib.sha256(before['split_children.json']).hexdigest())
        self.assertEqual({p.name:p.read_bytes() for p in self.root.iterdir()}, before)
        self.assertEqual(self.row['state'], 'query_not_complete')

    def test_recursive_complete_grandchildren_cover_parent(self):
        windows = [('2026-08-17','2026-08-18'),('2026-08-18','2026-08-20'),('2026-08-20','2026-08-24')]
        self.assertEqual(self.classify(*self.queries(windows))['state'], SUPERSEDED)

    def test_a_missing_child_or_winning_day_remains_pending(self):
        queries, winners = self.queries(self.marker['windows'][:1])
        result = self.classify(queries, winners)
        self.assertEqual(result['state'], 'split_parent_children_incomplete')
        self.assertEqual(result['missing_child_query_days'], ['2026-08-20','2026-08-21','2026-08-22','2026-08-23'])
        queries, winners = self.queries(self.marker['windows'])
        del winners[date(2026,8,19)]
        self.assertEqual(self.classify(queries,winners)['missing_child_query_days'], ['2026-08-19'])

    def test_overlapping_whole_parent_does_not_certify_child_capture(self):
        queries, winners = self.queries([('2026-08-17','2026-08-24')])
        self.assertNotEqual(self.classify(queries,winners)['state'], SUPERSEDED)

    def test_plan_or_partition_tampering_fails_closed(self):
        original = copy.deepcopy(self.marker)
        for field, value in [('query_sha256','foreign'),('windows',[['2026-08-17','2026-08-21'],['2026-08-21','2026-08-24']])]:
            self.marker = {**original, field:value};self.save()
            with self.subTest(field=field), self.assertRaisesRegex(ValueError,'evidence differs'):
                self.classify(*self.queries(original['windows']))

    def test_submitted_parent_is_never_called_superseded_unsubmitted(self):
        original = copy.deepcopy(self.manifest)
        for field, value in [('reserved_at','2026-11-03T00:00:00Z'),('job_complete',True),('rows_fetched',1),('actual_billed_bytes',10)]:
            self.manifest = {**original,field:value};self.save()
            with self.subTest(field=field), self.assertRaisesRegex(ValueError,'evidence differs'):
                self.classify(*self.queries(self.marker['windows']))

    def test_unsplit_pending_query_keeps_its_original_status(self):
        (self.root/'split_children.json').unlink()
        self.assertEqual(self.classify([],{}),self.row)


if __name__ == '__main__':
    unittest.main()
