import copy
import gzip
import json
from pathlib import Path
import tempfile
import unittest

from collect_depsdev import Collector, graph_counts, selected_version, retry_time


REGISTRY = {'tools': [{'tool_id': 'pytorch', 'pypi_package': 'torch'}]}
KEY = {'system': 'PYPI', 'name': 'torch', 'version': '2.5'}


def response(value=None, status=200, headers=None):
    return {'status': status, 'headers': headers or {}, 'requested_at': '2026-11-03T00:00:00Z',
            'observed_at': '2026-11-03T00:00:01Z', 'body': json.dumps(value).encode()}


class APIContracts(unittest.TestCase):
    def test_package_default_identity_and_fallback(self):
        data = {'packageKey': {'system': 'PYPI', 'name': 'torch'},
                'versions': [{'versionKey': KEY, 'isDefault': True}]}
        self.assertEqual(selected_version(data, 'torch'), ('2.5', 'provider_current_default'))
        data['versions'][0]['isDefault'] = False
        data['versions'].append({'versionKey': {**KEY, 'version': '3.0rc1'}})
        self.assertEqual(selected_version(data, 'torch')[0], '2.5')
        with self.assertRaises(ValueError): selected_version(data, 'numpy')

    def test_graph_bounds_and_proto_default_zero(self):
        self.assertEqual(graph_counts({'nodes': [{}, {}], 'edges': [{'toNode': 1}]}), (2, 1))
        self.assertEqual(graph_counts({'nodes': [{}]}), (1, 0))
        for graph in [{'nodes': []}, {'nodes': [{}], 'edges': [{'toNode': 2}]},
                      {'nodes': [{}], 'edges': [{'fromNode': True, 'toNode': 0}]}]:
            with self.assertRaises(ValueError): graph_counts(graph)

    def test_retry_after_respects_longer_provider_wait(self):
        self.assertEqual(retry_time({'retry-after': '600'}, '2026-11-03T00:00:00Z'), '2026-11-03T00:10:00+00:00')


class CollectorFixtures(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.registry = self.root/'registry.json'
        self.registry.write_text(json.dumps(REGISTRY)); self.out = self.root/'source'
        self.calls = []
        def fetch(endpoint):
            self.calls.append(endpoint)
            if endpoint.endswith(':requirements'): return response({'pypi': {'dependencies': [{'projectName': 'numpy', 'versionSpecifier': '>=1'}]}})
            if endpoint.endswith(':dependencies'): return response({'nodes': [{'versionKey': KEY}], 'edges': []})
            if '/versions/' in endpoint: return response({'versionKey': KEY, 'publishedAt': '2024-01-01T00:00:00Z'})
            return response({'packageKey': {'system': 'PYPI', 'name': 'torch'},
                             'versions': [{'versionKey': KEY, 'isDefault': True}]})
        self.fetch = fetch

    def collector(self, **kwargs):
        return Collector(self.registry, self.out, interval=0, fetcher=self.fetch, **kwargs)

    def test_current_snapshot_retains_four_bodies_and_never_becomes_historical(self):
        result = self.collector().run()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(len(result['requests']), 4)
        self.assertFalse(result['primary_historical_eligible'])
        self.assertFalse(result['tools']['pytorch']['historical_resolution_vintage'])
        self.assertEqual(result['tools']['pytorch']['direct_requirement_count'], 1)
        self.assertEqual(result['tools']['pytorch']['resolved_nodes'], 1)
        for item in result['requests']:
            self.assertTrue(gzip.decompress((self.out/item['raw_path']).read_bytes()))

    def test_request_limit_resumes_without_repeating_saved_requests(self):
        self.assertEqual(self.collector(max_requests=2).run()['status'], 'partial')
        result = self.collector(max_requests=2).run()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(len(self.calls), 4)

    def test_checksum_change_is_rejected_without_fetch(self):
        result = self.collector().run()
        path = self.out/result['requests'][0]['raw_path']; path.write_bytes(b'bad')
        result = self.collector().run()
        self.assertEqual(result['tools']['pytorch']['status'], 'failed')
        self.assertEqual(len(self.calls), 4)

    def test_404_is_retained_and_is_not_a_zero_dependency_claim(self):
        self.fetch = lambda endpoint: response({'message': 'not found'}, 404)
        result = self.collector().run()
        self.assertEqual(result['status'], 'complete_with_source_gaps')
        self.assertEqual(result['tools']['pytorch']['status'], 'unavailable')
        self.assertNotIn('direct_requirement_count', result['tools']['pytorch'])
        self.assertEqual(len(result['requests']), 1)
        self.assertEqual(len(self.collector().run()['requests']), 1)

    def test_missing_system_requirements_are_unknown_not_zero(self):
        ordinary = self.fetch
        self.fetch = lambda endpoint: response({}) if endpoint.endswith(':requirements') else ordinary(endpoint)
        result = self.collector().run()
        self.assertEqual(result['tools']['pytorch']['status'], 'failed')

    def test_429_stops_without_busy_retry(self):
        self.calls = []
        def limited(endpoint):
            self.calls.append(endpoint)
            return response({'message': 'slow down'}, 429, {'retry-after': '600'}) | {'observed_at': '2099-01-01T00:00:00Z'}
        self.fetch = limited
        result = self.collector().run()
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(len(self.calls), 1)
        self.collector().run()
        self.assertEqual(len(self.calls), 1)


if __name__ == '__main__': unittest.main(verbosity=2)
