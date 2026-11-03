import csv
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import collect_research_v1 as module


UTC = timezone.utc


def feed(ids, title='We use PyTorch software', abstract='Experiments use the PyTorch framework.', version=1,
         published='2023-01-03T12:00:00Z', updated=None):
    updated = updated or published
    entries = ''.join(f'''<entry><id>http://arxiv.org/abs/{pid}v{version}</id><title>{title}</title>
        <summary>{abstract}</summary><published>{published}</published><updated>{updated}</updated>
        <author><name>A Researcher</name><arxiv:affiliation>Example University</arxiv:affiliation></author>
        <category term="cs.LG"/><arxiv:primary_category term="cs.LG"/>
        <arxiv:doi>10.0000/example</arxiv:doi><link rel="alternate" href="https://arxiv.org/abs/{pid}v1"/></entry>'''
                      for pid in ids)
    return ('<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">' + entries + '</feed>').encode()


class Clock:
    def __init__(self):
        self.value = datetime(2026, 11, 3, 4, tzinfo=UTC)
        self.waits = []
    def now(self):
        return self.value
    def sleep(self, seconds):
        self.waits.append(seconds); self.value += timedelta(seconds=seconds)


class ParseAndRelevance(unittest.TestCase):
    def test_canonical_modern_legacy_and_host_validation(self):
        self.assertEqual(module.canonical_id('http://arxiv.org/abs/2301.00123v12'), '2301.00123')
        self.assertEqual(module.canonical_id('https://arxiv.org/pdf/cs/9901001v1.pdf'), 'cs/9901001')
        for bad in ['https://evil.example/abs/2301.00123v1', '../2301.00123', '2301.00123;rm', '']:
            with self.subTest(bad=bad), self.assertRaises(ValueError):module.canonical_id(bad)

    def test_explicit_v1_rich_metadata_and_missing_ids(self):
        papers, missing = module.parse_v1_feed(feed(['2301.00001']), ['2301.00001','2301.00002'], '2026-11-03T00:00:00Z')
        self.assertEqual(missing, ['2301.00002'])
        self.assertEqual(papers['2301.00001']['authors_v1'][0]['affiliation'], 'Example University')
        self.assertEqual(papers['2301.00001']['doi'], '10.0000/example')
        self.assertFalse(papers['2301.00001']['historical_feature_eligible'])

    def test_wrong_version_id_duplicate_and_future_dates_rejected(self):
        bad = [feed(['2301.00001'],version=2), feed(['2301.00002']),
               feed(['2301.00001','2301.00001']),
               feed(['2301.00001'],updated='2027-01-01T00:00:00Z')]
        for raw in bad:
            with self.subTest(raw=raw[:70]), self.assertRaises(ValueError):
                module.parse_v1_feed(raw,['2301.00001'],'2026-11-03T00:00:00Z')

    def test_unsafe_xml_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'declarations'):
            module.parse_v1_feed(b'<!DOCTYPE feed><feed/>',[],'2026-11-03T00:00:00Z')

    def test_ray_projection_and_generic_transformers_are_not_strict_library_matches(self):
        ray={'tool_id':'ray','name':'Ray','pypi_package':'ray','research_ambiguous':True,
             'research_exact_aliases':['Ray Tune','RLlib','Ray framework','ray-project/ray']}
        papers,_=module.parse_v1_feed(feed(['2301.00001'],title='Ray projection in physics',abstract='A ray projection method.'),['2301.00001'],'2026-11-03T00:00:00Z')
        result=module.classify(ray,papers['2301.00001'])
        self.assertFalse(result['strict_lexical_candidate']);self.assertEqual(result['match_status'],'ambiguous_v1_review_required')
        papers,_=module.parse_v1_feed(feed(['2301.00001'],title='Transformers for images',abstract='Transformer architectures are useful.'),['2301.00001'],'2026-11-03T00:00:00Z')
        transformer={'tool_id':'transformers','name':'Transformers','pypi_package':'transformers','research_ambiguous':True,
                     'research_exact_aliases':['Hugging Face Transformers','huggingface/transformers']}
        self.assertFalse(module.classify(transformer,papers['2301.00001'])['strict_lexical_candidate'])

    def test_context_match_remains_unreviewed_and_availability_is_explicit(self):
        tool={'tool_id':'ray','research_ambiguous':True,'research_exact_aliases':['Ray Tune']}
        papers,_=module.parse_v1_feed(feed(['2301.00001'],title='Ray Tune for tuning'),['2301.00001'],'2026-11-03T00:00:00Z')
        result=module.classify(tool,papers['2301.00001'])
        self.assertTrue(result['strict_lexical_candidate']);self.assertTrue(result['review_required'])
        self.assertEqual(result['assumed_v1_available_at'],'2023-01-05T12:00:00Z')
        self.assertFalse(result['manual_review_completed']);self.assertFalse(result['historical_feature_eligible'])


class CollectorFixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.data=self.root/'data';self.data.mkdir()
        self.registry=self.root/'tools.json'
        self.registry.write_text(json.dumps({'tools':[{'tool_id':'pytorch','name':'PyTorch','pypi_package':'torch',
                                                       'research_exact_aliases':['PyTorch'],'research_ambiguous':False}]}))
        self.candidates=self.root/'candidates.csv'
        with self.candidates.open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['tool_id','id','published','updated','observed_at']);writer.writeheader()
            for pid in ['2301.00001','2301.00002']:
                writer.writerow({'tool_id':'pytorch','id':'http://arxiv.org/abs/'+pid+'v2','published':'2023-01-03T12:00:00Z',
                                 'updated':'2024-01-01T00:00:00Z','observed_at':'2026-11-02T00:00:00Z'})
        self.clock=Clock();self.calls=[]
        self.disk=patch.object(module.shutil,'disk_usage',return_value=type('Disk',(),{'free':100*1024**3})())
        self.disk.start();self.addCleanup(self.disk.stop)

    def fetch(self,ids):
        self.calls.append(list(ids));stamp=module.iso(self.clock.now())
        return {'status':200,'headers':{},'requested_at':stamp,'observed_at':stamp,'request_url':'https://export.arxiv.org/api/query',
                'body':feed(ids)}

    def collector(self,**kwargs):
        return module.Collector(self.root/'output',self.registry,self.candidates,self.data,end='2026-11-03',
                                batch_size=1,fetch=self.fetch,sleep=self.clock.sleep,clock=self.clock.now,**kwargs)

    def test_request_limit_resumes_verified_raw_and_preserves_all_text(self):
        first=self.collector(max_requests=1);one=first.run();first.finish_spacing()
        self.assertEqual(one['status'],'partial');self.assertEqual(len(self.calls),1)
        second=self.collector(max_requests=2);two=second.run();second.finish_spacing()
        self.assertEqual(two['status'],'complete');self.assertEqual(len(self.calls),2)
        self.assertEqual(two['retrieved_v1_papers'],2)
        rows=[json.loads(x) for x in (self.root/'output/papers_v1.jsonl').read_text().splitlines()]
        self.assertEqual(len(rows),2);self.assertIn('abstract_v1',rows[0]);self.assertIn('authors_v1',rows[0])
        quality=module.read_json(self.root/'output/research_v1_quality.json')
        self.assertFalse(quality['relevance_precision_validated']);self.assertEqual(quality['review_required_pairs'],2)
        self.assertTrue(all(value>=10 for value in self.clock.waits))

    def test_cached_raw_tampering_is_rejected(self):
        collector=self.collector();result=collector.run()
        p=self.root/'output'/result['batches']['0']['raw_path'];p.write_bytes(gzip.compress(b'changed'))
        with self.assertRaisesRegex(ValueError,'checksum'):self.collector().run()

    def test_429_sets_shared_cooldown_and_no_additional_http(self):
        def limited(ids):
            self.calls.append(ids);stamp=module.iso(self.clock.now())
            return {'status':429,'headers':{'retry-after':'300'},'requested_at':stamp,'observed_at':stamp,
                    'request_url':module.API,'body':b'rate limited'}
        c=self.collector();c.fetch=limited;result=c.run();c.finish_spacing()
        self.assertEqual(result['status'],'partial');self.assertEqual(len(self.calls),1)
        until=module.stamp(module.read_json(self.data/'.arxiv-cooldown.json')['until'])
        self.assertGreater(until,self.clock.now())
        self.collector().run();self.assertEqual(len(self.calls),1)

    def test_incomplete_or_invalid_success_cannot_advance_batch(self):
        c=self.collector();c.fetch=lambda ids:{'status':200,'headers':{},'requested_at':module.iso(self.clock.now()),
            'observed_at':module.iso(self.clock.now()),'request_url':module.API,'body':feed([])}
        result=c.run();self.assertEqual(result['complete_batches'],0)
        self.assertEqual(result['batches']['0']['missing_ids'],['2301.00001'])
        self.assertEqual(result['retrieved_v1_papers'],0)

    def test_changed_candidate_universe_cannot_resume_silently(self):
        self.collector().run();self.candidates.write_text(self.candidates.read_text()+'pytorch,2301.00003,,,\n')
        with self.assertRaisesRegex(ValueError,'changed candidate universe'):self.collector()

    def test_frozen_registry_tampering_is_rejected(self):
        self.collector().run();(self.root/'output/tools.study.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'registry copy changed'):self.collector()

    def test_missing_paper_does_not_starve_later_batches(self):
        c=self.collector()
        def some_missing(ids):
            result=self.fetch(ids)
            if ids==['2301.00001']:result['body']=feed([])
            return result
        c.fetch=some_missing;result=c.run()
        self.assertEqual(result['status'],'partial');self.assertEqual(result['retrieved_v1_papers'],1)
        self.assertEqual(result['batches']['0']['status'],'incomplete_response')
        self.assertEqual(result['batches']['1']['status'],'complete')
        quality=module.read_json(self.root/'output/research_v1_quality.json')
        self.assertEqual(quality['review_sample_rows'],1)
        with (self.root/'output/research_review_sample.csv').open(newline='') as stream:
            row=next(csv.DictReader(stream));self.assertEqual(row['human_relevance_label'],'')


class WeeklySemantics(unittest.TestCase):
    def test_partial_retrieval_is_flagged_and_late_v1_metadata_is_not_available_early(self):
        rows=[{'tool_id':'pytorch','v1_retrieval_status':'retrieved','published':'2023-01-03T00:00:00Z',
               'match_status':'strict_alias_v1_candidate','strict_lexical_candidate':True,
               'assumed_v1_available_at':'2024-01-01T00:00:00Z'},
              {'tool_id':'pytorch','v1_retrieval_status':'unresolved'}]
        out=module.weekly_rows(rows,['pytorch'],'2023-01-01','2023-01-16')
        self.assertEqual(len(out),2);self.assertEqual(out[0]['strict_v1_candidate_count'],1)
        self.assertEqual(out[0]['strict_candidates_available_by_assumed_cutoff'],0)
        self.assertFalse(out[0]['seed_v1_retrieval_complete']);self.assertFalse(out[0]['historical_feature_eligible'])
        self.assertEqual(out[0]['feature_cutoff_utc'],'2023-01-11T00:00:00Z')


if __name__ == '__main__':unittest.main(verbosity=2)
