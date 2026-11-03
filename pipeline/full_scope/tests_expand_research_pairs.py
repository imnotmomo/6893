from datetime import datetime,timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import collect_research_v1 as reference
from expand_research_pairs import expand_source,digest,read,KIND


class Expansion(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.research=self.root/'research';self.source=self.research/'backfill'
        self.source.mkdir(parents=True);self.registry=self.root/'tools.json'
        self.no_http=mock.patch('urllib.request.urlopen',side_effect=AssertionError('HTTP forbidden')).start()
        self.addCleanup(mock.patch.stopall)

    def fixture(self,tool_count=2,existing_tools=None,published='2023-01-03T00:00:00Z',repeated=False):
        tools=[{'tool_id':f'tool-{n:02d}','pypi_package':f'tool-{n:02d}','name':f'Tool {n}',
            'research_exact_aliases':[f'Alias{n:02d}'],'research_ambiguous':n%2==1} for n in range(tool_count)]
        text=' '.join(tool['research_exact_aliases'][0] for tool in tools)
        if repeated:text+=' Alias01 Alias01 Alias01 Alias01Extra'
        self.registry.write_text(json.dumps({'tools':tools}));(self.source/'tools.study.json').write_bytes(self.registry.read_bytes())
        paper={'paper_id':'2301.00001','requested_version':1,'returned_id':'https://arxiv.org/abs/2301.00001v1',
            'title_v1':text,'abstract_v1':'Retained first-version software mention evidence.',
            'published':published,'v1_updated':published,'metadata_observed_at':'2026-11-03T00:30:00Z',
            'version_verified':True,'manual_review_completed':False,'historical_feature_eligible':False}
        fields=['tool_id','id','published','updated','observed_at','candidate_query']
        existing_tools=existing_tools if existing_tools is not None else [tools[0]['tool_id']]
        with (self.source/'candidates_input.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
            for tid in existing_tools:w.writerow({'tool_id':tid,'id':paper['paper_id'],'published':published,
                'updated':published,'observed_at':'2026-11-03T00:20:00Z','candidate_query':'original frozen query'})
        raw=b'<feed>retained original provider response</feed>'
        (self.source/'raw').mkdir();(self.source/'raw/batch.xml.gz').write_bytes(gzip.compress(raw,mtime=0))
        request={'raw_path':'raw/batch.xml.gz','raw_sha256':hashlib.sha256(raw).hexdigest(),
            'compressed_sha256':digest(self.source/'raw/batch.xml.gz'),'observed_at':paper['metadata_observed_at'],
            'requested_ids':[paper['paper_id']],'status':200,'requested_version':1}
        manifest={'source':reference.SOURCE,'format_version':1,'status':'complete','match_policy':reference.POLICY,
            'registry_sha256':digest(self.registry),'candidate_input_sha256':digest(self.source/'candidates_input.csv'),
            'history_start':'2023-01-01','history_end_exclusive':'2026-11-03','as_of':'2026-11-03T00:30:00Z',
            'created_at':'2026-11-03T00:15:00Z','updated_at':'2026-11-03T00:35:00Z','availability_lag_days':2,
            'historical_feature_eligible':False,'manual_review_completed':False,'discovery_coverage_certified':False,
            'candidate_pairs':len(existing_tools),'unique_candidate_papers':1,'retrieved_v1_papers':1,'requests':[request]}
        exporter=reference.Collector.__new__(reference.Collector)
        exporter.root=self.source;exporter.manifest=manifest;exporter.tools={t['tool_id']:t for t in tools}
        exporter.pairs=reference.load_pairs(self.source/'candidates_input.csv',{'tools':tools});exporter.ids=[paper['paper_id']]
        exporter.code_sha256=digest(Path(reference.__file__));exporter.export({paper['paper_id']:paper})
        (self.source/'collection_manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True))
        return paper,manifest

    def expand(self,source=None):return expand_source(source or self.source,self.registry,self.research)
    def lines(self,path):return [json.loads(line) for line in path.read_text().splitlines() if line]

    def test_all_30_frozen_alias_sets_are_scanned_once_per_pair(self):
        self.fixture(tool_count=30);output=self.expand();m=read(output/'collection_manifest.json')
        self.assertEqual(m['candidate_pairs'],30);self.assertEqual(m['derivation']['added_candidate_pairs'],29)
        self.assertEqual(m['derivation']['tools_scanned'],30);self.assertEqual(m['unique_candidate_papers'],1)
        pairs=self.lines(output/'tool_paper_v1.jsonl');self.assertEqual(len({r['tool_id'] for r in pairs}),30)
        self.assertTrue(all(r['manual_review_completed'] is False and r['historical_feature_eligible'] is False for r in pairs))
        with (output/'research_v1_weekly.csv').open() as f:weekly=list(csv.DictReader(f))
        self.assertEqual(len(weekly),6000)
        self.assertTrue(all(int(r['strict_v1_candidate_count'])==1 for r in weekly if r['week_start']=='2023-01-02'))
        self.no_http.assert_not_called()

    def test_repeated_aliases_create_one_candidate_with_exact_context_spans(self):
        self.fixture(repeated=True);output=self.expand();m=read(output/'collection_manifest.json')
        added=self.lines(output/m['derivation']['added_pairs_file'])
        self.assertEqual(len(added),1);row=added[0];self.assertEqual(row['tool_id'],'tool-01')
        self.assertEqual(len(row['alias_evidence']),4)
        for hit in row['alias_evidence']:
            self.assertEqual(row[hit['field']][hit['start']:hit['end']],hit['matched_text'])
            self.assertIn(hit['matched_text'],hit['context'])
        self.assertFalse(row['discovery_coverage_certified']);self.assertTrue(row['review_required'])

    def test_parent_raw_query_cohort_vintage_and_existing_rows_are_preserved(self):
        _,parent=self.fixture();before={str(p.relative_to(self.source)):digest(p) for p in self.source.rglob('*') if p.is_file()}
        original=self.lines(self.source/'tool_paper_v1.jsonl');output=self.expand();child=read(output/'collection_manifest.json')
        self.assertEqual({str(p.relative_to(self.source)):digest(p) for p in self.source.rglob('*') if p.is_file()},before)
        for name in ['raw/batch.xml.gz','papers_v1.jsonl','candidates_input.csv','tools.study.json']:
            self.assertEqual((output/name).read_bytes(),(self.source/name).read_bytes())
        for name in ['requests','as_of','created_at','updated_at','history_start','history_end_exclusive','candidate_input_sha256']:
            self.assertEqual(child[name],parent[name])
        ancestry=child['derived_from'];self.assertEqual(ancestry['manifest_sha256'],before['collection_manifest.json'])
        self.assertEqual(ancestry['source_relative_path'],'backfill')
        self.assertEqual((output/ancestry['manifest_copy_path']).read_bytes(),(self.source/'collection_manifest.json').read_bytes())
        after={(r['tool_id'],r['paper_id']):r for r in self.lines(output/'tool_paper_v1.jsonl')}
        for row in original:self.assertEqual(after[(row['tool_id'],row['paper_id'])],row)
        self.assertEqual(child['derivation']['classifier_code_sha256'],digest(Path(reference.__file__)))
        self.assertEqual(child['derivation']['registry_sha256'],digest(self.registry))
        self.assertEqual(child['derivation']['provider_requests_added'],0);self.assertEqual(child['derivation']['new_paper_ids'],0)

    def test_review_queue_receives_new_pairs_and_existing_review_notes_survive(self):
        self.fixture();path=self.source/'research_review_sample.csv'
        with path.open() as f:reader=csv.DictReader(f);fields=reader.fieldnames;sample=list(reader)
        sample[0]['review_notes']='Retained human note'
        with path.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(sample)
        m=read(self.source/'collection_manifest.json');m['files'][path.name]=digest(path)
        (self.source/'collection_manifest.json').write_text(json.dumps(m))
        output=self.expand()
        with (output/'research_review_queue.csv').open() as f:queue=list(csv.DictReader(f))
        self.assertEqual(len(queue),2)
        self.assertEqual(next(r for r in queue if r['tool_id']=='tool-00')['review_notes'],'Retained human note')
        self.assertEqual(next(r for r in queue if r['tool_id']=='tool-01')['human_relevance_label'],'')

    def test_repeated_parent_or_derived_invocation_does_not_append_duplicates(self):
        self.fixture();output=self.expand();fingerprint=digest(output/'collection_manifest.json')
        self.assertEqual(self.expand(),output);self.assertEqual(self.expand(output),output)
        self.assertEqual(digest(output/'collection_manifest.json'),fingerprint)
        self.assertEqual(len(self.lines(output/'tool_paper_v1.jsonl')),2)

    def test_already_known_pairs_and_out_of_window_matches_do_not_expand(self):
        self.fixture(existing_tools=['tool-00','tool-01'])
        self.assertEqual(self.expand(),self.source)

    def test_source_hash_or_registry_changes_are_rejected_before_output(self):
        self.fixture();raw=self.source/'raw/batch.xml.gz';raw.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash differs'):self.expand()
        self.assertFalse((self.research/'derived_runs').exists())

    def test_exact_frozen_registry_bytes_are_required(self):
        self.fixture();self.registry.write_text(self.registry.read_text()+'\n')
        with self.assertRaisesRegex(ValueError,'same-cohort'):self.expand()

    def test_output_cannot_mutate_the_parent_directory(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError,'inside its immutable parent'):
            expand_source(self.source,self.registry,self.research,self.source/'derived')


    def test_out_of_window_v1_text_does_not_invent_in_scope_pairs(self):
        self.fixture(published='2022-12-01T00:00:00Z')
        self.assertEqual(self.expand(),self.source)


if __name__=='__main__':unittest.main()
