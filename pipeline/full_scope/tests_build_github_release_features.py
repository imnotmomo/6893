from datetime import date,datetime,timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import build_github_release_features as module

UTC=timezone.utc


class ReleaseFeatures(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.requests=[]
        self.tool={'tool_id':'a','github_repo':'org/a','github_repo_id':10,'pypi_package':'a'}
        self.observed=datetime(2026,11,3,5,tzinfo=UTC)

    def release(self,identifier=1,**values):
        row={'id':identifier,'url':'https://api.github.com/repos/org/a/releases/'+str(identifier),
             'tag_name':'v1.0','body':'Full notes\nUnicode λ and literal <script>$x</script>',
             'created_at':'2019-01-01T00:00:00Z','published_at':'2023-01-04T12:00:00Z',
             'draft':False,'prerelease':False,'assets':[{'id':50,'name':'a.whl','download_count':1337}]}
        row.update(values);return row

    def record(self,body,phase,page=1,size=2):
        raw=json.dumps(body).encode();compressed=gzip.compress(raw,mtime=0)
        path=self.root/(str(len(self.requests))+'.json.gz');path.write_bytes(compressed)
        meta={'tool_id':'a','phase':phase,'kind':'repository' if phase=='identity' else 'releases',
              'page':page,'per_page':size,'observed_at':'2026-11-03T04:00:00Z',
              'raw_path':path.name,'raw_sha256':hashlib.sha256(raw).hexdigest(),
              'compressed_sha256':hashlib.sha256(compressed).hexdigest()}
        self.requests.append(meta);return meta

    def complete(self,rows=None):
        rows=rows if rows is not None else [self.release()]
        self.record({'id':10,'full_name':'org/a'},'identity')
        self.record(rows,'list');self.record(rows,'anchor')

    def inspect(self):return module.inspect_tool(self.tool,self.requests,self.root,self.observed)

    def test_publication_date_is_used_instead_of_tag_commit_date(self):
        self.complete();normalized,known,quality,seen=self.inspect()
        rows=module.weekly_rows('a',known,quality,seen,date(2023,1,1),date(2023,1,16))
        self.assertTrue(quality['features_complete']);self.assertEqual(rows[0][module.COUNTS[0]],1)
        self.assertEqual(normalized[0]['created_at'],'2019-01-01T00:00:00Z')
        self.assertEqual(normalized[0]['event_time_field'],'published_at')

    def test_older_publication_page_does_not_stop_later_page_events(self):
        self.record({'id':10,'full_name':'org/a'},'identity')
        first=[self.release(1,published_at='2022-01-01T00:00:00Z')]
        self.record(first,'list',1,1);self.record([self.release(2)],'list',2,1)
        self.record([],'list',3,1);self.record(first,'anchor',1,1)
        _,known,quality,seen=self.inspect()
        self.assertTrue(quality['retrieval_complete'])
        rows=module.weekly_rows('a',known,quality,seen,date(2023,1,1),date(2023,1,16))
        self.assertEqual(rows[0][module.COUNTS[0]],1)

    def test_no_release_is_zero_only_after_verified_empty_listing(self):
        self.complete([]);_,known,quality,seen=self.inspect()
        row=module.weekly_rows('a',known,quality,seen,date(2023,1,1),date(2023,1,16))[0]
        self.assertEqual(row[module.COUNTS[0]],0)
        self.requests=[];_,known,quality,seen=self.inspect()
        row=module.weekly_rows('a',known,quality,seen,date(2023,1,1),date(2023,1,16))[0]
        self.assertIsNone(row[module.COUNTS[0]]);self.assertIsNone(row['observed_partial_'+module.COUNTS[0]])

    def test_missing_or_changed_head_anchor_cannot_certify_counts(self):
        self.complete();self.requests.pop()
        self.assertFalse(self.inspect()[2]['features_complete'])
        self.record([self.release(2)],'anchor')
        self.assertFalse(self.inspect()[2]['features_complete'])

    def test_duplicate_list_ids_and_missing_pages_are_incomplete(self):
        self.record({'id':10,'full_name':'org/a'},'identity')
        first=[self.release()];self.record(first,'list',1,1)
        self.record(first,'list',2,1);self.record([],'list',3,1);self.record(first,'anchor',1,1)
        self.assertTrue(self.inspect()[2]['duplicate_release_ids']);self.assertFalse(self.inspect()[2]['retrieval_complete'])
        self.requests.pop(2)
        self.assertFalse(self.inspect()[2]['sequential_pages'])

    def test_drafts_remain_raw_but_missing_published_non_drafts_are_unknown(self):
        self.complete([self.release(draft=True,published_at=None)])
        normalized,known,quality,_=self.inspect()
        self.assertTrue(quality['features_complete']);self.assertEqual(known,[]);self.assertTrue(normalized[0]['draft'])
        self.requests=[];self.complete([self.release(published_at=None)])
        _,_,quality,_=self.inspect();self.assertFalse(quality['published_time_complete'])

    def test_future_or_naive_publication_is_not_backfilled_from_created_at(self):
        for value in ['2027-01-01T00:00:00Z','2023-01-04T00:00:00']:
            self.requests=[];self.complete([self.release(published_at=value)])
            _,known,quality,_=self.inspect();self.assertEqual(known,[]);self.assertFalse(quality['features_complete'])

    def test_notes_assets_and_uncertain_package_correspondence_are_preserved(self):
        self.complete();normalized,_,_,_=self.inspect();row=normalized[0]
        self.assertEqual(row['notes'],self.release()['body'])
        self.assertEqual(row['assets'][0]['download_count'],1337)
        self.assertFalse(row['pypi_release_correspondence_verified']);self.assertFalse(row['historical_primary_eligible'])

    def test_repository_identity_and_response_hashes_are_required(self):
        self.complete();self.requests[0]['raw_path']='outside'
        with self.assertRaises(ValueError):self.inspect()
        self.requests=[];self.record({'id':999,'full_name':'org/a'},'identity')
        self.record([self.release()],'list');self.record([self.release()],'anchor')
        self.assertFalse(self.inspect()[2]['identity_verified'])

    def test_prerelease_and_stable_counts_are_separate(self):
        self.complete([self.release(prerelease=True)])
        _,known,quality,seen=self.inspect()
        row=module.weekly_rows('a',known,quality,seen,date(2023,1,1),date(2023,1,16))[0]
        self.assertEqual([row[name] for name in module.COUNTS],[1,1,0])

    def test_head_anchor_must_follow_terminal_page(self):
        self.record({'id':10,'full_name':'org/a'},'identity')
        first=[self.release()];self.record(first,'list',1,1);self.record(first,'anchor',1,1)
        self.record([],'list',2,1)
        self.assertFalse(self.inspect()[2]['head_anchor_ids_match'])

    def test_new_attempt_can_recover_without_erasing_old_raw_evidence(self):
        self.complete();old_count=len(self.requests)
        for phase,body in [('identity',{'id':10,'full_name':'org/a'}),('list',[]),('anchor',[])]:
            item=self.record(body,phase);item['attempt']=2
        normalized,known,quality,_=self.inspect()
        self.assertTrue(quality['features_complete']);self.assertEqual(quality['selected_attempt'],2)
        self.assertEqual(len(self.requests),old_count+3);self.assertEqual(normalized,[])

    def test_full_build_retains_unknown_tools_and_hashes_immutable_artifacts(self):
        self.complete()
        registry=self.root/'registry.json'
        tools=[self.tool]+[{'tool_id':'t'+str(i),'github_repo':'org/t'+str(i),'github_repo_id':100+i,'pypi_package':'t'+str(i)} for i in range(29)]
        registry.write_text(json.dumps({'tools':tools}));cohort=module.sha(registry)
        (self.root/'tools.study.json').write_bytes(registry.read_bytes())
        (self.root/'collection_manifest.json').write_text(json.dumps({'source':module.SOURCE,'registry_sha256':cohort,
            'history_start':'2023-01-01','history_end_exclusive':'2023-01-17','requests':self.requests}))
        output=self.root/'features';report=module.build(self.root,registry,output)
        self.assertEqual(report['status'],'partial');self.assertEqual(report['weekly_rows'],60)
        self.assertEqual(report['partial_week_rows'],60);self.assertEqual(report['tools_feature_complete'],1)
        with (output/'github_releases_weekly.csv').open() as stream:rows=list(csv.DictReader(stream))
        self.assertEqual(next(r for r in rows if r['tool_id']=='t0')[module.COUNTS[0]],'')
        manifest=module.read(output/'manifest.json')
        for name,expected in manifest['files'].items():self.assertEqual(module.sha(output/name),expected)
        with self.assertRaises(FileExistsError):module.build(self.root,registry,output)


if __name__=='__main__':unittest.main(verbosity=2)
