import argparse,contextlib,copy,csv,gzip,io,json
from datetime import datetime,timezone
from pathlib import Path
import unittest
import build_github_activity as b
import collect_github_activity_updates as c
import github_activity_overlay as o
import tests_build_github_activity as legacy
from tests_build_github_activity import END,issue,pull,search_pull

NOW=datetime(2023,1,23,12,tzinfo=timezone.utc)


class Updates(unittest.TestCase):
    setUp=legacy.Fixture.setUp;save=legacy.Fixture.save;core=legacy.Fixture.core;search=legacy.Fixture.search
    def prepare(self):
        self.baseline=self.root/'baseline';self.incremental=self.root/'incremental';self.calls=[]
        snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state));args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=self.baseline,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        self.new_pr=pull(4,'2023-01-17T00:00:00Z','2023-01-19T00:00:00Z','2023-01-19T00:00:00Z',40);self.new_pr['updated_at']='2023-01-20T00:00:00Z'
        known=search_pull(self.new[1]);known['updated_at']='2023-01-20T00:00:00Z'
        created=issue(20,'2023-01-17T00:00:00Z',author=40);created['updated_at']='2023-01-19T00:00:00Z'
        self.rows=[known,search_pull(self.new_pr),created] # Real serialized-order mismatch is diagnostic.
    def fetch(self,url):
        self.calls.append(url)
        if '/issues?' in url:value=self.rows
        elif '/pulls/4' in url:value=self.new_pr
        else:value={'id':42,'full_name':'org/repo'}
        return {'status':200,'url':url,'body':json.dumps(value).encode(),'headers':{}}
    def step(self,cap=1):return c.run(self.registry_path,self.incremental,self.baseline,cap,0,self.fetch,NOW)
    def checkpoint(self):return json.loads((self.incremental/c.STATE).read_text())
    def overlay(self,name):
        out=self.root/name;q=o.build(self.baseline,self.incremental,self.registry_path,out,'2023-01-23T00:00:00Z')
        with (out/'github_activity_weekly.csv').open() as f:rows=list(csv.DictReader(f))
        return q,rows[-1]
    def test_known_prs_reuse_creation_and_new_pr_queues_one_detail(self):
        self.prepare();self.step();self.step();state=self.checkpoint()
        self.assertEqual(len(state['completed_cycles']),1);self.assertEqual(set(state['pending_details']),{'tool/4'})
        q,row=self.overlay('pending');self.assertEqual(row['github_issue_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'');self.assertEqual(row['github_pr_opened_count'],'')
        self.step();state=self.checkpoint();self.assertFalse(state['pending_details']);self.assertEqual(set(state['completed_details']),{'tool/4'})
        q,row=self.overlay('complete');self.assertEqual(row['github_pr_opened_count'],'');self.assertEqual(row['github_creation_authors'],'')
        self.assertEqual(row['observed_partial_github_pr_opened_count'],'1')
        self.assertEqual(sum('/pulls/4' in u for u in self.calls),1);self.assertFalse(any('/pulls?' in u for u in self.calls))
        self.assertTrue(state['completed_cycles'][0]['serialized_update_order_anomalies'])
    def test_partial_issue_cycle_never_advances_exact_counts(self):
        self.prepare();original=self.fetch
        def fetch(url):
            response=original(url)
            if '/issues?' in url:response['headers']={'Link':'<'+url+'&after=next>; rel="next"'}
            return response
        c.run(self.registry_path,self.incremental,self.baseline,1,0,fetch,NOW);c.run(self.registry_path,self.incremental,self.baseline,1,0,fetch,NOW)
        self.assertFalse(self.checkpoint()['completed_cycles']);self.assertIsNone(self.checkpoint()['streams']['tool/issues']['last_success_end_exclusive'])
    def test_duplicate_page_ids_and_changed_baseline_index_fail_closed(self):
        self.prepare();self.rows[2]['id']=self.rows[0]['id'];self.step();result=self.step()
        self.assertEqual(result['status'],'errors');self.assertFalse(self.checkpoint()['completed_cycles'])
        cache=next((self.incremental/'baseline_indexes').glob('*/pr_creation.json.gz'));cache.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'index hash'):self.step()
    def test_new_validated_baseline_resolves_creation_only_queue_without_http(self):
        self.prepare();self.step();self.step();self.assertIn('tool/4',self.checkpoint()['pending_details'])
        path=self.baseline/'normalized_records.jsonl.gz';raw=gzip.decompress(path.read_bytes());new=b.identity(self.new_pr,{'tool_id':'tool','github_repo':'org/repo','github_repo_id':42},{'observed_at':'2023-01-23T12:00:00Z','raw_path':'new-baseline-pr.json.gz'},True)
        path.write_bytes(gzip.compress(raw+(json.dumps(new)+'\n').encode(),mtime=0));manifest=json.loads((self.baseline/'manifest.json').read_text());manifest['files'][path.name]=b.sha(path);(self.baseline/'manifest.json').write_text(json.dumps(manifest))
        self.calls=[];result=self.step();self.assertEqual(result['requests'],0);self.assertFalse(self.calls);self.assertFalse(self.checkpoint()['pending_details'])
        _,row=self.overlay('reconciled');self.assertEqual(row['github_pr_opened_count'],'')
        self.assertEqual(row['observed_partial_github_pr_opened_count'],'1')
    def test_missing_merge_field_masks_only_merge_until_fresh_detail(self):
        self.prepare();self.rows[0]['pull_request'].pop('merged_at');self.step();self.step();q,row=self.overlay('missing')
        self.assertEqual(row['github_issue_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'')
        self.assertEqual(set(self.checkpoint()['pending_details']),{'tool/3','tool/4'})
    def test_registry_or_identity_change_cannot_start_a_new_cycle(self):
        self.prepare()
        def wrong(url):return {'status':200,'url':url,'body':b'{"id":77,"full_name":"org/repo"}','headers':{}}
        result=c.run(self.registry_path,self.incremental,self.baseline,1,0,wrong,NOW)
        self.assertEqual(result['status'],'errors');self.assertFalse(self.checkpoint()['completed_cycles'])
    def test_legacy_archive_is_preserved_as_separate_source(self):
        self.prepare();self.incremental.mkdir();legacy=self.incremental/'refresh_state.json';legacy.write_text('{"legacy":"untouched"}');before=b.sha(legacy)
        self.step();self.assertEqual(b.sha(legacy),before);self.assertEqual(self.checkpoint()['legacy_source_preserved']['sha256'],before)


if __name__=='__main__':unittest.main(verbosity=2)
