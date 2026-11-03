import argparse,contextlib,copy,csv,gzip,io,json
from datetime import datetime,timedelta,timezone
from pathlib import Path
import unittest
import build_github_activity as b
import github_activity_overlay as overlay
import native_pr_updates as native
import refresh_github_activity as r
import tests_build_github_activity as legacy
from tests_build_github_activity import END,REGISTRY,pull

NOW=datetime(2023,1,23,12,tzinfo=timezone.utc)
TOOL=REGISTRY['tools'][0]


def pr(number,created='2022-12-01T00:00:00Z',updated='2023-01-10T00:00:00Z',merged=None,closed=None,author=70):
    row=pull(number,created,merged,closed,author);row['updated_at']=updated;return row


class NativeUpdates(unittest.TestCase):
    setUp=legacy.Fixture.setUp;save=legacy.Fixture.save;core=legacy.Fixture.core;search=legacy.Fixture.search
    def prepare(self):
        self.base=self.root/'baseline';self.raw=self.root/'incremental';self.calls=[];snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state))
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=self.base,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        self.prefix=[pr(40,'2023-01-17T00:00:00Z','2023-01-21T00:00:00Z','2023-01-19T00:00:00Z','2023-01-19T00:00:00Z',80),
            pr(30,updated='2023-01-20T00:00:00Z',merged='2023-01-18T00:00:00Z',closed='2023-01-18T00:00:00Z')]+[pr(1000+i) for i in range(98)]
        self.boundary=[pr(3000+i,updated='2023-01-08T00:00:00Z') for i in range(100)]
        self.initial=r.initial_url(TOOL,'pulls','unused');self.next=self.initial+'&page=2';self.after=self.initial+'&page=3'
    def fetch(self,url):
        self.calls.append(url)
        if url=='https://api.github.com/repos/org/repo':value={'id':42,'full_name':'org/repo'};headers={}
        elif url==self.initial:value=self.prefix;headers={'Link':'<'+self.next+'>; rel="next"'}
        elif url==self.next:value=self.boundary;headers={'Link':'<'+self.after+'>; rel="next"'}
        else:raise AssertionError('A bounded prefix must not fetch later historical pages')
        return {'status':200,'url':url,'headers':headers,'body':json.dumps(value).encode()}
    def step(self,**kwargs):return native.run(self.registry_path,self.raw,self.base,1,0,self.fetch,NOW,**kwargs)
    def checkpoint(self):return json.loads((self.raw/native.STATE).read_text())
    def publish(self,name='overlay'):
        out=self.root/name;before=b.sha(self.base/'manifest.json');quality=overlay.build(self.base,self.raw,self.registry_path,out,'2023-01-23T00:00:00Z')
        with (out/'github_activity_weekly.csv').open() as f:rows=list(csv.DictReader(f))
        self.assertEqual(before,b.sha(self.base/'manifest.json'));return quality,rows[-1]
    def test_native_old_merge_and_new_pr_are_recovered_without_any_issue_wrapper(self):
        self.prepare();self.step();self.assertFalse(self.checkpoint()['completed_cycles']);self.step();state=self.checkpoint()
        self.assertEqual(state['streams']['tool']['last_success_end_exclusive'],'2023-01-23T00:00:00Z');self.assertEqual(self.calls,[self.initial,self.next])
        quality,row=self.publish();self.assertEqual(row['github_pr_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'2');self.assertEqual(row['github_pr_last_reported_closed_count'],'2')
        self.assertEqual(row['github_pr_creation_authors'],'1');self.assertEqual(row['github_issue_opened_count'],'');self.assertEqual(row['github_creation_authors'],'')
        self.assertFalse(quality['primary_eligible']);self.assertFalse(quality['historical_visibility_complete']);self.assertIn('native_pr_updates',quality['diagnostics'])
    def test_partial_native_prefix_does_not_claim_exact_zero_or_a_complete_week(self):
        self.prepare();self.step();quality,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'');self.assertEqual(row['github_pr_merged_count'],'');self.assertFalse(self.checkpoint()['completed_cycles'])
        self.assertEqual(quality['diagnostics']['native_pr_updates']['partial_streams'],['tool'])
    def test_wrapper_only_pr_cannot_inflate_an_independently_completed_native_series(self):
        self.prepare()
        wrapper=pr(990,'2023-01-17T00:00:00Z','2023-01-21T00:00:00Z','2023-01-19T00:00:00Z','2023-01-19T00:00:00Z',90)
        wrapper['url']='https://api.github.com/repos/org/repo/issues/990';wrapper['repository_url']='https://api.github.com/repos/org/repo';wrapper['pull_request']={'merged_at':wrapper.pop('merged_at')}
        wrapper.pop('base')
        item=b.identity(wrapper,TOOL,{'observed_at':'2023-01-23T12:00:00Z','raw_path':'retained-wrapper','stream':'issues_visible'},False)
        path=self.base/'normalized_records.jsonl.gz';raw=gzip.decompress(path.read_bytes())
        path.write_bytes(gzip.compress(raw+(json.dumps(item)+'\n').encode(),mtime=0))
        manifest=json.loads((self.base/'manifest.json').read_text());manifest['files'][path.name]=b.sha(path);(self.base/'manifest.json').write_text(json.dumps(manifest))
        self.step();self.step();quality,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'2')
        self.assertEqual(row['github_pr_creation_authors'],'1');self.assertEqual(row['observed_partial_github_pr_opened_count'],'2')
        self.assertEqual(row['pr_creation_measurement_view'],'independent_native_pr_observations')
    def test_full_old_boundary_certifies_zero_changes_with_a_real_observation_time(self):
        self.prepare();self.prefix=self.boundary;self.step();quality,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'0');self.assertEqual(row['github_pr_merged_count'],'0')
        self.assertEqual(quality['completed_update_observations'],0);self.assertGreaterEqual(b.utc(quality['observed_at']),b.utc('2023-01-23T00:00:00Z'))
    def test_nonmonotone_native_resource_is_unknown_and_never_walked_to_all_history(self):
        self.prepare();self.prefix[0],self.prefix[1]=self.prefix[1],self.prefix[0];result=self.step();state=self.checkpoint()
        self.assertEqual(result['status'],'unknown_boundaries');self.assertFalse(state['completed_cycles']);self.assertEqual(len(state['failed_cycles']),1)
        self.assertIsNone(state['streams']['tool']['last_success_end_exclusive']);calls=len(self.calls);self.step();self.assertEqual(len(self.calls),calls)
    def test_missing_page_or_changed_raw_hash_cannot_advance_coverage(self):
        self.prepare();self.step();self.step();state=self.checkpoint();state['completed_cycles'][0]['receipt_paths']=state['completed_cycles'][0]['receipt_paths'][1:]
        (self.raw/native.STATE).write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,'cycle/URL/window'):self.publish()
        state=self.checkpoint();state['completed_cycles'][0]['receipt_paths']=[m['raw_path'] for m in state['requests']];(self.raw/native.STATE).write_text(json.dumps(state))
        p=self.raw/state['requests'][0]['raw_path'];p.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'compressed body hash'):self.publish()
    def test_missing_merge_field_masks_merge_only_after_population_capture(self):
        self.prepare();self.prefix[0].pop('merged_at');self.step();self.step();_,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'');self.assertEqual(row['github_pr_last_reported_closed_count'],'2')
    def test_future_update_of_real_pre_study_closure_anomaly_does_not_stall_native_population(self):
        self.prepare();self.prefix[1].update(created_at='2022-09-24T08:46:56Z',closed_at='2022-08-16T11:07:44Z',merged_at=None)
        self.step();self.step();quality,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'1');self.assertEqual(row['github_pr_last_reported_closed_count'],'1')
        self.assertTrue(self.checkpoint()['completed_cycles']);self.assertFalse(quality['diagnostics']['native_event_time_uncertainty'])
    def test_in_study_native_event_anomaly_masks_event_series_but_preserves_creation(self):
        self.prepare();self.prefix[0].update(created_at='2023-01-20T00:00:00Z',closed_at='2023-01-19T00:00:00Z',merged_at=None)
        self.step();self.step();quality,row=self.publish()
        self.assertEqual(row['github_pr_opened_count'],'1');self.assertEqual(row['github_pr_merged_count'],'1');self.assertEqual(row['github_pr_last_reported_closed_count'],'')
        self.assertIn('tool/github_pr_last_reported_closed_count',quality['diagnostics']['native_event_time_uncertainty'])
    def test_page_cap_keeps_unknown_instead_of_starting_full_daily_history_scans(self):
        self.prepare();self.step(max_pages=1);state=self.checkpoint()
        self.assertFalse(state['completed_cycles']);self.assertIn('page cap',state['failed_cycles'][0]['reason']);self.assertEqual(self.calls,[self.initial])
    def test_rate_headers_pause_before_another_source_can_use_the_bucket(self):
        self.prepare();reset=int((NOW+timedelta(minutes=20)).timestamp())
        def limited(url):return {'status':429,'url':url,'headers':{'X-RateLimit-Remaining':'0','X-RateLimit-Reset':str(reset),'Retry-After':'30'},'body':b'{"message":"limited"}'}
        result=native.run(self.registry_path,self.raw,self.base,1,0,limited,NOW);self.assertGreater(r.dt(result['not_before']),NOW)
        called=[];again=native.run(self.registry_path,self.raw,self.base,1,0,lambda u:called.append(u),NOW);self.assertEqual(again['requests'],0);self.assertFalse(called)
    def test_initial_empty_native_result_needs_fresh_numeric_repository_proof(self):
        self.prepare();self.prefix=[]
        original=self.fetch
        def empty(url):
            response=original(url)
            if url==self.initial:response['headers']={}
            return response
        native.run(self.registry_path,self.raw,self.base,1,0,empty,NOW);self.assertFalse(self.checkpoint()['completed_cycles'])
        native.run(self.registry_path,self.raw,self.base,1,0,empty,NOW);self.assertTrue(self.checkpoint()['completed_cycles'])
        _,row=self.publish();self.assertEqual(row['github_pr_opened_count'],'0')


if __name__=='__main__':unittest.main(verbosity=2)
