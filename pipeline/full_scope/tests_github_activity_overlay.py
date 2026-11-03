import argparse,contextlib,copy,csv,gzip,io,json
from datetime import datetime,timezone
from pathlib import Path
import unittest
import build_github_activity as b
import github_activity_overlay as o
import refresh_github_activity as r
import tests_build_github_activity as legacy
from tests_build_github_activity import REGISTRY,END,issue,pull

NOW=datetime(2023,1,23,12,tzinfo=timezone.utc);FUTURE_END='2023-01-23T00:00:00Z'


class Overlay(unittest.TestCase):
    setUp=legacy.Fixture.setUp;save=legacy.Fixture.save;core=legacy.Fixture.core;search=legacy.Fixture.search
    def setup_sources(self):
        snapshot=self.root/'baseline-state.json';snapshot.write_text(json.dumps(self.state));self.base=self.root/'baseline'
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=self.base,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        self.incremental=self.root/'incremental';self.future_issue=issue(20,'2023-01-17T00:00:00Z',author=40);self.future_issue['updated_at']='2023-01-20T00:00:00Z'
        self.old_issue=issue(11,'2023-01-02T00:00:00Z','2023-01-18T00:00:00Z');self.old_issue['updated_at']='2023-01-20T00:00:00Z'
        self.future_pr=pull(4,'2023-01-17T00:00:00Z','2023-01-19T00:00:00Z','2023-01-19T00:00:00Z',40);self.future_pr['updated_at']='2023-01-20T00:00:00Z'
        self.old_pr=copy.deepcopy(self.new[1]);self.old_pr['closed_at']='2023-01-20T00:00:00Z';self.old_pr['updated_at']='2023-01-20T00:00:00Z'
    def fetch(self,url,pending_issue=False):
        headers={}
        if '/issues?' in url:
            data=[self.old_issue,self.future_issue]
            if pending_issue:headers={'Link':'<'+url+'&after=pending>; rel="next"'}
        elif '/pulls?' in url:data=[self.future_pr,self.old_pr]
        else:data={'id':42,'full_name':'org/repo'}
        return {'status':200,'url':url,'body':json.dumps(data).encode(),'headers':headers}
    def collect(self,pending_issue=False):
        for _ in range(2):r.run(self.registry_path,self.incremental,END,2,0,lambda url:self.fetch(url,pending_issue),NOW,FUTURE_END)
    def incremental_state(self):return json.loads((self.incremental/'refresh_state.json').read_text())
    def overlay(self,name='output'):
        output=self.root/name;q=o.build(self.base,self.incremental,self.registry_path,output,FUTURE_END)
        with (output/'github_activity_weekly.csv').open() as f:rows=list(csv.DictReader(f))
        return output,q,rows
    def test_completed_cycles_extend_weekly_features_without_changing_baseline(self):
        self.setup_sources();baseline_hash=b.sha(self.base/'manifest.json');self.collect();output,q,rows=self.overlay();new=rows[-1]
        self.assertEqual(new['week_start'],'2023-01-16');self.assertEqual(new['github_issue_opened_count'],'1');self.assertEqual(new['github_pr_opened_count'],'1')
        self.assertEqual(new['github_pr_merged_count'],'1');self.assertEqual(new['github_issue_last_reported_closed_count'],'1');self.assertEqual(new['github_pr_last_reported_closed_count'],'2')
        self.assertEqual(new['github_creation_authors'],'1');self.assertEqual(new['coverage_complete'],'True')
        self.assertEqual(b.sha(self.base/'manifest.json'),baseline_hash);self.assertFalse(q['historical_visibility_complete'])
        self.assertEqual(len(q['diagnostics']['completed_cycles']),2);self.assertEqual(q['completed_update_observations'],4)
        self.assertEqual(rows[0]['github_pr_opened_count'],'1');self.assertEqual(rows[0]['github_issue_opened_count'],'')
    def test_partial_cycle_excluded_but_completed_other_stream_is_usable(self):
        self.setup_sources();self.collect(True);_,q,rows=self.overlay();new=rows[-1]
        self.assertEqual(new['github_issue_opened_count'],'');self.assertEqual(new['github_pr_opened_count'],'1')
        self.assertEqual(new['observed_partial_github_issue_opened_count'],'0');self.assertTrue(q['diagnostics']['partial_cycles_excluded'])
        self.assertEqual(q['completed_update_observations'],2)
    def test_duplicate_page_ids_rejected_before_watermark_advance(self):
        self.setup_sources();self.future_issue['id']=self.old_issue['id'];self.collect();state=self.incremental_state()
        self.assertIsNone(state['streams']['tool/issues']['last_success_end_exclusive'])
        self.assertIn('Duplicate',state['streams']['tool/issues']['last_error']['message'])
    def test_changed_raw_or_sidecar_is_fatal(self):
        self.setup_sources();self.collect();meta=self.incremental_state()['requests'][-1];p=self.incremental/meta['raw_path'];p.write_bytes(b'tampered')
        with self.assertRaisesRegex(ValueError,'compressed body hash'):self.overlay()
    def test_completed_watermark_cannot_skip_unfetched_link(self):
        self.setup_sources();self.collect();state=self.incremental_state();state['completed_cycles'][0]['receipt_paths']=state['completed_cycles'][0]['receipt_paths'][:1]
        (self.incremental/'refresh_state.json').write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,'Incomplete update cycle'):self.overlay()
    def test_placeholder_author_masks_exact_creator_count(self):
        self.setup_sources();self.future_issue['user']={'id':10137,'login':'ghost'};self.collect();_,_,rows=self.overlay();new=rows[-1]
        self.assertEqual(new['github_issue_opened_count'],'1');self.assertEqual(new['github_issue_creation_authors'],'');self.assertEqual(new['github_creation_authors'],'')
        self.assertEqual(new['github_issue_unknown_creation_author_count'],'1')
    def test_cohort_and_duplicate_cycle_identity_fail_closed(self):
        self.setup_sources();self.collect();state=self.incremental_state();state['completed_cycles'].append(copy.deepcopy(state['completed_cycles'][0]));(self.incremental/'refresh_state.json').write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,'duplicate completed cycle'):self.overlay()
        state['completed_cycles'].pop();state['registry_sha256']='0'*64;(self.incremental/'refresh_state.json').write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError,'source/cohort'):self.overlay()
    def test_only_newest_completed_source_vintage_changes_current_closure(self):
        self.setup_sources();self.collect();_,_,rows=self.overlay('first');self.assertEqual(rows[-1]['github_issue_last_reported_closed_count'],'1')
        next_now=datetime(2023,1,30,12,tzinfo=timezone.utc);next_end='2023-01-30T00:00:00Z';self.old_issue['closed_at']=None;self.old_issue['updated_at']='2023-01-29T00:00:00Z';self.future_issue['updated_at']='2023-01-29T00:00:00Z'
        self.old_pr['updated_at']='2023-01-29T00:00:00Z';self.future_pr['updated_at']='2023-01-29T00:00:00Z'
        for _ in range(2):r.run(self.registry_path,self.incremental,END,2,0,self.fetch,next_now,next_end)
        out=self.root/'second';o.build(self.base,self.incremental,self.registry_path,out,next_end)
        with (out/'github_activity_weekly.csv').open() as f:rows=list(csv.DictReader(f))
        self.assertEqual(rows[-2]['github_issue_last_reported_closed_count'],'0')
        self.assertEqual(rows[-1]['github_issue_opened_count'],'0')


if __name__=='__main__':unittest.main(verbosity=2)
