import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from github_pr_history import canonical_identity, document, request_url, evaluate_pr_history, VERSION
from build_github_activity import identity, utc, raw_body

TOOL={'tool_id':'example','github_repo':'owner/example','github_repo_id':42}


def pr(number,created='2022-01-01T00:00:00Z'):
    return {'id':1000+number,'number':number,'url':f'https://api.github.com/repos/owner/example/pulls/{number}',
            'base':{'repo':{'id':42,'full_name':'owner/example'}},'created_at':created,
            'updated_at':created,'closed_at':None,'merged_at':None,'state':'open',
            'user':{'id':900+number,'login':'public-user'},'unrecognized_future_provider_field':{'kept':True}}


def receipt(page=1):
    return {'tool_id':'example','repo':'owner/example','kind':'pulls','per_page':100,'page':page,
            'url':f'https://api.github.com/repos/owner/example/pulls?state=all&sort=created&direction=asc&per_page=100&page={page}'}


class NativeIdentity(unittest.TestCase):
    def test_exact_native_query_and_canonical_identity(self):
        request_url(receipt(),TOOL)
        self.assertEqual(canonical_identity(pr(7),TOOL),(42,7))

    def test_other_filters_or_credentials_cannot_change_query_scope(self):
        for suffix in ['&since=2023-01-01','&page=2','&state=closed','#fragment']:
            value=receipt();value['url']+=suffix
            with self.subTest(suffix=suffix),self.assertRaises(ValueError):request_url(value,TOOL)
        value=receipt();value['url']=value['url'].replace('api.github.com','user@api.github.com')
        with self.assertRaises(ValueError):request_url(value,TOOL)

    def test_bool_or_foreign_repository_ids_never_become_canonical_identity(self):
        for value in [True,'42',43,None]:
            row=pr(7);row['base']['repo']['id']=value
            with self.subTest(value=value),self.assertRaises(ValueError):canonical_identity(row,TOOL)

    def test_number_url_and_base_repository_must_all_agree(self):
        for change in ['row_url','base_name','number']:
            row=pr(7)
            if change=='row_url':row['url']=row['url'].replace('/pulls/7','/issues/7')
            elif change=='base_name':row['base']['repo']['full_name']='owner/foreign'
            else:row['number']=False
            with self.subTest(change=change),self.assertRaises(ValueError):canonical_identity(row,TOOL)

    def test_proof_paths_and_bytes_are_hash_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder).resolve();path=root/'plan.json';raw=b'{"version":"test"}';path.write_bytes(raw)
            expected=hashlib.sha256(raw).hexdigest()
            self.assertEqual(document(root,'plan.json',expected),{'version':'test'})
            path.write_bytes(b'changed')
            with self.assertRaises(ValueError):document(root,'plan.json',expected)
            with self.assertRaises(ValueError):document(root,'../plan.json',expected)


class HistoryFixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.registry={'tools':[TOOL]};registry_bytes=json.dumps(self.registry).encode();self.cohort=hashlib.sha256(registry_bytes).hexdigest()
        self.state={'registry_sha256':self.cohort,'start':'2023-01-01T00:00:00Z','end_exclusive':'2026-11-03T00:00:00Z',
            'requests':[],'tools':{'example':{'repo':'owner/example','repo_id':42,'start_page':2,'last_page':2}}}
        self.current=self.save(2,[pr(101,'2023-01-01T00:00:00Z'),pr(102,'2023-01-02T00:00:00Z')],False)
        fields=['tool_id','page','kind','phase','per_page','url','raw_path','raw_sha256','compressed_sha256','record_count','observed_at']
        self.index={'version':'native_pr_current_reuse_index_v1','registry_sha256':self.cohort,
                    'entries':[{field:self.current[field] for field in fields}]}
        proof={'registry_sha256':self.cohort,'tools':{'example':{'github_repo':'owner/example','expected_repo_id':42,'observed_repo_id':42}}}
        self.write('repo_identity_proofs.json',proof)
        self.plan={'plan_version':VERSION,'created_at':'2026-11-03T01:00:00Z','registry_sha256':self.cohort,
            'event_window_start':self.state['start'],'event_window_end_exclusive':self.state['end_exclusive'],
            'request_phase':'pr_history_collect','request_stream':'core_pr_history',
            'query':{'state':'all','sort':'created','direction':'asc','per_page':100},
            'reused_current_index':{'path':'core_pr_history_reused_index.json'},
            'repo_identity_proofs':{'path':'repo_identity_proofs.json','sha256':self.hash('repo_identity_proofs.json')},
            'required_older_page_count':1,'reused_current_page_count':1,
            'tools':{'example':{'github_repo':'owner/example','github_repo_id':42,'current_start_page':2,
                'frozen_window_last_page':2,'required_older_first_page':1,'required_older_last_page':1,
                'required_older_count':1,'tail_probe_start_page':3,'frozen_final_reused_record_count':2}}}
        self.rebind()
        older=[pr(number) for number in range(1,101)]
        older[0].update(closed_at='2024-01-02T00:00:00Z',merged_at='2024-01-02T00:00:00Z',state='closed')
        self.older=self.save(1,older,True,'older');self.tail=self.save(3,[],True,'tail')
        self.checkpoint()

    def write(self,name,value):
        (self.root/name).write_text(json.dumps(value,sort_keys=True))
    def hash(self,name):return hashlib.sha256((self.root/name).read_bytes()).hexdigest()
    def rebind(self):
        self.write('core_pr_history_reused_index.json',self.index)
        self.plan['reused_current_index']['sha256']=self.hash('core_pr_history_reused_index.json')
        self.write('core_pr_history_plan.json',self.plan)
        self.binding={'path':'core_pr_history_plan.json','sha256':self.hash('core_pr_history_plan.json')}
        self.state['core_pr_history_plan']=self.binding
        for request in self.state['requests']:
            if request.get('phase')=='pr_history_collect':
                request['core_pr_history_plan_sha256']=self.binding['sha256']
        cp=self.state['tools']['example'].get('core_pr_history')
        if cp:cp['core_pr_history_plan_sha256']=self.binding['sha256']

    def save(self,page,rows,is_new,segment=None):
        r=receipt(page);r.update(observed_at='2026-11-03T02:00:00Z',registry_sha256=self.cohort,
            phase='pr_history_collect' if is_new else 'collect',transport='github_connector_json_body',record_count=len(rows))
        if is_new:r.update(stream='core_pr_history',plan_version=VERSION,github_repo_id=42,
            core_pr_history_plan_path=self.binding['path'],core_pr_history_plan_sha256=self.binding['sha256'],
            repo_identity_proof_path=self.plan['repo_identity_proofs']['path'],repo_identity_proof_sha256=self.plan['repo_identity_proofs']['sha256'],
            history_segment=segment,request_started_at='2026-11-03T01:59:59Z',terminal_page=not rows and segment=='tail')
        raw=json.dumps(rows).encode();path=f'raw-{len(self.state["requests"])}.json.gz'
        (self.root/path).write_bytes(gzip.compress(raw,mtime=0))
        r.update(raw_path=path,raw_sha256=hashlib.sha256(raw).hexdigest(),compressed_sha256=self.hash(path))
        self.state['requests'].append(r);return r

    def replace_body(self,request,rows):
        raw=json.dumps(rows).encode();(self.root/request['raw_path']).write_bytes(gzip.compress(raw,mtime=0))
        request.update(raw_sha256=hashlib.sha256(raw).hexdigest(),compressed_sha256=self.hash(request['raw_path']),record_count=len(rows))
        if self.state['tools']['example'].get('core_pr_history'):
            self.state['tools']['example']['core_pr_history']['record_observations']=sum(r['record_count'] for r in self.state['requests'] if r.get('phase')=='pr_history_collect')
    def body(self,request):return json.loads(gzip.decompress((self.root/request['raw_path']).read_bytes()))
    def checkpoint(self,terminal=None,refresh=None):
        terminal=terminal or self.tail
        cp={'status':'complete','older_pages_saved':sorted({r['page'] for r in self.state['requests'] if r.get('history_segment')=='older'}),
            'tail_pages_saved':sorted({r['page'] for r in self.state['requests'] if r.get('history_segment')=='tail'}),
            'tail_next_page':None,'terminal_page_number':terminal['page'],'terminal_raw_path':terminal['raw_path'],
            'core_pr_history_plan_path':self.binding['path'],'core_pr_history_plan_sha256':self.binding['sha256'],
            'plan_version':VERSION,'github_repo_id':42,'boundary_refresh_needed':False}
        if refresh:cp.update(boundary_refresh_page=refresh['page'],boundary_refresh_raw_path=refresh['raw_path'],boundary_refresh_record_count=refresh['record_count'])
        native=sorted([r for r in self.state['requests'] if r.get('phase')=='pr_history_collect'],key=lambda r:(utc(r['observed_at']),r['raw_path']))
        cp.update(record_observations=sum(r['record_count'] for r in native),last_receipt_raw_path=native[-1]['raw_path'],
            first_requested_at=min(r['request_started_at'] for r in native),last_observed_at=native[-1]['observed_at'])
        self.state['tools']['example']['core_pr_history']=cp
    def evaluate(self):
        return evaluate_pr_history(self.root,self.state,self.registry,identity,utc,raw_body,8*1024**2)

    def test_old_native_objects_and_unknown_provider_fields_remain_available(self):
        rows,quality,diag=self.evaluate();q=quality['example']
        self.assertTrue(q['capture_complete']);self.assertTrue(q['merge_count_eligible'])
        self.assertEqual(q['unique_pr_numbers'],102);self.assertEqual(len(rows),100)
        self.assertEqual(rows[0]['created_at'],'2022-01-01T00:00:00Z')
        self.assertEqual(rows[0]['merged_at'],'2024-01-02T00:00:00Z')
        self.assertEqual(rows[0]['source_stream'],'core_pr_history')
        self.assertTrue(self.body(self.older)[0]['unrecognized_future_provider_field']['kept'])
        self.assertFalse(q['historical_visibility_complete']);self.assertFalse(q['historical_primary_eligible'])
        self.assertEqual(len(diag['raw_requests_verified']),3)

    def test_missing_field_is_uncertainty_not_an_exact_zero(self):
        rows=self.body(self.older);rows[0].pop('merged_at');rows[1].pop('closed_at')
        self.replace_body(self.older,rows)
        normalized,quality,_=self.evaluate();q=quality['example']
        self.assertTrue(q['capture_complete']);self.assertFalse(q['merged_at_fields_complete'])
        self.assertFalse(q['closed_at_fields_complete']);self.assertFalse(q['merge_count_eligible'])
        self.assertEqual(q['missing_merged_at_numbers'],[1]);self.assertEqual(q['missing_closed_at_numbers'],[2])
        self.assertFalse(normalized[0]['merged_at_field_present'])
    def test_real_pre_study_native_closure_anomaly_is_retained_without_masking_study_counts(self):
        rows=self.body(self.older);rows[0].update(created_at='2022-09-24T08:46:56Z',closed_at='2022-08-16T11:07:44Z',merged_at=None)
        self.replace_body(self.older,rows);normalized,quality,_=self.evaluate();q=quality['example']
        self.assertTrue(q['capture_complete']);self.assertTrue(q['last_reported_closed_count_eligible'])
        self.assertTrue(q['merge_count_eligible']);self.assertEqual(normalized[0]['last_reported_closed_at'],'2022-08-16T11:07:44Z')
        self.assertEqual(q['event_time_anomalies'][0]['number'],1);self.assertFalse(q['event_time_anomalies'][0]['in_study_window'])
    def test_in_study_event_anomaly_masks_only_its_measurement_not_capture(self):
        rows=self.body(self.older);rows[0].update(created_at='2025-01-01T00:00:00Z',closed_at='2024-01-02T00:00:00Z',merged_at=None)
        self.replace_body(self.older,rows);_,quality,_=self.evaluate();q=quality['example']
        self.assertTrue(q['capture_complete']);self.assertTrue(q['merge_count_eligible']);self.assertFalse(q['last_reported_closed_count_eligible'])
        self.assertEqual(q['in_window_closed_at_chronology_uncertainty_numbers'],[1])
        rows[0]['merged_at']='2024-01-02T00:00:00Z';rows[0]['closed_at']=None;self.replace_body(self.older,rows)
        _,quality,_=self.evaluate();q=quality['example'];self.assertFalse(q['merge_count_eligible']);self.assertTrue(q['last_reported_closed_count_eligible'])
    def test_future_events_and_non_history_precreation_values_keep_strict_gates(self):
        rows=self.body(self.older);rows[0]['closed_at']='2027-01-01T00:00:00Z';self.replace_body(self.older,rows)
        with self.assertRaisesRegex(ValueError,'event time'):self.evaluate()
        row=pr(7,'2022-09-24T08:46:56Z');row['closed_at']='2022-08-16T11:07:44Z'
        with self.assertRaisesRegex(ValueError,'event time'):identity(row,TOOL,{'phase':'collect','observed_at':'2026-11-03T02:00:00Z','raw_path':'strict-current'},True)

    def test_duplicate_native_numbers_are_retained_and_disable_exactness(self):
        rows=self.body(self.older);rows[-1]=copy.deepcopy(rows[-2]);self.replace_body(self.older,rows)
        normalized,quality,_=self.evaluate();q=quality['example']
        self.assertEqual(len(normalized),100);self.assertTrue(q['capture_complete'])
        self.assertFalse(q['visible_offset_crawl_complete']);self.assertEqual(q['duplicate_pr_numbers'],[99])

    def test_unknown_provider_id_disables_certainty_without_discarding_object(self):
        rows=self.body(self.older);rows[0]['id']=None;self.replace_body(self.older,rows)
        normalized,quality,_=self.evaluate()
        self.assertEqual(len(normalized),100);self.assertTrue(quality['example']['capture_complete'])
        self.assertFalse(quality['example']['visible_offset_crawl_complete'])
        self.assertEqual(quality['example']['unknown_provider_id_numbers'],[1])

    def test_provider_id_collision_and_deleted_author_have_separate_gates(self):
        rows=self.body(self.older);rows[1]['id']=rows[0]['id'];self.replace_body(self.older,rows)
        _,quality,_=self.evaluate();q=quality['example']
        self.assertEqual(q['provider_id_collisions'],{'1001':[1,2]})
        self.assertTrue(q['capture_complete']);self.assertFalse(q['visible_offset_crawl_complete'])
        rows[1]['id']=1002;rows[0]['user']={'id':10137,'login':'ghost'};self.replace_body(self.older,rows)
        normalized,quality,_=self.evaluate();q=quality['example']
        self.assertTrue(q['merge_count_eligible']);self.assertFalse(q['author_fields_complete'])
        self.assertIsNone(normalized[0]['author_account_id'])
        self.assertEqual(normalized[0]['author_reported_account_id'],10137)

    def test_terminal_boolean_or_checkpoint_count_cannot_stand_in_for_evidence(self):
        self.tail['terminal_page']=False
        with self.assertRaisesRegex(ValueError,'actual empty tail'):self.evaluate()
        self.tail['terminal_page']=True
        checkpoint=self.state['tools']['example']['core_pr_history'];checkpoint['terminal_page_number']=True
        _,quality,_=self.evaluate();self.assertFalse(quality['example']['capture_complete'])
        self.checkpoint();self.state['tools']['example']['core_pr_history']['record_observations']=99
        _,quality,_=self.evaluate();self.assertFalse(quality['example']['capture_complete'])

    def test_all_offsets_and_actual_empty_tail_are_required(self):
        self.state['requests'].remove(self.older)
        _,quality,_=self.evaluate();self.assertFalse(quality['example']['capture_complete'])
        self.assertIn('required_older_pages_incomplete',quality['example']['reasons'])
        self.state['requests'].append(self.older);self.state['requests'].remove(self.tail)
        _,quality,_=self.evaluate();self.assertFalse(quality['example']['terminal_empty_page_verified'])

    def test_nonempty_tail_after_short_cache_needs_exact_boundary_refresh(self):
        self.replace_body(self.tail,[pr(201,'2024-01-01T00:00:00Z')]);self.tail['terminal_page']=False
        terminal=self.save(4,[],True,'tail');self.checkpoint(terminal)
        _,quality,_=self.evaluate();q=quality['example']
        self.assertFalse(q['capture_complete']);self.assertTrue(q['boundary_refresh_required'])
        refreshed=self.save(2,[pr(n,'2023-01-01T00:00:00Z' if n==101 else '2023-01-02T00:00:00Z') for n in range(101,201)],True,'boundary_refresh')
        self.checkpoint(terminal,refreshed)
        _,quality,_=self.evaluate();q=quality['example']
        self.assertTrue(q['capture_complete']);self.assertTrue(q['visible_offset_crawl_complete'])
        self.assertEqual(q['unique_pr_numbers'],201)
        # An approved refresh may append a missing suffix but cannot certify
        # a changed immutable prefix without marking the disagreement.
        rows=self.body(refreshed);rows[0]=pr(999,'2023-01-01T00:00:00Z');self.replace_body(refreshed,rows)
        self.checkpoint(terminal,refreshed)
        _,quality,_=self.evaluate();self.assertTrue(quality['example']['capture_complete'])
        self.assertFalse(quality['example']['boundary_prefix_preserved'])
        self.assertFalse(quality['example']['visible_offset_crawl_complete'])

    def test_hash_and_frozen_receipt_bindings_are_independent(self):
        path=self.root/self.older['raw_path'];original=path.read_bytes();path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash differs'):self.evaluate()
        path.write_bytes(original);self.index['entries'][0]['record_count']=99;self.rebind()
        with self.assertRaisesRegex(ValueError,'reused receipt differs'):self.evaluate()

    def test_original_boundaries_plan_dates_and_repository_identity_cannot_change(self):
        original=self.state['tools']['example']['start_page'];self.state['tools']['example']['start_page']=1
        with self.assertRaisesRegex(ValueError,'topology differs'):self.evaluate()
        self.state['tools']['example']['start_page']=original
        self.plan['event_window_start']='2022-01-01T00:00:00Z';self.rebind()
        with self.assertRaisesRegex(ValueError,'date/query scope differs'):self.evaluate()

    def test_short_intermediate_page_and_created_order_remain_uncertain(self):
        rows=self.body(self.older);self.replace_body(self.older,rows[:99])
        _,quality,_=self.evaluate();self.assertTrue(quality['example']['capture_complete'])
        self.assertFalse(quality['example']['intermediate_page_sizes_verified'])
        rows[0]['created_at']='2025-01-01T00:00:00Z';rows[0]['closed_at']=rows[0]['merged_at']=None
        self.replace_body(self.older,rows)
        _,quality,_=self.evaluate();self.assertFalse(quality['example']['created_order_monotone'])
        self.assertTrue(quality['example']['visible_offset_crawl_complete'])

    def test_seek_or_search_responses_never_enter_native_history(self):
        request=copy.deepcopy(self.older);request.update(phase='seek',stream='some_other_probe')
        self.state['requests'].append(request)
        rows,quality,diag=self.evaluate()
        self.assertEqual(len(rows),100);self.assertEqual(len(diag['raw_requests_verified']),3)
        self.assertTrue(quality['example']['capture_complete'])


if __name__=='__main__':unittest.main()
