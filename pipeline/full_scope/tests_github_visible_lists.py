import argparse,contextlib,copy,csv,gzip,hashlib,io,json
from pathlib import Path
import unittest
from urllib.parse import urlencode
from unittest.mock import patch
import build_github_activity as b
from github_visible_cursor import cursor_for,verify_receipts
import tests_build_github_activity as legacy
from tests_build_github_activity import REGISTRY,START,END,issue,search_pull

SINCE='2022-12-31T23:59:59Z'


class VisibleLists(unittest.TestCase):
    setUp=legacy.Fixture.setUp
    save=legacy.Fixture.save
    core=legacy.Fixture.core
    search=legacy.Fixture.search
    evaluate=legacy.Fixture.evaluate
    def proof(self):
        def raw(name,rows):
            path=self.root/name;path.parent.mkdir(parents=True,exist_ok=True)
            body=json.dumps(rows).encode();path.write_bytes(gzip.compress(body,mtime=0))
            return {'raw_path':name,'raw_sha256':hashlib.sha256(body).hexdigest(),'compressed_sha256':b.sha(path)}
        def doc(name,value):
            p=self.root/name;p.write_text(json.dumps(value));return b.sha(p)
        first=issue(90,'2022-01-01T00:00:00Z');first['updated_at']='2023-01-02T00:00:00Z'
        following=issue(91,'2022-01-01T00:00:00Z');following['updated_at']='2023-01-03T00:00:00Z'
        first_raw=raw('proof-first.json.gz',[first]);resume=raw('proof-resume.json.gz',[following]);anchor={k:first[k] for k in ['id','number','updated_at']}
        params={'state':'all','since':SINCE,'sort':'updated','direction':'asc','per_page':100};url='https://api.github.com/repos/org/repo/issues?'+urlencode(params)
        token=cursor_for(anchor);link='https://api.github.com/repositories/42/issues?'+urlencode({**params,'after':token,'page':2})
        receipt={'url':url,'headers':{'Link':'<'+link+'>; rel="next"'},**first_raw};receipt_sha=doc('provider.json',receipt)
        p={'provider_receipt_path':'provider.json','provider_receipt_sha256':receipt_sha,'provider_first_page_path':first_raw['raw_path'],
           'anchor':anchor,'provider_cursor':token,'generated_cursor':token,'provider_link':link,
           'connector_request_url':url+'&'+urlencode({'after':token,'page':2}),
           'cursor_only_resume_url':url+'&'+urlencode({'after':token}),
           'resume_first':following,'resume_last':following}
        for prefix in ['connector_resume','connector_cursor_only_resume']:
            p.update({prefix+'_'+k:v for k,v in resume.items()})
        proof_sha=doc('proof.json',p)
        identity={**self.state['requests'][0],'github_repo':'org/repo','expected_repo_id':42,'observed_repo_id':42,'method':'direct_PR_base_repo_id'}
        identity_sha=doc('identity.json',{'registry_sha256':self.cohort,'tools':{'tool':identity}})
        return {'cursor_conformance_path':'proof.json','cursor_conformance_receipt_sha256':proof_sha,
                'repo_identity_proof_path':'identity.json','repo_identity_proof_sha256':identity_sha}

    def visible(self,rows=None,terminal=True,pages=None,headers=True):
        if rows is None:
            rows=[search_pull(self.old),*[search_pull(p) for p in self.new],issue(10,'2022-12-20T00:00:00Z','2023-01-09T00:00:00Z'),
                  issue(11,'2023-01-02T00:00:00Z'),issue(12,'2023-01-08T00:00:00Z','2023-01-09T00:00:00Z',None)]
        for index,row in enumerate(rows):row.setdefault('updated_at','2023-01-10T00:00:%02dZ'%index)
        pages=[rows] if pages is None else pages
        if terminal:pages=[*pages,[]]
        refs=self.proof();anchor=None;token=None;requests=[]
        for i,body in enumerate(pages,1):
            if body:anchor={k:body[-1][k] for k in ['id','number','updated_at']}
            out=cursor_for(anchor);params={'state':'all','since':SINCE,'sort':'updated','direction':'asc','per_page':100}
            if token:params['after']=token
            r=self.save({**refs,'tool_id':'tool','github_repo_id':42,'repo':'org/repo','kind':'issues','phase':'issues_list','stream':'issues_visible',
                'page':i,'per_page':100,'since':SINCE,'sort':'updated','direction':'asc','cursor_in':token,'cursor_out':out,
                'last_anchor':anchor,'terminal_page':not body,'cursor_encoding':'observed_cursor_v2_msgpack_updated_at_issue_id',
                'request_started_at':'2023-01-16T02:%02d:00Z'%i,
                'url':'https://api.github.com/repos/org/repo/issues?'+urlencode(params)},body,'2023-01-16T02:%02d:01Z'%i)
            if headers:
                receipt={'request_url':r['url'],'status':200,'raw_sha256':r['raw_sha256'],'compressed_sha256':r['compressed_sha256'],
                    'headers':{'Link':'<https://api.github.com/repos/org/repo/issues?'+urlencode({**params,'after':out})+'>; rel="next"'} if body else {}}
                path=self.root/('http-%04d.json'%len(self.state['requests']));path.write_text(json.dumps(receipt))
                r.update(provider_link_receipt_path=path.name,provider_link_receipt_sha256=b.sha(path))
            token=out;requests.append(r)
        last=requests[-1]
        self.state['tools']['tool']['issues_visible']={**refs,'github_repo_id':42,'since':SINCE,'sort':'updated','direction':'asc','per_page':100,
            'status':'complete' if terminal else 'collecting','pages_saved':len(requests),'record_observations':sum(map(len,pages)),
            'next_page':len(requests)+1,'cursor_out':token,'last_anchor':anchor,'last_receipt_raw_path':last['raw_path']}
        if terminal:self.state['tools']['tool']['issues_visible']['terminal_raw_path']=last['raw_path']
        return requests

    def strict(self):
        records,intervals,core,diag=self.evaluate()
        return b.weekly_features(records,intervals,core,REGISTRY,START,END,visible_quality=diag['visible_list_quality']),records,diag

    def test_exhaustive_list_uses_published_events_and_old_objects(self):
        self.visible();rows,records,diag=self.strict();q=diag['visible_list_quality']['tool']
        self.assertTrue(q['visible_list_pagination_complete']);self.assertTrue(q['core_pr_created_id_agreement'])
        self.assertEqual([r['github_issue_opened_count'] for r in rows],[2,0]);self.assertEqual([r['github_pr_merged_count'] for r in rows],[1,1])
        self.assertEqual(rows[1]['github_issue_last_reported_closed_count'],2)
        self.assertFalse(rows[0]['primary_eligible']);self.assertIsNone(rows[0]['github_issue_creation_authors'])
        self.assertEqual(set(records[('tool',42,2)]['_view_records']),{'core_pr','issues_visible'})

    def test_search_omission_does_not_remove_real_get_issue(self):
        req=self.visible();body=b.raw_body(self.root.resolve(),req[0],self.cohort,64*1024**2)
        extra=issue(13,'2023-01-03T00:00:00Z');extra['updated_at']='2023-01-10T00:00:09Z';body.append(extra)
        self.state['requests']=[r for r in self.state['requests'] if r not in req];self.visible(body)
        rows,records,diag=self.strict();q=diag['visible_list_quality']['tool']
        self.assertEqual(rows[0]['github_issue_opened_count'],3);self.assertIn(('tool',42,13),records)
        self.assertFalse(q['search_index_created_issue_id_agreement']);self.assertEqual(q['visible_issue_ids_absent_from_search'],[13])

    def test_independent_pr_view_disagreement_masks_hybrid_without_erasing_core_creation(self):
        self.visible([search_pull(self.new[0]),issue(11,'2023-01-02T00:00:00Z')])
        rows,records,diag=self.strict();self.assertEqual(len(records),6)
        self.assertEqual([r['github_pr_opened_count'] for r in rows],[1,1]);self.assertEqual(rows[0]['github_issue_opened_count'],1)
        self.assertTrue(all(r['github_pr_merged_count'] is None for r in rows));self.assertTrue(rows[0]['github_pr_creation_authors_complete'])
        self.assertTrue(all(r['github_creation_authors'] is None for r in rows))
        self.assertEqual(diag['visible_list_quality']['tool']['only_core_created_pr_numbers'],[3])
        self.assertEqual(sum(r['observed_partial_github_pr_opened_count'] for r in rows),2)

    def test_short_page_is_not_terminal_and_old_merge_remains_unknown(self):
        self.visible(terminal=False);rows,_,diag=self.strict()
        self.assertFalse(diag['visible_list_quality']['tool']['visible_list_pagination_complete'])
        self.assertIn('no_actual_empty_terminal_page',diag['visible_list_quality']['tool']['reasons'])
        self.assertIsNone(rows[0]['github_issue_opened_count']);self.assertEqual(rows[0]['github_pr_opened_count'],1)
        self.assertTrue(all(r['github_pr_merged_count'] is None for r in rows))

    def test_cursor_chain_tamper_and_gap_fail_closed(self):
        req=self.visible();req[-1]['cursor_in']='incorrect'
        rows,_,diag=self.strict();self.assertFalse(diag['visible_list_quality']['tool']['visible_list_pagination_complete'])
        self.assertTrue(all(r['github_issue_opened_count'] is None for r in rows))
        req[-1]['page']=3
        self.assertIn('missing_visible_pages',self.strict()[2]['visible_list_quality']['tool']['reasons'])

    def test_tied_timestamps_allow_lower_ids_but_repeated_boundary_fails(self):
        a=issue(99,'2023-01-02T00:00:00Z');z=issue(98,'2023-01-02T00:00:00Z')
        for r in [a,z]:r['updated_at']='2023-01-10T00:00:00Z'
        self.visible([a,z],pages=[[a],[z]])
        self.assertTrue(self.strict()[2]['visible_list_quality']['tool']['visible_list_pagination_complete'])
        self.state['requests']=[r for r in self.state['requests'] if r.get('phase')!='issues_list']
        self.visible([a,a],pages=[[a],[a]])
        self.assertFalse(self.strict()[2]['visible_list_quality']['tool']['visible_list_pagination_complete'])

    def test_provider_proof_or_numeric_identity_tamper_is_fatal(self):
        req=self.visible();path=self.root/'provider.json';p=json.loads(path.read_text());p['headers']['Link']=p['headers']['Link'].replace('repositories/42','repositories/77');path.write_text(json.dumps(p))
        with self.assertRaisesRegex(ValueError,'proof hash differs'):self.strict()

    def test_explicit_path_correction_binds_original_receipt_without_mutating_it(self):
        req=self.visible();correct=req[0]['cursor_conformance_path'];req[0]['cursor_conformance_path']='wrong-path.json'
        correction={'raw_path':req[0]['raw_path'],'raw_sha256':req[0]['raw_sha256'],'tool_id':'tool','page':1,
            'original_observed_at':req[0]['observed_at'],'original_path':'wrong-path.json','original_sha256':req[0]['cursor_conformance_receipt_sha256'],
            'corrected_path':correct,'corrected_sha256':req[0]['cursor_conformance_receipt_sha256']}
        with self.assertRaisesRegex(ValueError,'proof path'):self.strict()
        path=self.root/'corrections.json';path.write_text(json.dumps({'correction_version':1,'corrections':[correction]}))
        self.state['proof_reference_corrections']={'path':path.name,'sha256':b.sha(path)}
        self.assertTrue(self.strict()[2]['visible_list_quality']['tool']['visible_list_pagination_complete'])
        self.assertEqual(req[0]['cursor_conformance_path'],'wrong-path.json')
        correction['raw_sha256']='0'*64;path.write_text(json.dumps({'correction_version':1,'corrections':[correction]}));self.state['proof_reference_corrections']['sha256']=b.sha(path)
        with self.assertRaisesRegex(ValueError,'does not bind'):self.strict()

    def test_missing_merge_field_or_conflicting_api_timestamp_is_unknown(self):
        req=self.visible();body=b.raw_body(self.root.resolve(),req[0],self.cohort,64*1024**2)
        body[0]['pull_request'].pop('merged_at')
        self.state['requests']=[r for r in self.state['requests'] if r not in req];self.visible(body)
        self.assertTrue(all(r['github_pr_merged_count'] is None for r in self.strict()[0]))

    def test_newer_search_cannot_replace_authoritative_current_fields(self):
        self.visible();self.search('issues_created',[issue(11,'2023-01-02T00:00:00Z',author=99),issue(12,'2023-01-08T00:00:00Z','2023-01-09T00:00:00Z',None)],observed='2023-01-16T03:00:00Z')
        rows,records,_=self.strict();self.assertEqual(records[('tool',42,11)]['author_account_id'],10)
        self.assertEqual(records[('tool',42,11)]['_view_records']['search_index']['author_account_id'],99)

    def test_body_derived_method_is_disqualified_even_after_true_empty_terminal(self):
        self.visible(headers=False);rows,_,diag=self.strict()
        self.assertFalse(diag['visible_list_quality']['tool']['visible_list_pagination_complete'])
        self.assertIn('body_derived_cursor_method_disqualified',diag['visible_list_quality']['tool']['reasons'])
        self.assertTrue(all(row['github_issue_opened_count'] is None for row in rows))
        # Actual Flax issue3705 disproved the initial finite matching probes.
        last={'id':2147757991,'updated_at':'2026-04-06T17:02:41Z'}
        self.assertEqual(cursor_for(last),'Y3Vyc29yOnYyOpLPAAABnWO/F2jOgAQvpw==')
        self.assertNotEqual(cursor_for(last),'Y3Vyc29yOnYyOpLPAAABjdeEghDOgAQvpw==')

    def test_actual_opaque_link_is_used_instead_of_serialized_timestamp_key(self):
        req=self.visible();opaque='actual-provider-sort-key'
        first_path=self.root/req[0]['provider_link_receipt_path'];receipt=json.loads(first_path.read_text())
        params={'state':'all','since':SINCE,'sort':'updated','direction':'asc','per_page':100,'after':opaque}
        receipt['headers']['Link']='<https://api.github.com/repositories/42/issues?'+urlencode(params)+'>; rel="next"'
        first_path.write_text(json.dumps(receipt));req[0]['provider_link_receipt_sha256']=b.sha(first_path);req[0]['cursor_out']=opaque
        req[1]['cursor_in']=opaque;req[1]['cursor_out']=opaque;req[1]['url']='https://api.github.com/repos/org/repo/issues?'+urlencode(params)
        last_path=self.root/req[1]['provider_link_receipt_path'];last=json.loads(last_path.read_text());last['request_url']=req[1]['url'];last_path.write_text(json.dumps(last));req[1]['provider_link_receipt_sha256']=b.sha(last_path)
        self.state['tools']['tool']['issues_visible']['cursor_out']=opaque
        self.assertTrue(self.strict()[2]['visible_list_quality']['tool']['visible_list_pagination_complete'])

    def native_rows(self,complete):
        records,intervals,core,diag=self.evaluate()
        item=b.identity(self.old,REGISTRY['tools'][0],{'observed_at':'2023-01-16T03:00:00Z','raw_path':'native-old-pr.json.gz'},True)
        item.update(source_view='core_pr_history',source_stream='core_pr_history');b.merge_record(records,item,1000)
        history={'tool':{'capture_complete':complete,'merge_count_eligible':complete,'last_reported_closed_count_eligible':complete}}
        rows=b.weekly_features(records,intervals,core,REGISTRY,START,END,visible_quality=diag['visible_list_quality'],native_pr_history_quality=history)
        return rows,records

    def test_complete_native_history_supplies_old_created_events_without_issue_agreement(self):
        rows,records=self.native_rows(True)
        self.assertEqual([r['github_pr_opened_count'] for r in rows],[1,1])
        self.assertEqual([r['github_pr_merged_count'] for r in rows],[1,1])
        self.assertEqual([r['github_pr_last_reported_closed_count'] for r in rows],[1,1])
        self.assertTrue(all(r['github_issue_opened_count'] is None for r in rows))
        self.assertTrue(all(r['pr_merge_measurement_view']=='native_pr_all_history' for r in rows))
        self.assertIn('core_pr_history',records[('tool',42,1)]['_view_records'])
        self.assertEqual(records[('tool',42,1)]['pr_created_at'],'2022-12-20T00:00:00Z')
        self.assertFalse(rows[0]['primary_eligible'])

    def test_partial_native_history_preserves_core_creation_and_unknown_event_coverage(self):
        rows,_=self.native_rows(False)
        self.assertEqual([r['github_pr_opened_count'] for r in rows],[1,1])
        self.assertTrue(all(r['github_pr_merged_count'] is None for r in rows))
        self.assertEqual(sum(r['observed_partial_github_pr_merged_count'] for r in rows),2)
        self.assertTrue(all(not r['native_pr_history_capture_complete'] for r in rows))

    def test_capture_completion_is_not_erased_by_hybrid_review_flags(self):
        self.visible([search_pull(self.new[0]),issue(11,'2023-01-02T00:00:00Z')])
        snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state));out=self.root/'captured'
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=out,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        manifest=json.loads((out/'manifest.json').read_text());quality=json.loads((out/'source_quality.json').read_text())
        self.assertTrue(manifest['collection_complete']);self.assertTrue(quality['collection_complete']);self.assertEqual(manifest['status'],'needs_review')
        self.assertFalse(quality['primary_eligible'])

    def test_pr_and_issue_creation_clocks_remain_distinct_across_week_boundary(self):
        direct=copy.deepcopy(self.new);direct[0].update(created_at='2023-01-08T23:59:59Z',merged_at='2023-01-10T00:00:00Z',closed_at='2023-01-10T00:00:00Z')
        self.core(direct,observed='2023-01-16T02:00:00Z')
        api=[search_pull(p) for p in direct];api[0]['created_at']='2023-01-09T00:00:00Z'
        self.visible(api);rows,records,diag=self.strict();record=records[('tool',42,2)]
        self.assertEqual(record['pr_created_at'],'2023-01-08T23:59:59Z')
        self.assertEqual(record['issue_created_at'],'2023-01-09T00:00:00Z')
        self.assertEqual([r['github_pr_opened_count'] for r in rows],[1,1])
        self.assertTrue(diag['visible_list_quality']['tool']['core_pr_created_id_agreement'])
        self.assertEqual(len(diag['visible_list_quality']['tool']['issue_vs_pr_creation_timestamp_differences']),1)

    def test_frozen_build_binds_proof_provenance_and_all_source_views(self):
        self.visible();snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state));out=self.root/'output'
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=out,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        quality=json.loads((out/'source_quality.json').read_text());self.assertTrue(quality['collection_complete'])
        self.assertIn('provider.json',quality['diagnostics']['proof_file_hashes'])
        with gzip.open(out/'normalized_records.jsonl.gz','rt') as f:records=[json.loads(line) for line in f]
        self.assertTrue(all('source_view_observations' in r for r in records))
        self.assertFalse(quality['historical_visibility_complete'])


if __name__=='__main__':unittest.main(verbosity=2)
