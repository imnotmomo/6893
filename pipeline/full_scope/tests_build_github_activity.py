import argparse
from collections import defaultdict
import contextlib,copy,gzip,hashlib,io,json
from pathlib import Path
import tempfile,unittest
from urllib.parse import urlencode

import build_github_activity as builder
from search_plan import plan,STREAMS


START='2023-01-01T00:00:00Z';END='2023-01-16T00:00:00Z'
REGISTRY={'tools':[{'tool_id':'tool','github_repo':'org/repo','github_repo_id':42}]}


def pull(number,created,merged=None,closed=None,author=10):
    return {'id':1000+number,'number':number,'url':'https://api.github.com/repos/org/repo/pulls/'+str(number),
            'base':{'repo':{'id':42,'full_name':'org/repo'}},'created_at':created,'closed_at':closed,
            'merged_at':merged,'user':{'id':author} if author is not None else None,'state':'closed' if closed else 'open'}

def search_pull(row):
    result={k:v for k,v in row.items() if k not in {'base','merged_at'}}
    result['id']=50000+row['number'];result['url']=result['url'].replace('/pulls/','/issues/')
    result['repository_url']='https://api.github.com/repos/org/repo'
    result['pull_request']={'merged_at':row.get('merged_at')}
    return result

def issue(number,created,closed=None,author=10):
    row=search_pull(pull(number,created,closed=closed,author=author));row.pop('pull_request');return row


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.registry_path=self.root/'registry.json';self.registry_path.write_text(json.dumps(REGISTRY))
        self.cohort=builder.sha(self.registry_path)
        self.state={'source':builder.SOURCE,'registry_sha256':self.cohort,'start':START,'end_exclusive':END,
            'started_at':'2023-01-16T00:10:00Z','updated_at':'2023-01-16T01:00:00Z','requests':[],
            'tools':{'tool':{'repo':'org/repo','repo_id':42,'start_page':1,'last_page':1,
                            'boundary_previous_last_created':None,'boundary_first_last_created':'2023-01-09T00:00:00Z'}}}
        self.jobs={job['stream']:job for job in plan(self.state,REGISTRY)['pending']}
        self.old=pull(1,'2022-12-20T00:00:00Z','2023-01-09T00:00:00Z','2023-01-09T00:00:00Z',30)
        self.new=[pull(2,'2023-01-02T00:00:00Z','2023-01-08T23:59:59Z','2023-01-08T23:59:59Z',10),
                  pull(3,'2023-01-09T00:00:00Z',author=20)]
        self.core(self.new)
        self.search('issues_created',[issue(11,'2023-01-02T00:00:00Z'),
            issue(12,'2023-01-08T00:00:00Z','2023-01-09T00:00:00Z',None)])
        self.search('issues_old_closed',[issue(10,'2022-12-20T00:00:00Z','2023-01-09T00:00:00Z')])
        self.search('pulls_old_closed',[search_pull(self.old)])
        self.search('pulls_old_merged',[search_pull(self.old)])
        self.search('pulls_created_census',[search_pull(self.new[0])],total=2)

    def save(self,metadata,body,observed='2023-01-16T01:00:00Z'):
        path=self.root/'raw'/('%04d.json.gz'%len(self.state['requests']));path.parent.mkdir(exist_ok=True)
        raw=json.dumps(body).encode();path.write_bytes(gzip.compress(raw,mtime=0))
        metadata={**metadata,'registry_sha256':self.cohort,'observed_at':observed,'raw_path':str(path.relative_to(self.root)),
            'transport':'github_connector_json_body',
            'raw_sha256':hashlib.sha256(raw).hexdigest(),'compressed_sha256':builder.sha(path),
            'record_count':len(body) if isinstance(body,list) else len(body['items'])}
        self.state['requests'].append(metadata);self.state['updated_at']=max(self.state['updated_at'],observed)
        return metadata

    def core(self,rows,page=1,observed='2023-01-16T01:00:00Z'):
        return self.save({'tool_id':'tool','repo':'org/repo','kind':'pulls','phase':'collect','page':page,'per_page':100,
            'url':'https://api.github.com/repos/org/repo/pulls?'+urlencode({'state':'all','sort':'created','direction':'asc','per_page':100,'page':page})},rows,observed)

    def search(self,stream,items,total=None,page=1,incomplete=False,observed='2023-01-16T01:00:00Z',version=builder.VERSION):
        job=self.jobs[stream];total=len(items) if total is None else total
        metadata={**job,'page':page,'phase':'search_probe' if page==1 else 'search_collect',
            'search_total_count':total,'search_incomplete':incomplete,'query_semantics_version':version,
            'url':'https://api.github.com/search/issues?'+urlencode({'q':job['query'],'sort':'created','order':'asc','per_page':job['per_page'],'page':page})}
        if not metadata['count_only']:metadata.pop('count_only')
        return self.save(metadata,{'total_count':total,'incomplete_results':incomplete,'items':items},observed)

    def evaluate(self):return builder.evaluate(self.root,self.state,REGISTRY)
    def weekly(self):
        records,intervals,core,diagnostics=self.evaluate()
        return builder.weekly_features(records,intervals,core,REGISTRY,START,END),records,diagnostics

    def test_full_census_old_created_merge_and_utc_boundaries(self):
        rows,records,diagnostics=self.weekly();first,second=rows
        self.assertEqual(len(records),6);self.assertEqual(first['week_start'],'2023-01-02')
        self.assertEqual(first['github_pr_opened_count'],1);self.assertEqual(second['github_pr_opened_count'],1)
        self.assertEqual(first['github_pr_merged_count'],1);self.assertEqual(second['github_pr_merged_count'],1)
        self.assertEqual(second['github_issue_last_reported_closed_count'],2)
        self.assertEqual(second['github_pr_last_reported_closed_count'],1)
        self.assertFalse(first['primary_eligible']);self.assertFalse(first['historical_visibility_complete'])
        self.assertTrue(all(c['complete'] for c in diagnostics['leaf_checks'].values()))

    def test_unknown_authors_have_known_lower_bound_and_union_is_not_sum(self):
        rows,_,_=self.weekly();first=rows[0]
        self.assertIsNone(first['github_issue_creation_authors'])
        self.assertEqual(first['observed_partial_github_issue_creation_authors'],1)
        self.assertEqual(first['observed_partial_github_pr_creation_authors'],1)
        self.assertEqual(first['observed_partial_github_creation_authors'],1)
        self.assertIsNone(first['github_creation_authors']);self.assertFalse(first['coverage_complete'])

    def test_shared_ghost_placeholder_is_unknown_not_one_exact_creator(self):
        changed=copy.deepcopy(self.new);changed[0]['user']={'id':10137,'login':'ghost','type':'User'}
        self.core(changed,observed='2023-01-16T02:00:00Z')
        rows,records,_=self.weekly();author=records[('tool',42,2)]
        self.assertEqual(author['author_reported_account_id'],10137);self.assertIsNone(author['author_account_id'])
        self.assertEqual(author['author_identity_status'],'shared_deleted_placeholder')
        self.assertEqual(rows[0]['github_pr_opened_count'],1)
        self.assertIsNone(rows[0]['github_pr_creation_authors'])
        self.assertEqual(rows[0]['observed_partial_github_pr_creation_authors'],0)
        self.assertEqual(rows[0]['github_pr_unknown_creation_author_count'],1)

    def test_direct_pr_id_and_search_issue_id_are_deduplicated_by_number(self):
        self.core([self.old,*self.new],observed='2023-01-16T02:00:00Z')
        records,_,core,_=self.evaluate()
        self.assertEqual(len(records),6);self.assertTrue(core['tool']['created_census_complete'])
        self.assertEqual(records[('tool',42,1)]['evidence_count'],3)

    def test_direct_merge_timestamp_outranks_newer_search_timestamp(self):
        self.core([self.old,*self.new],observed='2023-01-16T02:00:00Z')
        changed=search_pull(self.old);changed['pull_request']['merged_at']='2023-01-08T00:00:00Z'
        self.search('pulls_old_merged',[changed],observed='2023-01-16T03:00:00Z')
        rows,records,_=self.weekly()
        self.assertEqual(records[('tool',42,1)]['merged_at'],'2023-01-09T00:00:00Z')
        self.assertEqual([r['github_pr_merged_count'] for r in rows],[1,1])

    def test_missing_old_merge_detail_keeps_counts_unknown(self):
        changed=search_pull(self.old);changed['pull_request']['merged_at']=None
        self.search('pulls_old_merged',[changed],observed='2023-01-16T02:00:00Z')
        self.search('pulls_old_closed',[changed],observed='2023-01-16T02:00:00Z')
        rows,_,diagnostics=self.weekly()
        self.assertTrue(all(r['github_pr_merged_count'] is None for r in rows))
        self.assertEqual(diagnostics['missing_merge_details'][0]['number'],1)
        self.assertEqual(rows[0]['observed_partial_github_pr_merged_count'],1)

    def test_search_count_mismatch_remains_diagnostic_without_dropping_core_prs(self):
        self.search('pulls_created_census',[search_pull(self.new[0])],total=3,observed='2023-01-16T02:00:00Z')
        rows,_,_=self.weekly()
        self.assertEqual([r['github_pr_opened_count'] for r in rows],[1,1])
        self.assertFalse(self.evaluate()[2]['tool']['search_index_count_agreement'])
        self.assertEqual(sum(r['observed_partial_github_pr_opened_count'] for r in rows),2)

    def test_large_count_only_census_is_not_truncated_to_search_rows(self):
        self.search('pulls_created_census',[search_pull(self.new[0])],total=2048,observed='2023-01-16T02:00:00Z')
        _,_,core,_=self.evaluate();self.assertEqual(core['tool']['expected_created_pr_count'],2048)
        self.assertTrue(core['tool']['created_census_complete']);self.assertFalse(core['tool']['search_index_count_agreement'])

    def test_missing_or_mutating_search_pages_keep_scientific_counts_null(self):
        samples=[issue(100+i,'2023-01-02T00:00:%02dZ'%(i%60)) for i in range(100)]
        self.search('issues_created',samples,total=101,observed='2023-01-16T02:00:00Z')
        rows,_,_=self.weekly();self.assertIsNone(rows[0]['github_issue_opened_count'])
        self.search('issues_created',[issue(300,'2023-01-09T00:00:00Z'),issue(301,'2023-01-09T00:01:00Z')],total=102,page=2,observed='2023-01-16T03:00:00Z')
        rows,_,diagnostics=self.weekly();self.assertIsNone(rows[0]['github_issue_opened_count'])
        reasons=[r for v in diagnostics['leaf_checks'].values() for r in v['reasons']]
        self.assertIn('changing_count_or_incomplete_search',reasons)

    def test_rejected_probe_cannot_override_valid_partition(self):
        self.search('issues_created',[],total=999,version=None,observed='2023-01-16T04:00:00Z')
        rows,_,diagnostics=self.weekly();self.assertEqual(rows[0]['github_issue_opened_count'],2)
        self.assertEqual(diagnostics['rejected_query_probe_count'],1)

    def test_reopened_issue_does_not_invent_a_full_closure_event_log(self):
        older=[issue(11,'2023-01-02T00:00:00Z','2023-01-03T00:00:00Z'),issue(12,'2023-01-08T00:00:00Z','2023-01-09T00:00:00Z',None)]
        self.search('issues_created',older,observed='2023-01-15T01:00:00Z')
        rows,records,_=self.weekly();self.assertIsNone(records[('tool',42,11)]['last_reported_closed_at'])
        self.assertEqual(rows[0]['github_issue_last_reported_closed_count'],0)

    def test_changed_raw_bytes_fail_before_publication(self):
        request=self.state['requests'][0];(self.root/request['raw_path']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash differs'):self.evaluate()

    def test_fractional_observation_time_selects_the_newest_fields(self):
        changed=[issue(11,'2023-01-02T00:00:00Z',author=99),
                 issue(12,'2023-01-08T00:00:00Z','2023-01-09T00:00:00Z',None)]
        self.search('issues_created',changed,observed='2023-01-16T01:00:00.500Z')
        rows,records,_=self.weekly()
        self.assertEqual(records[('tool',42,11)]['author_account_id'],99)
        self.assertEqual(rows[0]['observed_partial_github_creation_authors'],2)

    def test_missing_direct_merge_is_not_overridden_by_search(self):
        direct=copy.deepcopy(self.old);direct['merged_at']=None
        self.core([direct,*self.new],observed='2023-01-16T02:00:00Z')
        rows,records,diagnostics=self.weekly()
        self.assertIsNone(records[('tool',42,1)]['merged_at'])
        self.assertTrue(all(row['github_pr_merged_count'] is None for row in rows))
        self.assertEqual(diagnostics['missing_merge_details'][0]['number'],1)

    def test_issue_closure_requires_creation_coverage_before_its_week(self):
        records,intervals,core,_=self.evaluate()
        intervals[('tool','issues_created')]=[(builder.utc('2023-01-09T00:00:00Z'),builder.utc(END))]
        rows=builder.weekly_features(records,intervals,core,REGISTRY,START,END)
        self.assertEqual(rows[1]['github_issue_opened_count'],0)
        self.assertIsNone(rows[1]['github_issue_last_reported_closed_count'])

    def test_repo_identity_and_transport_mismatch_are_rejected(self):
        changed=copy.deepcopy(self.new);changed[0]['base']['repo']['id']=777
        self.core(changed,observed='2023-01-16T02:00:00Z')
        with self.assertRaisesRegex(ValueError,'numeric ID differs'):self.evaluate()
        self.state['requests'].pop()
        self.state['requests'][0]['transport']='separate_urllib_archive'
        with self.assertRaisesRegex(ValueError,'Do not mix'):self.evaluate()

    def test_nonfirst_core_page_needs_a_hash_verified_ordinal_boundary_probe(self):
        self.state['requests'][0]['phase']='retained_unselected_probe'
        self.state['tools']['tool'].update(start_page=2,last_page=2,boundary_previous_last_created='2022-12-20T00:00:00Z')
        self.core(self.new,page=2)
        self.assertFalse(self.evaluate()[2]['tool']['created_census_complete'])
        probe=self.save({'tool_id':'tool','repo':'org/repo','kind':'pulls','phase':'seek','page':100,'per_page':1,
            'url':'https://api.github.com/repos/org/repo/pulls?'+urlencode({'state':'all','sort':'created','direction':'asc','per_page':1,'page':100})},[self.old])
        quality=self.evaluate()[2]['tool'];self.assertTrue(quality['created_census_complete']);self.assertEqual(quality['boundary_raw_path'],probe['raw_path'])
        (self.root/probe['raw_path']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash differs'):self.evaluate()

    def test_partial_calendar_intersections_are_retained_separately(self):
        records,intervals,core,_=self.evaluate()
        rows=builder.weekly_features(records,intervals,core,REGISTRY,START,'2023-01-10T00:00:00Z',True)
        self.assertEqual(len(rows),3)
        self.assertFalse(rows[0]['week_complete']);self.assertEqual(rows[0]['effective_window_start'],START)
        self.assertFalse(rows[-1]['week_complete']);self.assertEqual(rows[-1]['effective_window_end_exclusive'],'2023-01-10T00:00:00Z')
        self.assertTrue(rows[1]['week_complete'])

    def test_snapshot_build_hash_binds_all_outputs_and_keeps_primary_false(self):
        snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state))
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,
            output=self.root/'output',max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):builder.build(args)
        manifest=json.loads((args.output/'manifest.json').read_text());quality=json.loads((args.output/'source_quality.json').read_text())
        self.assertEqual(manifest['status'],'partial');self.assertEqual(quality['weekly_rows'],2)
        # Search-only legacy archives remain readable but cannot certify an issue census.
        self.assertFalse(quality['collection_complete'])
        self.assertEqual(quality['partial_calendar_week_rows'],1);self.assertFalse(quality['primary_eligible'])
        for name,digest in manifest['files'].items():self.assertEqual(builder.sha(args.output/name),digest)


class TransportMetadata(unittest.TestCase):
    def test_only_reported_safe_metadata_is_described(self):
        legacy={'transport':'github_connector_json_body'}
        jsonp={'transport':'github_connector_jsonp_envelope','safe_meta':{'status':200,'ETag':'receipt-etag','Link':[]}}
        result=builder.transport_summary([legacy,jsonp])
        self.assertEqual(result['transport'],'mixed_connector_profiles')
        self.assertEqual(result['safe_metadata_receipt_counts'],{'etag':1,'link':1,'status':1})
        self.assertEqual(result['api_version'],'unreported');self.assertNotIn('x-ratelimit-remaining',result['safe_metadata_receipt_counts'])
        self.assertEqual(builder.transport_summary([legacy])['etag_and_rate_headers'],'unreported')

if __name__=='__main__':unittest.main(verbosity=2)
