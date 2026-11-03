import copy
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import assemble_full_scope as module


def fingerprint(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class SpineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.cohort='frozen-cohort-sha';self.ids={'a','b'}
        self.base=pd.DataFrame({'tool_id':['a','b','a','b'],
            'week_start':pd.to_datetime(['2023-01-02','2023-01-02','2023-01-09','2023-01-09'],utc=True),
            'primary__dl_downloads':[10,20,11,22]})

    def source(self,kind,rows,status='complete',quality=None,extra=None):
        root=self.root/kind;root.mkdir(exist_ok=True)
        spec=module.SOURCE_SPECS[kind];feature=root/spec['files'][0]
        pd.DataFrame(rows).to_csv(feature,index=False)
        manifest={'source':spec['source'],'registry_sha256':self.cohort,'status':status,
            'history_start':'2023-01-01','history_end_exclusive':'2023-01-16','observed_at':'2026-11-03T04:00:00Z',
            'files':{feature.name:fingerprint(feature)}}
        if spec['quality']:
            qpath=root/spec['quality'][0];qpath.write_text(json.dumps(quality or {'registry_sha256':self.cohort,'status':'passed'}))
            manifest['files'][qpath.name]=fingerprint(qpath)
        if extra:manifest.update(extra)
        (root/spec['manifest'][0]).write_text(json.dumps(manifest))
        return root

    def test_missing_sources_stay_null_without_changing_keys(self):
        out=module.join_source(self.base.copy(),None,'research',{})
        self.assertTrue(out['research__strict_v1_candidate_count'].isna().all())
        self.assertFalse(out['research__record_present'].any())
        self.assertTrue(out[['tool_id','week_start']].equals(self.base[['tool_id','week_start']]))
        self.assertTrue(out['primary__dl_downloads'].equals(self.base['primary__dl_downloads']))

    def test_exact_join_does_not_forward_fill_missing_weeks_or_tools(self):
        frame=pd.DataFrame({'tool_id':['a'],'week_start':pd.to_datetime(['2023-01-02'],utc=True),
                            'count':[0],'record_present':[True],'source_collection_complete':[False],
                            'historical_primary_eligible':[False]})
        out=module.join_source(self.base.copy(),frame,'research',{})
        self.assertEqual(out.loc[0,'research__count'],0)
        self.assertTrue(out.loc[1:,'research__count'].isna().all())
        self.assertEqual(int(out.research__record_present.sum()),1)

    def test_duplicate_unknown_tool_and_non_monday_keys_rejected(self):
        for rows in [[{'tool_id':'a','week_start':'2023-01-02'}]*2,
                     [{'tool_id':'unknown','week_start':'2023-01-02'}],
                     [{'tool_id':'a','week_start':'2023-01-03'}]]:
            with self.subTest(rows=rows),self.assertRaises(ValueError):module.keyed(pd.DataFrame(rows),self.ids)

    def test_hash_cohort_and_source_type_mismatch_rejected(self):
        root=self.source('research',[{'tool_id':'a','week_start':'2023-01-02','seed_v1_retrieval_complete':True,
                                      'historical_feature_eligible':False,'strict_v1_candidate_count':0}])
        path=root/'collection_manifest.json';original=json.loads(path.read_text())
        for key,value in [('registry_sha256','other'),('source','unrelated')]:
            changed=copy.deepcopy(original);changed[key]=value;path.write_text(json.dumps(changed))
            with self.subTest(key=key),self.assertRaises(ValueError):module.inspect_source('research',root,self.cohort,self.ids)
        path.write_text(json.dumps(original));(root/'research_v1_weekly.csv').write_text('changed')
        with self.assertRaisesRegex(ValueError,'hash/path mismatch'):module.inspect_source('research',root,self.cohort,self.ids)

    def test_research_partial_observations_are_retained_without_false_zero_features(self):
        root=self.source('research',[{'tool_id':'a','week_start':'2023-01-02','seed_v1_retrieval_complete':False,
                                      'historical_feature_eligible':False,'strict_v1_candidate_count':2,
                                      'strict_candidates_available_by_assumed_cutoff':2}],status='partial')
        frame,catalog,_=module.inspect_source('research',root,self.cohort,self.ids)
        self.assertTrue(pd.isna(frame.iloc[0].strict_v1_candidate_count))
        self.assertEqual(frame.iloc[0].observed_partial_strict_v1_candidate_count,2)
        self.assertFalse(catalog['collection_complete']);self.assertFalse(frame.historical_primary_eligible.any())

    def test_research_false_primary_promotion_is_rejected(self):
        root=self.source('research',[{'tool_id':'a','week_start':'2023-01-02','seed_v1_retrieval_complete':True,
                                      'historical_feature_eligible':True,'strict_v1_candidate_count':1}])
        with self.assertRaisesRegex(ValueError,'Unreviewed research'):module.inspect_source('research',root,self.cohort,self.ids)

    def test_future_temporal_cutoff_is_rejected(self):
        frame=pd.DataFrame({'week_start':pd.to_datetime(['2023-01-02'],utc=True),'feature_cutoff':['2023-01-12']})
        with self.assertRaisesRegex(ValueError,'boundary differs'):module.validate_temporal_boundaries(frame)

    def test_declared_history_and_observation_windows_are_enforced(self):
        rows=[{'tool_id':'a','week_start':'2023-01-02','seed_v1_retrieval_complete':True,
               'historical_feature_eligible':False,'strict_v1_candidate_count':1}]
        for extra in [{'history_end_exclusive':'2023-01-02'},
                      {'history_start':'2023-01-09'},
                      {'observed_at':'2022-12-31T00:00:00Z'}]:
            root=self.source('research',rows,extra=extra)
            with self.subTest(extra=extra),self.assertRaises(ValueError):
                module.inspect_source('research',root,self.cohort,self.ids)

    def test_dependency_invalid_inbound_does_not_mask_valid_outbound(self):
        root=self.source('dependency',[{'tool_id':'a','week_start':'2023-01-02',
            'dep_inbound_event_time_eligible':False,'dep_outbound_event_time_eligible':True,
            'dep_new_declaring_packages':3,'dep_outbound_unique_dependencies':5}],status='needs_review')
        frame,catalog,_=module.inspect_source('dependency',root,self.cohort,self.ids)
        self.assertTrue(pd.isna(frame.iloc[0].dep_new_declaring_packages))
        self.assertEqual(frame.iloc[0].observed_dep_new_declaring_packages,3)
        self.assertEqual(frame.iloc[0].dep_outbound_unique_dependencies,5)
        self.assertFalse(catalog['historical_primary_eligible'])

    def test_verified_dependency_capture_is_separate_from_feature_review(self):
        root=self.source('dependency',[{'tool_id':'a','week_start':'2023-01-02',
            'dep_inbound_event_time_eligible':False,'dep_outbound_event_time_eligible':True,
            'dep_new_declaring_packages':3,'dep_outbound_unique_dependencies':5}],status='needs_review',extra={'collection_complete':True})
        frame,catalog,_=module.inspect_source('dependency',root,self.cohort,self.ids)
        self.assertTrue(catalog['collection_complete']);self.assertEqual(catalog['collection_completeness_basis'],'verified_capture_flag')
        self.assertTrue(pd.isna(frame.iloc[0].dep_new_declaring_packages));self.assertEqual(frame.iloc[0].observed_dep_new_declaring_packages,3)
        self.assertEqual(frame.iloc[0].dep_outbound_unique_dependencies,5);self.assertFalse(frame.historical_primary_eligible.any())

    def test_collection_capture_flag_cannot_be_a_truthy_string(self):
        root=self.source('dependency',[{'tool_id':'a','week_start':'2023-01-02',
            'dep_inbound_event_time_eligible':False,'dep_outbound_event_time_eligible':True,
            'dep_new_declaring_packages':3,'dep_outbound_unique_dependencies':5}],extra={'collection_complete':'false'})
        with self.assertRaisesRegex(ValueError,'explicit boolean'):module.inspect_source('dependency',root,self.cohort,self.ids)

    def test_contributor_partial_weeks_cannot_join_as_complete_activity(self):
        root=self.source('git',[{'tool_id':'a','week_start_utc':'2023-01-02','complete_utc_week':False,
            'reachable_coverage_days':6,'first_parent_coverage_days':6,'historical_public_availability':'unknown',
            'primary_eligible':False,'git_reachable_author_count':4}])
        frame,_,_=module.inspect_source('git',root,self.cohort,self.ids)
        self.assertTrue(frame.empty)

    def test_contributor_full_week_flag_cannot_hide_clipped_window(self):
        root=self.source('git',[{'tool_id':'a','week_start_utc':'2023-01-02','complete_utc_week':True,
            'window_start_utc':'2023-01-03','window_end_exclusive_utc':'2023-01-09',
            'reachable_coverage_days':7,'first_parent_coverage_days':7,'historical_public_availability':'unknown',
            'primary_eligible':False,'git_reachable_author_count':4}])
        with self.assertRaisesRegex(ValueError,'contradicts actual event window'):
            module.inspect_source('git',root,self.cohort,self.ids)

    def test_download_partial_week_unknown_and_frozen_reference_stays_separate(self):
        root=self.source('downloads',[{'tool_id':'a','week_start':'2023-01-02','rich_week_complete':False,
            'rich_dl_downloads':12,'frozen_primary_downloads':10,'difference_from_frozen_primary':2}],status='partial')
        frame,_,_=module.inspect_source('downloads',root,self.cohort,self.ids)
        self.assertTrue(pd.isna(frame.iloc[0].rich_dl_downloads))
        self.assertEqual(frame.iloc[0].observed_rich_dl_downloads,12)
        out=module.join_source(self.base.copy(),frame,'downloads',{})
        self.assertEqual(out.iloc[0].primary__dl_downloads,10)

    def test_download_completeness_cannot_hide_missing_retrieved_days(self):
        root=self.source('downloads',[{'tool_id':'a','week_start':'2023-01-02','rich_week_complete':True,
            'rich_dl_downloads':12,'download_coverage_days':7,'query_coverage_days':6}])
        with self.assertRaisesRegex(ValueError,'lacks seven retrieved days'):
            module.inspect_source('downloads',root,self.cohort,self.ids)

    def test_hash_bound_quality_supplies_download_interval_and_primary_binding(self):
        quality={'registry_sha256':self.cohort,'status':'passed','start':'2023-01-01',
                 'end_exclusive':'2023-01-16','built_at':'2026-11-03T04:00:00Z',
                 'frozen_primary_manifest_sha256':'primary-reference-sha'}
        root=self.source('downloads',[{'tool_id':'a','week_start':'2023-01-02',
            'rich_week_complete':True,'rich_dl_downloads':12}],quality=quality,
            extra={'history_start':None,'history_end_exclusive':None,'observed_at':None})
        frame,catalog,_=module.inspect_source('downloads',root,self.cohort,self.ids)
        self.assertEqual(catalog['history_end_exclusive'],'2023-01-16')
        self.assertEqual(frame.iloc[0].source_observed_at,quality['built_at'])
        self.assertEqual(catalog['referenced_primary_manifest_sha256'],'primary-reference-sha')

    def test_github_partial_retrieval_is_not_zero_activity(self):
        root=self.source('github_api',[{'tool_id':'a','week_start':'2023-01-02',
            'coverage_complete':False,'historical_visibility_complete':False,
            'github_issue_opened_count':0,'github_pr_opened_count':3}],status='partial')
        frame,catalog,_=module.inspect_source('github_api',root,self.cohort,self.ids)
        self.assertTrue(pd.isna(frame.iloc[0].github_issue_opened_count))
        self.assertTrue(pd.isna(frame.iloc[0].github_pr_opened_count))
        self.assertEqual(frame.iloc[0].observed_partial_github_issue_opened_count,0)
        self.assertEqual(frame.iloc[0].observed_partial_github_pr_opened_count,3)
        self.assertFalse(catalog['historical_primary_eligible'])

    def test_github_individual_complete_stream_survives_global_partial_status(self):
        root=self.source('github_api',[{'tool_id':'a','week_start':'2023-01-02',
            'retrieval_complete':False,'historical_visibility_complete':False,
            'github_issue_opened_count':2,'github_issue_opened_count_complete':True,
            'github_pr_opened_count':None,'github_pr_opened_count_complete':False,
            'observed_partial_github_pr_opened_count':3}],status='partial')
        frame,catalog,_=module.inspect_source('github_api',root,self.cohort,self.ids)
        self.assertEqual(frame.iloc[0].github_issue_opened_count,2)
        self.assertTrue(pd.isna(frame.iloc[0].github_pr_opened_count))
        self.assertEqual(frame.iloc[0].observed_partial_github_pr_opened_count,3)
        self.assertFalse(frame.columns.duplicated().any())
        self.assertFalse(catalog['collection_complete'])

    def test_github_feature_completeness_cannot_certify_missing_count(self):
        root=self.source('github_api',[{'tool_id':'a','week_start':'2023-01-02',
            'retrieval_complete':False,'github_issue_opened_count':None,
            'github_issue_opened_count_complete':True}],status='partial')
        with self.assertRaisesRegex(ValueError,'contradicts a missing count'):
            module.inspect_source('github_api',root,self.cohort,self.ids)

    def test_repository_release_counts_remain_separate_from_package_release_features(self):
        row={'tool_id':'a','week_start':'2023-01-02','retrieval_complete':True,'published_time_complete':True,
            'week_complete':True,'historical_primary_eligible':False,'pypi_release_correspondence_verified':False}
        for name in module.SOURCE_SPECS['releases']['defaults']:row[name]=1;row[name+'_complete']=True
        root=self.source('releases',[row]);frame,catalog,_=module.inspect_source('releases',root,self.cohort,self.ids)
        out=module.join_source(self.base.copy(),frame,'releases',catalog)
        self.assertEqual(out.iloc[0].releases__github_repo_release_published_count,1)
        self.assertEqual(out.iloc[0].primary__dl_downloads,10)
        self.assertFalse(out.releases__historical_primary_eligible.any())
        self.assertEqual(catalog['event_time_field'],'published_at')

    def test_partial_release_counts_keep_lower_bounds_without_exact_zero(self):
        row={'tool_id':'a','week_start':'2023-01-02','retrieval_complete':False,'published_time_complete':True,
            'week_complete':True,'historical_primary_eligible':False,'pypi_release_correspondence_verified':False}
        for name in module.SOURCE_SPECS['releases']['defaults']:row[name]=2;row[name+'_complete']=False
        root=self.source('releases',[row],status='partial');frame,_,_=module.inspect_source('releases',root,self.cohort,self.ids)
        self.assertTrue(frame.github_repo_release_published_count.isna().all())
        self.assertEqual(frame.iloc[0].observed_partial_github_repo_release_published_count,2)

    def test_raw_only_source_is_not_falsely_complete_weekly_data(self):
        root=self.root/'github_api';root.mkdir()
        raw=root/'response.json';raw.write_text('[]')
        manifest={'source':'github_direct_issues_pulls','registry_sha256':self.cohort,'status':'complete',
            'files':{'response.json':fingerprint(raw)}}
        (root/'manifest.json').write_text(json.dumps(manifest))
        frame,catalog,_=module.inspect_source('github_api',root,self.cohort,self.ids)
        self.assertIsNone(frame);self.assertTrue(catalog['raw_retrieval_complete'])
        self.assertFalse(catalog['collection_complete'])
        out=module.join_source(self.base.copy(),frame,'github_api',catalog)
        self.assertTrue(out.github_api__github_issue_opened_count.isna().all())
        self.assertEqual(catalog['missing_grid_rows'],len(self.base))

    def test_source_change_during_load_fails_closed(self):
        root=self.source('research',[{'tool_id':'a','week_start':'2023-01-02','seed_v1_retrieval_complete':True,
                                      'historical_feature_eligible':False,'strict_v1_candidate_count':0}])
        original=pd.read_csv
        def race(path,*args,**kwargs):
            value=original(path,*args,**kwargs)
            manifest=root/'collection_manifest.json';body=json.loads(manifest.read_text());body['updated_at']='changed';manifest.write_text(json.dumps(body))
            return value
        with patch.object(module.pd,'read_csv',side_effect=race),self.assertRaisesRegex(ValueError,'changed while reading'):
            module.inspect_source('research',root,self.cohort,self.ids)

    def test_dictionary_distinguishes_targets_from_allowed_predictors(self):
        frame=self.base.copy();frame['primary__target_log_growth']=[1.,2.,3.,4.];frame['research__strict_v1_candidate_count']=0
        result=module.dictionary(frame,{'dl_downloads'})
        self.assertTrue(result['primary__dl_downloads']['allowed_in_existing_primary_models'])
        self.assertEqual(result['primary__target_log_growth']['role'],'target_or_future_label_not_predictor')
        self.assertFalse(result['research__strict_v1_candidate_count']['allowed_in_existing_primary_models'])

    def test_nonfinite_and_negative_count_values_rejected_but_growth_can_be_negative(self):
        for frame in [pd.DataFrame({'author_count':[-1]}),pd.DataFrame({'author_count':[1.5]}),pd.DataFrame({'author_count':['invalid']}),pd.DataFrame({'growth':[np.inf]})]:
            with self.assertRaises(ValueError):module.validate_values(frame)
        module.validate_values(pd.DataFrame({'author_growth_4w':[-2.]}))


class SupplementalSources(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.ids={'a','b'}
        self.registry=json.dumps({'tools':[{'tool_id':tid} for tid in sorted(self.ids)]}).encode()
        self.cohort=hashlib.sha256(self.registry).hexdigest()

    def source(self,kind,change=None):
        root=self.root/kind;root.mkdir(exist_ok=True)
        (root/'tools.study.json').write_bytes(self.registry)
        raw=b'[{"provider":"retained source evidence"}]';compressed=gzip.compress(raw,mtime=0)
        (root/'raw.json.gz').write_bytes(compressed)
        spec=module.SUPPLEMENT_SPECS[kind]
        if kind=='stars':
            rows=[{'tool_id':'a','api_week_start_epoch':1672531200,'api_week_end_epoch':1673136000,
                   'stars':7,'primary_eligible':False,'historical_vintage':False,
                   **{'day_'+str(i):1 for i in range(7)}}]
        else:
            rows=[{'tool_id':tid,'status':'complete' if tid=='a' else 'provider_missing',
                   'primary_historical_eligible':False,'historical_resolution_vintage':False,
                   'direct_requirement_count':3 if tid=='a' else None,
                   'resolved_nodes':4 if tid=='a' else None,'resolved_edges':3 if tid=='a' else None}
                  for tid in sorted(self.ids)]
        if change:change(rows)
        csv_path=root/spec['csv'];pd.DataFrame(rows).to_csv(csv_path,index=False)
        body={'source':spec['source'],'registry_sha256':self.cohort,'tool_ids':sorted(self.ids),'status':'partial',
              spec['csv_sha']:fingerprint(csv_path),'tools':{'a':{'validation_status':'passed'},'b':{'status':'deferred'}},
              'primary_eligible':False,'historical_vintage':False,'point_in_time_snapshot':False,
              'primary_historical_eligible':False,'historical_resolution_vintage':False,
              'requests':[{'raw_path':'raw.json.gz','raw_sha256':hashlib.sha256(raw).hexdigest(),
                           'compressed_sha256':hashlib.sha256(compressed).hexdigest()}]}
        (root/spec['manifest']).write_text(json.dumps(body))
        if kind=='stars':
            (root/spec['quality']).write_text(json.dumps({'registry_sha256':self.cohort,
                'weekly_csv_sha256':fingerprint(csv_path),'boundary_alignment_certified_utc':False}))
        return root

    def test_sunday_star_buckets_remain_separate_from_utc_panel(self):
        root=self.source('stars')
        catalog,payload,_=module.inspect_supplement('stars',root,self.cohort,self.ids)
        self.assertFalse(catalog['joined_to_utc_panel']);self.assertFalse(catalog['historical_primary_eligible'])
        self.assertEqual(catalog['tools_complete'],1);self.assertFalse(catalog['collection_complete'])
        self.assertIn('supplementary_star_api_calendar.csv',payload)
        self.assertEqual(pd.Timestamp(1672531200,unit='s',tz='UTC').day_of_week,6)

    def collector_star_fixture(self,status='passed',missing_tool=False):
        """Production collector uses 'passed', including its global quality report."""
        ids={'tool'+str(i) for i in range(30)};registry=json.dumps({'tools':[{'tool_id':t} for t in sorted(ids)]}).encode()
        cohort=hashlib.sha256(registry).hexdigest();root=self.root/('collector-'+status+str(missing_tool));root.mkdir()
        (root/'tools.study.json').write_bytes(registry);spec=module.SUPPLEMENT_SPECS['stars']
        passed=sorted(ids)[:-1] if missing_tool else sorted(ids)
        rows=[{'tool_id':tool,'api_week_start_epoch':1672531200,'api_week_end_epoch':1673136000,
            'stars':7,'primary_eligible':False,'historical_vintage':False,**{'day_'+str(i):1 for i in range(7)}} for tool in passed]
        csv_path=root/spec['csv'];pd.DataFrame(rows).to_csv(csv_path,index=False)
        raw=b'[{"provider":"retained star-history response"}]';compressed=gzip.compress(raw,mtime=0);(root/'raw.json.gz').write_bytes(compressed)
        tools={tool:{'status':'passed','validation_status':'passed','identity_verified':True,'required_start_covered':True} for tool in passed}
        if missing_tool:tools[next(iter(ids-set(passed)))]={'status':'deferred'}
        manifest={'source':'github_direct_stars','registry_sha256':cohort,'tool_ids':sorted(ids),'status':status,
            'tools_passed':len(passed),'weekly_rows':len(rows),'tools':tools,'weekly_csv_sha256':fingerprint(csv_path),
            'primary_eligible':False,'historical_vintage':False,'point_in_time_snapshot':False,
            'requests':[{'raw_path':'raw.json.gz','raw_sha256':hashlib.sha256(raw).hexdigest(),'compressed_sha256':hashlib.sha256(compressed).hexdigest()}]}
        (root/spec['manifest']).write_text(json.dumps(manifest))
        quality={'registry_sha256':cohort,'validation_status':status,'weekly_csv_sha256':fingerprint(csv_path),
            'boundary_alignment_certified_utc':False,'primary_eligible':False,'historical_vintage':False}
        (root/spec['quality']).write_text(json.dumps(quality));return root,cohort,ids

    def test_real_collector_passed_terminal_status_with_all30_is_complete(self):
        for status in ['passed','complete']:
            root,cohort,ids=self.collector_star_fixture(status)
            catalog,payload,_=module.inspect_supplement('stars',root,cohort,ids)
            self.assertTrue(catalog['collection_complete']);self.assertEqual(catalog['tools_complete'],30)
            self.assertEqual(catalog['retrieval_status'],'complete_with_source_limits')
            self.assertFalse(catalog['joined_to_utc_panel']);self.assertFalse(catalog['historical_primary_eligible'])

    def test_all30_tool_passes_do_not_promote_a_partial_collection(self):
        root,cohort,ids=self.collector_star_fixture('partial')
        catalog,_,_=module.inspect_supplement('stars',root,cohort,ids)
        self.assertEqual(catalog['tools_complete'],30);self.assertFalse(catalog['collection_complete'])

    def test_passed_terminal_status_cannot_hide_a_missing_tool(self):
        root,cohort,ids=self.collector_star_fixture('passed',missing_tool=True)
        catalog,_,_=module.inspect_supplement('stars',root,cohort,ids)
        self.assertEqual(catalog['tools_complete'],29);self.assertFalse(catalog['collection_complete'])

    def test_star_arithmetic_and_false_historical_flags_fail_closed(self):
        for update in [lambda r:r[0].update(stars=8),lambda r:r[0].update(primary_eligible=True)]:
            root=self.source('stars',update)
            with self.assertRaises(ValueError):module.inspect_supplement('stars',root,self.cohort,self.ids)

    def test_supplemental_raw_response_integrity_is_required(self):
        root=self.source('stars');(root/'raw.json.gz').write_bytes(b'altered')
        with self.assertRaisesRegex(ValueError,'compressed response changed'):
            module.inspect_supplement('stars',root,self.cohort,self.ids)

    def test_depsdev_retains_provider_gap_without_backdating_current_graph(self):
        root=self.source('depsdev')
        catalog,payload,_=module.inspect_supplement('depsdev',root,self.cohort,self.ids)
        self.assertEqual(catalog['source_gap_tools'],['b']);self.assertEqual(catalog['tools_requested'],2)
        self.assertEqual(catalog['tools_complete'],1);self.assertFalse(catalog['joined_to_utc_panel'])
        self.assertFalse(catalog['historical_primary_eligible'])
        rows=pd.read_csv(payload['supplementary_depsdev_current.csv'][0])
        self.assertTrue(pd.isna(rows.loc[rows.tool_id.eq('b'),'resolved_nodes']).all())

    def test_current_graph_cannot_omit_missing_tool_or_claim_history(self):
        for update in [lambda r:r.pop(),lambda r:r[0].update(historical_resolution_vintage=True)]:
            root=self.source('depsdev',update)
            with self.assertRaises(ValueError):module.inspect_supplement('depsdev',root,self.cohort,self.ids)


@unittest.skipUnless(importlib.util.find_spec('pyarrow'),'Full Parquet integration runs on c0')
class FullAssembly(unittest.TestCase):
    def test_full_write_keeps_original_snapshot_immutable_and_gap_deferred(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);primary=root/'primary';primary.mkdir()
            frame=pd.DataFrame({'tool_id':['a','b','a','b'],
                'week_start':pd.to_datetime(['2023-01-02','2023-01-02','2023-01-09','2023-01-09'],utc=True),
                'feature_cutoff':pd.to_datetime(['2023-01-11','2023-01-11','2023-01-18','2023-01-18'],utc=True),
                'dl_downloads':[1,2,3,4]})
            frame.to_parquet(primary/'weekly_features.parquet',index=False)
            (primary/'tools.study.json').write_text(json.dumps({'tools':[{'tool_id':'a'},{'tool_id':'b'}]}))
            cohort=fingerprint(primary/'tools.study.json')
            quality={'validation_status':'passed','content_checks':{'status':'passed'},'cohort_sha256':cohort,
                     'input_hashes':{'weekly_features.parquet':fingerprint(primary/'weekly_features.parquet')}}
            (primary/'data_quality.json').write_text(json.dumps(quality))
            (primary/'study_status.json').write_text(json.dumps({'as_of':'2023-01-17T00:00:00Z'}))
            (primary/'model_config.json').write_text(json.dumps({'feature_groups':{'dl':['dl_downloads'],'gh':[],'research':[]}}))
            manifest={'status':'validated_primary','profile':'validated_primary','registry_sha256':cohort,
                      'files':{p.name:fingerprint(p) for p in primary.iterdir()}}
            (primary/'study_manifest.json').write_text(json.dumps(manifest));before=fingerprint(primary/'study_manifest.json')
            result=module.assemble(primary,root/'out',{},expected_tools=2)
            self.assertEqual(result['status'],'assembled_partial');self.assertEqual(result['rows'],4)
            output=pd.read_parquet(root/'out/full_scope_weekly.parquet')
            self.assertTrue(output['exploratory__attention_adoption_gap'].isna().all())
            self.assertTrue(output['dependency__dep_new_declaring_packages'].isna().all())
            self.assertEqual(fingerprint(primary/'study_manifest.json'),before)
            module.verify_files(root/'out',result['files'])


if __name__=='__main__':unittest.main(verbosity=2)
