"""Fail-closed publication and dashboard tests; no cloud calls or model fitting."""
import ast
import copy
import importlib.util
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import unittest

import pandas as pd


ROOT=Path(__file__).resolve().parent
APP_PATH=ROOT/'dashboard/app.py'
if not APP_PATH.is_file():APP_PATH=ROOT.parent/'dashboard/app.py'
spec=importlib.util.spec_from_file_location('publisher',ROOT/'publish_study.py')
publisher=importlib.util.module_from_spec(spec);spec.loader.exec_module(publisher)


def readiness_function():
    # Load the real pure gate without importing Streamlit or running the app.
    tree=ast.parse(APP_PATH.read_text())
    statements=[node for node in tree.body if
                isinstance(node,ast.FunctionDef) and node.name=='primary_ready' or
                isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='PRIMARY_MODELS' for t in node.targets)]
    namespace={};exec(compile(ast.Module(body=statements,type_ignores=[]),'app.py','exec'),namespace)
    return namespace['primary_ready']


class PublicationQuality(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.run=self.root/'runs/new';self.run.mkdir(parents=True)
        self.asof=publisher.utc('2026-11-02T20:59:20Z')
        for name in ['tools.study.json','daily_normalized.parquet','weekly_features.parquet','source_quality.json']:
            (self.run/name).write_bytes(('immutable fixture: '+name).encode())
        self.quality={
            'profile':'validated_primary','validation_status':'passed','source_policy':'capture_quality_v1',
            'content_checks':{'status':'passed'},'raw_spot_checks':{'status':'passed'},'enabled_feature_groups':['dl','release','controls'],
            'as_of':self.asof.isoformat(),'cohort_sha256':publisher.sha256(self.run/'tools.study.json'),
            'source_policy_sha256':publisher.sha256(self.run/'source_quality.json'),
            'input_hashes':{name:publisher.sha256(self.run/name) for name in ['daily_normalized.parquet','weekly_features.parquet','source_quality.json']},
            'excluded_feature_groups':['gh','research'],
            'sources':{'pypi':{'status':'primary_enabled'},'pypi_releases':{'status':'primary_enabled'},
                       'gharchive':{'status':'capture_limited','primary_eligible':False,'capture_validated':False},'arxiv':{'status':'quarantined'}}}
        self.save_quality()

    def save_quality(self):
        (self.run/'data_quality.json').write_text(json.dumps(self.quality))

    def test_enabled_sources_pass_with_gh_and_research_excluded(self):
        self.assertEqual(publisher.validate_source_quality(self.run,self.asof)['validation_status'],'passed')

    def test_retrieval_complete_cannot_replace_semantic_validation(self):
        self.quality['validation_status']='retrieval_complete';self.save_quality()
        with self.assertRaisesRegex(ValueError,'semantic'):publisher.validate_source_quality(self.run,self.asof)

    def test_changed_features_invalidate_quality_proof(self):
        (self.run/'weekly_features.parquet').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'different weekly_features'):publisher.validate_source_quality(self.run,self.asof)

    def test_cannot_enable_capture_limited_gh_or_quarantined_research(self):
        for source,field,value in [('gharchive','primary_eligible',True),('arxiv','status','primary_enabled')]:
            original=copy.deepcopy(self.quality)
            self.quality['sources'][source][field]=value;self.save_quality()
            with self.assertRaises(ValueError):publisher.validate_source_quality(self.run,self.asof)
            self.quality=original

    def test_wrong_cohort_and_endpoint_fail(self):
        original=copy.deepcopy(self.quality)
        for key,value in [('cohort_sha256','wrong'),('as_of','2026-11-03T20:59:20Z')]:
            self.quality=copy.deepcopy(original);self.quality[key]=value;self.save_quality()
            with self.assertRaises(ValueError):publisher.validate_source_quality(self.run,self.asof)

    def test_correction_keeps_predecessor_immutable_and_latest_on_failure(self):
        previous=self.root/'runs/old';previous.mkdir();(previous/'study_manifest.json').write_text('{"status":"validated"}')
        before=publisher.sha256(previous/'study_manifest.json')
        (self.root/'latest').symlink_to('runs/old',target_is_directory=True)
        lineage=publisher.previous_snapshot(self.root)
        self.assertEqual(lineage['manifest_sha256'],before)
        self.quality['validation_status']='failed';self.save_quality()
        with self.assertRaises(ValueError):publisher.validate_source_quality(self.run,self.asof)
        self.assertEqual((self.root/'latest').resolve(),previous.resolve())
        self.assertEqual(publisher.sha256(previous/'study_manifest.json'),before)
        publisher.atomic_latest(self.root,self.run)
        self.assertEqual((self.root/'latest').resolve(),self.run.resolve())
        self.assertEqual(publisher.sha256(previous/'study_manifest.json'),before)


class DashboardReadiness(unittest.TestCase):
    def setUp(self):
        self.ready=readiness_function()
        self.status={'primary_analysis_ready':True,'publication_status':'validated_primary',
                     'spark_validation':{'ok':True,'mismatch_rows':0}}
        self.metrics={'profile':'validated_primary','best_model':'training_median','analysis_type':'post_audit_corrective_sensitivity',
                      'excluded_feature_groups':['gh','research']}
        self.quality={'profile':'validated_primary','validation_status':'passed',
                      'sources':{'pypi':{'status':'primary_enabled'},'pypi_releases':{'status':'primary_enabled'},
                                 'gharchive':{'primary_eligible':False}}}

    def test_old_retrieval_only_snapshot_is_hidden(self):
        self.assertFalse(self.ready({'publication_status':'validated'}, {}, {}))

    def test_current_complete_snapshot_can_show_baseline_winner(self):
        self.assertTrue(self.ready(self.status,self.metrics,self.quality))

    def test_pre_spark_partial_snapshot_is_hidden(self):
        self.status['spark_validation']['ok']=False
        self.assertFalse(self.ready(self.status,self.metrics,self.quality))

    def test_source_failure_or_old_gh_winner_is_hidden(self):
        self.metrics['best_model']='ridge_gh'
        self.assertFalse(self.ready(self.status,self.metrics,self.quality))
        self.metrics['best_model']='ridge_dl';self.quality['validation_status']='failed'
        self.assertFalse(self.ready(self.status,self.metrics,self.quality))


class SupplementarySources(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve()
        tree=ast.parse(APP_PATH.read_text())
        names={'_signature','_read_supplementary','load_supplementary'}
        statements=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names or
                    isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SUPPLEMENTARY_SOURCES' for t in node.targets)]
        self.ns={'Path':Path,'pd':pd,'hashlib':hashlib,'json':json,'re':re,'io':io}
        exec(compile(ast.Module(body=statements,type_ignores=[]),'app.py','exec'),self.ns)
        self.ns['_cached_supplementary']=self.ns['_read_supplementary']

    def fixture(self,kind='git',partial=False,eligible=False):
        relative,manifest_name,csv_name=self.ns['SUPPLEMENTARY_SOURCES'][kind]
        root=self.base/relative;root.mkdir(parents=True)
        if kind=='git':
            frame=pd.DataFrame([{'tool_id':'example','week_start_utc':'2026-10-26',
                                 'git_reachable_commit_count':5,'git_first_parent_commit_count':2,
                                 'primary_eligible':eligible}])
            manifest={'source':'github_git_default_branch','status':'complete','requested_tools':1,'completed_tools':1,
                      'tools':[{'tool_id':'example','status':'complete'}]}
        else:
            frame=pd.DataFrame([{'tool_id':'example','api_week_start_epoch':1792886400,'stars':3,
                                 'historical_vintage':False,'primary_eligible':eligible}])
            manifest={'source':'github_direct_stars','status':'partial' if partial else 'passed',
                      'tool_ids':['example','pending'] if partial else ['example'],'tools_passed':1,
                      'tools':{'example':{'status':'passed'}}}
        raw=frame.to_csv(index=False).encode();(root/csv_name).write_bytes(raw)
        sha=hashlib.sha256(raw).hexdigest()
        if kind=='git':manifest['files']={csv_name:sha}
        else:manifest['weekly_csv_sha256']=sha
        (root/manifest_name).write_text(json.dumps(manifest))
        return root,csv_name

    def test_valid_git_and_partial_stars_keep_separate_coverage(self):
        self.fixture();self.fixture('stars',partial=True)
        git=self.ns['load_supplementary'](self.base,'git')
        stars=self.ns['load_supplementary'](self.base,'stars')
        self.assertEqual(git['state'],'ready');self.assertFalse(git['partial'])
        self.assertEqual(stars['state'],'ready');self.assertTrue(stars['partial'])
        self.assertEqual((stars['completed'],stars['requested']),(1,2))

    def test_missing_collection_is_optional_and_checksum_failure_hides_csv(self):
        self.assertEqual(self.ns['load_supplementary'](self.base,'git')['state'],'absent')
        root,csv=self.fixture();(root/csv).write_text('tool_id,untrusted\nother,1\n')
        result=self.ns['load_supplementary'](self.base,'git')
        self.assertEqual(result['state'],'unverified');self.assertIn('hash',result['message'])

    def test_changed_signatures_and_escaping_symlink_are_rejected(self):
        root,csv=self.fixture()
        signatures=tuple((name,self.ns['_signature'](root/name)) for name in ['manifest.json',csv])
        (root/'manifest.json').write_text((root/'manifest.json').read_text()+'\n')
        with self.assertRaisesRegex(ValueError,'changed while loading'):
            self.ns['_read_supplementary'](root,'git',signatures)
        other=self.base.parent/('outside-'+self.base.name);other.mkdir();self.addCleanup(lambda:other.rmdir())
        (root/'manifest.json').unlink();(root/csv).unlink();root.rmdir();root.symlink_to(other,target_is_directory=True)
        result=self.ns['load_supplementary'](self.base,'git')
        self.assertEqual(result['state'],'unverified');self.assertIn('outside',result['message'])

    def test_supplementary_flags_cannot_enable_primary_features(self):
        self.fixture('stars',eligible=True)
        result=self.ns['load_supplementary'](self.base,'stars')
        self.assertEqual(result['state'],'unverified');self.assertIn('excluded',result['message'])

if __name__=='__main__':unittest.main(verbosity=2)
