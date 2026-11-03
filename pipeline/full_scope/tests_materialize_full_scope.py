from datetime import datetime,timedelta,timezone
import importlib
import json
from pathlib import Path
import tempfile
import subprocess,sys,time,signal
import unittest
from unittest.mock import patch

import materialize_full_scope as module

UTC=timezone.utc


class Materialization(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.project=Path(self.temp.name)/'project';self.data=self.project/'data'
        self.code=Path(self.temp.name)/'code';self.code.mkdir()
        (self.code/'build_download_dimension_features.py').write_text('# local builder fixture\n')
        self.location=patch.object(module,'__file__',str(self.code/'materialize_full_scope.py'))
        self.location.start();self.addCleanup(self.location.stop)
        (self.project/'config').mkdir(parents=True)
        (self.project/'config/tools.study.json').write_text(json.dumps({'tools':[{'tool_id':str(i)} for i in range(30)]}))
        primary=self.data/'study/latest';primary.mkdir(parents=True)
        (primary/'study_manifest.json').write_text('{}')
        (primary/'study_status.json').write_text(json.dumps({'as_of':'2023-01-09T01:00:00Z'}))
        self.archive=self.data/'raw/full_scope/bigquery';self.archive.mkdir(parents=True)
        self.now=datetime(2026,11,3,4,tzinfo=UTC)

    def query(self,name='pilot',start='2023-01-02',end='2023-01-09',complete=True):
        folder=self.archive/name;folder.mkdir()
        plan={'source':'pypi_download_dimensions','start':start,'end_exclusive':end,'query_sha256':'query-'+name}
        (folder/'query_plan.json').write_text(json.dumps(plan))
        (folder/'decoded_records.jsonl.gz').write_bytes(b'fixture-not-read-by-the-cheap-gate')
        body={'status':'complete' if complete else 'exporting','query_sha256':plan['query_sha256'],
              'total_rows':0,'rows_fetched':0,'all_pages_fetched':True,'job_complete':True}
        (folder/'manifest.json').write_text(json.dumps(body))
        if complete:
            (folder/'completion_receipt.json').write_text(json.dumps({'query_sha256':plan['query_sha256'],
                'all_pages_fetched':True,'job_complete':True,'total_rows':0,'output_sha256':'body-content-sha'}))
        return folder

    def test_only_complete_receipts_advance_materialization(self):
        self.query(complete=False)
        result=module.plan(self.project,{},self.now)
        self.assertFalse(result['build_rich_features']);self.assertEqual(result['completed_query_receipts'],0)
        self.assertFalse(result['paid_queries_enabled'])

    def test_partial_build_waits_six_hours_after_prior_version(self):
        self.query()
        state={'rich_signature':'older-receipt-set','rich_built_at':(self.now-timedelta(hours=5)).isoformat()}
        self.assertFalse(module.plan(self.project,state,self.now)['build_rich_features'])
        self.assertTrue(module.plan(self.project,state,self.now+timedelta(hours=1))['build_rich_features'])

    def test_complete_history_can_materialize_without_partial_delay(self):
        self.query(start='2023-01-01')
        result=module.plan(self.project,{'rich_built_at':self.now.isoformat(),'rich_signature':'old'},self.now)
        self.assertTrue(result['history_covered']);self.assertTrue(result['build_rich_features'])

    def test_existing_equivalent_first_build_is_adopted(self):
        folder=self.query();inventory,_=module.receipt_inventory(self.archive,'2023-01-01','2023-01-09')
        root=self.data/'full_scope/download_dimension_features';root.mkdir(parents=True)
        quality={'queries':inventory,'built_at':self.now.isoformat(),'end_exclusive':'2023-01-09',
                 'code_sha256':{'build_download_dimension_features.py':module.sha(self.code/'build_download_dimension_features.py')}}
        (root/'download_dimension_quality.json').write_text(json.dumps(quality))
        result=module.plan(self.project,{},self.now)
        self.assertTrue(result['adopt_existing_rich_signature']);self.assertFalse(result['build_rich_features'])

    def test_rich_classifier_sha_is_part_of_binding_and_blocks_stale_adoption(self):
        self.query();inventory,_=module.receipt_inventory(self.archive,'2023-01-01','2023-01-09')
        helper=self.code/'download_split_coverage.py';helper.write_text('# first classifier')
        root=self.data/'full_scope/download_dimension_features';root.mkdir(parents=True)
        quality={'queries':inventory,'built_at':self.now.isoformat(),'end_exclusive':'2023-01-09',
            'code_sha256':{'build_download_dimension_features.py':module.sha(self.code/'build_download_dimension_features.py'),
                           'download_split_coverage.py':module.sha(helper)}}
        (root/'download_dimension_quality.json').write_text(json.dumps(quality))
        first=module.plan(self.project,{},self.now);self.assertTrue(first['adopt_existing_rich_signature'])
        helper.write_text('# changed classifier')
        changed=module.plan(self.project,{'rich_signature':first['rich_signature'],'rich_built_at':self.now.isoformat()},self.now)
        self.assertNotEqual(changed['rich_signature'],first['rich_signature']);self.assertFalse(changed['build_rich_features'])
        self.assertFalse(module.plan(self.project,{},self.now)['adopt_existing_rich_signature'])

    def test_changed_source_assembly_is_throttled_to_hourly(self):
        state={'assembled_at':self.now.isoformat(),'spine_signature':'different-old-sources'}
        self.assertFalse(module.plan(self.project,state,self.now+timedelta(minutes=59))['assemble_changed_sources'])
        self.assertTrue(module.plan(self.project,state,self.now+timedelta(hours=1))['assemble_changed_sources'])

    def test_inconsistent_completed_receipt_fails_closed(self):
        folder=self.query();path=folder/'completion_receipt.json';body=json.loads(path.read_text())
        body['total_rows']=1;path.write_text(json.dumps(body))
        with self.assertRaisesRegex(ValueError,'inconsistent receipt'):
            module.plan(self.project,{},self.now)

    def test_failed_builder_preserves_previous_canonical_pointer(self):
        self.query();base=self.data/'full_scope';base.mkdir()
        previous=base/'previous';previous.mkdir();link=base/'download_dimensions';link.symlink_to(previous)
        with patch.object(module,'bounded',return_value=124),self.assertRaisesRegex(RuntimeError,'preserved previous canonical pointer'):
            module.execute(self.project)
        self.assertEqual(link.resolve(),previous.resolve())
    def test_measured_full_history_deadline_reaches_builder_without_changing_resource_caps(self):
        self.query();seen=[]
        def fails(command,timeout,*unused):seen.append((command,timeout));return 124
        with patch.object(module,'bounded',side_effect=fails),self.assertRaises(RuntimeError):module.execute(self.project)
        self.assertEqual(seen[0][1],7200);command=seen[0][0]
        for flag,value in [('--memory-limit','3GB'),('--threads','1'),('--max-temp-gib','10'),('--minimum-free-gib','15')]:
            self.assertEqual(command[command.index(flag)+1],value)
        recipe=module.plan(self.project,{},self.now)
        self.assertEqual(recipe['rich_resource_profile']['timeout_seconds'],7200)
        self.assertGreater(recipe['maximum_runtime_seconds'],7200)
        script=Path(__file__).with_name('run-full-scope-materialize.sh').read_text()
        self.assertIn('135m',script);self.assertIn('--kill-after=20s',script)

    def test_atomic_publication_refuses_real_directory_replacement(self):
        target=self.data/'version';target.mkdir();link=self.data/'canonical';link.mkdir()
        with self.assertRaisesRegex(ValueError,'not a symlink'):module.atomic_link(link,target)
        self.assertTrue(link.is_dir());self.assertFalse(link.is_symlink())

    def test_success_exit_without_weekly_features_cannot_publish(self):
        self.query();base=self.data/'full_scope';base.mkdir()
        previous=base/'previous';previous.mkdir();link=base/'download_dimensions';link.symlink_to(previous)
        def fake_build(command,*unused):
            output=Path(command[command.index('--output')+1]);output.mkdir()
            evidence=output/'evidence.txt';evidence.write_text('retained')
            (output/'manifest.json').write_text(json.dumps({'source':'pypi_download_dimensions','status':'partial',
                'registry_sha256':module.sha(self.project/'config/tools.study.json'),
                'files':{'evidence.txt':module.sha(evidence)}}))
            return 0
        with patch.object(module,'bounded',side_effect=fake_build),self.assertRaisesRegex(ValueError,'lacks verified weekly features'):
            module.execute(self.project)
        self.assertEqual(link.resolve(),previous.resolve())

    def test_materializer_prefers_verified_research_latest_pointer(self):
        old=self.data/'raw/research_v1/backfill';old.mkdir(parents=True)
        latest=self.data/'raw/research_v1/latest';latest.symlink_to(old)
        self.assertEqual(module.sources(self.project)['research'],latest)
        initial=self.data/'raw/depsdev/full-scope-initial';initial.mkdir(parents=True)
        graph=self.data/'raw/depsdev/latest';graph.symlink_to(initial)
        self.assertEqual(module.sources(self.project)['depsdev'],graph)

    def test_github_canonical_source_is_default_and_explicit_override_is_respected(self):
        canonical=self.data/'full_scope/github_activity'
        self.assertEqual(module.sources(self.project)['github_api'],canonical)
        override=self.data/'other_github_snapshot'
        self.assertEqual(module.sources(self.project,override)['github_api'],override)

    def overlay_fixture(self):
        base=self.data/'full_scope/github_activity';base.mkdir(parents=True);(base/'manifest.json').write_text('{"vintage":"baseline"}')
        raw=self.data/'raw/github_direct_incremental';raw.mkdir(parents=True)
        body={'registry_sha256':'cohort','completed_cycles':[{'cycle_id':'c','receipt_paths':['raw/a.gz']}],
            'requests':[{'raw_path':'raw/a.gz','raw_sha256':'first'}],'streams':{}}
        (raw/'refresh_state.json').write_text(json.dumps(body))
        for name in ['github_activity_overlay.py','build_github_activity.py','refresh_github_activity.py']:(self.code/name).write_text('# overlay fixture')
        return base,raw,body

    def test_github_overlay_prefers_completed_materialized_update_source(self):
        baseline,_,_=self.overlay_fixture();updated=self.data/'full_scope/github_activity_updated';updated.mkdir()
        cohort=module.sha(self.project/'config/tools.study.json')
        quality={'source':'github_direct_issues_pulls','registry_sha256':cohort,'baseline_manifest_sha256':module.sha(baseline/'manifest.json')}
        (updated/'source_quality.json').write_text(json.dumps(quality))
        (updated/'manifest.json').write_text(json.dumps({'source':'github_direct_issues_pulls','registry_sha256':cohort,
            'files':{'source_quality.json':module.sha(updated/'source_quality.json')}}))
        self.assertEqual(module.sources(self.project)['github_api'],updated)
        (baseline/'manifest.json').write_text('{"vintage":"new-complete-source"}')
        self.assertEqual(module.sources(self.project)['github_api'],baseline)
        self.assertTrue((updated/'manifest.json').is_file())
    def test_changed_or_unbound_overlay_cannot_hide_a_valid_baseline(self):
        baseline,_,_=self.overlay_fixture();updated=self.data/'full_scope/github_activity_updated';updated.mkdir()
        (updated/'manifest.json').write_text('{"source":"github_direct_issues_pulls"}')
        (updated/'source_quality.json').write_text('{}')
        self.assertEqual(module.sources(self.project)['github_api'],baseline)

    def test_github_overlay_signature_ignores_partial_progress_and_rebuilds_completed_cycles(self):
        base,raw,body=self.overlay_fixture();first=module.github_overlay_recipe(self.project,{},self.now)
        self.assertTrue(first['build']);state={'github_overlay_signature':first['signature']}
        body['requests'].append({'raw_path':'partial.gz','raw_sha256':'pending'})
        body['streams']['tool/issues']={'cycle':{'pages':1}}
        (raw/'refresh_state.json').write_text(json.dumps(body))
        self.assertFalse(module.github_overlay_recipe(self.project,state,self.now)['build'])
        body['completed_cycles'].append({'cycle_id':'new','receipt_paths':['partial.gz']})
        (raw/'refresh_state.json').write_text(json.dumps(body))
        self.assertTrue(module.github_overlay_recipe(self.project,state,self.now)['build'])

    def test_changed_pr_detail_completion_rebuilds_overlay_without_new_issue_cycle(self):
        base,raw,body=self.overlay_fixture();body['completed_details']={}
        (raw/'activity_update_state.json').write_text(json.dumps(body))
        first=module.github_overlay_recipe(self.project,{},self.now);state={'github_overlay_signature':first['signature']}
        body['completed_details']['tool/4']={'raw_path':'detail.gz','pr_created_at':'2026-11-01T00:00:00Z'}
        body['requests'].append({'raw_path':'detail.gz','raw_sha256':'actual-detail'})
        (raw/'activity_update_state.json').write_text(json.dumps(body))
        self.assertTrue(module.github_overlay_recipe(self.project,state,self.now)['build'])

    def test_native_cycles_can_materialize_independently_and_partial_native_pages_cannot(self):
        base,raw,body=self.overlay_fixture();(raw/'refresh_state.json').unlink()
        native={'registry_sha256':'cohort','completed_cycles':[{'cycle_id':'native','end_exclusive':'2026-11-03T00:00:00Z','receipt_paths':['native.gz']}],
                'requests':[{'raw_path':'native.gz','raw_sha256':'verified'}],'streams':{}}
        path=raw/'native_pr_update_state.json';path.write_text(json.dumps(native))
        first=module.github_overlay_recipe(self.project,{},self.now);self.assertTrue(first['build'])
        native['requests'].append({'raw_path':'partial-native.gz','raw_sha256':'pending'});native['streams']['tool']={'cycle':{'pages':1}}
        path.write_text(json.dumps(native))
        self.assertFalse(module.github_overlay_recipe(self.project,{'github_overlay_signature':first['signature']},self.now)['build'])

    def test_github_overlay_new_baseline_or_code_invalidates_binding(self):
        base,raw,body=self.overlay_fixture();first=module.github_overlay_recipe(self.project,{},self.now)
        state={'github_overlay_signature':first['signature']};(base/'manifest.json').write_text('{"vintage":"fresh"}')
        self.assertTrue(module.github_overlay_recipe(self.project,state,self.now)['build'])
        self.assertEqual(first['end_exclusive'],'2026-11-03T00:00:00Z')
        self.assertEqual(module.github_overlay_recipe(self.project,state,datetime(2027,2,1,tzinfo=UTC))['end_exclusive'],'2027-01-01T00:00:00Z')

    def test_old_materialization_state_detects_new_latest_observation(self):
        old=self.data/'raw/depsdev/observations/old';old.mkdir(parents=True);(old/'manifest.json').write_text('{"vintage":"old"}')
        latest=self.data/'raw/depsdev/latest';latest.symlink_to(old)
        previous=module.checkpoint_signature(self.project,module.sources(self.project))
        fresh=old.parent/'fresh';fresh.mkdir();(fresh/'manifest.json').write_text('{"vintage":"fresh"}')
        module.atomic_link(latest,fresh)
        recipe=module.plan(self.project,{'spine_signature':previous,
            'assembled_at':(self.now-timedelta(hours=2)).isoformat()},self.now)
        self.assertTrue(recipe['assemble_changed_sources'])
        self.assertEqual(Path(recipe['source_inputs']['depsdev']).resolve(),latest.resolve())


class BoundedProcessGroups(unittest.TestCase):
    def test_outer_termination_reaches_worker_and_its_child(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);worker=root/'worker.py';grandchild=root/'grandchild.py'
            common="import signal,time,sys\nfrom pathlib import Path\ndef stop(signum,frame):\n Path(sys.argv[1]+'.stopped').write_text('terminated')\n raise SystemExit(0)\nsignal.signal(signal.SIGTERM,stop)\nPath(sys.argv[1]+'.ready').write_text('ready')\n"
            grandchild.write_text(common+'time.sleep(60)\n')
            worker.write_text(common+"import subprocess\nsubprocess.Popen([sys.executable,sys.argv[2],sys.argv[1]+'-child'])\ntime.sleep(60)\n")
            launcher=root/'launcher.py'
            launcher.write_text("import sys,os\nsys.path.insert(0,"+repr(str(Path(module.__file__).resolve().parent))+")\nimport materialize_full_scope as m\nm.bounded([sys.executable,"+repr(str(worker))+","+repr(str(root/'worker'))+","+repr(str(grandchild))+"],60,"+repr(str(root/'log'))+",os.environ.copy())\n")
            parent=subprocess.Popen([sys.executable,str(launcher)])
            try:
                deadline=time.monotonic()+5
                while not (root/'worker-child.ready').exists() and time.monotonic()<deadline:time.sleep(.02)
                self.assertTrue((root/'worker-child.ready').is_file())
                parent.send_signal(signal.SIGTERM);self.assertEqual(parent.wait(timeout=8),143)
                deadline=time.monotonic()+2
                while not (root/'worker-child.stopped').exists() and time.monotonic()<deadline:time.sleep(.02)
                self.assertTrue((root/'worker.stopped').is_file());self.assertTrue((root/'worker-child.stopped').is_file())
            finally:
                if parent.poll() is None:parent.kill();parent.wait()

if __name__=='__main__':unittest.main(verbosity=2)
