import unittest
from datetime import date
import json
from pathlib import Path
import tempfile
from unittest import mock

from run_metadata_backfill import endpoint,finalize_latest
from assemble_full_scope import sha


class MetadataSchedule(unittest.TestCase):
    def test_bootstrap_not_replaced_by_prior_monday(self):
        self.assertEqual(endpoint(date(2026,11,3),date(2026,11,3)),date(2026,11,3))

    def test_weekly_completed_date(self):
        initial=date(2026,11,3)
        self.assertEqual(endpoint(date(2026,11,8),initial),initial)
        self.assertEqual(endpoint(date(2026,11,9),initial),date(2026,11,9))
        self.assertEqual(endpoint(date(2026,11,15),initial),date(2026,11,9))

    def test_2026_end_cap_includes_final_partial_week(self):
        initial=date(2026,11,3)
        self.assertEqual(endpoint(date(2027,1,1),initial),date(2027,1,1))
        self.assertEqual(endpoint(date(2027,8,1),initial),date(2027,1,1))


class MetadataPublication(unittest.TestCase):
    def setUp(self):
        self.review=mock.patch('run_metadata_backfill.reviewed_source',side_effect=lambda output,registry:output).start()
        self.addCleanup(mock.patch.stopall)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
        self.data=self.root/'data';self.registry=self.root/'registry.json'
        self.registry.write_text(json.dumps({'tools':[{'tool_id':'pytorch','pypi_package':'torch'}]}))
        self.plan=self.root/'plan.json';self.plan.write_text(json.dumps({'end_exclusive':'2023-01-09'}))
        self.export=self.root/'export';self.export.mkdir()
        (self.export/'manifest.json').write_text(json.dumps({'status':'complete'}))
        self.receipt={'query_sha256':'a'*64,'output_sha256':'b'*64,'job_complete':True,'all_pages_fetched':True}
        (self.export/'completion_receipt.json').write_text(json.dumps(self.receipt))
        self.output=self.data/'full_scope/dependency_history_runs'/('2023-01-09-'+'a'*12+'-'+'b'*12)
        self.output.mkdir(parents=True)
        self.quality={'status':'passed','registry_sha256':sha(self.registry),'history_start':'2023-01-02',
            'history_end_exclusive':'2023-01-09','observed_at':'2023-01-10T00:00:00Z',
            'query_receipt_sha256':sha(self.export/'completion_receipt.json'),'query_plan_sha256':sha(self.plan),
            'input_metadata_sha256':self.receipt['output_sha256']}
        (self.output/'dependency_quality.json').write_text(json.dumps(self.quality))
        (self.output/'weekly_dependency_features.csv').write_text(
            'tool_id,week_start,feature_cutoff,event_window_end_exclusive,dep_inbound_event_time_eligible,dep_outbound_event_time_eligible,dep_ever_declaring_packages\n'
            'pytorch,2023-01-02,2023-01-11,2023-01-09,True,False,1\n')
        self.refresh_manifest()
        self.old=self.data/'full_scope/old';self.old.mkdir()
        self.pointer=self.data/'full_scope/dependency_history';self.pointer.symlink_to(self.old)

    def refresh_manifest(self):
        files={p.name:sha(p) for p in self.output.iterdir() if p.is_file() and p.name!='manifest.json'}
        (self.output/'manifest.json').write_text(json.dumps({'status':'passed',
            'source':'pypi_immutable_distribution_metadata','registry_sha256':sha(self.registry),'files':files}))

    def finalize(self):return finalize_latest([(self.plan,self.export)],self.data,self.registry)

    def test_valid_existing_output_moves_pointer_without_rebuilding(self):
        with mock.patch('run_metadata_backfill.subprocess.run') as run:
            self.finalize();self.finalize();run.assert_not_called()
        self.assertEqual(self.pointer.resolve(),self.output.resolve())

    def test_repeated_finalization_keeps_reviewed_masks_instead_of_base_pointer(self):
        import shutil
        reviewed=self.output.with_name(self.output.name+'-review-exact')
        shutil.copytree(self.output,reviewed)
        self.review.side_effect=lambda output,registry:reviewed
        self.finalize();self.finalize()
        self.assertEqual(self.pointer.resolve(),reviewed)
        self.assertEqual(self.review.call_count,2)

    def test_failed_conflict_review_preserves_previous_publication(self):
        self.review.side_effect=ValueError('Conflict review failed')
        with self.assertRaisesRegex(ValueError,'Conflict review failed'):self.finalize()
        self.assertEqual(self.pointer.resolve(),self.old)

    def test_receipt_mismatch_preserves_previous_pointer(self):
        for field in ['input_metadata_sha256','query_receipt_sha256','query_plan_sha256']:
            with self.subTest(field=field):
                changed={**self.quality,field:'c'*64}
                (self.output/'dependency_quality.json').write_text(json.dumps(changed));self.refresh_manifest()
                with self.assertRaisesRegex(ValueError,'another query receipt'):self.finalize()
                self.assertEqual(self.pointer.resolve(),self.old)

    def test_changed_output_or_missing_weekly_features_preserves_pointer(self):
        weekly=self.output/'weekly_dependency_features.csv';weekly.write_text(weekly.read_text()+'tampered\n')
        with self.assertRaisesRegex(ValueError,'hash/path mismatch'):self.finalize()
        self.assertEqual(self.pointer.resolve(),self.old)
        weekly.unlink();self.refresh_manifest()
        with self.assertRaisesRegex(ValueError,'lacks validated weekly'):self.finalize()
        self.assertEqual(self.pointer.resolve(),self.old)

    def test_incomplete_source_never_publishes(self):
        self.receipt['all_pages_fetched']=False
        (self.export/'completion_receipt.json').write_text(json.dumps(self.receipt))
        self.finalize();self.assertEqual(self.pointer.resolve(),self.old)

    def test_failed_builder_never_moves_pointer(self):
        import shutil
        shutil.rmtree(self.output)
        with mock.patch('run_metadata_backfill.subprocess.run',return_value=mock.Mock(returncode=1)):
            with self.assertRaisesRegex(ValueError,'previous publication retained'):self.finalize()
        self.assertEqual(self.pointer.resolve(),self.old)

    def test_existing_real_directory_is_preserved(self):
        self.pointer.unlink();self.pointer.mkdir()
        marker=self.pointer/'keep';marker.write_text('retained')
        with self.assertRaisesRegex(ValueError,'Canonical dependency path is a directory'):self.finalize()
        self.assertEqual(marker.read_text(),'retained')


if __name__=='__main__':unittest.main()
