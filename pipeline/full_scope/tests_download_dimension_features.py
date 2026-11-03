import argparse
import copy
import contextlib
from datetime import date
import gzip
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pandas as pd
import build_download_dimension_features as builder


REGISTRY={'tools':[{'tool_id':'pytorch','pypi_package':'torch'}, {'tool_id':'mlflow','pypi_package':'mlflow'}]}
MAPPING={'torch':'pytorch','mlflow':'mlflow'}


def row(day, package='torch', version='1', downloads=60, excluded=10, **kwargs):
    result={'package':package,'date':str(day),'package_version':version,'artifact_type':'bdist_wheel',
            'python_major_minor':'3.10','python_implementation':'CPython','operating_system':'Linux',
            'installer':'pip','country_code':'US','ci_report':'reported_true','http_status':200,
            'downloads':downloads,'excluded_requests':excluded,'logged_objects':downloads+excluded}
    result.update(kwargs);return result


def fixture_rows():
    rows=[]
    for day in builder.days('2023-01-02','2023-01-09'):
        rows.append(row(day))
        rows.append(row(day,version='2',downloads=40,excluded=0,python_major_minor=None,
                        operating_system=None,country_code=None,ci_report='unknown',http_status=None))
        # Metadata-only requests must not become adoption/download counts.
        rows.append(row(day,version='metadata-only',downloads=0,excluded=7))
        if day>=date(2023,1,5):rows.append(row(day,package='mlflow',downloads=10,excluded=0,ci_report='reported_false'))
    return rows


def query(root, name, start, end, rows, observed='2026-11-03T00:00:00Z'):
    folder=root/name;folder.mkdir()
    parameters=[{'name':name,'parameterType':{'type':'DATE'},'parameterValue':{'value':value}}
                for name,value in [('start_date',start),('end_date',end)]]
    parameters.append({'name':'packages','parameterType':{'type':'ARRAY','arrayType':{'type':'STRING'}},
                       'parameterValue':{'arrayValues':[{'value':name} for name in MAPPING]}})
    plan={'source':builder.SOURCE,'query':'SELECT source_fixture','query_parameters':parameters,
          'start':start,'end_exclusive':end,'package_to_tool':MAPPING}
    plan['query_sha256']=hashlib.sha256(json.dumps({'query':plan['query'],'parameters':plan['query_parameters']},sort_keys=True).encode()).hexdigest()
    (folder/'query_plan.json').write_text(json.dumps(plan))
    payload=''.join(json.dumps(r)+'\n' for r in rows).encode()
    path=folder/'decoded_records.jsonl.gz';path.write_bytes(gzip.compress(payload,mtime=0))
    receipt={'query_sha256':plan['query_sha256'],'job_complete':True,'all_pages_fetched':True,
             'total_rows':len(rows),'output_sha256':builder.sha(path),'observed_at':observed,'cloud_job_id':'fixture-'+name}
    (folder/'completion_receipt.json').write_text(json.dumps(receipt))
    return folder


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.first={'pytorch':date(2022,1,1),'mlflow':date(2023,1,5)}
        self.reference={(tool,str(day)):100 if tool=='pytorch' else 10 if day>=date(2023,1,5) else None
                        for tool in MAPPING.values() for day in builder.days('2023-01-02','2023-01-09')}
        query(self.root,'week','2023-01-02','2023-01-09',fixture_rows())

    def inputs(self,start='2023-01-02',end='2023-01-09'):
        queries,winners,pending=builder.collect_inputs(self.root,MAPPING,start,end)
        frame,validated=builder.pandas_inputs(queries,winners,MAPPING,1000)
        grid=builder.daily_grid(builder.aggregate_daily(frame),MAPPING,winners,self.first,self.reference,start,end)
        return queries,winners,frame,grid

    def test_arithmetic_dimensions_and_unknowns_have_explicit_meaning(self):
        queries,winners,frame,grid=self.inputs()
        composition=builder.pandas_compositions(frame,grid)
        weekly=builder.week_features(grid,builder.composition_summary(composition),'2023-01-02','2023-01-09')
        torch=weekly[weekly.tool_id.eq('pytorch')].iloc[0]
        self.assertEqual(torch.rich_dl_downloads,700)
        self.assertEqual(torch.rich_excluded_requests,119)
        self.assertEqual(torch.dl_version_distinct_known,2)
        self.assertEqual(torch.dl_version_top_known_value,'1')
        self.assertAlmostEqual(torch.dl_version_top_known_share,.6)
        self.assertAlmostEqual(torch.dl_os_unknown_share,.4)
        self.assertAlmostEqual(torch.dl_ci_reported_true_share,.6)
        self.assertAlmostEqual(torch.dl_ci_reported_unknown_share,.4)
        self.assertAlmostEqual(torch.dl_version_entropy,-.6*math.log(.6)-.4*math.log(.4))
        self.assertEqual(torch.dl_http_top_known_value,'200')
        self.assertFalse(torch.primary_model_promoted)
        self.assertTrue(torch.availability_lag_is_assumption)

    def test_pre_artifact_and_partial_weeks_are_not_zero_filled(self):
        _,_,frame,grid=self.inputs()
        early=grid[grid.tool_id.eq('mlflow') & grid.date.lt('2023-01-05')]
        self.assertEqual(len(early),3);self.assertTrue(early.downloads.isna().all())
        weekly=builder.week_features(grid,builder.composition_summary(builder.pandas_compositions(frame,grid)),
                                     '2023-01-02','2023-01-09')
        mlflow=weekly[weekly.tool_id.eq('mlflow')].iloc[0]
        self.assertFalse(mlflow.rich_week_complete)
        self.assertTrue(pd.isna(mlflow.rich_dl_downloads))
        self.assertTrue(pd.isna(mlflow.dl_ci_reported_false_share))

    def test_empty_newer_whole_query_replaces_old_positive_rows(self):
        query(self.root,'refresh','2023-01-02','2023-01-09',[],observed='2026-11-03T01:00:00Z')
        _,winners,frame,grid=self.inputs()
        self.assertTrue(frame.empty)
        self.assertEqual(len(winners),7)
        self.assertTrue(grid.downloads.isna().all())
        self.assertTrue(grid.loc[~grid.pre_visible_artifact,'observation_status'].eq('cohort_day_not_observed').all())

    def test_reference_differences_remain_visible_without_changing_old_data(self):
        self.reference[('pytorch','2023-01-04')]=101
        before=copy.deepcopy(self.reference)
        _,_,frame,grid=self.inputs()
        differences=grid[grid.difference_from_frozen_primary.notna() & grid.difference_from_frozen_primary.ne(0)]
        self.assertEqual(len(differences),1)
        self.assertEqual(differences.iloc[0].difference_from_frozen_primary,-1)
        self.assertEqual(self.reference,before)

    def test_partial_collection_produces_unknown_future_grid(self):
        _,winners,frame,grid=self.inputs(end='2023-01-16')
        self.assertEqual(len(grid),28)
        future=grid[grid.date.ge('2023-01-09')]
        self.assertTrue(future.downloads.isna().all())
        self.assertTrue(future.raw_downloads.isna().all())
        self.assertEqual(len(winners),7)

    def test_bad_counts_duplicate_keys_and_query_bounds_are_rejected(self):
        base=row('2023-01-02')
        for values in [{'downloads':True},{'downloads':1.5},{'downloads':-1},
                       {'excluded_requests':999},{'ci_report':None},{'date':'2023-01-01'}]:
            with self.assertRaises(ValueError):builder.validate_record({**base,**values},MAPPING,'2023-01-02','2023-01-09')
        query(self.root,'duplicate','2023-01-02','2023-01-09',[base,base],observed='2026-11-03T01:00:00Z')
        queries,winners,_=builder.collect_inputs(self.root,MAPPING,'2023-01-02','2023-01-09')
        with self.assertRaisesRegex(ValueError,'Duplicate'):builder.pandas_inputs(queries,winners,MAPPING,1000)

    def test_receipt_hash_or_row_count_tampering_is_rejected(self):
        path=self.root/'week/completion_receipt.json';receipt=json.loads(path.read_text())
        path.write_text(json.dumps({**receipt,'all_pages_fetched':False}))
        with self.assertRaisesRegex(ValueError,'receipt'):self.inputs()
        path.write_text(json.dumps({**receipt,'total_rows':receipt['total_rows']+1}))
        with self.assertRaisesRegex(ValueError,'row count'):self.inputs()

    def test_known_release_dates_only_define_recent_version_share(self):
        _,_,frame,grid=self.inputs()
        weekly=builder.week_features(grid,builder.composition_summary(builder.pandas_compositions(frame,grid)),
                                     '2023-01-02','2023-01-09')
        self.assertTrue(weekly.dl_recent_version_28d_share_lower_bound.isna().all())
        weekly=builder.add_release_features(weekly,frame,grid,{('torch','1'):date(2022,1,1),('torch','2'):date(2023,1,1)})
        row=weekly[weekly.tool_id.eq('pytorch')].iloc[0]
        self.assertAlmostEqual(row.dl_release_date_known_share,1)
        self.assertAlmostEqual(row.dl_recent_version_28d_share_lower_bound,.4)
        self.assertEqual(row.dl_release_after_download_requests,0)

    def test_storage_reserve_fails_closed(self):
        with mock.patch.object(builder.shutil,'disk_usage',return_value=argparse.Namespace(free=0)):
            with self.assertRaisesRegex(RuntimeError,'reserve reached'):
                builder.DiskGuard(self.root,15)

    def test_query_parameters_must_bind_interval_and_cohort(self):
        plan=json.loads((self.root/'week/query_plan.json').read_text())
        builder.validate_plan_parameters(plan,MAPPING)
        plan['start']='2023-01-01'
        with self.assertRaisesRegex(ValueError,'date parameters'):
            builder.validate_plan_parameters(plan,MAPPING)

    def test_publication_binds_outputs_and_preserves_frozen_primary(self):
        registry=self.root/'registry.json';registry.write_text(json.dumps(REGISTRY))
        primary=self.root/'primary';primary.mkdir();(primary/'tools.study.json').write_bytes(registry.read_bytes())
        pd.DataFrame([{'tool_id':tool,'first_visible_artifact':str(first)} for tool,first in self.first.items()]).to_csv(primary/'coverage.csv',index=False)
        frozen=primary/'daily_normalized.parquet';frozen.write_bytes(b'frozen-primary-placeholder')
        (primary/'study_manifest.json').write_text(json.dumps({'status':'validated_primary','files':
            {name:builder.sha(primary/name) for name in ['daily_normalized.parquet','coverage.csv']}}))
        reference=pd.DataFrame([{'tool_id':tool,'date':day,'dl_downloads':count}
                                for (tool,day),count in self.reference.items()])
        args=argparse.Namespace(query_root=self.root,registry=registry,primary_snapshot=primary,
            output=self.root/'published',start='2023-01-02',end_exclusive='2023-01-09',backend='pandas',
            pandas_max_rows=1000,memory_limit='512MB',max_temp_gib=1,minimum_free_gib=15,dependency_history=None)
        before=builder.sha(frozen)
        with mock.patch.object(builder.pd,'read_parquet',return_value=reference),mock.patch.object(
            builder.shutil,'disk_usage',return_value=argparse.Namespace(free=100*1024**3)),contextlib.redirect_stdout(io.StringIO()):
            builder.build(args)
        manifest=builder.read(args.output/'manifest.json');quality=builder.read(args.output/'download_dimension_quality.json')
        self.assertEqual(manifest['status'],'passed');self.assertEqual(quality['daily_rows'],14)
        self.assertEqual(quality['weekly_rows'],2);self.assertEqual(quality['complete_weekly_rows'],1)
        self.assertEqual(quality['pre_artifact_tool_days'],3);self.assertEqual(builder.sha(frozen),before)
        for name,digest in manifest['files'].items():self.assertEqual(builder.sha(args.output/name),digest)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB not installed in this reference-test environment')
    def test_composition_summary_is_independent_of_the_large_rich_parquet(self):
        queries,winners,frame,grid=self.inputs();work=self.root/'cached';work.mkdir()
        backend=builder.DuckBackend(queries,winners,MAPPING,work,memory_limit='64MB',threads=1)
        self.addCleanup(backend.con.close)
        actual=backend.con.execute("SELECT current_setting('threads'),current_setting('preserve_insertion_order')").fetchone()
        self.assertEqual(actual,(1,False))
        summaries,count=backend.write_and_summarize(work,grid)
        expected=builder.pandas_compositions(frame,grid)
        self.assertEqual(count,len(expected))
        backend.rich_path.rename(work/'rich-no-longer-readable.parquet')
        # A repeated summary/count may read only the already materialized small
        # composition; the previous UNION view would reopen the missing rich file.
        retained=backend.con.execute('SELECT COUNT(*),SUM(downloads) FROM composition').fetchone()
        self.assertEqual(retained,(len(expected),int(expected.downloads.sum())))
        with self.assertRaises(ValueError):builder.DuckBackend(queries,winners,MAPPING,work,threads=3)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB not installed in this reference-test environment')
    def test_changed_completed_input_rebuilds_values_and_reports_reference_difference(self):
        registry=self.root/'registry.json';registry.write_text(json.dumps(REGISTRY));primary=self.root/'primary';primary.mkdir()
        (primary/'tools.study.json').write_bytes(registry.read_bytes())
        pd.DataFrame([{'tool_id':tool,'first_visible_artifact':str(first)} for tool,first in self.first.items()]).to_csv(primary/'coverage.csv',index=False)
        frozen=primary/'daily_normalized.parquet';frozen.write_bytes(b'unchanged primary evidence')
        (primary/'study_manifest.json').write_text(json.dumps({'status':'validated_primary','files':{n:builder.sha(primary/n) for n in ['daily_normalized.parquet','coverage.csv']}}))
        reference=pd.DataFrame([{'tool_id':t,'date':d,'dl_downloads':n} for (t,d),n in self.reference.items()]);before=builder.sha(frozen)
        args=argparse.Namespace(query_root=self.root,registry=registry,primary_snapshot=primary,output=self.root/'first-version',
            start='2023-01-02',end_exclusive='2023-01-09',backend='duckdb',pandas_max_rows=1000,memory_limit='64MB',threads=1,
            max_temp_gib=1,minimum_free_gib=15,dependency_history=None)
        with mock.patch.object(builder.pd,'read_parquet',return_value=reference),mock.patch.object(builder.shutil,'disk_usage',return_value=argparse.Namespace(free=100*1024**3)),contextlib.redirect_stdout(io.StringIO()):
            builder.build(args)
            changed=fixture_rows();changed[0]['downloads']+=1;changed[0]['logged_objects']+=1
            query(self.root,'refresh','2023-01-02','2023-01-09',changed,observed='2026-11-03T01:00:00Z')
            args.output=self.root/'second-version';builder.build(args)
        first=builder.read(self.root/'first-version/download_dimension_quality.json');second=builder.read(args.output/'download_dimension_quality.json')
        self.assertEqual(first['frozen_primary_difference_tool_days'],0);self.assertEqual(second['frozen_primary_difference_tool_days'],1)
        self.assertEqual(second['validation_status'],'passed_with_reference_differences');self.assertEqual(builder.sha(frozen),before)
        self.assertEqual(second['runtime']['duckdb_resource_profile']['threads'],1)
        self.assertEqual(set(second['winning_input_by_date'].values()),{str((self.root/'refresh/decoded_records.jsonl.gz').resolve())})
        before_rows=pd.read_csv(self.root/'first-version/weekly_download_dimension_features.csv');after_rows=pd.read_csv(args.output/'weekly_download_dimension_features.csv')
        self.assertEqual(before_rows.loc[before_rows.tool_id.eq('pytorch'),'rich_dl_downloads'].iloc[0],700)
        self.assertEqual(after_rows.loc[after_rows.tool_id.eq('pytorch'),'rich_dl_downloads'].iloc[0],701)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB not installed in this reference-test environment')
    def test_duckdb_streaming_matches_hand_checked_reference(self):
        queries,winners,frame,grid=self.inputs()
        work=self.root/'duck';work.mkdir()
        backend=builder.DuckBackend(queries,winners,MAPPING,work)
        pd.testing.assert_frame_equal(backend.daily().sort_values(['tool_id','date']).reset_index(drop=True),
                                      builder.aggregate_daily(frame).sort_values(['tool_id','date']).reset_index(drop=True),
                                      check_dtype=False)
        summaries,_=backend.write_and_summarize(work,grid)
        actual=builder.week_features(grid,summaries,'2023-01-02','2023-01-09')
        expected=builder.week_features(grid,builder.composition_summary(builder.pandas_compositions(frame,grid)),
                                       '2023-01-02','2023-01-09')
        pd.testing.assert_frame_equal(actual,expected,check_dtype=False,check_like=True)
        release_root=self.root/'releases';release_root.mkdir()
        releases=[{'package':'torch','version':version,'provider_upload_time':published,'identity_valid':True}
                  for version,published in [('1','2022-01-01T00:00:00Z'),('2','2023-01-01T00:00:00Z')]]
        artifact=release_root/'artifacts.jsonl.gz'
        artifact.write_bytes(gzip.compress(''.join(json.dumps(r)+'\n' for r in releases).encode()))
        (release_root/'manifest.json').write_text(json.dumps({'source':'pypi_immutable_distribution_metadata',
            'registry_sha256':'fixture-cohort','status':'passed','files':{'artifacts.jsonl.gz':builder.sha(artifact)}}))
        released,binding=backend.release_summaries(release_root,'fixture-cohort')
        torch=released[released.tool_id.eq('pytorch')].iloc[0]
        self.assertEqual(torch.total,700);self.assertEqual(torch.known,700);self.assertEqual(torch.recent,280)
        self.assertEqual(torch.after_requests,0)
        self.assertEqual(binding['artifacts_sha256'],builder.sha(artifact))
        backend.con.close()

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB not installed')
    def test_parser_cache_keeps_json_scalar_types_and_is_removed_before_publication(self):
        queries,winners,frame,grid=self.inputs();work=self.root/'parser-cache';work.mkdir()
        backend=builder.DuckBackend(queries,winners,MAPPING,work,memory_limit='64MB')
        self.addCleanup(backend.con.close)
        self.assertTrue(backend.raw_cache.is_dir())
        self.assertEqual(backend.con.execute('SELECT DISTINCT json_type(downloads) FROM raw_input').fetchall(),[('UBIGINT',)])
        self.assertEqual({r[0] for r in backend.con.execute('SELECT DISTINCT source_path FROM raw_input').fetchall()},
            {q['input_path'] for q in queries})
        backend.daily();self.assertFalse(backend.raw_cache.exists())
        actual=backend.con.execute('SELECT * FROM rich ORDER BY tool_id,date,package_version').df()
        expected=frame[['tool_id',*builder.RAW_FIELDS]].sort_values(['tool_id','date','package_version']).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True),expected,check_dtype=False)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'),'DuckDB not installed')
    def test_parser_cache_cannot_coerce_invalid_json_counts_or_hide_duplicates(self):
        for index,value in enumerate([True,'60',60.0,-1,2**64]):
            folder=self.root/('bad-scalar-'+str(index));folder.mkdir()
            bad=row('2023-01-02');bad['downloads']=value
            query(folder,'input','2023-01-02','2023-01-09',[bad])
            queries,winners,_=builder.collect_inputs(folder,MAPPING,'2023-01-02','2023-01-09')
            work=folder/'work';work.mkdir()
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'nonnegative integer'):
                builder.DuckBackend(queries,winners,MAPPING,work,memory_limit='64MB')
        folder=self.root/'cache-duplicate';folder.mkdir();base=row('2023-01-02')
        query(folder,'input','2023-01-02','2023-01-09',[base,base])
        queries,winners,_=builder.collect_inputs(folder,MAPPING,'2023-01-02','2023-01-09');work=folder/'work';work.mkdir()
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            builder.DuckBackend(queries,winners,MAPPING,work,memory_limit='64MB')


if __name__=='__main__':unittest.main(verbosity=2)
