import unittest
import gzip
import hashlib
import json
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from build_weekly import weekly_rows,select_snapshots,latest_observations,utc_naive
from source_quality import GH_NUMERIC, annotate_daily_quality, load_policy, validate_panel, raw_spot_checks

class Features(unittest.TestCase):
    def daily(self):
        d=pd.DataFrame(index=pd.date_range('2023-01-02',periods=210))
        for c in ['dl_downloads','gh_event_count','gh_push_events','gh_issue_events','gh_pr_events','gh_watch_events','release_count']:d[c]=1.0
        d['gh_active_actors']=np.nan
        return d
    def compute(self,d):
        return weekly_rows(d,pd.Timestamp('2022-01-01'),'tool',pd.Timestamp('2023-08-01'))
    def test_full_future_weeks_after_cutoff(self):
        w=self.compute(self.daily());r=w.iloc[8]
        self.assertGreater(r.outcome_start,r.feature_cutoff)
        self.assertEqual((r.outcome_end_exclusive-r.outcome_start).days,56)
        self.assertEqual(r.target_log_growth,0)
        self.assertFalse(w.iloc[-1].coverage_complete)
    def test_missing_day_is_not_zero(self):
        d=self.daily();d.loc['2023-02-01','dl_downloads']=np.nan
        w=self.compute(d)
        self.assertTrue(pd.isna(w.loc[w.week_start==pd.Timestamp('2023-01-30'),'dl_downloads'].iloc[0]))
    def test_gap_week_and_future_cannot_change_features(self):
        d=self.daily();a=self.compute(d);origin=a.iloc[8].week_start
        d.loc[origin+pd.Timedelta(days=7):origin+pd.Timedelta(days=13),'dl_downloads']=1000
        b=self.compute(d)
        self.assertEqual(a.iloc[8].target_log_growth,b.iloc[8].target_log_growth)
        d.loc[origin+pd.Timedelta(days=14):origin+pd.Timedelta(days=69),'dl_downloads']=10
        c=self.compute(d)
        self.assertGreater(c.iloc[8].target_log_growth,0)
        self.assertEqual(a.iloc[8].dl_lag_0,c.iloc[8].dl_lag_0)
        self.assertEqual(a.iloc[8].dl_log_change_8w,c.iloc[8].dl_log_change_8w)
    def test_actor_marker_must_be_on_monday_and_coverage_complete(self):
        d=self.daily();d.loc['2023-01-03','gh_active_actors']=99
        self.assertTrue(pd.isna(self.compute(d).iloc[0].gh_active_actors))
        d.loc['2023-01-02','gh_active_actors']=5
        self.assertEqual(self.compute(d).iloc[0].gh_active_actors,5)
        d.loc['2023-01-04','gh_event_count']=np.nan
        self.assertTrue(pd.isna(self.compute(d).iloc[0].gh_active_actors))
    def test_partial_first_calendar_week_is_excluded(self):
        d=self.daily().iloc[1:]
        self.assertEqual(self.compute(d).iloc[0].week_start,pd.Timestamp('2023-01-09'))
    def test_label_waits_for_complete_future_and_availability_lag(self):
        d=self.daily();w=self.compute(d);r=w.iloc[8]
        before=weekly_rows(d,pd.Timestamp('2022-01-01'),'tool',r.label_available_at-pd.Timedelta(seconds=1))
        after=weekly_rows(d,pd.Timestamp('2022-01-01'),'tool',r.label_available_at)
        self.assertFalse(before.loc[before.week_start==r.week_start,'coverage_complete'].iloc[0])
        self.assertTrue(after.loc[after.week_start==r.week_start,'coverage_complete'].iloc[0])

class Snapshots(unittest.TestCase):
    def setUp(self):
        self.tools={'tool':{'tool_id':'tool','pypi_package':'tool','github_repo':'org/tool','github_repo_id':7,'arxiv_query':'abs:Tool'}}
        self.registries={'plan':self.tools}
    def job(self,name,source,updated,start='2023-01-02',end='2023-01-09'):
        return {'job_id':name,'plan_sha':'plan','source':source,'tool_id':'__all__' if source in ['pypi','gharchive'] else 'tool','start_date':pd.Timestamp(start).date(),'end_date':pd.Timestamp(end).date(),'updated_at':pd.Timestamp(updated,tz='UTC')}
    def row(self,job,record):
        return {'job_id':job['job_id'],'source':job['source'],'tool_id':'tool','observed_at':job['updated_at'],'payload':record}
    def select(self,jobs,rows):
        coverage,winners,eligible=select_snapshots(jobs,self.tools,self.registries)
        return latest_observations(rows,eligible,winners)[0]
    def test_newer_absent_github_type_replaces_old_positive(self):
        old=self.job('old','gharchive','2023-01-10');new=self.job('new','gharchive','2023-01-11')
        rows=[self.row(old,{'date':'2023-01-02','event_type':'IssuesEvent','event_count':3}),self.row(new,{'date':'2023-01-02','event_type':'PushEvent','event_count':2})]
        result=self.select([old,new],rows)
        self.assertEqual(len(result),1)
        self.assertEqual(next(iter(result.values()))[1]['event_type'],'PushEvent')
    def test_successful_empty_download_response_replaces_old_positive(self):
        old=self.job('old','pypi','2023-01-10');new=self.job('new','pypi','2023-01-11')
        self.assertEqual(self.select([old,new],[self.row(old,{'date':'2023-01-02','downloads':99})]),{})
    def test_different_repository_mapping_is_excluded(self):
        self.registries={'plan':{'tool':{**self.tools['tool'],'github_repo_id':999}}}
        job=self.job('old','gharchive','2023-01-10')
        coverage,winners,eligible=select_snapshots([job],self.tools,self.registries)
        self.assertEqual(eligible,{})
        self.assertEqual(coverage['gharchive']['tool'],set())
    def test_unique_actor_markers_require_exact_calendar_week(self):
        good=self.job('good','gharchive','2023-01-10')
        partial=self.job('partial','gharchive','2023-01-11',end='2023-01-05')
        sunday=self.job('sunday','gharchive','2023-01-12',start='2023-01-01',end='2023-01-08')
        marker={'date':'2023-01-02','event_type':'__all_types__','action':'__window_unique_actors__','active_actors':5}
        result=self.select([good,partial,sunday],[self.row(good,marker),self.row(partial,{**marker,'active_actors':9}),self.row(sunday,{**marker,'active_actors':20})])
        self.assertEqual(len(result),1)
        self.assertEqual(next(iter(result.values()))[1]['active_actors'],5)
    def test_naive_asof_is_explicitly_interpreted_as_utc(self):
        self.assertEqual(utc_naive('2023-01-02'),pd.Timestamp('2023-01-02'))
        self.assertEqual(utc_naive('2023-01-02T02:00:00+02:00'),pd.Timestamp('2023-01-02'))

class SourceQuality(unittest.TestCase):
    def daily(self, start='2025-05-19', days=14):
        d=pd.DataFrame(index=pd.date_range(start,periods=days))
        for col in ['dl_downloads','release_count',*GH_NUMERIC]:d[col]=1.0
        d['gh_active_actors']=np.nan
        d.loc[d.index.weekday==0,'gh_active_actors']=5
        return d
    def test_nonempty_global_table_never_certifies_capture(self):
        d=self.daily('2024-01-01',7)
        d['global_archive_table_num_rows']=5000000
        d.loc[:,GH_NUMERIC]=0
        quality=annotate_daily_quality(d,pd.Timestamp('2020-01-01'))
        self.assertTrue(quality.gh_retrieval_complete.all())
        self.assertFalse(quality.gh_capture_validated.any())
        self.assertFalse(quality.gh_primary_eligible.any())
        self.assertEqual(set(quality.gh_capture_quality),{'unvalidated_capture'})
        self.assertTrue(quality.observed_gh_event_count.eq(0).all())
    def test_degradation_masks_overlap_week_but_keeps_observed_capture(self):
        d=self.daily()
        normalized=annotate_daily_quality(d,pd.Timestamp('2020-01-01'))
        self.assertEqual(normalized.loc['2025-05-23','gh_event_count'],1)
        self.assertTrue(pd.isna(normalized.loc['2025-05-24','gh_event_count']))
        self.assertEqual(normalized.loc['2025-05-24','observed_gh_event_count'],1)
        w=weekly_rows(d,pd.Timestamp('2020-01-01'),'tool',pd.Timestamp('2025-06-02'))
        self.assertTrue(w[GH_NUMERIC].isna().all().all())
        self.assertTrue(w.observed_gh_event_count.eq(7).all())
        self.assertTrue(w.gh_retrieval_complete.all())
        self.assertFalse(w.gh_capture_validated.any())
        self.assertTrue(w.gh_capture_quality.eq('known_capture_degradation').all())
    def test_policy_cannot_reenable_uncertified_gh(self):
        policy=load_policy();policy['sources']['gharchive']['primary_eligible']=True
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'unsafe.json';path.write_text(json.dumps(policy))
            with self.assertRaisesRegex(ValueError,'unsafe'):load_policy(path)
    def test_primary_gate_distinguishes_pre_artifact_and_missing_retrieval(self):
        d=self.daily('2023-01-01',28)
        first=pd.Timestamp('2023-01-04')
        d.loc[:'2023-01-03',['dl_downloads','release_count']]=np.nan
        q=annotate_daily_quality(d,first)
        q.insert(0,'date',q.index.strftime('%Y-%m-%d'));q.insert(0,'tool_id','tool')
        w=weekly_rows(d,first,'tool',pd.Timestamp('2023-01-29'))
        result=validate_panel(q.reset_index(drop=True),w,{'tool':{}},pd.Timestamp('2023-01-29'),{'tool':first})
        self.assertEqual(result['status'],'passed',result)
        self.assertEqual(result['primary_missing_counts']['daily']['dl_downloads'],3)
        q.loc[q.date=='2023-01-09','dl_downloads']=np.nan
        bad=validate_panel(q,w,{'tool':{}},pd.Timestamp('2023-01-29'),{'tool':first})
        self.assertEqual(bad['status'],'failed')
        self.assertEqual(bad['unexpected_primary_missing_counts']['dl_downloads'],1)
    def test_gate_rejects_bad_counts_even_when_grid_is_complete(self):
        d=self.daily('2023-01-01',28);q=annotate_daily_quality(d,pd.Timestamp('2020-01-01'))
        q.insert(0,'date',q.index.strftime('%Y-%m-%d'));q.insert(0,'tool_id','tool')
        w=weekly_rows(d,pd.Timestamp('2020-01-01'),'tool',pd.Timestamp('2023-01-29'))
        q.loc[q.index[2],'dl_downloads']=-1
        result=validate_panel(q,w,{'tool':{}},pd.Timestamp('2023-01-29'),{'tool':pd.Timestamp('2020-01-01')})
        self.assertIn('daily_invalid_count:dl_downloads',result['errors'])
    def test_raw_body_check_counts_versions_once_and_detects_wrong_release(self):
        # Independent hand-authored HTTP bodies, not output from the extractor.
        d=self.daily('2023-01-01',15)
        d['release_count']=0;d.loc['2023-01-11','release_count']=1
        d.loc[d.index.weekday==0,'gh_active_actors']=1
        q=annotate_daily_quality(d,pd.Timestamp('2020-01-01'))
        q.insert(0,'date',q.index.strftime('%Y-%m-%d'));q.insert(0,'tool_id','tool')
        w=weekly_rows(d,pd.Timestamp('2020-01-01'),'tool',pd.Timestamp('2023-01-16'))
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);objects=[];eligible={};winners={};latest={}
            def store(job,source,obj):
                raw=json.dumps(obj).encode();path=job+'.json.gz';(root/path).write_bytes(gzip.compress(raw))
                objects.append({'job_id':job,'source':source,'tool_id':'tool','raw_path':path,'sha256':hashlib.sha256(raw).hexdigest(),'observed_at':'2023-01-17'})
                eligible[job]={'tools':{'tool'}}
            def ledger(records):
                fields=[{'name':k,'type':'INT64' if isinstance(v,int) else 'STRING'} for k,v in records[0].items()]
                return {'responses':[{'response':{'schema':{'fields':fields},'rows':[{'f':[{'v':r.get(f['name'])} for f in fields]} for r in records]}}]}
            pypi=[];gh=[]
            for day in pd.date_range('2023-01-02','2023-01-15'):
                text=str(day.date())
                pypi.append({'tool_id':'tool','date':text,'downloads':1,'logged_objects':3,'excluded_requests':2})
                gh.append({'tool_id':'tool','date':text,'event_type':'PushEvent','action':'','event_count':1,'active_actors':1,'missing_event_id_rows':0})
                for source in ['pypi','gharchive']:winners[(source,'tool',day.date())]=source
            gh.append({'tool_id':'tool','date':'2023-01-09','event_type':'__all_types__','action':'__window_unique_actors__','event_count':7,'active_actors':1,'missing_event_id_rows':0})
            latest[('gharchive','tool','2023-01-09','__all_types__','__window_unique_actors__')]=(None,{},'gharchive')
            store('pypi','pypi',ledger(pypi));store('gharchive','gharchive',ledger(gh))
            store('release','pypi_releases',{'info':{'name':'tool'},'releases':{'1.0':[{'upload_time_iso_8601':'2023-01-11T01:00:00Z'},{'upload_time_iso_8601':'2023-01-11T02:00:00Z'}]}})
            result=raw_spot_checks(root,objects,{'tool':{'pypi_package':'tool'}},eligible,winners,latest,q,w)
            self.assertEqual(result['status'],'passed',result)
            self.assertEqual(result['checked']['visible_release_versions'],1)
            self.assertEqual(result['checked']['visible_artifact_files'],2)
            w.loc[w.week_start==pd.Timestamp('2023-01-09'),'release_count']=2
            result=raw_spot_checks(root,objects,{'tool':{'pypi_package':'tool'}},eligible,winners,latest,q,w)
            self.assertIn('Raw distinct release-version mismatch:tool',result['errors'])

if __name__=='__main__':unittest.main()
