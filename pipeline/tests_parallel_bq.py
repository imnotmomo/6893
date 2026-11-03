"""Offline financial/concurrency regressions; all network clients are mocked."""
import concurrent.futures
import contextlib
import copy
from datetime import date, timedelta
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import parallel_bq as runner
import bigquery_sources as sources


MiB=1024**2


class ParallelBudget(unittest.TestCase):
    def test_four_workers_cannot_reserve_above_global_cap(self):
        b=runner.b
        with tempfile.TemporaryDirectory(prefix='6893-parallel-budget-') as directory:
            root=Path(directory)
            with b.connect(read_only=False,data_dir=root) as con:
                con.execute(b.SCHEMA)
                for offset in range(4):
                    start=date(2023,1,2)+timedelta(days=offset)
                    b.add_job(con,'fixture-plan','pypi','__all__',start,start+timedelta(days=1),'history',0)
                result=con.execute('SELECT * FROM backfill.jobs ORDER BY start_date')
                columns=[item[0] for item in result.description]
                jobs=[dict(zip(columns,row)) for row in result.fetchall()]
            config={'bigquery_project':'offline-fixture','max_bytes_per_query':20*MiB,
                    'max_total_query_bytes':25*MiB,'allow_billed_queries':True}
            submitted=[];append_lock=threading.Lock()

            def fake_fetch(source,project,registry,start,end,cap,**kwargs):
                if kwargs.get('dry_run_only'):
                    return {'ok':True,'status':'dry_run_ok',
                            'request_metadata':{'estimated_bytes_processed':7*MiB}}
                with append_lock:submitted.append((start,cap))
                return {'ok':False,'status':'pending','source':source,'rows':[],
                        'job_id':'offline_'+start.isoformat().replace('-',''),
                        'request_metadata':{'project':project,'maximum_bytes_billed':cap},
                        'error':None}

            with patch.object(b,'fetch_aggregate',side_effect=fake_fetch),contextlib.redirect_stdout(io.StringIO()):
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(lambda job:runner.perform(job,root,config,{'tools':[]}),jobs))
            with b.connect(read_only=False,data_dir=root) as con:
                reservations=con.execute('SELECT job_id,reserved_bytes,reserved_at FROM backfill.budget ORDER BY job_id').fetchall()
                self.assertEqual(sum(row[1] for row in reservations),20*MiB)
                self.assertLessEqual(sum(row[1] for row in reservations),config['max_total_query_bytes'])
                result=con.execute("SELECT * FROM backfill.jobs WHERE state='query_pending' ORDER BY job_id LIMIT 1")
                pending=dict(zip([item[0] for item in result.description],result.fetchone()))
                self.assertEqual(con.execute("SELECT count(*) FROM backfill.jobs WHERE state='blocked'").fetchone()[0],2)
            self.assertEqual(len(submitted),2)
            self.assertTrue(all(cap==10*MiB for _,cap in submitted))

            # Polling a persisted pending result must not submit or reserve again.
            previous=json.loads(pending['pending'])
            with patch.object(b,'fetch_aggregate') as no_submit,patch.object(b,'resume_aggregate',return_value=copy.deepcopy(previous)) as resume,contextlib.redirect_stdout(io.StringIO()):
                runner.perform(pending,root,config,{'tools':[]})
            no_submit.assert_not_called();resume.assert_called_once()
            with b.connect(data_dir=root) as con:
                self.assertEqual(con.execute('SELECT job_id,reserved_bytes,reserved_at FROM backfill.budget ORDER BY job_id').fetchall(),reservations)


class StableJobIdentity(unittest.TestCase):
    def test_retry_reuses_job_but_deliberate_refresh_uses_new_job(self):
        class OfflineClient:
            def __init__(self):self.jobs={};self.created=[];self.ledger=[]
            def request(self,method,path,payload=None,**kwargs):
                if method=='GET':return self.jobs.get(path.rsplit('/',1)[-1])
                if payload['configuration'].get('dryRun'):
                    return {'statistics':{'totalBytesProcessed':str(7*MiB)}}
                identifier=payload['jobReference']['jobId']
                self.created.append(identifier)
                self.jobs[identifier]=copy.deepcopy(payload)
                return self.jobs[identifier]
        client=OfflineClient()
        tools={'tools':[{'tool_id':'fixture','pypi_package':'fixture'}]}

        def pending(_client,result,_timeout):
            return {**result,'ok':False,'status':'pending'}

        with patch.object(sources,'_Client',return_value=client),patch.object(sources,'_poll',side_effect=pending):
            first=sources.fetch_aggregate('pypi','offline-fixture',tools,'2023-01-02','2023-01-03',10*MiB)
            retry=sources.fetch_aggregate('pypi','offline-fixture',tools,'2023-01-02','2023-01-03',10*MiB)
            refresh=sources.fetch_aggregate('pypi','offline-fixture',tools,'2023-01-02','2023-01-03',10*MiB,job_id='radar_refresh_fixture')
        self.assertEqual(first['job_id'],retry['job_id'])
        self.assertNotEqual(first['job_id'],refresh['job_id'])
        self.assertEqual(len(client.created),2)
        self.assertEqual(len(set(client.created)),2)


if __name__=='__main__':unittest.main()
