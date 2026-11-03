"""Offline arXiv rate-limit regressions; real temporary DB, no network calls."""
import contextlib
from datetime import date,datetime,timedelta,timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import arxiv_backfill as arxiv
import backfill as b


NOW=datetime(2026,11,2,21,30,tzinfo=timezone.utc)


def limited_response(retry_after=None):
    headers={} if retry_after is None else {'Retry-After':str(retry_after)}
    return b'rate limited',{'status':429,'reason':'Too Many Requests',
            'response_url':arxiv.API_URL,'headers':headers,'body_truncated':False}


class LibraryRateLimit(unittest.TestCase):
    def test_429_returns_after_one_http_attempt_for_every_retry_after(self):
        for retry_after in [None,5,60,360]:
            with self.subTest(retry_after=retry_after):
                with patch.object(arxiv,'_request_once',return_value=limited_response(retry_after)) as request,patch.object(arxiv.time,'sleep') as sleep:
                    result=arxiv.fetch_arxiv_page('abs:Fixture','2023-01-01','2023-01-02')
                request.assert_called_once();sleep.assert_not_called()
                self.assertFalse(result['ok'])
                self.assertEqual(result['attempt_count'],1)
                self.assertEqual(len(result['attempts']),1)
                self.assertEqual(result['http']['status'],429)
                self.assertGreaterEqual(result['error']['retry_after_seconds'],arxiv.MIN_REQUEST_INTERVAL_SECONDS)
                if retry_after is not None:self.assertGreaterEqual(result['error']['retry_after_seconds'],retry_after)

    def test_successive_http_starts_are_spaced_by_at_least_ten_seconds(self):
        class Response:
            reason='OK';headers={}
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def getcode(self):return 200
            def geturl(self):return arxiv.API_URL
            def read(self,_limit):return b'<feed/>'
        self.assertGreaterEqual(arxiv.MIN_REQUEST_INTERVAL_SECONDS,10)
        with patch.object(arxiv,'_LAST_REQUEST_STARTED',None),patch.object(arxiv._OPENER,'open',side_effect=lambda *args,**kwargs:Response()) as opener,patch.object(arxiv.time,'monotonic',side_effect=[100,104,110]),patch.object(arxiv.time,'sleep') as sleep:
            arxiv._request_once(arxiv.API_URL)
            arxiv._request_once(arxiv.API_URL)
        self.assertEqual(opener.call_count,2)
        sleep.assert_called_once_with(6)


class DispatcherRateLimit(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix='6893-rate-limit-')
        self.addCleanup(self.directory.cleanup)
        self.project=Path(self.directory.name);self.data=self.project/'data';configdir=self.project/'config';configdir.mkdir()
        self.registry={'tools':[{'tool_id':name,'name':name,'pypi_package':name,'github_repo':'fixture/'+name,'github_repo_id':index+1,'arxiv_query':'abs:'+name} for index,name in enumerate(['first','second'])]}
        (configdir/'tools.json').write_text(json.dumps(self.registry))
        config={'data_dir':str(self.data),'tool_registry':'config/tools.json','start_date':'2023-01-01','max_end_exclusive':'2027-01-01','minimum_free_GiB':15,'max_bytes_per_query':50*1024**3,'max_total_query_bytes':12*1024**4,'bigquery_project':'offline-fixture','allow_billed_queries':True}
        self.config=configdir/'backfill.json';self.config.write_text(json.dumps(config))
        self.plan=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+b.encode(self.registry).encode()).hexdigest()
        with patch.object(b,'now',return_value=NOW),b.connect(read_only=False,data_dir=self.data) as con:
            con.execute(b.SCHEMA)
            for tool in ['first','second']:b.add_job(con,self.plan,'arxiv',tool,date(2023,1,1),date(2023,2,1),'history',0)
            paid=b.add_job(con,self.plan,'pypi','__all__',date(2023,1,1),date(2023,1,2),'history',0)
            con.execute("UPDATE backfill.jobs SET state='done' WHERE job_id=?",[paid])
            con.execute('INSERT INTO backfill.budget VALUES (?,?,?)',[paid,123456,NOW])

    def state(self):
        with b.connect(data_dir=self.data) as con:
            return {'jobs':con.execute('SELECT job_id,state,attempts,next_attempt,error,pending FROM backfill.jobs ORDER BY job_id').fetchall(),
                    'budget':con.execute('SELECT * FROM backfill.budget ORDER BY job_id').fetchall(),
                    'records':con.execute('SELECT count(*) FROM backfill.records').fetchone()[0],
                    'objects':con.execute('SELECT count(*) FROM backfill.objects').fetchone()[0]}

    def run_main(self):
        with patch.object(b,'ROOT',self.project),patch.object(b,'now',return_value=NOW),patch.object(b.time,'sleep'),patch.object(b.shutil,'disk_usage',return_value=types.SimpleNamespace(free=100*1024**3)),patch.object(sys,'argv',['backfill.py','--config',str(self.config),'--max-tasks','20','--max-seconds','600']),contextlib.redirect_stdout(io.StringIO()):
            return b.main()

    def test_active_cooldown_skips_planner_all_apis_and_budget_writes(self):
        (self.data/'.arxiv-cooldown.json').write_text(b.encode({'until':NOW+timedelta(seconds=300),'reason':'fixture 429'}))
        before=self.state()
        with patch.object(b,'plan') as planner,patch.object(b,'fetch_arxiv_page') as research,patch.object(b,'fetch_aggregate') as query,patch.object(b,'resume_aggregate') as resume,patch.object(b,'releases') as releases,patch.object(b,'reserve') as reserve:
            self.assertEqual(self.run_main(),0)
        for operation in [planner,research,query,resume,releases,reserve]:operation.assert_not_called()
        self.assertEqual(self.state(),before)

    def test_429_defers_one_job_breaks_batch_and_respects_longer_retry_after(self):
        # One execution per fixture: test both the minimum global cooldown and a
        # longer source Retry-After. The second arXiv job must remain untouched.
        for retry_after,expected_delay in [(60,120),(360,360)]:
            with self.subTest(retry_after=retry_after):
                before=self.state()
                with patch.object(b,'plan'),patch.object(b,'fetch_arxiv_page',side_effect=arxiv.fetch_arxiv_page) as research,patch.object(arxiv,'_request_once',return_value=limited_response(retry_after)) as request,patch.object(b,'fetch_aggregate') as query,patch.object(b,'resume_aggregate') as resume,patch.object(b,'releases') as releases,patch.object(b,'reserve') as reserve:
                    self.assertEqual(self.run_main(),0)
                research.assert_called_once();request.assert_called_once()
                for operation in [query,resume,releases,reserve]:operation.assert_not_called()
                marker=json.loads((self.data/'.arxiv-cooldown.json').read_text())
                self.assertGreaterEqual(datetime.fromisoformat(marker['until']),NOW+timedelta(seconds=expected_delay))
                with b.connect(data_dir=self.data) as con:
                    state=con.execute("SELECT state,attempts,next_attempt FROM backfill.jobs WHERE source='arxiv' ORDER BY attempts DESC").fetchall()
                self.assertEqual(state[0][0:2],('retry',1))
                self.assertGreaterEqual(state[0][2],NOW+timedelta(seconds=expected_delay))
                self.assertEqual(state[1][0:2],('pending',0))
                after=self.state();self.assertEqual(after['budget'],before['budget']);self.assertEqual(after['records'],before['records']);self.assertEqual(after['objects'],before['objects'])
                # Reset only this isolated fixture between independent subcases.
                with b.connect(read_only=False,data_dir=self.data) as con:
                    con.execute("UPDATE backfill.jobs SET state='pending',attempts=0,next_attempt=?,error=NULL,pending=NULL WHERE source='arxiv'",[NOW])
                (self.data/'.arxiv-cooldown.json').unlink()


if __name__=='__main__':unittest.main()
