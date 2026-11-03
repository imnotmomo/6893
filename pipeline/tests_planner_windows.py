"""Offline calendar-window regression checks using real temporary DuckDB files."""
from datetime import date,datetime,timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import backfill as b


BOOTSTRAP_END=date(2026,11,2)


class YearlyBootstrap(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(prefix='6893-planner-window-')
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        self.registry={'tools':[{'tool_id':f'tool-{index}','pypi_package':f'tool-{index}',
                               'github_repo':f'fixture/tool-{index}','github_repo_id':index+1,
                               'arxiv_query':f'abs:"fixture tool {index}"'} for index in range(30)]}
        self.yearly={tool['tool_id'] for tool in self.registry['tools'][9:]}
        self.config={'start_date':'2023-01-01','history_end_exclusive':'2026-11-02',
                     'max_end_exclusive':'2027-01-01','arxiv_yearly_tool_ids':sorted(self.yearly)}
        self.registry_bytes=b.encode(self.registry).encode()
        self.digest=hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+self.registry_bytes).hexdigest()

    def plan(self,on_date):
        instant=datetime.combine(on_date,datetime.min.time(),tzinfo=timezone.utc)
        with patch.object(b,'now',return_value=instant),patch.object(b,'fetch_arxiv_page') as research,patch.object(b,'fetch_aggregate') as query,patch.object(b,'resume_aggregate') as resume,patch.object(b,'releases') as releases:
            b.plan(self.root,self.config,self.registry,self.digest)
        for operation in [research,query,resume,releases]:operation.assert_not_called()

    def history(self):
        with b.connect(data_dir=self.root) as con:
            return con.execute("SELECT job_id,tool_id,start_date,end_date FROM backfill.jobs WHERE plan_sha=? AND source='arxiv' AND generation='history' AND state<>'superseded' ORDER BY tool_id,start_date,end_date",[self.digest]).fetchall()

    def seed_completed_bootstrap(self):
        self.plan(BOOTSTRAP_END)
        with b.connect(read_only=False,data_dir=self.root) as con:
            con.execute("UPDATE backfill.jobs SET state='done'")
            identifier=con.execute("SELECT job_id FROM backfill.jobs WHERE source='pypi' LIMIT 1").fetchone()[0]
            con.execute('INSERT INTO backfill.budget VALUES (?,?,?)',[identifier,123456,datetime(2026,11,2,tzinfo=timezone.utc)])
            self.budget=con.execute('SELECT * FROM backfill.budget ORDER BY job_id').fetchall()
        self.bootstrap={row[0] for row in self.history() if row[1] in self.yearly}
        self.assertEqual(len(self.bootstrap),84)

    def assert_budget_and_registry_unchanged(self):
        with b.connect(data_dir=self.root) as con:
            self.assertEqual(con.execute('SELECT * FROM backfill.budget ORDER BY job_id').fetchall(),self.budget)
            self.assertEqual(con.execute('SELECT count(*) FROM backfill.records').fetchone()[0],0)
        self.assertEqual(b.encode(self.registry).encode(),self.registry_bytes)
        self.assertEqual(hashlib.sha256(b.FORMAT_VERSION.encode()+b'\0'+self.registry_bytes).hexdigest(),self.digest)

    def test_november_two_plan_is_identical_and_has_only_84_year_windows(self):
        self.seed_completed_bootstrap();before=self.history();self.plan(BOOTSTRAP_END)
        self.assertEqual(self.history(),before)
        self.assertEqual({row[0] for row in self.history() if row[1] in self.yearly},self.bootstrap)
        self.assertTrue(all(row[3]<=BOOTSTRAP_END for row in self.history()))
        self.assert_budget_and_registry_unchanged()

    def test_november_three_adds_one_day_without_expanding_2026_bootstrap(self):
        self.seed_completed_bootstrap();self.plan(date(2026,11,3))
        yearly=[row for row in self.history() if row[1] in self.yearly]
        self.assertTrue(self.bootstrap<={row[0] for row in yearly})
        extra=[row for row in yearly if row[0] not in self.bootstrap]
        self.assertEqual(len(extra),21)
        self.assertTrue(all((row[2],row[3])==(BOOTSTRAP_END,date(2026,11,3)) for row in extra))
        self.assertFalse(any(row[2]==date(2026,1,1) and row[3]==date(2026,11,3) for row in yearly))
        self.assert_budget_and_registry_unchanged()

    def test_december_outage_catchup_is_gap_free_and_preserves_bootstrap_ids(self):
        self.seed_completed_bootstrap();self.plan(date(2026,12,15));self.plan(date(2026,12,15))
        yearly=[row for row in self.history() if row[1] in self.yearly]
        self.assertEqual(len(yearly),126)
        self.assertTrue(self.bootstrap<={row[0] for row in yearly})
        for tool in self.yearly:
            windows=[(row[2],row[3]) for row in yearly if row[1]==tool]
            self.assertEqual(windows[-2:],[(BOOTSTRAP_END,date(2026,12,1)),(date(2026,12,1),date(2026,12,15))])
            self.assertEqual(windows[0][0],date(2023,1,1))
            self.assertTrue(all(left[1]==right[0] for left,right in zip(windows,windows[1:])))
        ordinary=[row for row in self.history() if row[1] not in self.yearly]
        self.assertTrue(any(row[2]==date(2026,12,1) and row[3]==date(2026,12,15) for row in ordinary))
        self.assert_budget_and_registry_unchanged()

    def test_superseding_does_not_touch_partial_or_post_bootstrap_work(self):
        self.seed_completed_bootstrap();tool=sorted(self.yearly)[0]
        with b.connect(read_only=False,data_dir=self.root) as con:
            partial=b.add_job(con,self.digest,'arxiv',tool,date(2023,1,1),date(2023,2,1),'history',2)
            untouched=b.add_job(con,self.digest,'arxiv',tool,date(2023,2,1),date(2023,3,1),'history',2)
            incremental=b.add_job(con,self.digest,'arxiv',tool,BOOTSTRAP_END,date(2026,11,3),'history',2)
            con.execute("UPDATE backfill.jobs SET attempts=1,cursor=100 WHERE job_id=?",[partial])
        self.plan(date(2026,11,3))
        with b.connect(data_dir=self.root) as con:
            self.assertEqual(con.execute('SELECT state,attempts,cursor FROM backfill.jobs WHERE job_id=?',[partial]).fetchone(),('pending',1,100))
            self.assertEqual(con.execute('SELECT state FROM backfill.jobs WHERE job_id=?',[untouched]).fetchone()[0],'superseded')
            self.assertEqual(con.execute('SELECT state FROM backfill.jobs WHERE job_id=?',[incremental]).fetchone()[0],'pending')
        self.assert_budget_and_registry_unchanged()


if __name__=='__main__':unittest.main()
