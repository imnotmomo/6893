"""Synthetic local Git tests; no public clones, credentials or paid queries."""
import copy
from datetime import date
import gzip
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('git_history',ROOT/'collect_git_history.py')
collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)


class GitFixture(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);self.repo=self.root/'working';self.env=collector.git_environment()
        self.git('init','-b','main',str(self.repo),cwd=self.root)
        self.git('config','user.name','Synthetic fixture')
        self.git('config','user.email','fixture@example.invalid')
        self.commit('root.txt','root','2023-01-01T12:00:00Z')
        self.root_sha=self.git('rev-parse','HEAD')
        self.git('switch','-c','topic')
        self.commit('branch.txt','branch','2023-01-09T12:00:00Z')
        self.branch_sha=self.git('rev-parse','HEAD')
        self.git('switch','main')
        self.commit('main.txt','main','2023-01-08T12:00:00Z')
        merge_env={**self.env,'GIT_AUTHOR_DATE':'2027-01-01T12:00:00Z','GIT_COMMITTER_DATE':'2023-01-07T12:00:00Z'}
        self.git('merge','--no-ff','topic','-m','Merge topic with deliberately inverted time',env=merge_env)
        self.tip=self.git('rev-parse','HEAD')
        self.bare=self.root/'bare.git'
        self.git('clone','--bare','--no-local',str(self.repo),str(self.bare),cwd=self.root)

    def git(self,*args,cwd=None,env=None):
        result=subprocess.run(['git',*args],cwd=cwd or self.repo,env=env or self.env,
                              check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        return result.stdout.strip()

    def commit(self,name,message,when):
        (self.repo/name).write_text(message)
        self.git('add',name)
        self.git('commit','-m',message,env={**self.env,'GIT_AUTHOR_DATE':when,'GIT_COMMITTER_DATE':when})

    def inspect(self):
        return collector.inspect_repository(self.bare,self.root/'metadata.tsv',time.monotonic()+10,
                                            self.env,collector.stamp('2026-11-02T20:00:00Z'))

    def test_complete_graph_and_strict_first_parent_chain(self):
        records,first_parent,report=self.inspect()
        self.assertEqual(report['tip_sha'],self.tip)
        self.assertEqual(report['default_branch_at_collection'],'main')
        self.assertEqual(report['reachable_commits'],4)
        self.assertEqual(report['first_parent_commits'],3)
        self.assertNotIn(self.branch_sha,first_parent)
        self.assertEqual(len(records),4)
        self.assertTrue(all(parent in records for row in records.values() for parent in row['parents']))

    def test_time_inversions_and_future_author_are_flagged_without_pruning(self):
        records,_,report=self.inspect()
        self.assertEqual(report['date_anomalies']['child_committer_before_parent'],2)
        self.assertEqual(report['date_anomalies']['author_after_observed'],1)
        self.assertIn('author_after_committer',records[self.tip]['date_anomaly_flags'])
        self.assertEqual(len(records[self.tip]['parent_time_inversions']),2)
        self.assertIn(self.branch_sha,records)

    def test_weekly_counts_use_committer_utc_and_distinguish_mainline(self):
        records,first_parent,_=self.inspect()
        rows=collector.weekly_rows('fixture',records,first_parent,date(2023,1,1),date(2023,1,23))
        weeks={row['week_start_utc']:row for row in rows}
        self.assertEqual(weeks['2022-12-26']['git_reachable_commit_count'],1)
        self.assertFalse(weeks['2022-12-26']['complete_utc_week'])
        self.assertEqual(weeks['2023-01-02']['git_reachable_commit_count'],2)
        self.assertEqual(weeks['2023-01-02']['git_first_parent_commit_count'],2)
        self.assertEqual(weeks['2023-01-09']['git_reachable_commit_count'],1)
        self.assertEqual(weeks['2023-01-09']['git_first_parent_commit_count'],0)
        self.assertTrue(weeks['2023-01-09']['complete_utc_week'])
        self.assertTrue(all(row['primary_eligible'] is False for row in rows))

    def test_shallow_repository_is_rejected(self):
        shallow=self.root/'shallow.git'
        self.git('clone','--bare','--depth','1',self.repo.as_uri(),str(shallow),cwd=self.root)
        with self.assertRaisesRegex(ValueError,'Shallow'):
            collector.inspect_repository(shallow,self.root/'shallow.tsv',time.monotonic()+10,
                                         self.env,collector.stamp('2026-11-02T20:00:00Z'))

    def test_missing_parent_or_duplicate_sha_is_rejected(self):
        records,_,_=self.inspect();broken=copy.deepcopy(records);broken.pop(self.root_sha)
        with self.assertRaisesRegex(ValueError,'parent is absent'):collector.verify_graph(broken,self.tip)
        duplicate=self.root/'duplicate.tsv'
        row=self.root_sha+'\t1\t1\t\n';duplicate.write_text(row+row)
        with self.assertRaisesRegex(ValueError,'Duplicate'):collector.parse_log(duplicate)

    def test_compressed_metadata_is_deterministic_and_preserves_parent_links(self):
        records,_,_=self.inspect();one=self.root/'one.jsonl.gz';two=self.root/'two.jsonl.gz'
        collector.write_commits(one,records);collector.write_commits(two,records)
        self.assertEqual(collector.digest(one),collector.digest(two))
        with gzip.open(one,'rt') as stream:decoded=[json.loads(line) for line in stream]
        self.assertEqual({row['sha'] for row in decoded},set(records))
        self.assertEqual(next(row for row in decoded if row['sha']==self.tip)['parents'],records[self.tip]['parents'])
        with self.assertRaises(FileExistsError):collector.write_commits(one,records)


class OperationalContracts(unittest.TestCase):
    def test_no_fabricated_zero_before_first_retained_commit(self):
        sha='a'*40;unix=int(collector.stamp('2024-01-03T12:00:00Z').timestamp())
        records={sha:{'sha':sha,'committer_unix':unix,'author_unix':unix,'parents':[]}}
        rows=collector.weekly_rows('new',records,{sha},date(2023,12,25),date(2024,1,15))
        self.assertIsNone(rows[0]['git_reachable_commit_count'])
        self.assertIsNone(rows[0]['git_first_parent_commit_count'])
        self.assertEqual(rows[0]['reachable_coverage_days'],0)
        self.assertEqual(rows[1]['git_reachable_commit_count'],1)
        self.assertFalse(rows[1]['complete_utc_week'])
        self.assertEqual(rows[2]['git_reachable_commit_count'],0)

    def test_command_deadline_terminates_child(self):
        start=time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            collector.command([sys.executable,'-c','import time; time.sleep(5)'],
                              time.monotonic()+0.1,collector.git_environment())
        self.assertLess(time.monotonic()-start,2)

    def test_disk_cap_and_private_git_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'oversized').write_bytes(b'x'*1024)
            with self.assertRaisesRegex(RuntimeError,'disk cap'):collector.guard(root,0,0.00000001)
        env=collector.git_environment()
        self.assertEqual(env['GIT_CONFIG_GLOBAL'],collector.os.devnull)
        self.assertEqual(env['GIT_TERMINAL_PROMPT'],'0')
        self.assertNotIn('GIT_CONFIG_COUNT',env)


if __name__=='__main__':unittest.main(verbosity=2)
