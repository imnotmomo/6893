"""Real local Git fixtures and retained-identity/count semantics; no cloud calls."""
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
spec=importlib.util.spec_from_file_location('git_contributors',ROOT/'collect_git_contributors.py')
collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)


class RealGitFixture(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);self.repo=self.root/'working';self.env=collector.git_environment()
        self.git('init','-b','main',str(self.repo),cwd=self.root)
        self.git('config','user.name','Fixture committer');self.git('config','user.email','committer@example.invalid')
        self.commit('root.txt','2022-12-01T12:00:00Z','Alice First','ALICE@Example.Invalid')
        self.root_sha=self.git('rev-parse','HEAD')
        self.git('switch','-c','topic')
        self.commit('branch.txt','2023-01-09T12:00:00Z','Bób\tContributor','bob@example.invalid')
        self.branch_sha=self.git('rev-parse','HEAD')
        self.git('switch','main')
        self.commit('main.txt','2023-01-08T12:00:00Z','Alice Renamed','alice@example.invalid')
        self.main_sha=self.git('rev-parse','HEAD')
        merge_env={**self.env,'GIT_AUTHOR_DATE':'2027-01-01T12:00:00Z',
                   'GIT_COMMITTER_DATE':'2023-01-07T12:00:00Z',
                   'GIT_AUTHOR_NAME':'dependabot[bot]',
                   'GIT_AUTHOR_EMAIL':'49699333+dependabot[bot]@users.noreply.github.com'}
        self.git('merge','--no-ff','topic','-m','Synthetic timestamp inversion',env=merge_env)
        self.tip=self.git('rev-parse','HEAD');self.bare=self.root/'bare.git'
        self.git('clone','--bare','--no-local',str(self.repo),str(self.bare),cwd=self.root)

    def git(self,*args,cwd=None,env=None):
        value=subprocess.run(['git',*args],cwd=cwd or self.repo,env=env or self.env,
                             capture_output=True,text=True,check=True)
        return value.stdout.strip()

    def commit(self,name,when,author,email):
        (self.repo/name).write_text(name);self.git('add',name)
        self.git('commit','-m','Synthetic commit',env={**self.env,'GIT_AUTHOR_DATE':when,
                 'GIT_COMMITTER_DATE':when,'GIT_AUTHOR_NAME':author,'GIT_AUTHOR_EMAIL':email})

    def inspect(self):
        return collector.inspect_repository(self.bare,self.root/'metadata.nul',time.monotonic()+10,
                                            self.env,collector.stamp('2026-11-03T03:00:00Z'))

    def test_graph_is_complete_and_separate_first_parent_is_verified(self):
        records,first,info=self.inspect()
        self.assertEqual(info['tip_sha'],self.tip);self.assertEqual(info['reachable_commits'],4)
        self.assertEqual(info['first_parent_commits'],3);self.assertNotIn(self.branch_sha,first)
        self.assertTrue(all(parent in records for row in records.values() for parent in row['parents']))

    def test_email_identity_deduplicates_name_and_case_changes_and_retains_roles(self):
        records,_,_=self.inspect()
        self.assertEqual(records[self.root_sha]['author_id'],records[self.main_sha]['author_id'])
        self.assertNotEqual(records[self.main_sha]['author_id'],records[self.main_sha]['committer_id'])
        self.assertEqual(records[self.main_sha]['author_identity_basis'],'email')
        self.assertEqual(records[self.root_sha]['committer_id'],records[self.branch_sha]['committer_id'])

    def test_bot_match_is_explicit_heuristic_and_timestamps_are_not_pruned(self):
        records,_,info=self.inspect()
        self.assertTrue(records[self.tip]['author_bot_heuristic'])
        self.assertFalse(records[self.main_sha]['author_bot_heuristic'])
        self.assertEqual(info['date_anomalies']['author_after_observed'],1)
        self.assertEqual(info['date_anomalies']['child_committer_before_parent'],2)
        self.assertEqual(len(records),4)

    def test_export_has_only_pseudonyms_and_is_deterministically_compressed(self):
        records,_,_=self.inspect();one=self.root/'one.gz';two=self.root/'two.gz'
        collector.write_commits(one,records);collector.write_commits(two,records)
        self.assertEqual(collector.digest(one),collector.digest(two))
        content=gzip.decompress(one.read_bytes()).decode()
        for raw in ['example.invalid','users.noreply','Alice First','Bób','dependabot[bot]']:
            self.assertNotIn(raw,content)
        decoded=[json.loads(line) for line in content.splitlines()]
        self.assertEqual(len(decoded),4);self.assertTrue(all(len(row['author_id'])==64 for row in decoded))
        with self.assertRaises(FileExistsError):collector.write_commits(one,records)

    def test_new_authors_use_pre_study_history_and_mainline_scope_separately(self):
        records,first,_=self.inspect()
        rows=collector.weekly_rows('fixture',records,first,date(2023,1,1),date(2023,1,23),'2026-11-03T03:00:00Z')
        weeks={row['week_start_utc']:row for row in rows}
        # Alice predates the study; only the bot is new on the January2 week.
        self.assertEqual(weeks['2023-01-02']['git_reachable_new_author_count'],1)
        self.assertEqual(weeks['2023-01-02']['git_reachable_author_count'],2)
        self.assertEqual(weeks['2023-01-02']['git_reachable_bot_author_commit_count'],1)
        self.assertEqual(weeks['2023-01-09']['git_reachable_new_author_count'],1)
        self.assertEqual(weeks['2023-01-09']['git_first_parent_new_author_count'],0)
        self.assertTrue(all(row['historical_public_availability']=='unknown' and row['primary_eligible'] is False for row in rows))
        self.assertTrue(all(row['observed_at']=='2026-11-03T03:00:00Z' for row in rows))
        self.assertTrue(all(set(row)==set(collector.WEEK_FIELDS) for row in rows))
        self.assertFalse((self.root/'metadata.nul').exists())

    def test_shallow_graph_is_rejected(self):
        shallow=self.root/'shallow.git';self.git('clone','--bare','--depth','1',self.repo.as_uri(),str(shallow),cwd=self.root)
        with self.assertRaisesRegex(ValueError,'Shallow'):
            collector.inspect_repository(shallow,self.root/'shallow.nul',time.monotonic()+10,self.env,
                                         collector.stamp('2026-11-03T03:00:00Z'))

    def test_missing_parent_and_duplicate_metadata_are_rejected(self):
        records,_,_=self.inspect();broken=copy.deepcopy(records);broken.pop(self.root_sha)
        with self.assertRaisesRegex(ValueError,'parent is absent'):collector.verify_graph(broken,self.tip)
        duplicate=self.root/'duplicate.nul'
        row=b'\0'.join([self.root_sha.encode(),b'1',b'1',b'',b'A',b'a@example.invalid',b'C',b'c@example.invalid'])+b'\0\n'
        duplicate.write_bytes(row+row)
        with self.assertRaisesRegex(ValueError,'Duplicate'):collector.parse_log(duplicate)


class IdentityAndWindows(unittest.TestCase):
    def record(self,num,when,author='alice',committer='maintainer'):
        unix=int(collector.stamp(when).timestamp())
        return {'sha':f'{num:040x}','committer_unix':unix,'author_unix':unix,'parents':[],
                'author_id':author,'committer_id':committer,'author_bot_heuristic':False,
                'committer_bot_heuristic':False}

    def test_identity_normalization_fallback_missing_and_separation(self):
        a=collector.canonical_identity('Alice',' ALICE@Example.Invalid ')
        b=collector.canonical_identity('Renamed Person','alice@example.invalid')
        self.assertEqual(a['id'],b['id'])
        self.assertNotEqual(a['id'],collector.canonical_identity('Alice','other@example.invalid')['id'])
        self.assertEqual(collector.canonical_identity('Ａlice  Name','')['id'],collector.canonical_identity('Alice Name','')['id'])
        self.assertEqual(collector.canonical_identity('','')['basis'],'missing')
        self.assertIsNone(collector.canonical_identity('','')['id'])
        self.assertFalse(collector.canonical_identity('Robot expert','person@example.invalid')['bot_heuristic'])

    def test_distinct_rolling_union_is_not_sum_of_weekly_uniques(self):
        records={r['sha']:r for r in [self.record(1,'2022-11-01T00:00:00Z'),
                 self.record(2,'2023-01-02T00:00:00Z'),self.record(3,'2023-01-09T00:00:00Z'),
                 self.record(4,'2023-01-16T00:00:00Z'),self.record(5,'2023-01-23T00:00:00Z')]}
        rows=collector.weekly_rows('t',records,set(records),date(2023,1,1),date(2023,1,30))
        row=next(row for row in rows if row['week_start_utc']=='2023-01-23')
        self.assertEqual(row['git_reachable_author_count_4w'],1)
        self.assertEqual(row['git_reachable_commit_count_4w'],4)
        self.assertEqual(row['git_reachable_author_count_previous_4w'],0)
        self.assertEqual(row['git_reachable_author_growth_4w'],1)

    def test_missing_identity_keeps_observed_lower_bound_and_marks_exact_unknown(self):
        items=[self.record(1,'2022-11-01T00:00:00Z'),self.record(2,'2023-01-09T00:00:00Z',None)]
        records={r['sha']:r for r in items}
        row=next(row for row in collector.weekly_rows('t',records,set(records),date(2023,1,1),date(2023,1,23)) if row['week_start_utc']=='2023-01-09')
        self.assertEqual(row['git_reachable_observed_author_count'],0)
        self.assertEqual(row['git_reachable_author_identity_missing_commits'],1)
        self.assertIsNone(row['git_reachable_author_count'])
        self.assertIsNone(row['git_reachable_author_count_4w'])

    def test_no_pre_history_zeros_and_partial_rolling_windows_stay_unknown(self):
        item=self.record(1,'2023-01-11T00:00:00Z');records={item['sha']:item}
        rows=collector.weekly_rows('t',records,set(records),date(2023,1,1),date(2023,1,26))
        self.assertIsNone(rows[0]['git_reachable_author_count'])
        self.assertIsNone(rows[0]['git_reachable_commit_count'])
        self.assertFalse(rows[-1]['complete_utc_week'])
        self.assertIsNone(rows[-1]['git_reachable_author_count_4w'])
        self.assertLess(rows[-1]['git_reachable_rolling_4w_coverage_days'],28)

    def test_current_partial_week_cannot_leak_into_earlier_features(self):
        a=self.record(1,'2022-11-01T00:00:00Z');b=self.record(2,'2023-01-16T00:00:00Z')
        before={x['sha']:x for x in [a,b]};after={**before}
        future=self.record(3,'2023-01-24T00:00:00Z','new-person');after[future['sha']]=future
        r1=collector.weekly_rows('t',before,set(before),date(2023,1,1),date(2023,1,30))
        r2=collector.weekly_rows('t',after,set(after),date(2023,1,1),date(2023,1,30))
        self.assertEqual(r1[:-1],r2[:-1])

    def test_nul_format_rejects_incomplete_record_without_exposing_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'bad';p.write_bytes(b'alice@example.invalid\0broken')
            with self.assertRaisesRegex(ValueError,'Incomplete NUL'):collector.parse_log(p)

    def test_same_pseudonym_with_bot_name_variant_is_not_counted_twice(self):
        one=self.record(1,'2023-01-02T00:00:00Z');two=self.record(2,'2023-01-03T00:00:00Z')
        two['author_bot_heuristic']=True;records={one['sha']:one,two['sha']:two}
        rows=collector.weekly_rows('t',records,set(records),date(2023,1,2),date(2023,1,9))
        self.assertEqual(rows[0]['git_reachable_observed_author_count'],1)
        self.assertEqual(rows[0]['git_reachable_observed_bot_author_count'],1)
        self.assertEqual(rows[0]['git_reachable_observed_nonbot_heuristic_author_count'],0)


if __name__=='__main__':unittest.main(verbosity=2)
