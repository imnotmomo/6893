from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import refresh_full_scope as module

UTC=timezone.utc


class PublicRefresh(unittest.TestCase):
    def setUp(self):
        self.expander=patch.object(module,'expand_source',side_effect=lambda source,*args:source).start()
        self.addCleanup(patch.stopall)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.project=Path(self.temp.name)/'project';self.data=self.project/'data'
        (self.project/'config').mkdir(parents=True)
        self.registry=self.project/'config/tools.study.json'
        self.registry.write_text(json.dumps({'tools':[{'tool_id':str(i)} for i in range(30)]}))
        self.cohort=hashlib.sha256(self.registry.read_bytes()).hexdigest()
        self.now=datetime(2026,11,3,4,tzinfo=UTC)
        self.research=self.data/'raw/research_v1/backfill';self.research.mkdir(parents=True)
        self.research_manifest={'status':'complete','history_end_exclusive':'2026-11-03'}
        self.write_research(self.research_manifest)
        self.git=self.data/'raw/github_git_contributors/runs/current';self.git.mkdir(parents=True)
        self.git_manifest={'status':'complete','registry_sha256':self.cohort,'requested_tools':30,
                           'completed_tools':30,'created_at':(self.now-timedelta(hours=1)).isoformat()}
        self.write_git(self.git_manifest)
        self.depsdev=self.data/'raw/depsdev/full-scope-initial';self.depsdev.mkdir(parents=True)
        self.depsdev_manifest={'status':'complete_with_source_gaps','updated_at':(self.now-timedelta(hours=1)).isoformat()}
        (self.depsdev/'manifest.json').write_text(json.dumps(self.depsdev_manifest))

    def write_research(self,body):(self.research/'collection_manifest.json').write_text(json.dumps(body))
    def write_git(self,body):(self.git/'manifest.json').write_text(json.dumps(body))

    def complete_parent(self):
        body={'source':'arxiv_first_version_metadata','status':'complete','history_start':'2023-01-01',
            'history_end_exclusive':'2026-11-03','as_of':'2026-11-03T03:00:00Z',
            'created_at':'2026-11-03T02:00:00Z','updated_at':'2026-11-03T03:10:00Z',
            'requests':[{'observed_at':'2026-11-03T03:00:00Z'}],
            'candidate_input_sha256':'a'*64,'candidate_pairs':10,'unique_candidate_papers':8,'retrieved_v1_papers':8,
            'files':{'papers_v1.jsonl':'a'*64,'candidates_input.csv':'b'*64,'tools.study.json':self.cohort}}
        self.write_research(body);return body

    def derived(self,parent=None,name='derived'):
        parent=parent or self.research;body=module.read(parent/'collection_manifest.json')
        path=self.data/'raw/research_v1/derived_runs'/name;path.mkdir(parents=True)
        parent_sha=hashlib.sha256((parent/'collection_manifest.json').read_bytes()).hexdigest()
        copy='lineage/collection_manifest-'+parent_sha+'.json';(path/copy).parent.mkdir()
        (path/copy).write_bytes((parent/'collection_manifest.json').read_bytes())
        inputs={'kind':module.RESEARCH_DERIVATION,'parent_manifest_sha256':parent_sha,'registry_sha256':self.cohort,
            'code_sha256':'c'*64,'classifier_code_sha256':'d'*64}
        fingerprint=hashlib.sha256(json.dumps(inputs,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
        child={**body,'candidate_pairs':body['candidate_pairs']+2,
            'derived_from':{'source_relative_path':str(parent.relative_to(self.data/'raw/research_v1')),
                'manifest_sha256':parent_sha,'manifest_copy_path':copy},
            'derivation':{**inputs,'input_fingerprint':fingerprint,'candidate_only':True,'provider_requests_added':0,
                'new_paper_ids':0,'original_query_cohort_unchanged':True,'all_retained_papers_all_tool_aliases_scanned':True,
                'tools_scanned':30,'added_candidate_pairs':2},
            'files':{**body['files'],copy:parent_sha}}
        (path/'collection_manifest.json').write_text(json.dumps(child));return path

    def fake_research(self):
        fake=pd.DataFrame({'tool_id':[str(i) for i in range(30)],'seed_v1_retrieval_complete':[True]*30})
        return patch('assemble_full_scope.inspect_source',return_value=(fake,{'collection_complete':True},None))

    def test_unchanged_complete_cycle_has_no_http_or_clone_actions(self):
        result=module.plan(self.project,{},self.now)
        self.assertEqual(result['actions'],[]);self.assertFalse(result['paid_queries_enabled'])

    def test_weekly_research_cycle_freezes_new_output_and_bounds_requests(self):
        self.write_research({**self.research_manifest,'history_end_exclusive':'2026-10-27'})
        result=module.plan(self.project,{},self.now)
        action=next(a for a in result['actions'] if a['kind']=='research')
        self.assertNotEqual(result['research_active'],str(self.research))
        self.assertEqual(action['command'][action['command'].index('--max-requests')+1],'8')
        self.assertEqual(action['command'][action['command'].index('--max-seconds')+1],'120')
        self.assertEqual(result['research_last_complete'],str(self.research))

    def test_partial_cycle_resumes_same_frozen_universe(self):
        active=self.data/'raw/research_v1/refresh/active';active.mkdir(parents=True)
        (active/'collection_manifest.json').write_text(json.dumps({'status':'partial','history_end_exclusive':'2026-11-01'}))
        result=module.plan(self.project,{'research_active':str(active),'research_last_complete':str(self.research)},self.now)
        self.assertEqual(result['research_active'],str(active))
        self.assertEqual([a['kind'] for a in result['actions']],['research'])

    def test_contributor_refresh_is_daily_with_per_tool_120_second_cap(self):
        self.write_git({**self.git_manifest,'created_at':(self.now-timedelta(days=1)).isoformat()})
        result=module.plan(self.project,{},self.now)
        action=next(a for a in result['actions'] if a['kind']=='git')
        self.assertEqual(action['command'][action['command'].index('--timeout-seconds')+1],'120')
        self.assertLessEqual(action['timeout_seconds'],result['total_deadline_seconds'])
        recent={'git_last_attempt':(self.now-timedelta(hours=1)).isoformat()}
        self.assertFalse(any(a['kind']=='git' for a in module.plan(self.project,recent,self.now)['actions']))

    def test_weekly_current_graph_cycle_is_bounded_and_preserves_source_gaps(self):
        (self.depsdev/'manifest.json').write_text(json.dumps({**self.depsdev_manifest,
            'updated_at':(self.now-timedelta(days=7)).isoformat()}))
        result=module.plan(self.project,{},self.now)
        action=next(a for a in result['actions'] if a['kind']=='depsdev')
        self.assertEqual(action['command'][action['command'].index('--max-requests')+1],'150')
        self.assertEqual(action['command'][action['command'].index('--interval')+1],'1')
        self.assertEqual(result['depsdev_last_complete'],str(self.depsdev))
        self.assertNotEqual(result['depsdev_active'],str(self.depsdev))

    def test_incomplete_quality_cannot_move_research_latest(self):
        latest=self.data/'raw/research_v1/latest';old=self.data/'raw/research_v1/old';old.mkdir();latest.symlink_to(old)
        with patch.object(module,'plan',return_value={'research_active':str(self.research),
                'research_last_complete':None,'depsdev_active':str(self.depsdev),
                'depsdev_last_complete':None,'cohort_sha256':self.cohort,'actions':[]}),\
                patch('assemble_full_scope.inspect_source',return_value=(None,{'collection_complete':False},None)),\
                self.assertRaisesRegex(ValueError,'not fully retrieved'):
            module.execute(self.project)
        self.assertEqual(latest.resolve(),old.resolve())

    def test_verified_complete_source_publishes_without_network_or_spine_mutation(self):
        fake=pd.DataFrame({'tool_id':[str(i) for i in range(30)],'seed_v1_retrieval_complete':[True]*30})
        with patch.object(module,'plan',return_value={'research_active':str(self.research),
                'research_last_complete':None,'depsdev_active':str(self.depsdev),
                'depsdev_last_complete':None,'cohort_sha256':self.cohort,'actions':[]}),\
                patch('assemble_full_scope.inspect_source',return_value=(fake,{'collection_complete':True},None)),\
                patch('assemble_full_scope.inspect_supplement',return_value=({'retrieval_attempts_complete':True,'rows_available':30},None,None)),\
                patch.object(module,'bounded') as network:
            result=module.execute(self.project)
        network.assert_not_called()
        self.assertEqual((self.data/'raw/research_v1/latest').resolve(),self.research.resolve())
        self.assertEqual(result['status'],'public_sources_checked')
        self.assertFalse((self.data/'full_scope/latest').exists())
        self.assertEqual((self.data/'raw/depsdev/latest').resolve(),self.depsdev.resolve())

    def test_stale_state_adopts_newer_graph_latest_before_planning(self):
        fresh=self.data/'raw/depsdev/observations/fresh';fresh.mkdir(parents=True)
        (fresh/'manifest.json').write_text(json.dumps({'status':'complete_with_source_gaps',
            'requests':[{'observed_at':(self.now-timedelta(minutes=10)).isoformat()}]}))
        state={'depsdev_active':str(self.depsdev),'depsdev_last_complete':str(self.depsdev)}
        with patch.object(module,'validated_latest',side_effect=lambda project,kind,*args:fresh if kind=='depsdev' else None):
            result=module.plan(self.project,state,self.now)
        self.assertEqual(result['depsdev_active'],str(fresh));self.assertEqual(result['depsdev_last_complete'],str(fresh))
        self.assertEqual(result['actions'],[])

    def test_research_latest_cutoff_supersedes_older_complete_state(self):
        fresh=self.data/'raw/research_v1/refresh/fresh';fresh.mkdir(parents=True)
        (fresh/'collection_manifest.json').write_text(json.dumps({'status':'complete','history_end_exclusive':'2026-11-03'}))
        self.write_research({'status':'complete','history_end_exclusive':'2026-11-02'})
        with patch.object(module,'validated_latest',side_effect=lambda project,kind,*args:fresh if kind=='research' else None):
            result=module.plan(self.project,{'research_active':str(self.research)},self.now)
        self.assertEqual(result['research_active'],str(fresh));self.assertEqual(result['research_last_complete'],str(fresh))

    def test_newer_partial_research_cycle_is_preserved(self):
        active=self.data/'raw/research_v1/refresh/active';active.mkdir(parents=True)
        (active/'collection_manifest.json').write_text(json.dumps({'status':'partial','history_end_exclusive':'2026-11-10'}))
        selected,completed=module.research_cycle(self.data,{'research_active':str(active)},self.now,self.research)
        self.assertEqual(selected,active);self.assertEqual(completed,str(self.research))

    def test_publication_cannot_rollback_actual_vintage_after_checkpoint_rewrite(self):
        fresh=self.data/'raw/depsdev/observations/fresh';fresh.mkdir(parents=True)
        new={'status':'complete_with_source_gaps','requests':[{'observed_at':'2026-11-03T03:50:00Z'}]}
        (fresh/'manifest.json').write_text(json.dumps(new))
        old={**self.depsdev_manifest,'updated_at':'2026-11-03T05:00:00Z',
             'requests':[{'observed_at':'2026-11-03T03:00:00Z'}]}
        (self.depsdev/'manifest.json').write_text(json.dumps(old))
        with patch.object(module,'validate_source',return_value=old),\
                patch.object(module,'validated_latest',return_value=fresh),\
                patch('materialize_full_scope.atomic_link') as publish:
            selected=module.publish_monotonic(self.project,'depsdev',self.depsdev,self.cohort,{str(i) for i in range(30)})
        self.assertEqual(selected,fresh);publish.assert_not_called()

    def test_research_future_cutoff_cannot_publish(self):
        self.write_research({'status':'complete','history_end_exclusive':'2026-11-04'})
        fake=pd.DataFrame({'tool_id':[str(i) for i in range(30)],'seed_v1_retrieval_complete':[True]*30})
        with patch('assemble_full_scope.inspect_source',return_value=(fake,{'collection_complete':True},None)),\
                self.assertRaisesRegex(ValueError,'future source cutoff'):
            module.validate_source(self.project,'research',self.research,self.cohort,{str(i) for i in range(30)},self.now)

    def test_original_period_cap_stops_completed_research_and_contributor_cycles(self):
        self.write_research({'status':'complete','history_end_exclusive':'2027-01-01'})
        later=datetime(2027,1,15,4,tzinfo=UTC)
        self.write_git({**self.git_manifest,'history_end_exclusive':'2027-01-01'})
        result=module.plan(self.project,{},later)
        self.assertEqual(result['history_end_exclusive'],'2027-01-01')
        self.assertFalse(any(a['kind'] in {'git','research'} for a in result['actions']))

    def test_capped_partial_cycle_resumes_frozen_end_then_plans_final_period(self):
        later=datetime(2027,1,15,4,tzinfo=UTC)
        self.write_research({'status':'partial','history_end_exclusive':'2026-12-28'})
        result=module.plan(self.project,{},later)
        research=next(a for a in result['actions'] if a['kind']=='research')
        git=next(a for a in result['actions'] if a['kind']=='git')
        self.assertEqual(research['command'][research['command'].index('--end-exclusive')+1],'2026-12-28')
        self.assertEqual(git['command'][git['command'].index('--end-exclusive')+1],'2027-01-01')
        self.write_research({'status':'complete','history_end_exclusive':'2026-12-28'})
        result=module.plan(self.project,{},later)
        research=next(a for a in result['actions'] if a['kind']=='research')
        self.assertNotEqual(result['research_active'],str(self.research))
        self.assertEqual(research['command'][research['command'].index('--end-exclusive')+1],'2027-01-01')

    def test_equal_vintage_verified_descendant_wins_and_ancestor_cannot_rollback(self):
        self.complete_parent();child=self.derived();latest=self.data/'raw/research_v1/latest';latest.symlink_to(self.research)
        ids={str(i) for i in range(30)}
        with self.fake_research():
            selected=module.publish_monotonic(self.project,'research',child,self.cohort,ids)
            self.assertEqual(selected,child);self.assertEqual(latest.resolve(),child)
            selected=module.publish_monotonic(self.project,'research',self.research,self.cohort,ids)
        self.assertEqual(selected,child);self.assertEqual(latest.resolve(),child)
        self.assertEqual(self.expander.call_args.args[0],child)
        active,completed=module.research_cycle(self.data,{'research_active':str(self.research)},self.now,child)
        self.assertEqual(active,child);self.assertEqual(completed,str(child))

    def test_sibling_cannot_win_equal_vintage_and_ancestry_can_span_two_derivations(self):
        self.complete_parent();first=self.derived(name='first');sibling=self.derived(name='sibling')
        grandchild=self.derived(first,'grandchild');latest=self.data/'raw/research_v1/latest';latest.symlink_to(first)
        ids={str(i) for i in range(30)}
        with self.fake_research():
            selected=module.publish_monotonic(self.project,'research',sibling,self.cohort,ids)
            self.assertEqual(selected,first)
            selected=module.publish_monotonic(self.project,'research',grandchild,self.cohort,ids)
            self.assertEqual(selected,grandchild)
            selected=module.publish_monotonic(self.project,'research',self.research,self.cohort,ids)
        self.assertEqual(selected,grandchild);self.assertEqual(latest.resolve(),grandchild)

    def test_parent_copy_hash_path_and_http_vintage_are_verified(self):
        self.complete_parent();child=self.derived();manifest=child/'collection_manifest.json';original=manifest.read_bytes()
        ids={str(i) for i in range(30)};body=json.loads(original);copy=child/body['derived_from']['manifest_copy_path']
        raw=copy.read_bytes();copy.write_bytes(b'changed copy')
        with self.fake_research(),self.assertRaisesRegex(ValueError,'copy hash binding'):module.validate_source(self.project,'research',child,self.cohort,ids,self.now)
        copy.write_bytes(raw)
        body['derived_from']['source_relative_path']='../outside';manifest.write_text(json.dumps(body))
        with self.fake_research(),self.assertRaisesRegex(ValueError,'path leaves'):module.validate_source(self.project,'research',child,self.cohort,ids,self.now)
        body=json.loads(original);body['requests'].append({'observed_at':'2026-11-03T03:30:00Z'});manifest.write_text(json.dumps(body))
        with self.fake_research(),self.assertRaisesRegex(ValueError,'provider/query vintage'):module.validate_source(self.project,'research',child,self.cohort,ids,self.now)
        manifest.write_bytes(original)
        with self.fake_research():module.validate_source(self.project,'research',child,self.cohort,ids,self.now)

    def test_expansion_failure_preserves_published_latest_and_last_complete_state(self):
        self.complete_parent();old=self.data/'raw/research_v1/old';old.mkdir()
        old_body={**module.read(self.research/'collection_manifest.json'),'history_end_exclusive':'2026-11-02'}
        (old/'collection_manifest.json').write_text(json.dumps(old_body))
        latest=self.data/'raw/research_v1/latest';latest.symlink_to(old)
        recipe={'research_active':str(self.research),'research_last_complete':None,'depsdev_active':str(self.depsdev),
                'depsdev_last_complete':None,'cohort_sha256':self.cohort,'actions':[]}
        self.expander.side_effect=ValueError('offline derived failure')
        with self.fake_research(),patch.object(module,'plan',return_value=recipe),patch.object(module,'bounded') as network,\
                self.assertRaisesRegex(module.ResearchExpansionError,'previous latest retained'):
            module.execute(self.project)
        network.assert_not_called();self.assertEqual(latest.resolve(),old)
        state=module.read(self.data/'full_scope/refresh_state.json')
        self.assertEqual(state['research_last_complete'],str(old))
        self.assertEqual(state['research_active'],str(self.research));self.assertIn('research_expansion_error',state)
        self.assertFalse((self.data/'full_scope/latest').exists())

    def test_completed_cycle_expands_before_publication_without_other_http_actions(self):
        self.complete_parent();child=self.derived();self.expander.side_effect=lambda source,*args:child
        recipe={'research_active':str(self.research),'research_last_complete':None,'depsdev_active':str(self.depsdev),
                'depsdev_last_complete':None,'cohort_sha256':self.cohort,'actions':[]}
        with self.fake_research(),patch.object(module,'plan',return_value=recipe),\
                patch('assemble_full_scope.inspect_supplement',return_value=({'retrieval_attempts_complete':True,'rows_available':30},None,None)),\
                patch.object(module,'bounded') as network:
            result=module.execute(self.project)
        network.assert_not_called();self.expander.assert_called_once()
        self.assertEqual(result['research_selected'],str(child));self.assertEqual((self.data/'raw/research_v1/latest').resolve(),child)
        state=module.read(self.data/'full_scope/refresh_state.json')
        self.assertEqual(state['research_active'],str(child));self.assertEqual(state['research_last_complete'],str(child))
        self.assertFalse((self.data/'full_scope/latest').exists())

    def test_local_adoption_updates_stale_state_but_preserves_newer_partial_cycle(self):
        self.complete_parent();child=self.derived();latest=self.data/'raw/research_v1/latest';latest.symlink_to(self.research)
        state_path=self.data/'full_scope/refresh_state.json'
        module.write(state_path,{'research_active':str(self.research),'research_last_complete':str(self.research),'other_state':'unchanged'})
        with self.fake_research(),patch.object(module,'bounded') as network:
            result=module.adopt_research_derivation(self.project,child)
        network.assert_not_called();state=module.read(state_path)
        self.assertEqual(result['research_selected'],str(child));self.assertEqual(state['research_active'],str(child))
        self.assertEqual(state['research_last_complete'],str(child));self.assertEqual(state['other_state'],'unchanged')
        partial=self.data/'raw/research_v1/refresh/newer';partial.mkdir(parents=True)
        (partial/'collection_manifest.json').write_text(json.dumps({'status':'partial','history_end_exclusive':'2026-11-10'}))
        module.write(state_path,{**state,'research_active':str(partial)})
        with self.fake_research():module.adopt_research_derivation(self.project,self.research)
        state=module.read(state_path)
        self.assertEqual(latest.resolve(),child);self.assertEqual(state['research_active'],str(partial))
        self.assertEqual(state['research_last_complete'],str(child))
        self.assertFalse((self.data/'full_scope/latest').exists())


if __name__=='__main__':unittest.main(verbosity=2)
