import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import numpy as np
import pandas as pd
import git_sensitivity_study as study
import model_study as core


def weekly_fixture():
    rows=[]
    for week in pd.date_range('2025-01-06',periods=8,freq='W-MON',tz='UTC'):
        rows.append({'tool_id':'a','week_start_utc':str(week.date()),'window_start_utc':str(week.date()),
                     'window_end_exclusive_utc':str((week+pd.Timedelta(days=7)).date()),
                     'git_reachable_commit_count':4,'git_first_parent_commit_count':2,
                     'reachable_coverage_days':7,'first_parent_coverage_days':7,'complete_utc_week':True,
                     'historical_public_availability':'unknown','primary_eligible':False})
    return pd.DataFrame(rows)


class GitSensitivityTests(unittest.TestCase):
    def test_complete_weeks_and_past_rolling_features_only(self):
        frame=weekly_fixture()
        result=study.git_features(frame)
        self.assertFalse(result.git_history_eligible.iloc[:3].any())
        self.assertTrue(result.git_history_eligible.iloc[3:].all())
        self.assertEqual(result['git_reachable_commit_count_sum_4w'].iloc[3],16)
        changed=frame.copy();changed.loc[7,'git_reachable_commit_count']=99999
        earlier=study.git_features(changed)
        pd.testing.assert_frame_equal(result.iloc[:7],earlier.iloc[:7])

    def test_partial_unknown_week_is_not_zero_or_eligible(self):
        frame=weekly_fixture()
        frame.loc[2,['reachable_coverage_days','first_parent_coverage_days']]=3
        frame.loc[2,'complete_utc_week']=False
        result=study.git_features(frame)
        self.assertTrue(result.loc[2,study.COUNTS].isna().all())
        self.assertFalse(result.git_history_eligible.iloc[:6].any())
        self.assertTrue(result.git_history_eligible.iloc[6:].all())
        bad=frame.copy();bad.loc[2,'complete_utc_week']=True
        with self.assertRaisesRegex(ValueError,'complete-week flag'):
            study.git_features(bad)

    def test_gaps_duplicate_counts_and_availability_are_rejected(self):
        frame=weekly_fixture()
        with self.assertRaisesRegex(ValueError,'grid has gaps'):
            study.git_features(frame.drop(index=3))
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            study.git_features(pd.concat([frame,frame.iloc[:1]]))
        for column,value in [('git_reachable_commit_count',-1),('git_first_parent_commit_count',5),
                             ('historical_public_availability','known'),('primary_eligible',True)]:
            bad=frame.copy();bad.loc[0,column]=value
            with self.assertRaises(ValueError): study.git_features(bad)

    def test_four_recipes_do_not_modify_primary_allowlist(self):
        before=set(core.CORE_FEATURE_ALLOWLIST)
        config={'recipes':{name+'_dl_release':{'kind':name,'features':['dl_lag_0','release_count','tool_age_weeks']}
                           for name in ['ridge','histgb']}}
        recipes=study.recipes(config)
        self.assertEqual(len(recipes),4)
        for name,recipe in recipes.items():
            self.assertEqual(any(c.startswith('git_') for c in recipe['features']),name.endswith('_git'))
        self.assertEqual(before,core.CORE_FEATURE_ALLOWLIST)
        self.assertFalse(any(c.startswith('git_') for c in before))

    def test_published_reference_predictions_are_preserved_on_common_keys(self):
        rows=[]
        for model in core.MODEL_IDS:
            for tool in ['a','b']:
                rows.append({'tool_id':tool,'week_start':pd.Timestamp('2026-01-05',tz='UTC'),
                             'model':model,'prediction_log_growth':.123456789})
        original=pd.DataFrame(rows)
        keys=original.loc[original.tool_id.eq('a'),['tool_id','week_start']].drop_duplicates()
        reference=study.references_for_keys(original,keys)
        self.assertEqual(len(reference),7)
        self.assertTrue(reference.prediction_log_growth.eq(.123456789).all())
        self.assertEqual(set(reference.model),{'published_'+model for model in core.MODEL_IDS})
        classified=study.classify(reference)
        self.assertTrue(classified.profile.eq('retrospective_git_sensitivity').all())
        self.assertTrue(classified.historical_public_availability.eq('unknown').all())
        self.assertFalse(classified.primary_eligible.any())
        with self.assertRaisesRegex(ValueError,'identical common'):
            study.references_for_keys(original.iloc[1:],keys)

    def test_hash_gate_detects_changed_bytes_and_escaping_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);(root/'source').write_bytes(b'original')
            manifest={'files':{'source':study.sha(root/'source')}}
            study.verify_files(root,manifest)
            (root/'source').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'changed'):
                study.verify_files(root,manifest)
            with self.assertRaisesRegex(ValueError,'changed'):
                study.verify_files(root,{'files':{'../outside':'a'*64}})

    def test_git_manifest_requires_full_bound_cohort(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            (root/'manifest.json').write_text(json.dumps({'status':'partial_failed'}))
            with self.assertRaisesRegex(ValueError,'explicitly frozen'):
                study.validate_git(root,'f'*64,'a'*64,[str(i) for i in range(30)])
            with self.assertRaisesRegex(ValueError,'complete for the bound thirty-tool'):
                study.validate_git(root,study.sha(root/'manifest.json'),'a'*64,[str(i) for i in range(30)])

if __name__=='__main__': unittest.main()
