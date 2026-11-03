import unittest
from collections import defaultdict
from audit_dependency_conflicts import signature,classify
from build_dependency_features import normalize_artifacts
from tests_dependencies import artifact

REG={'torch':'pytorch'}


class Scope(unittest.TestCase):
    def rows(self,first,second,source='consumer'):
        a=artifact(source,'1','2022-01-01T00:00:00Z',first);b={**a,'requires_dist':second}
        normalized,_,_=normalize_artifacts([a,b],'2023-01-01T00:00:00Z','2023-01-01')
        return [signature(row) for row in normalized]
    def test_foreign_variance_does_not_quarantine_cohort_membership(self):
        row=classify(self.rows([],['foreign>=1']),REG,{})
        self.assertEqual(row['classification'],'foreign_requirement_variance');self.assertEqual(row['candidate_inbound_targets'],[])
    def test_specifiers_and_markers_do_not_change_all_condition_declared_membership(self):
        row=classify(self.rows(['torch>=1'],["torch>=2; extra == 'train'"]),REG,{})
        self.assertEqual(row['candidate_inbound_targets'],[])
        self.assertFalse(row['requires_dist_semantic_equal'])
    def test_actual_target_membership_difference_is_a_candidate(self):
        row=classify(self.rows([],['torch']),REG,{})
        self.assertEqual(row['candidate_inbound_targets'],['torch'])
    def test_only_selected_cohort_versions_have_outbound_candidates(self):
        records=self.rows(['numpy>=1'],['numpy>=2'],'torch')
        self.assertEqual(classify(records,REG,{})['candidate_outbound_selected_weeks'],[])
        self.assertEqual(classify(records,REG,{('torch','1'):['2023-01-02']})['candidate_outbound_selected_weeks'],['2023-01-02'])
        for row in records:row['provider_upload_time']='2024-01-01T00:00:00Z'
        self.assertEqual(classify(records,REG,{('torch','1'):['2023-01-02']})['candidate_outbound_selected_weeks'],[])


if __name__=='__main__':unittest.main()
