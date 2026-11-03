import argparse,contextlib,csv,io,json
from pathlib import Path
import unittest
import build_github_activity as b
import reframe_github_activity as module
import tests_build_github_activity as legacy
import tests_github_visible_lists as visible
from tests_build_github_activity import issue,search_pull


class Reframe(unittest.TestCase):
    setUp=legacy.Fixture.setUp;save=legacy.Fixture.save;core=legacy.Fixture.core;search=legacy.Fixture.search;proof=visible.VisibleLists.proof;visible=visible.VisibleLists.visible
    def build_source(self):
        self.visible([search_pull(self.new[0]),issue(11,'2023-01-02T00:00:00Z')]);snapshot=self.root/'state.json';snapshot.write_text(json.dumps(self.state));out=self.root/'source'
        args=argparse.Namespace(source_root=self.root,state_snapshot=snapshot,registry=self.registry_path,output=out,max_body_mib=64,max_records=1000)
        with contextlib.redirect_stdout(io.StringIO()):b.build(args)
        return out
    def test_preserves_normalized_evidence_and_independent_core_series(self):
        source=self.build_source();output=self.root/'reframed';q=module.reframe(source,output)
        self.assertEqual(b.sha(source/'normalized_records.jsonl.gz'),b.sha(output/'normalized_records.jsonl.gz'))
        self.assertTrue(q['collection_complete']);self.assertEqual(q['status'],'needs_review');self.assertFalse(q['primary_eligible'])
        with (output/'github_activity_weekly.csv').open() as f:rows=list(csv.DictReader(f))
        self.assertEqual([r['github_pr_opened_count'] for r in rows],['1','1']);self.assertTrue(all(r['github_pr_merged_count']=='' for r in rows))
        self.assertIn('feature_policy_code_sha256',q);self.assertEqual(q['normalization_manifest_sha256'],b.sha(source/'manifest.json'))
        manifest=json.loads((output/'manifest.json').read_text())
        for name,digest in manifest['files'].items():self.assertEqual(b.sha(output/name),digest)
    def test_changed_input_is_rejected_before_fresh_publication(self):
        source=self.build_source();(source/'normalized_records.jsonl.gz').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'hash/path'):module.reframe(source,self.root/'output')
        self.assertFalse((self.root/'output').exists())


if __name__=='__main__':unittest.main(verbosity=2)
