import gzip,hashlib,json
from pathlib import Path
import tempfile,unittest

from prepare_github_snapshot import prepare,sha


class EvidenceBundle(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.registry=self.root/'tools.study.json';self.registry.write_text(json.dumps({'tools':[{'tool_id':'tool'}]}))
        raw=json.dumps([{'id':2**40}]).encode();path=self.root/'raw/one.json.gz';path.parent.mkdir();path.write_bytes(gzip.compress(raw,mtime=0))
        self.old=self.root/'updated_cursor_conformance.json';self.old.write_text(json.dumps({'version':'v1'}))
        self.new=self.root/'updated_cursor_conformance_v2_ties.json'
        self.new.write_text(json.dumps({'version':'v2','base_proof_path':self.old.name,'base_proof_sha256':sha(self.old)}))
        request={'raw_path':'raw/one.json.gz','raw_sha256':hashlib.sha256(raw).hexdigest(),'compressed_sha256':sha(path),
            'tool_id':'tool','page':1,'observed_at':'2026-11-03T00:00:00Z',
            'cursor_conformance_path':self.old.name,'cursor_conformance_receipt_sha256':sha(self.new)}
        entry={'raw_path':request['raw_path'],'raw_sha256':request['raw_sha256'],'tool_id':'tool','page':1,
            'original_observed_at':request['observed_at'],'original_path':self.old.name,'original_sha256':sha(self.new),
            'corrected_path':self.new.name,'corrected_sha256':sha(self.new)}
        self.correction=self.root/'proof_reference_corrections.json';self.correction.write_text(json.dumps({'corrections':[entry]}))
        self.state={'source':'github_direct_issues_pulls','registry_sha256':sha(self.registry),'requests':[request],
            'tools':{'tool':{'issues_visible':{'last_receipt_raw_path':request['raw_path'],
                'cursor_conformance_path':self.old.name,'cursor_conformance_receipt_sha256':sha(self.new)}}},
            'proof_reference_corrections':{'path':self.correction.name,'sha256':sha(self.correction)}}
        self.path=self.root/'collection_state.json';self.save()
    def save(self):self.path.write_text(json.dumps(self.state))
    def prepare(self):return prepare(self.root,self.path,self.registry,self.root/'bundles','ubuntu@example:/archive')

    def test_bound_correction_uses_read_overlay_without_rewriting_original_receipts(self):
        before=self.path.read_bytes();bundle=self.prepare();folder=Path(bundle['bundle'])
        self.assertEqual((folder/bundle['frozen_state_name']).read_bytes(),before)
        self.assertEqual(self.path.read_bytes(),before)
        self.assertEqual(bundle['proof_evidence'][self.correction.name]['correction_receipts_verified'],1)
        self.assertEqual((folder/self.old.name).read_bytes(),self.old.read_bytes())
        self.assertEqual((folder/self.new.name).read_bytes(),self.new.read_bytes())
        self.assertEqual((folder/self.correction.name).read_bytes(),self.correction.read_bytes())
        self.assertEqual(self.prepare(),bundle)
    def test_changed_transitive_base_proof_is_rejected(self):
        self.old.write_text(json.dumps({'version':'changed'}))
        with self.assertRaisesRegex(ValueError,'proof'):self.prepare()
    def test_correction_must_match_original_observation_not_only_file_checksum(self):
        document=json.loads(self.correction.read_text());document['corrections'][0]['original_observed_at']='2026-11-02T00:00:00Z'
        self.correction.write_text(json.dumps(document));self.state['proof_reference_corrections']['sha256']=sha(self.correction);self.save()
        with self.assertRaisesRegex(ValueError,'original observation'):self.prepare()
    def test_path_escape_is_rejected_before_reading(self):
        self.state['proof_reference_corrections']['path']='../../updated_escape.json';self.save()
        with self.assertRaisesRegex(ValueError,'proof path'):self.prepare()
    def test_jsonp_contract_hash_code_and_flax_counterexample_are_transitive(self):
        code=self.root/'sha256_v8.js';code.write_text('// hash implementation evidence, never executed\n')
        contract=self.root/'jsonp_v3_contract.json';contract.write_text(json.dumps({'sha256_code_path':code.name,'sha256_code_sha256':sha(code)}))
        decoded=b'[1]';page=self.root/'flax_outlier_provider_page.json.gz';page.write_bytes(gzip.compress(decoded,mtime=0))
        receipt=self.root/'flax_outlier_provider_receipt.json';receipt.write_text(json.dumps({'compressed_sha256':sha(page),'raw_sha256':hashlib.sha256(decoded).hexdigest()}))
        mismatch=self.root/'flax_cursor_serialized_field_mismatch.json';mismatch.write_text(json.dumps({'provider_receipt_path':receipt.name,'provider_page_path':page.name}))
        self.state['method_disqualification']={'path':mismatch.name,'sha256':sha(mismatch)}
        self.state['requests'][0].update(jsonp_contract_path=contract.name,jsonp_contract_sha256=sha(contract));self.save()
        bundle=self.prepare();folder=Path(bundle['bundle'])
        for p in [code,contract,page,receipt,mismatch]:self.assertEqual((folder/p.name).read_bytes(),p.read_bytes())
        code.write_text('// changed')
        with self.assertRaisesRegex(ValueError,'proof'):self.prepare()

    def test_raw_only_hash_in_transport_diagnostic_is_not_a_compressed_hash(self):
        request=self.state['requests'][0];diagnostic=self.root/'v3_initial_transport_label_diagnostic.json'
        diagnostic.write_text(json.dumps({'verified_requests':[{'raw_path':request['raw_path'],'raw_sha256':request['raw_sha256']}]}))
        request.update(v3_transport_label_diagnostic_path=diagnostic.name,v3_transport_label_diagnostic_sha256=sha(diagnostic));self.save()
        bundle=self.prepare();self.assertIn(diagnostic.name,bundle['ancillary_files'])

    def test_native_pr_history_plan_preserves_reused_index_lineage(self):
        request=self.state['requests'][0];index=self.root/'core_pr_history_reused_index.json'
        index.write_text(json.dumps({'registry_sha256':self.state['registry_sha256'],'pages':[{k:request[k] for k in ['raw_path','raw_sha256','compressed_sha256']}]}))
        plan=self.root/'core_pr_history_plan.json';plan.write_text(json.dumps({'reused_index_path':index.name,'reused_index_sha256':sha(index)}))
        self.state['core_pr_history_plan']={'path':plan.name,'sha256':sha(plan)};self.save()
        result=self.prepare();self.assertIn(plan.name,result['ancillary_files']);self.assertIn(index.name,result['ancillary_files'])
        index.write_text('{}')
        with self.assertRaisesRegex(ValueError,'proof'):self.prepare()

    def test_arbitrary_javascript_is_not_an_allowed_proof_reference(self):
        unknown=self.root/'unexpected.js';unknown.write_text('arbitrary source')
        self.state['requests'][0]['implementation_path']=unknown.name;self.save()
        with self.assertRaisesRegex(ValueError,'allowlist'):self.prepare()

    def test_mutable_runtime_log_is_frozen_and_changes_create_new_lineage(self):
        log=self.root/'connector_rate_limit_receipts.jsonl';log.write_text(json.dumps({'remaining':99})+'\n')
        first=self.prepare();log.write_text(json.dumps({'remaining':98})+'\n');second=self.prepare()
        self.assertNotEqual(first['bundle'],second['bundle']);self.assertEqual(first['state_sha256'],second['state_sha256'])
        old=Path(first['bundle'])/'connector_rate_limit_receipts.jsonl'
        self.assertEqual(json.loads(old.read_text())['remaining'],99)
        self.assertEqual(first['ancillary_rsync_argv'][-2],first['bundle']+'/')
    def test_no_since_provider_view_probe_and_its_raw_evidence_are_frozen(self):
        request=self.state['requests'][0];path=self.root/'spacy_no_since_probe_result.json'
        path.write_text(json.dumps({'target_number':12422,'target_found':False,'terminal_actual_no_next_link':True,
            'receipts':[{k:request[k] for k in ['raw_path','raw_sha256','compressed_sha256']}]}))
        result=self.prepare();folder=Path(result['bundle'])
        self.assertEqual((folder/path.name).read_bytes(),path.read_bytes())
        self.assertIn(request['raw_path'],result['transitive_proof_raw_files'])
        self.assertFalse(json.loads((folder/path.name).read_text())['target_found'])


if __name__=='__main__':unittest.main(verbosity=2)
