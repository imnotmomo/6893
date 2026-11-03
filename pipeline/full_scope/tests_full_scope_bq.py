"""Cost and pagination tests with no network, cloud login or real database."""
import copy
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
for directory in [HERE, HERE/'dependencies', HERE.parent, Path.home()/'6893/pipeline']:
    sys.path.insert(0,str(directory))


def load_engine():
    # Keep the zoneinfo extension in sys.modules while the temporary backfill
    # stub is removed. Python3.11 clears a removed extension's module state.
    ZoneInfo('America/Los_Angeles')
    stub = types.ModuleType('backfill')
    stub.connect = lambda **kwargs: (_ for _ in ()).throw(AssertionError('Fixture ledger not installed'))
    spec = importlib.util.spec_from_file_location('full_scope_bq_under_test', HERE/'full_scope_bq.py')
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'backfill':stub}):
        spec.loader.exec_module(module)
    return module


engine = load_engine()
MIB = 1024**2
SCHEMA = {'fields':[{'name':'tool_id','type':'STRING'},{'name':'downloads','type':'INTEGER'}]}


def response_rows(values):
    return [{'f':[{'v':name},{'v':str(number)}]} for name,number in values]


class Cursor:
    def __init__(self,value):self.value=value
    def fetchone(self):return self.value


class Ledger:
    def __init__(self):self.rows={};self.inserts=[]
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,sql,args=None):
        args=args or []
        if sql.startswith('SELECT reserved_bytes'):
            value=self.rows.get(args[0]);return Cursor((value,) if value is not None else None)
        if sql.startswith('SELECT coalesce(sum'):
            return Cursor((sum(self.rows.values()),))
        if sql.startswith('INSERT INTO backfill.budget'):
            identifier,value,_=args
            if identifier in self.rows:raise AssertionError('Duplicate ledger reservation')
            self.rows[identifier]=value;self.inserts.append((identifier,value));return Cursor(None)
        if sql.startswith('UPDATE backfill.budget'):
            value,identifier=args;self.rows[identifier]=value;return Cursor(None)
        raise AssertionError('Unexpected fixture SQL: '+sql)


class Cloud:
    def __init__(self):
        self.calls=[];self.ledger=[];self.ledger_size=0;self.job=None;self.submissions=[]
        self.estimate=20*MIB;self.billed=10*MIB;self.statement='SELECT'
        self.fail_insert_response=False;self.fail_page_token=None;self.failed_page=False
        self.pages={None:{'jobComplete':True,'schema':SCHEMA,'totalRows':'3','pageToken':'next',
                          'rows':response_rows([('torch',100),('tensorflow',50)])},
                    'next':{'jobComplete':True,'schema':SCHEMA,'totalRows':'3',
                            'rows':response_rows([('transformers',75)])}}
    def request(self,method,path,**kwargs):
        self.calls.append((method,path,copy.deepcopy(kwargs)))
        if method=='POST':
            payload=kwargs['payload']
            if payload['configuration'].get('dryRun'):
                return {'statistics':{'query':{'totalBytesProcessed':str(self.estimate),'statementType':self.statement}}}
            self.submissions.append(copy.deepcopy(payload))
            self.job=copy.deepcopy(payload)
            self.job['status']={'state':'DONE'}
            stats={'totalBytesProcessed':str(self.estimate),'statementType':self.statement}
            if self.billed is not None:stats['totalBytesBilled']=str(self.billed)
            self.job['statistics']={'query':stats}
            if self.fail_insert_response:
                self.fail_insert_response=False;raise OSError('Lost insert response after server accepted job')
            return copy.deepcopy(self.job)
        if '/queries/' in path:
            token=kwargs.get('params',{}).get('pageToken')
            if token==self.fail_page_token and self.fail_page_token is not None and not self.failed_page:
                self.failed_page=True;raise OSError('Temporary result-page transport failure')
            return copy.deepcopy(self.pages[token])
        if '/jobs/' in path:
            if self.job and path.endswith('/jobs/'+self.job['jobReference']['jobId']):return copy.deepcopy(self.job)
            return None
        raise AssertionError('Unexpected cloud request')


class StreamingBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.data=self.root/'data';self.data.mkdir()
        (self.root/'config').mkdir();self.out=self.root/'result';self.plan_path=self.root/'plan.json'
        self.config={'data_dir':str(self.data),'bigquery_project':'test-project','allow_billed_queries':True,
                     'max_bytes_per_query':50*MIB,'max_total_query_bytes':100*MIB,'minimum_free_GiB':15}
        self.write_config();self.plan={'source':'pypi_rich_test','query':'SELECT @package AS tool_id, 1 AS downloads',
            'query_parameters':[{'name':'package','parameterType':{'type':'STRING'},'parameterValue':{'value':'torch'}}]}
        self.write_plan();self.cloud=Cloud();self.ledger=Ledger()
        self.patches=[patch.object(engine,'ROOT',self.root),
                      patch.object(engine,'_Client',lambda token:self.cloud),
                      patch.object(engine,'backfill',types.SimpleNamespace(connect=lambda **kwargs:self.ledger)),
                      patch.object(engine.shutil,'disk_usage',return_value=types.SimpleNamespace(free=100*1024**3))]
        for item in self.patches:item.start();self.addCleanup(item.stop)

    def write_config(self):
        (self.root/'config/backfill.json').write_text(json.dumps(self.config))
    def write_plan(self):
        self.plan['query_sha256']=hashlib.sha256(json.dumps({'query':self.plan['query'],'parameters':self.plan['query_parameters']},sort_keys=True).encode()).hexdigest()
        self.plan_path.write_text(json.dumps(self.plan))
    def run_engine(self,execute=True):
        return engine.run(self.plan_path,self.out,execute=execute,max_seconds=30,page_size=1000)
    def manifest(self):return json.loads((self.out/'manifest.json').read_text())

    def free_policy(self, projects=None):
        projects=projects or [('test-free-one',100*MIB,0)]
        month=engine.datetime.now(engine.ZoneInfo('America/Los_Angeles')).strftime('%Y-%m')
        items=[]
        for project,cap,baseline in projects:
            evidence=self.data/(project+'-baseline.json')
            evidence.write_text(json.dumps({'project_id':project,'billingEnabled':False,'billing_month':month,
                'baseline_usage_bytes':baseline,'observed_at':engine.now()}))
            items.append({'project_id':project,'billing_month':month,'allocation_limit_bytes':cap,
                'baseline_usage_bytes':baseline,'baseline_evidence':{'path':str(evidence),'sha256':engine.sha(evidence)}})
        policy={'format_version':1,'enabled':True,'sources':['pypi_rich_test'],
            'allow_paid_fallback':False,'free_projects':items}
        (self.root/'config/full_scope_execution.json').write_text(json.dumps(policy))
        return policy

    def routes(self):
        path=self.data/'full_scope_query_routes.json'
        return json.loads(path.read_text())['routes'] if path.exists() else {}

    def sandbox_guard(self):
        item=patch.object(engine,'verify_unbilled',side_effect=lambda client,project:
            {'project_id':project,'observed_at':engine.now(),'response':{'projectId':project,'billingEnabled':False}})
        return item

    def test_dry_run_never_reserves_or_submits_paid_job(self):
        result=self.run_engine(False)
        self.assertEqual(result['status'],'dry_run_ok');self.assertEqual(self.ledger.rows,{})
        self.assertEqual(self.cloud.submissions,[]);self.assertEqual(len(self.cloud.calls),1)

    def test_existing_reservation_reused_and_cannot_be_enlarged(self):
        self.assertEqual(engine.reserve(self.data,'same',20*MIB,30*MIB),20*MIB)
        self.assertEqual(engine.reserve(self.data,'same',20*MIB,30*MIB),20*MIB)
        self.assertEqual(len(self.ledger.inserts),1)
        with self.assertRaisesRegex(ValueError,'enlarged'):engine.reserve(self.data,'same',30*MIB,100*MIB)

    def test_global_budget_and_per_query_caps_stop_before_paid_submission(self):
        self.ledger.rows['previous-source']=90*MIB
        with self.assertRaisesRegex(ValueError,'Global cumulative'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[]);self.assertEqual(sum(self.ledger.rows.values()),90*MIB)
        self.out=self.root/'second-result';self.cloud.estimate=60*MIB
        with self.assertRaisesRegex(ValueError,'per-query cap'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[])

    def test_all_pages_are_decoded_losslessly_and_billing_reconciles(self):
        result=self.run_engine()
        self.assertEqual(result['status'],'complete');self.assertEqual(result['rows_fetched'],3)
        self.assertEqual(len(result['pages']),2);self.assertEqual(len(self.cloud.submissions),1)
        self.assertEqual(sum(self.ledger.rows.values()),10*MIB)
        rows=[json.loads(line) for line in gzip.decompress((self.out/'decoded_records.jsonl.gz').read_bytes()).splitlines()]
        self.assertEqual(rows,[{'tool_id':'torch','downloads':100},{'tool_id':'tensorflow','downloads':50},{'tool_id':'transformers','downloads':75}])
        result_calls=[entry for entry in self.cloud.calls if '/queries/' in entry[1]]
        self.assertEqual(result_calls[1][2]['params']['pageToken'],'next')
        self.assertTrue(all(entry[2]['params']['maxResults']==1000 for entry in result_calls))

    def test_lost_insert_response_resumes_same_cloud_job_without_duplicate_insert(self):
        self.cloud.fail_insert_response=True
        with self.assertRaises(OSError):self.run_engine()
        frozen_id=self.manifest()['job_id'];self.assertEqual(sum(self.ledger.rows.values()),20*MIB)
        result=self.run_engine()
        self.assertEqual(result['status'],'complete');self.assertEqual(result['job_id'],frozen_id)
        self.assertEqual(len(self.cloud.submissions),1);self.assertEqual(len(self.ledger.inserts),1)

    def test_mid_page_failure_resumes_at_saved_token_without_duplicate_paid_job(self):
        self.cloud.fail_page_token='next'
        with self.assertRaises(OSError):self.run_engine()
        self.assertEqual(self.manifest()['rows_fetched'],2)
        result=self.run_engine();self.assertEqual(result['rows_fetched'],3)
        self.assertEqual(len(self.cloud.submissions),1)
        first_pages=[call for call in self.cloud.calls if '/queries/' in call[1] and not call[2]['params'].get('pageToken')]
        self.assertEqual(len(first_pages),1)

    def test_cloud_job_parameter_cap_and_location_tampering_is_rejected(self):
        self.cloud.fail_insert_response=True
        with self.assertRaises(OSError):self.run_engine()
        original=copy.deepcopy(self.cloud.job)
        for kind in ['parameter','cap','location']:
            job=copy.deepcopy(original)
            if kind=='parameter':job['configuration']['query']['queryParameters'][0]['parameterValue']['value']='other'
            elif kind=='cap':job['configuration']['query']['maximumBytesBilled']=str(30*MIB)
            else:job['jobReference']['location']='EU'
            self.cloud.job=job
            with self.subTest(kind=kind),self.assertRaisesRegex(ValueError,'frozen query'):self.run_engine()
        self.assertEqual(len(self.cloud.submissions),1)

    def test_retained_page_hash_and_completion_receipt_tampering_are_rejected(self):
        self.run_engine();self.cloud.calls.clear()
        receipt=self.out/'completion_receipt.json';original=receipt.read_bytes();v=json.loads(original);v['total_rows']=4;receipt.write_text(json.dumps(v))
        with self.assertRaisesRegex(ValueError,'Combined decoded'):self.run_engine()
        receipt.write_bytes(original)
        page=self.out/self.manifest()['pages'][0]['raw'];page.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'Retained page changed'):self.run_engine()
        self.assertEqual(self.cloud.calls,[])

    def test_corrupt_combined_output_is_rejected_without_rebuying_completed_query(self):
        self.run_engine();self.cloud.calls.clear();reserved=copy.deepcopy(self.ledger.rows)
        (self.out/'decoded_records.jsonl.gz').write_bytes(b'corrupt completed body')
        with self.assertRaisesRegex(ValueError,'Combined decoded result changed'):self.run_engine()
        self.assertEqual(self.cloud.calls,[])
        self.assertEqual(len(self.cloud.submissions),1)
        self.assertEqual(self.ledger.rows,reserved)

    def test_repeated_tokens_and_truncated_totals_never_publish_complete(self):
        self.cloud.pages['next']['pageToken']='next'
        with self.assertRaisesRegex(ValueError,'Repeated result page token'):self.run_engine()
        self.assertNotEqual(self.manifest()['status'],'complete');self.assertFalse((self.out/'completion_receipt.json').exists())
        self.out=self.root/'truncated';self.cloud=Cloud();self.ledger=Ledger()
        (self.data/'full_scope_query_routes.json').unlink()
        self.cloud.pages[None]['totalRows']='4';self.cloud.pages['next']['totalRows']='4'
        with self.assertRaisesRegex(ValueError,'Result rows are incomplete'):self.run_engine()
        self.assertNotEqual(self.manifest()['status'],'complete');self.assertFalse((self.out/'completion_receipt.json').exists())

    def test_page_schema_and_total_row_counts_cannot_change(self):
        original=copy.deepcopy(self.cloud.pages)
        self.cloud.pages['next']['schema']={'fields':[{'name':'changed','type':'STRING'}]}
        with self.assertRaisesRegex(ValueError,'schema changed'):self.run_engine()
        self.out=self.root/'changed-total';self.cloud=Cloud();self.ledger=Ledger()
        (self.data/'full_scope_query_routes.json').unlink()
        self.cloud.pages=copy.deepcopy(original);self.cloud.pages['next']['totalRows']='4'
        with self.assertRaisesRegex(ValueError,'row count changed'):self.run_engine()

    def test_statement_type_must_be_select_before_any_paid_query(self):
        for statement in ['DELETE','SCRIPT',None]:
            self.out=self.root/('statement-'+str(statement));self.cloud=Cloud();self.cloud.statement=statement
            with self.subTest(statement=statement),self.assertRaisesRegex(ValueError,'SELECT'):self.run_engine()
            self.assertEqual(self.cloud.submissions,[])

    def test_query_fingerprint_and_resume_plan_cannot_change(self):
        self.plan['query_sha256']='wrong';self.plan_path.write_text(json.dumps(self.plan))
        with self.assertRaisesRegex(ValueError,'fingerprint'):self.run_engine()
        self.write_plan();self.run_engine(False)
        self.plan['query_parameters'][0]['parameterValue']['value']='other';self.write_plan()
        with self.assertRaisesRegex(ValueError,'Resume plan changed'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[])

    def test_missing_previously_submitted_job_does_not_resubmit_and_unknown_charge_stays_reserved(self):
        self.cloud.fail_page_token='next';self.cloud.billed=None
        with self.assertRaises(OSError):self.run_engine()
        self.assertEqual(sum(self.ledger.rows.values()),20*MIB)
        self.cloud.job=None
        with self.assertRaisesRegex(ValueError,'potentially duplicate'):self.run_engine()
        self.assertEqual(len(self.cloud.submissions),1)

    def test_busy_output_cannot_read_or_replace_another_export_manifest(self):
        self.out.mkdir();sentinel=b'completed manifest owned by the running export'
        (self.out/'manifest.json').write_bytes(sentinel)
        with (self.out/'.collection.lock').open('a') as lock:
            engine.fcntl.flock(lock,engine.fcntl.LOCK_EX|engine.fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):self.run_engine()
        self.assertEqual((self.out/'manifest.json').read_bytes(),sentinel)
        self.assertEqual(self.cloud.calls,[])
        self.assertEqual(self.ledger.rows,{})

    def test_new_free_route_keeps_old_budget_and_disables_paid_fallback(self):
        self.ledger.rows['old-authorized-study']=30*MIB
        self.free_policy();self.config['allow_billed_queries']=False;self.write_config()
        with self.sandbox_guard() as guard:result=self.run_engine()
        self.assertEqual(result['definition']['project'],'test-free-one')
        self.assertEqual(self.ledger.rows['old-authorized-study'],30*MIB)
        self.assertEqual(len(self.ledger.inserts),1)
        guard.assert_called_once_with(self.cloud,'test-free-one')
        route=next(iter(self.routes().values()))
        self.assertEqual(route['free_reserved_bytes'],20*MIB)
        self.assertEqual(route['route_kind'],'unbilled_sandbox')
        self.assertTrue((self.out/'billing_disabled_verification.json').is_file())

    def test_old_submitted_job_resumes_original_project_after_config_switch(self):
        self.cloud.fail_insert_response=True
        with self.assertRaises(OSError):self.run_engine()
        original=self.manifest();self.config['bigquery_project']='other-paid-project';self.write_config();self.free_policy()
        with self.sandbox_guard() as guard:result=self.run_engine()
        self.assertEqual(result['job_id'],original['job_id'])
        self.assertEqual(result['definition']['project'],'test-project')
        self.assertEqual(len(self.cloud.submissions),1);guard.assert_not_called()
        self.assertEqual(len(self.ledger.inserts),1)

    def test_completed_old_output_stays_byte_identical_after_switch(self):
        self.run_engine();original=(self.out/'manifest.json').read_bytes();old=copy.deepcopy(self.ledger.rows)
        self.config['bigquery_project']='other-paid-project';self.write_config();self.free_policy();self.cloud.calls.clear()
        self.run_engine()
        self.assertEqual((self.out/'manifest.json').read_bytes(),original)
        self.assertEqual(self.ledger.rows,old);self.assertEqual(self.cloud.calls,[])

    def test_unsubmitted_old_dry_run_is_archived_and_revalidated_on_free_project(self):
        self.run_engine(False);old=(self.out/'manifest.json').read_bytes();dry=(self.out/'dry_run.json').read_bytes()
        self.free_policy()
        with self.sandbox_guard():result=self.run_engine()
        previous=result['execution_route_history'][0]
        self.assertEqual((self.out/previous['manifest_path']).read_bytes(),old)
        self.assertEqual((self.out/previous['dry_run_path']).read_bytes(),dry)
        self.assertEqual(engine.sha(self.out/previous['manifest_path']),previous['manifest_sha256'])
        self.assertEqual(result['dry_run_project'],'test-free-one')
        self.assertEqual(self.cloud.submissions[0]['jobReference']['projectId'],'test-free-one')

    def test_other_output_and_parameter_order_reuse_one_original_cloud_job(self):
        self.plan['query_parameters'].append({'name':'extra','parameterType':{'type':'STRING'},'parameterValue':{'value':'same'}})
        self.write_plan();self.free_policy()
        with self.sandbox_guard():first=self.run_engine()
        self.plan['query_parameters'].reverse();self.write_plan();self.out=self.root/'another-output'
        self.config['bigquery_project']='other-paid-project';self.write_config()
        with self.sandbox_guard() as guard:second=self.run_engine()
        self.assertEqual(first['job_id'],second['job_id']);self.assertEqual(len(self.cloud.submissions),1)
        self.assertEqual(len(self.routes()),1);self.assertEqual(len(self.ledger.inserts),1);guard.assert_not_called()

    def test_concurrent_logical_claims_share_one_route_and_allowance(self):
        self.free_policy();policy=engine.execution_policy(self.plan)
        def claim(index):
            return engine.select_route(self.data,self.plan,None,self.root/('concurrent-'+str(index)),
                self.config,policy,cap=20*MIB,claim=True)
        with ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(claim,range(8)))
        self.assertEqual(len({r['job_id'] for r in results}),1)
        self.assertEqual(len(self.routes()),1);self.assertEqual(len(self.ledger.inserts),1)
        self.assertEqual(sum(self.ledger.rows.values()),20*MIB)

    def test_verified_baseline_and_prior_claims_enforce_ordered_project_caps(self):
        self.free_policy([('test-free-one',30*MIB,10*MIB),('test-free-two',20*MIB,0)])
        with self.sandbox_guard():first=self.run_engine()
        self.assertEqual(first['definition']['project'],'test-free-one')
        self.plan['query_parameters'][0]['parameterValue']['value']='different';self.write_plan()
        self.out=self.root/'second-free-query';self.cloud=Cloud()
        with self.sandbox_guard():second=self.run_engine()
        self.assertEqual(second['definition']['project'],'test-free-two')
        self.plan['query_parameters'][0]['parameterValue']['value']='third';self.write_plan()
        self.out=self.root/'third-free-query';self.cloud=Cloud()
        with self.sandbox_guard(),self.assertRaisesRegex(ValueError,'free query allocation exhausted'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[])
        self.assertEqual(sum(r['free_reserved_bytes'] for r in self.routes().values()),40*MIB)

    def test_global_cap_still_applies_to_sandbox_scans_before_submission(self):
        self.free_policy();self.ledger.rows['old-history']=90*MIB
        with self.sandbox_guard() as guard,self.assertRaisesRegex(ValueError,'Global cumulative'):self.run_engine()
        self.assertEqual(self.ledger.rows,{'old-history':90*MIB});self.assertEqual(self.cloud.submissions,[])
        self.assertFalse(self.routes());guard.assert_not_called()

    def test_unknown_free_job_charge_holds_both_reservations_across_account_switch(self):
        self.free_policy();self.cloud.fail_insert_response=True;self.cloud.billed=None
        with self.sandbox_guard(),self.assertRaises(OSError):self.run_engine()
        route=next(iter(self.routes().values()));old_id=route['job_id']
        self.free_policy([('test-free-two',100*MIB,0)])
        with self.sandbox_guard() as guard:result=self.run_engine()
        self.assertEqual(result['job_id'],old_id);self.assertEqual(result['definition']['project'],'test-free-one')
        self.assertEqual(self.ledger.rows[old_id],20*MIB)
        self.assertEqual(next(iter(self.routes().values()))['free_reserved_bytes'],20*MIB)
        self.assertEqual(len(self.cloud.submissions),1);guard.assert_not_called()

    def test_missing_completed_cloud_job_in_another_folder_cannot_be_rebought(self):
        self.free_policy()
        with self.sandbox_guard():self.run_engine()
        self.cloud=Cloud();self.out=self.root/'copy-result'
        with self.sandbox_guard(),self.assertRaisesRegex(ValueError,'potentially duplicate'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[]);self.assertEqual(len(self.ledger.inserts),1)

    def test_old_unindexed_reservation_is_imported_from_canonical_archives(self):
        self.out=self.data/'raw/full_scope/bigquery/old-output';self.run_engine()
        (self.data/'full_scope_query_routes.json').unlink()
        self.config['bigquery_project']='other-paid-project';self.write_config();self.free_policy()
        original_id=self.manifest()['job_id'];self.out=self.root/'new-folder'
        with self.sandbox_guard() as guard:result=self.run_engine()
        self.assertEqual(result['job_id'],original_id);self.assertEqual(result['definition']['project'],'test-project')
        self.assertEqual(len(self.cloud.submissions),1);guard.assert_not_called()

    def test_changed_or_expired_baseline_blocks_only_new_routes(self):
        policy=self.free_policy();path=Path(policy['free_projects'][0]['baseline_evidence']['path'])
        path.write_text('{}')
        with self.assertRaisesRegex(ValueError,'evidence'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[]);self.assertEqual(self.ledger.rows,{})
        self.free_policy()
        with self.sandbox_guard():self.run_engine()
        path.write_text('{}');self.cloud.calls.clear();self.run_engine()
        self.assertEqual(self.cloud.calls,[])

    def test_live_billing_guard_failure_never_inserts_and_unknown_reservation_is_held(self):
        self.free_policy()
        with patch.object(engine,'verify_unbilled',side_effect=ValueError('billing enabled')):
            with self.assertRaisesRegex(ValueError,'billing enabled'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[])
        self.assertEqual(sum(self.ledger.rows.values()),20*MIB)
        self.assertEqual(next(iter(self.routes().values()))['free_reserved_bytes'],20*MIB)

    def test_provider_billing_receipt_requires_explicit_unlinked_disabled_project(self):
        for value in [{'projectId':'test-free-one','billingEnabled':True},
                      {'projectId':'wrong-project','billingEnabled':False},
                      {'projectId':'test-free-one'},
                      {'projectId':'test-free-one','billingEnabled':False,'billingAccountName':'billingAccounts/linked'}]:
            session=types.SimpleNamespace(request=lambda *args,**kwargs:types.SimpleNamespace(status_code=200,content=json.dumps(value).encode()))
            client=types.SimpleNamespace(token=None,session=session)
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'billing enabled|ambiguous'):engine.verify_unbilled(client,'test-free-one')
        value={'projectId':'test-free-one','billingEnabled':False,'billingAccountName':''}
        session=types.SimpleNamespace(request=lambda *args,**kwargs:types.SimpleNamespace(status_code=200,content=json.dumps(value).encode()))
        receipt=engine.verify_unbilled(types.SimpleNamespace(token=None,session=session),'test-free-one')
        self.assertEqual(receipt['response'],value);self.assertEqual(len(receipt['response_sha256']),64)

    def test_expired_month_and_billing_enabled_baseline_never_authorize_new_jobs(self):
        for change in ['expired','enabled']:
            policy=self.free_policy()
            item=policy['free_projects'][0];path=Path(item['baseline_evidence']['path']);value=json.loads(path.read_text())
            if change=='expired':item['billing_month']=value['billing_month']='2000-01'
            else:value['billingEnabled']=True
            path.write_text(json.dumps(value));item['baseline_evidence']['sha256']=engine.sha(path)
            (self.root/'config/full_scope_execution.json').write_text(json.dumps(policy))
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'billing month|baseline evidence'):self.run_engine()
        self.assertEqual(self.cloud.calls,[]);self.assertEqual(self.ledger.rows,{})

    def test_registry_first_crash_holds_allowance_and_resumes_same_job_without_losing_old_ledger(self):
        self.free_policy([('test-free-one',20*MIB,0)]);self.ledger.rows['old-history']=30*MIB
        original=self.ledger.execute;failed=False
        def execute(sql,args=None):
            nonlocal failed
            if sql.startswith('INSERT INTO backfill.budget') and not failed:
                failed=True;raise OSError('Crash after durable route reservation')
            return original(sql,args)
        self.ledger.execute=execute
        with self.sandbox_guard(),self.assertRaisesRegex(OSError,'durable route'):self.run_engine()
        held=next(iter(self.routes().values()));self.assertEqual(held['free_reserved_bytes'],20*MIB)
        self.assertEqual(self.ledger.rows,{'old-history':30*MIB});self.assertEqual(self.cloud.submissions,[])
        with self.sandbox_guard():result=self.run_engine()
        self.assertEqual(result['job_id'],held['job_id']);self.assertEqual(len(self.cloud.submissions),1)
        self.assertEqual(self.ledger.rows['old-history'],30*MIB);self.assertEqual(len(self.routes()),1)

    def test_orphan_old_reservation_refuses_new_project_even_without_output_binding(self):
        _,old=engine.identity(self.plan,self.config['bigquery_project']);self.ledger.rows[old]=20*MIB
        self.free_policy()
        with self.assertRaisesRegex(ValueError,'lacks a retained execution binding'):self.run_engine()
        self.assertEqual(self.cloud.calls,[]);self.assertEqual(self.ledger.rows,{old:20*MIB})

    def test_new_paid_insert_cannot_use_pinned_old_reservation_when_fallback_is_disabled(self):
        self.run_engine(False);manifest=self.manifest()
        engine.reserve(self.data,manifest['job_id'],20*MIB,100*MIB)
        manifest['status']='reserved';(self.out/'manifest.json').write_text(json.dumps(manifest))
        self.free_policy()
        with self.assertRaisesRegex(ValueError,'New paid submission disabled'):self.run_engine()
        self.assertEqual(self.cloud.submissions,[])
        self.assertEqual(next(iter(self.routes().values()))['execution_project'],'test-project')


if __name__=='__main__':unittest.main(verbosity=2)
