import gzip,hashlib,importlib.util,json,tempfile,unittest
from datetime import datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlencode
from refresh_github_activity import run,next_link,initial_url,dt,pause_until,archive,checked_url
from search_plan import plan
NOW=datetime(2026,11,3,12,tzinfo=timezone.utc)
END="2026-11-03T00:00:00Z"
TOOL={"tool_id":"a","github_repo":"owner/a","github_repo_id":123}
def response(url,body,headers=None,status=200):
 return {"status":status,"url":url,"body":json.dumps(body).encode(),"headers":headers or {}}
def item(number=1,updated="2026-11-02T00:00:00Z"):
 return {"id":number,"number":number,"created_at":"2023-01-02T00:00:00Z","updated_at":updated}
class RefreshTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.registry=self.root/"registry.json"
  self.registry.write_text(json.dumps({"tools":[TOOL]}));self.data=self.root/"data"
 def tearDown(self):self.tmp.cleanup()
 def run_it(self,fetch,cap=4,**kwargs):return run(self.registry,self.data,END,cap,0,fetch,NOW,**kwargs)
 def state(self):return json.loads((self.data/"refresh_state.json").read_text())
 def identity(self,url):return response(url,{"id":123,"full_name":"owner/a"})
 def prepare(self,kind="issues"):
  self.run_it(self.identity,cap=2)
  state=self.state();state["cursor"]=0 if kind=="issues" else 1
  (self.data/"refresh_state.json").write_text(json.dumps(state))
 def test_request_budget_and_fair_identity_checkpoint(self):
  calls=[]
  result=self.run_it(lambda url:(calls.append(url) or self.identity(url)),cap=1)
  self.assertEqual(result["requests"],1);self.assertEqual(len(calls),1)
  self.assertTrue(self.state()["streams"]["a/issues"]["cycle"]["identity_verified"])
  self.assertNotIn("a/pulls",self.state()["streams"])
 def test_complete_overlap_and_hash_bound_raw(self):
  self.prepare();result=self.run_it(lambda url:response(url,[item()]),cap=1)
  self.assertEqual(result["streams_completed"],1)
  state=self.state();self.assertEqual(state["streams"]["a/issues"]["last_success_end_exclusive"],END)
  meta=state["requests"][-1];raw=gzip.decompress((self.data/meta["raw_path"]).read_bytes())
  self.assertEqual(hashlib.sha256(raw).hexdigest(),meta["raw_sha256"])
  self.assertEqual(meta["window_since"],"2026-10-27T00:00:00Z")
 def test_follows_exact_cursor_and_keeps_watermark_pending(self):
  self.prepare();nxt=initial_url(TOOL,"issues","2026-10-27T00:00:00Z")+"&after=publiccursor"
  self.run_it(lambda url:response(url,[item()],{"Link":"<"+nxt+">; rel=\"next\""}),cap=1)
  stream=self.state()["streams"]["a/issues"]
  self.assertIsNone(stream["last_success_end_exclusive"]);self.assertEqual(stream["cycle"]["next_url"],nxt)
 def test_pull_follows_actual_link_after_serialized_overlap_boundary(self):
  self.prepare("pulls");nxt=initial_url(TOOL,"pulls","ignored")+"&page=2"
  result=self.run_it(lambda url:response(url,[item(2),item(1,"2026-10-26T23:59:59Z")],{"Link":"<"+nxt+">; rel=\"next\""}),cap=1)
  self.assertEqual(result["streams_completed"],0)
  self.assertEqual(self.state()["streams"]["a/pulls"]["cycle"]["next_url"],nxt)
 def test_rate_headers_stop_and_resume_no_advance_on_429(self):
  reset=int((NOW+timedelta(minutes=20)).timestamp())
  result=self.run_it(lambda url:response(url,{"message":"rate limited"},{"X-RateLimit-Remaining":"0","X-RateLimit-Reset":str(reset),"Retry-After":"30"},429))
  self.assertEqual(result["requests"],1);self.assertEqual(result["status"],"errors")
  called=[];again=self.run_it(lambda url:called.append(url))
  self.assertEqual(again["status"],"rate_wait");self.assertEqual(called,[])
  self.assertFalse(self.state()["streams"]["a/issues"]["cycle"]["identity_verified"])
 def test_identity_mismatch_blocks_stream(self):
  self.run_it(lambda url:response(url,{"id":999,"full_name":"owner/a"}),cap=1)
  self.assertFalse(self.state()["streams"]["a/issues"]["cycle"]["identity_verified"])
 def test_network_failure_counts_attempt_and_backs_off(self):
  def failed(url):raise TimeoutError("fixture timeout")
  result=self.run_it(failed)
  self.assertEqual(result["requests"],1);self.assertEqual(len(result["errors"]),1)
  self.assertGreater(dt(result["not_before"]),NOW)
 def test_hostile_link_rejected_without_watermark_advance(self):
  self.prepare()
  self.run_it(lambda url:response(url,[item()],{"Link":"<https://example.org/private>; rel=\"next\""}),cap=1)
  self.assertIsNone(self.state()["streams"]["a/issues"]["last_success_end_exclusive"])
 def test_malformed_link_cannot_certify_a_terminal_issue_result(self):
  self.prepare()
  self.run_it(lambda url:response(url,[item()],{"Link":"broken-next-header"}),cap=1)
  self.assertIsNone(self.state()["streams"]["a/issues"]["last_success_end_exclusive"])
  self.assertIn("Malformed",self.state()["streams"]["a/issues"]["last_error"]["message"])
 def test_changed_since_rejected(self):
  self.prepare();nxt=initial_url(TOOL,"issues","2020-01-01T00:00:00Z")+"&page=2"
  self.run_it(lambda url:response(url,[item()],{"Link":"<"+nxt+">; rel=\"next\""}),cap=1)
  self.assertIn("changed update watermark",self.state()["streams"]["a/issues"]["last_error"]["message"])
 def test_required_scope_parameters_cannot_be_stripped_from_actual_links(self):
  valid=initial_url(TOOL,'issues','2026-10-27T00:00:00Z')+'&page=2'
  from urllib.parse import urlsplit,parse_qsl,urlunsplit
  for key in ['state','sort','direction','per_page','since']:
   parsed=urlsplit(valid);stripped=urlunsplit(parsed._replace(query=urlencode([(k,v) for k,v in parse_qsl(parsed.query) if k!=key])))
   with self.subTest(key=key),self.assertRaises(ValueError):next_link('<'+stripped+'>; rel="next"',TOOL,'issues')
  self.assertEqual(next_link('<'+valid+'>; rel="next"',TOOL,'issues'),valid)
 def test_response_redirect_cannot_silently_change_issue_filters(self):
  self.prepare()
  def changed(url):return response(url.replace('state=all','state=open'),[item()])
  self.run_it(changed,cap=1)
  self.assertIsNone(self.state()['streams']['a/issues']['last_success_end_exclusive'])
 def test_changed_registry_fails_before_network(self):
  self.run_it(self.identity,cap=1);self.registry.write_text(json.dumps({"tools":[dict(TOOL,github_repo_id=124)]}))
  with self.assertRaisesRegex(ValueError,"cohort"):self.run_it(self.identity)
 def test_order_and_future_endpoint_fail_closed(self):
  self.prepare()
  self.run_it(lambda url:response(url,[item(2),item(1,"2026-11-01T00:00:00Z")]),cap=1)
  self.assertEqual(self.state()["streams"]["a/issues"]["last_success_end_exclusive"],END)
  self.assertTrue(self.state()["completed_cycles"][0]["serialized_update_order_anomalies"])
  with self.assertRaisesRegex(ValueError,"future"):self.run_it(self.identity,end_exclusive="2026-11-04T00:00:00Z")
 def test_request_limit_rejected(self):
  with self.assertRaises(ValueError):self.run_it(self.identity,cap=11)
 def test_identical_identity_response_preserves_distinct_cycle_provenance(self):
  url='https://api.github.com/repos/owner/a';observed='2026-11-03T12:00:00Z'
  first=archive(self.data,TOOL,'repository',url,self.identity(url),
    {'since':'2026-10-01T00:00:00Z','end_exclusive':'2026-11-01T00:00:00Z'},observed)
  second=archive(self.data,TOOL,'repository',url,self.identity(url),
    {'since':'2026-10-02T00:00:00Z','end_exclusive':'2026-11-02T00:00:00Z'},observed)
  self.assertNotEqual(first['raw_path'],second['raw_path'])
  path=self.data/first['raw_path']
  saved=json.loads(path.with_suffix(path.suffix+'.manifest.json').read_text())
  self.assertEqual(saved['window_end_exclusive'],'2026-11-01T00:00:00Z')
class SearchPartitionTests(unittest.TestCase):
 def setUp(self):self.state={"start":"2023-01-01T00:00:00Z","end_exclusive":END,"requests":[]}
 def test_large_result_recurses_without_paging_past_1000(self):
  pending=plan(self.state,{"tools":[TOOL]})["pending"]
  job=pending[0]
  self.state["requests"].append(dict(job,search_total_count=1001,search_incomplete=False))
  result=plan(self.state,{"tools":[TOOL]})
  children=[p for p in result["pending"] if p["stream"]==job["stream"]]
  self.assertEqual(len(children),2);self.assertEqual(children[0]["window_end_exclusive"],children[1]["window_start"])
  self.assertTrue(all(p["page"]==1 for p in children))
 def test_incomplete_probe_not_promoted_and_exact_pages_required(self):
  job=plan(self.state,{"tools":[TOOL]})["pending"][0]
  self.state["requests"]=[dict(job,search_total_count=101,search_incomplete=True)]
  self.assertEqual(plan(self.state,{"tools":[TOOL]})["leaves"],[])
  self.state["requests"]=[dict(job,search_total_count=101,search_incomplete=False)]
  result=plan(self.state,{"tools":[TOOL]})
  leaf=result["leaves"][0];self.assertEqual(leaf["missing_pages"],[2]);self.assertFalse(leaf["complete"])
  self.state["requests"].append(dict(job,page=2,search_total_count=101,search_incomplete=False))
  self.assertTrue(plan(self.state,{"tools":[TOOL]})["leaves"][0]["complete"])
if __name__=="__main__":unittest.main()
