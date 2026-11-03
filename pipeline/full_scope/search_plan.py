"""Deterministic bounded Search partition plan from archived GitHub responses."""
import hashlib,json,math
from datetime import datetime,timezone,timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parent
STREAMS={
 "issues_created":("issues","is:issue","created",False),
 "issues_old_closed":("issues","is:issue","closed",True),
 "pulls_old_closed":("pulls","is:pr","closed",True),
 "pulls_old_merged":("pulls","is:pr","merged",True),
 "pulls_created_census":("pulls","is:pr","created",False),
}
def split_time(start,end):
 a=datetime.fromisoformat(start.replace("Z","+00:00"));b=datetime.fromisoformat(end.replace("Z","+00:00"))
 n=int((b-a).total_seconds())
 if n<=1:raise ValueError("More than 1000 results in one second; cannot certify this partition")
 from datetime import timedelta
 return (a+timedelta(seconds=n//2)).isoformat().replace("+00:00","Z")
def job_id(tid,stream,start,end):
 return hashlib.sha256(("range_v2:"+tid+":"+stream+":"+start+":"+end).encode()).hexdigest()[:20]
def plan(state,registry):
 requests=state["requests"];byjob={}
 for r in requests:
  if "search_job_id" in r:byjob.setdefault(r["search_job_id"],[]).append(r)
 pending=[];leaves=[];branches=[];errors=[]
 def walk(tool,stream,start,end):
  kind,qualifier,datefield,old=STREAMS[stream];jid=job_id(tool["tool_id"],stream,start,end)
  inclusive_end=(datetime.fromisoformat(end.replace("Z","+00:00"))-timedelta(seconds=1)).isoformat().replace("+00:00","Z")
  query="repo:"+tool["github_repo"]+" "+qualifier+" "+datefield+":"+start+".."+inclusive_end+(" created:<"+state["start"] if old else "")
  count_only=stream=='pulls_created_census'
  job={"search_job_id":jid,"query_semantics_version":"single_inclusive_date_range_v2","tool_id":tool["tool_id"],"repo":tool["github_repo"],"kind":kind,"stream":stream,"window_start":start,"window_end_exclusive":end,"date_field":datefield,"query":query,"per_page":1 if count_only else 100,'count_only':count_only}
  entries=byjob.get(jid,[]);probes=[x for x in entries if x.get("page")==1 and not x.get("search_incomplete")]
  if not probes:
   pending.append(dict(job,page=1,phase="search_probe"));return
  count=probes[-1]["search_total_count"]
  if count>1000 and not count_only:
   try:mid=split_time(start,end)
   except ValueError as e:errors.append(dict(job,error=str(e)));return
   branches.append(dict(job,total_count=count))
   walk(tool,stream,start,mid);walk(tool,stream,mid,end);return
  pages={x["page"]:x for x in entries if not x.get("search_incomplete")}
  total_pages=1 if count_only else max(1,math.ceil(count/100));missing=[p for p in range(1,total_pages+1) if p not in pages]
  for page in missing:pending.append(dict(job,page=page,phase="search_collect"))
  leaves.append(dict(job,total_count=count,total_pages=total_pages,missing_pages=missing,complete=not missing))
 for tool in registry["tools"]:
  for stream in STREAMS:walk(tool,stream,state["start"],state["end_exclusive"])
 # Round robin sources so no one tool blocks all smaller histories.
 pending.sort(key=lambda x:(x["page"],x["window_start"],x["tool_id"],x["stream"]))
 return {"pending":pending,"leaves":leaves,"branches":branches,"errors":errors}
def main():
 state=json.loads((ROOT/"collection_state.json").read_text());registry=json.loads((ROOT/"tools.study.json").read_text())
 p=plan(state,registry);(ROOT/"search_partition_plan.json").write_text(json.dumps(p,indent=2)+"\n")
 complete=sum(x["complete"] for x in p["leaves"])
 print(json.dumps({"pending_count":len(p["pending"]),"completed_leaf_count":complete,"leaf_count":len(p["leaves"]),"branch_count":len(p["branches"]),"errors":p["errors"],"tasks":p["pending"][:12]}))
if __name__=="__main__":main()
