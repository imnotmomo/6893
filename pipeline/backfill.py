#!/usr/bin/env python3
"""Resumable c0 historical collection; cron dispatches bounded work batches."""

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
import uuid

ROOT = Path.home() / "6893"
sys.path.insert(0, str(ROOT / "duckdb"))
sys.path.insert(0, str(ROOT / "pipeline"))
from query import connect
from collect_pilot import fetch_json
from arxiv_backfill import fetch_arxiv_page
from bigquery_sources import fetch_aggregate, resume_aggregate

FORMAT_VERSION = "2026-11-02-v1"
SCHEMA = """
CREATE SCHEMA IF NOT EXISTS backfill;
CREATE TABLE IF NOT EXISTS backfill.jobs (
  job_id VARCHAR PRIMARY KEY, plan_sha VARCHAR, source VARCHAR, tool_id VARCHAR,
  start_date DATE, end_date DATE, generation VARCHAR, state VARCHAR,
  cursor BIGINT DEFAULT 0, attempts INTEGER DEFAULT 0, priority INTEGER,
  next_attempt TIMESTAMPTZ, updated_at TIMESTAMPTZ, error JSON, pending JSON
);
CREATE TABLE IF NOT EXISTS backfill.objects (
  object_id VARCHAR PRIMARY KEY, job_id VARCHAR, source VARCHAR, cursor BIGINT,
  observed_at TIMESTAMPTZ, raw_path VARCHAR, sha256 VARCHAR,
  raw_bytes BIGINT, compressed_bytes BIGINT, metadata JSON
);
CREATE TABLE IF NOT EXISTS backfill.records (
  job_id VARCHAR, cursor BIGINT, record_key VARCHAR, source VARCHAR, tool_id VARCHAR,
  observed_at TIMESTAMPTZ, payload JSON,
  PRIMARY KEY(job_id, cursor, record_key)
);
CREATE TABLE IF NOT EXISTS backfill.budget (
  job_id VARCHAR PRIMARY KEY, reserved_bytes BIGINT, reserved_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS backfill.job_reuse (
  job_id VARCHAR PRIMARY KEY, reused_job_id VARCHAR, reused_plan_sha VARCHAR,
  mapping_sha256 VARCHAR, reused_at TIMESTAMPTZ, metadata JSON
);
"""


def now():
    return datetime.now(timezone.utc)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def atomically(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def key(*parts):
    return hashlib.sha256(encode(parts).encode()).hexdigest()[:32]


def months(start, end):
    while start < end:
        following = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
        yield start, min(end, following)
        start = following


def years(start, end):
    while start < end:
        following = min(end,date(start.year+1,1,1))
        yield start,following
        start=following


def weeks(start, end):
    while start < end:
        days = 7 if start.weekday() == 0 else 7 - start.weekday()
        following = min(end, start + timedelta(days=days))
        yield start, following
        start = following


def add_job(con, digest, source, tool, start, end, generation, priority):
    identifier = key(FORMAT_VERSION, digest, source, tool, start, end, generation)
    con.execute("INSERT INTO backfill.jobs (job_id,plan_sha,source,tool_id,start_date,end_date,generation,state,priority,next_attempt,updated_at) VALUES (?,?,?,?,?,?,?,'pending',?,?,?) ON CONFLICT DO NOTHING", [identifier,digest,source,tool,start,end,generation,priority,now(),now()])
    return identifier


def source_mapping(tool, source):
    """Hash only extraction-relevant identity, not new display/review metadata."""
    mapping={"tool_id":tool["tool_id"],"source":source}
    if source=="pypi_releases":
        mapping["package"]=re.sub(r"[-_.]+","-",tool["pypi_package"].lower())
    elif source=="arxiv":
        mapping["query"]=" ".join(tool["arxiv_query"].split())
        mapping["package"]=re.sub(r"[-_.]+","-",tool["pypi_package"].lower())
        repo_id=tool.get("github_repo_id")
        mapping["repository"]=str(repo_id) if repo_id is not None else tool.get("github_repo","").lower()
    else:
        raise ValueError("Only per-tool release/arXiv history can be reused")
    return hashlib.sha256(encode(mapping).encode()).hexdigest()


def retained_mappings(root, current_digest):
    """Require a valid retained registry and extraction format before reuse."""
    output={}
    for snapshot in sorted((root/"raw/backfill/registries").glob("*/tools.json")):
        digest=snapshot.parent.name
        if digest==current_digest:
            continue
        try:
            registry=json.loads(snapshot.read_text())
            payload=encode(registry).encode()
            manifest=json.loads(snapshot.with_name("manifest.json").read_text())
            expected=hashlib.sha256(FORMAT_VERSION.encode()+b"\0"+payload).hexdigest()
            if (digest!=expected or manifest.get("plan_sha256")!=digest
                    or manifest.get("format_version")!=FORMAT_VERSION
                    or manifest.get("registry_sha256")!=hashlib.sha256(payload).hexdigest()):
                continue
            for tool in registry["tools"]:
                for source in ("pypi_releases","arxiv"):
                    output[(digest,source,tool["tool_id"])]=source_mapping(tool,source)
        except (OSError,ValueError,TypeError,KeyError):
            # Missing/invalid provenance must cause a fresh collection, not reuse.
            continue
    return output


def schedule_history(con,root,digest,source,tool,start,end,priority,prior_mappings):
    identifier=add_job(con,digest,source,tool["tool_id"],start,end,"history",priority)
    current=con.execute("SELECT state,attempts,pending FROM backfill.jobs WHERE job_id=?",[identifier]).fetchone()
    # Never override started work, partial pages, pending queries, or reservations.
    if current!=("pending",0,None):
        return
    for table in ("records","objects","budget"):
        if con.execute("SELECT 1 FROM backfill."+table+" WHERE job_id=? LIMIT 1",[identifier]).fetchone():
            return
    mapping_sha=source_mapping(tool,source)
    candidates=con.execute("SELECT job_id,plan_sha FROM backfill.jobs WHERE plan_sha<>? AND source=? AND tool_id=? AND start_date=? AND end_date=? AND generation='history' AND state='done' ORDER BY updated_at DESC",[digest,source,tool["tool_id"],start,end]).fetchall()
    for old_job,old_plan in candidates:
        if prior_mappings.get((old_plan,source,tool["tool_id"]))!=mapping_sha:
            continue
        # A completed zero-row window still needs its retained raw response.
        objects=con.execute("SELECT raw_path FROM backfill.objects WHERE job_id=?",[old_job]).fetchall()
        if not objects or not all((root/raw_path).is_file() for (raw_path,) in objects):
            continue
        timestamp=now()
        metadata={"reason":"identical_completed_history_window_and_source_mapping",
                  "records":"retained_under_original_job_id_not_copied",
                  "source":source,"tool_id":tool["tool_id"],
                  "start":str(start),"end_exclusive":str(end),"format_version":FORMAT_VERSION}
        con.execute("BEGIN")
        con.execute("INSERT OR REPLACE INTO backfill.job_reuse VALUES (?,?,?,?,?,?)",[identifier,old_job,old_plan,mapping_sha,timestamp,encode(metadata)])
        con.execute("UPDATE backfill.jobs SET state='reused',updated_at=?,error=NULL WHERE job_id=?",[timestamp,identifier])
        con.execute("COMMIT")
        return


def plan(root, config, registry, digest):
    start = date.fromisoformat(config["start_date"])
    stop = min(date.fromisoformat(config.get("max_end_exclusive","2027-01-01")), now().date(), date(2027,1,1))
    recent_end = min(now().date(), date(2027,1,1))
    recent_start = max(start, recent_end - timedelta(days=7))
    prior_mappings=retained_mappings(root,digest)
    with connect(read_only=False, data_dir=root) as con:
        con.execute(SCHEMA)
        for tool in registry["tools"]:
            schedule_history(con,root,digest,"pypi_releases",tool,start,stop,0,prior_mappings)
            if tool["tool_id"] in config.get("arxiv_yearly_tool_ids",[]):
                bootstrap_end=min(stop,date.fromisoformat(config["history_end_exclusive"]))
                if bootstrap_end<start:
                    raise ValueError("arXiv bootstrap endpoint precedes the study start")
                # Replace only untouched calendar-month candidate tasks. Completed/partial
                # response provenance remains intact. Freeze the yearly bootstrap so
                # a later cron invocation never expands and re-fetches its final year.
                for left,right in months(start,bootstrap_end):
                    identifier=key(FORMAT_VERSION,digest,"arxiv",tool["tool_id"],left,right,"history")
                    con.execute("UPDATE backfill.jobs SET state='superseded',updated_at=? WHERE job_id=? AND state='pending' AND cursor=0 AND attempts=0",[now(),identifier])
                windows=list(years(start,bootstrap_end))+list(months(bootstrap_end,stop))
            else:
                windows=months(start,stop)
            for left,right in windows:
                schedule_history(con,root,digest,"arxiv",tool,left,right,2,prior_mappings)
        for source in ("pypi","gharchive"):
            for left,right in weeks(start,stop):
                add_job(con,digest,source,"__all__",left,right,"history",0)
        if recent_start < recent_end and now().date()<=date(2027,1,8):
            generation = "refresh:" + now().date().isoformat()
            for tool in registry["tools"]:
                add_job(con,digest,"pypi_releases",tool["tool_id"],recent_start,recent_end,generation,3)
                add_job(con,digest,"arxiv",tool["tool_id"],recent_start,recent_end,generation,3)
            for source in ("pypi","gharchive"):
                add_job(con,digest,source,"__all__",recent_start,recent_end,generation,3)


def next_job(root, digest, source=None):
    with connect(read_only=False,data_dir=root) as con:
        predicate=" AND source=?" if source else ""
        parameters=[digest,now()]+([source] if source else [])
        result=con.execute("SELECT * FROM backfill.jobs WHERE plan_sha=? AND state NOT IN ('done','split','reused','superseded') AND next_attempt<=?"+predicate+" ORDER BY priority, attempts, updated_at LIMIT 1",parameters)
        row=result.fetchone()
        return dict(zip([col[0] for col in result.description],row)) if row else None


def defer(root, job, state, error, pending=None, delay=3600):
    if pending:
        pending={key:value for key,value in pending.items() if key!="raw_bytes"}
    with connect(read_only=False,data_dir=root) as con:
        con.execute("UPDATE backfill.jobs SET state=?,attempts=attempts+1,next_attempt=?,updated_at=?,error=?,pending=? WHERE job_id=?",[state,now()+timedelta(seconds=delay),now(),encode(error),encode(pending) if pending else None,job["job_id"]])


def split(root,job):
    if (job["end_date"]-job["start_date"]).days < 2:
        defer(root,job,"blocked_source",{"message":"Window cannot be split below one UTC day"},delay=86400)
        return
    middle=job["start_date"]+(job["end_date"]-job["start_date"])//2
    with connect(read_only=False,data_dir=root) as con:
        con.execute("BEGIN")
        for left,right in [(job["start_date"],middle),(middle,job["end_date"])]:
            add_job(con,job["plan_sha"],job["source"],job["tool_id"],left,right,job["generation"],job["priority"])
        con.execute("UPDATE backfill.jobs SET state='split',updated_at=? WHERE job_id=?",[now(),job["job_id"]])
        con.execute("COMMIT")


def releases(tool,start,end):
    result=fetch_json("https://pypi.org/pypi/"+tool["pypi_package"]+"/json")
    rows=[]
    if result["ok"]:
        for version,files in result["json"].get("releases",{}).items():
            for entry in files:
                uploaded=entry.get("upload_time_iso_8601") or entry.get("upload_time")
                if uploaded and start.isoformat()<=uploaded[:10]<end.isoformat():
                    rows.append({"tool_id":tool["tool_id"],"package":tool["pypi_package"],"version":version,"filename":entry.get("filename"),"uploaded_at":uploaded,"artifact_sha256":entry.get("digests",{}).get("sha256"),"yanked_now":entry.get("yanked"),"requires_python":entry.get("requires_python"),"metadata_observed_now":True,"historical_dependency_snapshot":False})
    return {"ok":result["ok"],"status":"ok" if result["ok"] else "failed","raw_bytes":result.get("raw_bytes",b""),"rows":rows,"error":result.get("error"),"metadata":{"request_url":result["request_url"],"http":result.get("http"),"history_scope":"visible release artifacts uploaded in the requested dates; deleted artifacts may be absent"}}


def reserve(root,job,estimate,maximum):
    with connect(read_only=False,data_dir=root) as con:
        existing=con.execute("SELECT reserved_bytes FROM backfill.budget WHERE job_id=?",[job["job_id"]]).fetchone()
        if existing:
            return int(existing[0]) if int(existing[0])>=estimate else None
        used=con.execute("SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget").fetchone()[0]
        if used+estimate>maximum:
            return None
        con.execute("INSERT INTO backfill.budget VALUES (?,?,?)",[job["job_id"],estimate,now()])
        return estimate


def save(root,job,result):
    raw=result.get("raw_bytes") or encode(result.get("rows",[])).encode()
    digest=hashlib.sha256(raw).hexdigest()
    relative=Path("raw/backfill")/job["source"]/job["job_id"]/(str(job["cursor"])+"-"+digest[:16]+".gz")
    compressed=gzip.compress(raw,compresslevel=6,mtime=0)
    atomically(root/relative,compressed)
    assert hashlib.sha256(gzip.decompress((root/relative).read_bytes())).hexdigest()==digest
    observed=now()
    rows=result.get("rows",result.get("papers",[]))
    metadata={"source":job["source"],"start":str(job["start_date"]),"end_exclusive":str(job["end_date"]),"plan_sha256":job["plan_sha"],"format_version":FORMAT_VERSION,"request":result.get("metadata",result.get("request_metadata",{})),"request_url":result.get("request_url"),"http":result.get("http"),"pagination":result.get("pagination"),"job_id":result.get("job_id"),"coverage":result.get("coverage"),"source_completeness":"not_inferred_from_missing_rows","observed_at":observed.isoformat(),"raw_sha256":digest,"raw_path":str(relative)}
    atomically((root/relative).with_suffix(".manifest.json"),(encode(metadata)+"\n").encode())
    with connect(read_only=False,data_dir=root) as con:
        con.execute("BEGIN")
        con.execute("INSERT OR REPLACE INTO backfill.objects VALUES (?,?,?,?,?,?,?,?,?,?)",[key(job["job_id"],job["cursor"],digest),job["job_id"],job["source"],job["cursor"],observed,str(relative),digest,len(raw),len(compressed),encode(metadata)])
        for row in rows:
            if job["source"]=="arxiv":
                record_key=row["id"]
            elif job["source"]=="pypi_releases":
                record_key=key(row["package"],row["version"],row["filename"],row["artifact_sha256"])
            else:
                record_key=key(row)
            con.execute("INSERT OR REPLACE INTO backfill.records VALUES (?,?,?,?,?,?,?)",[job["job_id"],job["cursor"],record_key,job["source"],row.get("tool_id",job["tool_id"]),observed,encode(row)])
        statistics=result.get("request_metadata",{}).get("statistics",{})
        if "totalBytesBilled" in statistics:
            billed=int(statistics["totalBytesBilled"])
            reserved=con.execute("SELECT reserved_bytes FROM backfill.budget WHERE job_id=?",[job["job_id"]]).fetchone()
            assert reserved and billed<=reserved[0], "Reported query billing exceeds its reservation"
            con.execute("UPDATE backfill.budget SET reserved_bytes=? WHERE job_id=?",[billed,job["job_id"]])
        pagination=result.get("pagination",{})
        complete=pagination.get("complete",True)
        cursor=pagination.get("next_cursor")
        con.execute("UPDATE backfill.jobs SET state=?,cursor=?,attempts=attempts+1,next_attempt=?,updated_at=?,error=NULL,pending=NULL WHERE job_id=?",["done" if complete else "pending",cursor if cursor is not None else job["cursor"],observed,observed,job["job_id"]])
        con.execute("COMMIT")
    return len(rows)


def export_status(root,digest,config):
    with connect(read_only=False,data_dir=root) as con:
        states=[{"source":source,"state":state,"jobs":count} for source,state,count in con.execute("SELECT source,state,count(*) FROM backfill.jobs WHERE plan_sha=? GROUP BY source,state ORDER BY source,state",[digest]).fetchall()]
        records=[{"source":source,"records":count} for source,count in con.execute("SELECT source,count(*) FROM backfill.records GROUP BY source ORDER BY source").fetchall()]
        objects=con.execute("SELECT count(*),coalesce(sum(raw_bytes),0),coalesce(sum(compressed_bytes),0) FROM backfill.objects").fetchone()
        reserved=con.execute("SELECT coalesce(sum(reserved_bytes),0) FROM backfill.budget").fetchone()[0]
        reused=[{"source":source,"windows":count} for source,count in con.execute("SELECT j.source,count(*) FROM backfill.job_reuse r JOIN backfill.jobs j USING(job_id) WHERE j.plan_sha=? GROUP BY j.source ORDER BY j.source",[digest]).fetchall()]
        status={"updated_at":now().isoformat(),"plan_sha256":digest,"start_date":config["start_date"],"history_end_exclusive":min(now().date(),date(2027,1,1)).isoformat(),"requested_max_end_exclusive":config.get("max_end_exclusive","2027-01-01"),"tool_count":config["tool_count"],"states":states,"records":records,"raw_objects":objects[0],"raw_bytes":objects[1],"compressed_bytes":objects[2],"reserved_query_bytes":reserved,"free_GiB":round(shutil.disk_usage(root).free/1024**3,2),"optional_dependency_history":"not_claimed_without_verified_historical_snapshots"}
        status["reused_history_windows"]=reused
        status["reuse_semantics"]="reused windows reference completed prior jobs; records and budget remain cumulative"
        path=root/"processed/backfill/records.parquet"
        path.parent.mkdir(parents=True,exist_ok=True)
        temp=path.with_name("records."+uuid.uuid4().hex+".parquet")
        con.execute("COPY backfill.records TO '"+str(temp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
        assert con.execute("SELECT count(*) FROM read_parquet(?)",[str(temp)]).fetchone()[0]==con.execute("SELECT count(*) FROM backfill.records").fetchone()[0]
        temp.replace(path)
    atomically(root/"backfill-status.json",(encode(status)+"\n").encode())
    return status


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=ROOT/"config/backfill.json")
    parser.add_argument("--max-tasks",type=int,default=50)
    parser.add_argument("--max-seconds",type=int,default=600)
    parser.add_argument("--status-only",action="store_true")
    args=parser.parse_args()
    config=json.loads(args.config.read_text())
    root=Path(config.get("data_dir",str(ROOT/"data"))).expanduser().resolve()
    registry=json.loads((ROOT/config["tool_registry"]).read_text())
    config["tool_count"]=len(registry["tools"])
    registry_payload=encode(registry).encode()
    registry_sha=hashlib.sha256(registry_payload).hexdigest()
    digest=hashlib.sha256(FORMAT_VERSION.encode()+b"\0"+registry_payload).hexdigest()
    registry_by_id={tool["tool_id"]:tool for tool in registry["tools"]}
    registry_folder=root/"raw/backfill/registries"/digest
    registry_folder.mkdir(parents=True,exist_ok=True)
    atomically(registry_folder/"tools.json",registry_payload)
    atomically(registry_folder/"manifest.json",(encode({"registry_sha256":registry_sha,"plan_sha256":digest,"format_version":FORMAT_VERSION})+"\n").encode())
    with (root/".pilot-collection.lock").open("a") as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print(encode({"status":"skipped","reason":"another collection process holds the project lock"}))
            return 0
        time.sleep(3)
        cooldown=root/".arxiv-cooldown.json"
        if cooldown.exists():
            until=datetime.fromisoformat(json.loads(cooldown.read_text())["until"])
            if until>now():
                print(encode({"status":"paused_rate_limit","resume_after":until}),flush=True)
                return 0
        plan(root,config,registry,digest)
        if args.status_only:
            print(json.dumps(export_status(root,digest,config),indent=2))
            return 0
        started=time.monotonic()
        for iteration in range(args.max_tasks):
            if time.monotonic()-started>args.max_seconds or shutil.disk_usage(root).free<config.get("minimum_free_GiB",15)*1024**3:
                break
            preferred=("pypi","gharchive","pypi_releases","arxiv")[iteration%4]
            job=next_job(root,digest,preferred) or next_job(root,digest)
            if job is None:
                break
            try:
                if job["source"]=="arxiv":
                    result=fetch_arxiv_page(registry_by_id[job["tool_id"]]["arxiv_query"],job["start_date"],job["end_date"],cursor=job["cursor"],page_size=100)
                elif job["source"]=="pypi_releases":
                    result=releases(registry_by_id[job["tool_id"]],job["start_date"],job["end_date"])
                else:
                    project=os.environ.get("BQ_PROJECT_ID",config.get("bigquery_project"))
                    cap=int(os.environ.get("BQ_MAX_BYTES_PER_QUERY",config["max_bytes_per_query"]))
                    if job.get("pending"):
                        result=resume_aggregate(json.loads(job["pending"]),poll_timeout=30)
                    else:
                        preview=fetch_aggregate(job["source"],project,registry,job["start_date"],job["end_date"],cap,dry_run_only=True)
                        if preview["status"]!="dry_run_ok":
                            result=preview
                        elif not config.get("allow_billed_queries",False):
                            result={**preview,"ok":False,"status":"blocked_budget","error":{"message":"Query execution is disabled until a total scan budget is set"}}
                        else:
                            estimate=int(preview["request_metadata"]["estimated_bytes_processed"])
                            rounded=max(10*1024**2,((estimate+10*1024**2-1)//(10*1024**2))*(10*1024**2))
                            execution_cap=reserve(root,job,min(cap,rounded),config["max_total_query_bytes"])
                            if execution_cap is None:
                                result={**preview,"ok":False,"status":"blocked_budget","error":{"message":"Total query reservation cap reached or an existing reservation needs review"}}
                            else:
                                refresh_id="radar_refresh_"+job["job_id"] if job["generation"].startswith("refresh:") else None
                                result=fetch_aggregate(job["source"],project,registry,job["start_date"],job["end_date"],execution_cap,poll_timeout=30,job_id=refresh_id)
                if result.get("ok") and result.get("status","ok")=="ok":
                    count=save(root,job,result)
                    print(encode({"source":job["source"],"tool":job["tool_id"],"start":str(job["start_date"]),"end":str(job["end_date"]),"records":count,"status":"saved"}),flush=True)
                elif job["source"]=="arxiv" and "429" in str((result.get("error") or {}).get("message","")):
                    delay=max(120,float((result.get("error") or {}).get("retry_after_seconds") or 0))
                    defer(root,job,"retry",result.get("error"),delay=delay)
                    atomically(root/".arxiv-cooldown.json",(encode({"until":now()+timedelta(seconds=delay),"reason":"HTTP 429; global arXiv cooldown"})+"\n").encode())
                    print(encode({"status":"paused_rate_limit","delay_seconds":delay,"source":"arxiv"}),flush=True)
                    break
                elif (result.get("error") or {}).get("code")=="split_required":
                    split(root,job)
                elif (result.get("error") or {}).get("type")=="bytes_cap_exceeded":
                    split(root,job)
                elif result.get("status")=="pending":
                    defer(root,job,"query_pending",result.get("error"),result,delay=60)
                else:
                    defer(root,job,"blocked" if "blocked" in result.get("status","") else "retry",result.get("error") or {"message":result.get("status")},delay=21600 if "blocked" in result.get("status","") else 1800)
                    print(encode({"source":job["source"],"status":result.get("status"),"error":result.get("error")}),flush=True)
            except Exception as error:
                defer(root,job,"retry",{"type":type(error).__name__,"message":str(error)},delay=1800)
                print(encode({"source":job["source"],"status":"retry","error":str(error)}),flush=True)
        report=export_status(root,digest,config)
        print(json.dumps(report,indent=2),flush=True)
        time.sleep(3)
    return 0


if __name__=="__main__":
    raise SystemExit(main())
