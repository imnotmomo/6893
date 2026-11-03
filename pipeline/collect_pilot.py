#!/usr/bin/env python3
"""Collect a bounded, current metadata pilot. This does not backfill model features."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import http.client
import json
from pathlib import Path
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "duckdb"))
from query import connect
from arxiv_source import fetch_candidate_arxiv_metadata


USER_AGENT = "EECS6893-AITechnologyRadar/0.1 (https://github.com/imnotmomo/6893)"
SCHEMA = """
CREATE SCHEMA IF NOT EXISTS pilot;
CREATE TABLE IF NOT EXISTS pilot.runs (
  run_id VARCHAR PRIMARY KEY, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
  status VARCHAR, config_sha256 VARCHAR, collector_sha256 VARCHAR, summary JSON
);
ALTER TABLE pilot.runs ADD COLUMN IF NOT EXISTS manifest_sha256 VARCHAR;
CREATE TABLE IF NOT EXISTS pilot.tools (
  run_id VARCHAR, tool_id VARCHAR, name VARCHAR, pypi_package VARCHAR,
  github_repo VARCHAR, mapping_status VARCHAR, mapping JSON,
  PRIMARY KEY (run_id, tool_id)
);
CREATE TABLE IF NOT EXISTS pilot.source_responses (
  run_id VARCHAR, tool_id VARCHAR, source VARCHAR, observed_at TIMESTAMPTZ,
  status VARCHAR, http_status INTEGER, request_url VARCHAR, raw_path VARCHAR,
  raw_sha256 VARCHAR, raw_bytes BIGINT, compressed_bytes BIGINT,
  response_headers JSON, record JSON, error JSON,
  PRIMARY KEY (run_id, tool_id, source)
);
CREATE TABLE IF NOT EXISTS pilot.paper_candidates (
  run_id VARCHAR, tool_id VARCHAR, paper_id VARCHAR, title VARCHAR,
  published VARCHAR, updated VARCHAR, summary VARCHAR, authors JSON,
  links JSON, requires_manual_review BOOLEAN,
  PRIMARY KEY (run_id, tool_id, paper_id)
);
"""


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def fetch_json(url):
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip", "Accept": "application/json"}
    if urllib.parse.urlparse(url).netloc == "api.github.com":
        headers.update({"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10"})
    outcome = {"ok": False, "request_url": url, "raw_bytes": b"", "http": {"status": None, "headers": {}}, "error": None}
    for attempt in range(2):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=25) as response:
                outcome["http"] = {"status": response.status, "headers": dict(response.headers)}
                stream = gzip.GzipFile(fileobj=response) if response.headers.get("Content-Encoding") == "gzip" else response
                body = stream.read(32 * 1024 * 1024 + 1)
                if len(body) > 32 * 1024 * 1024:
                    raise ValueError("Response exceeds the 32 MiB pilot limit")
                outcome["raw_bytes"] = body
                outcome["json"] = json.loads(body)
                outcome["ok"] = True
                outcome["error"] = None
                return outcome
        except urllib.error.HTTPError as error:
            outcome["http"] = {"status": error.code, "headers": dict(error.headers)}
            outcome["raw_bytes"] = error.read(64 * 1024)
            outcome["error"] = {"type": "http", "message": str(error)}
            if error.code not in (429, 500, 502, 503, 504):
                break
        except (OSError, ValueError, EOFError, http.client.HTTPException) as error:
            outcome["error"] = {"type": type(error).__name__, "message": str(error)}
        if attempt == 0:
            time.sleep(3)
    return outcome


def normalize(source, payload):
    if source == "github":
        keys = ["id", "full_name", "html_url", "description", "created_at", "updated_at", "pushed_at", "stargazers_count", "forks_count", "open_issues_count", "subscribers_count", "archived", "disabled", "default_branch", "language"]
        return {**{key: payload.get(key) for key in keys}, "snapshot_only": True}
    info = payload["info"]
    return {
        "name": info["name"], "version": info["version"], "summary": info.get("summary"),
        "requires_python": info.get("requires_python"), "requires_dist": info.get("requires_dist"),
        "project_urls": info.get("project_urls"), "releases_visible_now": len(payload.get("releases", {})),
        "last_serial": payload.get("last_serial"), "snapshot_only": True,
        "download_counts": None, "download_counts_status": "requires_separate_historical_download_source",
    }


def save_response(root, run_id, tool_id, source, result):
    body = result.get("raw_bytes") or b""
    http = result.get("http") or {}
    relative = None
    compressed_size = 0
    if body:
        suffix = "xml" if source == "arxiv" else "json"
        path = root / "raw/pilot" / run_id / source / f"{tool_id}.{suffix}.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        compressed = gzip.compress(body, compresslevel=6, mtime=0)
        path.write_bytes(compressed)
        assert hashlib.sha256(gzip.decompress(path.read_bytes())).digest() == hashlib.sha256(body).digest()
        relative = str(path.relative_to(root))
        compressed_size = len(compressed)
    return {
        "tool_id": tool_id, "source": source, "observed_at": now(),
        "status": "ok" if result["ok"] else "failed", "http_status": http.get("status"),
        "request_url": result["request_url"], "raw_path": relative,
        "raw_sha256": hashlib.sha256(body).hexdigest() if body else None,
        "raw_bytes": len(body), "compressed_bytes": compressed_size,
        "response_headers": http.get("headers", {}), "record": result.get("record", {}),
        "error": result.get("error"), "papers": result.get("papers", []),
    }


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def validate_manifest(root, manifest):
    run_id = manifest["run_id"]
    assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", run_id), "Invalid run ID"
    directory = (root / "raw/pilot" / run_id).resolve()
    assert hashlib.sha256((directory / "config.original.json").read_bytes()).hexdigest() == manifest["config_sha256"], "Original config checksum mismatch"
    assert hashlib.sha256((directory / "collector.py").read_bytes() + (directory / "arxiv_source.py").read_bytes()).hexdigest() == manifest["collector_sha256"], "Collector source checksum mismatch"
    for row in manifest["responses"]:
        if row["raw_path"]:
            path = (root / row["raw_path"]).resolve()
            assert path.is_relative_to(directory), "Raw response path is outside this run"
            compressed = path.read_bytes()
            body = gzip.decompress(compressed)
            assert len(compressed) == row["compressed_bytes"] and len(body) == row["raw_bytes"]
            assert hashlib.sha256(body).hexdigest() == row["raw_sha256"], "Raw response checksum mismatch"


def persist(root, manifest):
    validate_manifest(root, manifest)
    run_id = manifest["run_id"]
    manifest_hash = hashlib.sha256(encode(manifest).encode()).hexdigest()
    export = root / "processed/pilot" / run_id
    export.mkdir(parents=True, exist_ok=True)
    with connect(read_only=False, data_dir=root) as database:
        database.execute(SCHEMA)
        previous = database.execute("SELECT manifest_sha256 FROM pilot.runs WHERE run_id = ?", [run_id]).fetchone()
        assert not previous or not previous[0] or previous[0] == manifest_hash, "Run IDs are immutable; changed manifests require a new run"
        database.execute("BEGIN TRANSACTION")
        try:
            database.execute("INSERT OR REPLACE INTO pilot.runs (run_id, started_at, finished_at, status, config_sha256, collector_sha256, summary, manifest_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [run_id, manifest["started_at"], manifest["finished_at"], manifest["status"], manifest["config_sha256"], manifest["collector_sha256"], encode(manifest["summary"]), manifest_hash])
            for tool in manifest["tools"]:
                database.execute("INSERT OR REPLACE INTO pilot.tools VALUES (?, ?, ?, ?, ?, ?, ?)", [run_id, tool["tool_id"], tool["name"], tool["pypi_package"], tool["github_repo"], tool["mapping_status"], encode(tool)])
            for row in manifest["responses"]:
                database.execute("INSERT OR REPLACE INTO pilot.source_responses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, row["tool_id"], row["source"], row["observed_at"], row["status"], row["http_status"], row["request_url"], row["raw_path"], row["raw_sha256"], row["raw_bytes"], row["compressed_bytes"], encode(row["response_headers"]), encode(row["record"]), encode(row["error"])])
                for paper in row["papers"]:
                    database.execute("INSERT OR REPLACE INTO pilot.paper_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, row["tool_id"], paper["id"], paper["title"], paper.get("published"), paper.get("updated"), paper.get("summary"), encode(paper.get("authors", [])), encode(paper.get("links", [])), True])
            database.execute("COMMIT")
        except Exception:
            database.execute("ROLLBACK")
            raise
        counts = {}
        for table in ("runs", "tools", "source_responses", "paper_candidates"):
            query = f"SELECT * FROM pilot.{table} WHERE run_id = {sql_string(run_id)}"
            temporary = export / f"{table}.{uuid.uuid4().hex}.tmp.parquet"
            target = export / f"{table}.parquet"
            database.execute(f"COPY ({query}) TO {sql_string(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)")
            expected = database.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
            actual = database.execute("SELECT count(*) FROM read_parquet(?)", [str(temporary)]).fetchone()[0]
            assert expected == actual, f"Parquet row mismatch: {table}"
            temporary.replace(target)
            counts[table] = actual
    assert counts["tools"] == len(manifest["tools"])
    assert counts["source_responses"] == len(manifest["responses"])
    assert counts["paper_candidates"] == len({(row["tool_id"], paper["id"]) for row in manifest["responses"] for paper in row["papers"]})
    return {"run_id": run_id, "status": manifest["status"], **manifest["summary"], "database_rows": counts, "export_path": str(export), "database_path": str(root / "radar.duckdb"), "model_ready": False}


def collect(root, config, limit, arxiv_max):
    tools = json.loads(config.read_text())["tools"][:limit]
    assert tools and len({tool["tool_id"] for tool in tools}) == len(tools)
    for tool in tools:
        assert re.fullmatch(r"[a-z0-9][a-z0-9_-]*", tool["tool_id"])
    assert shutil.disk_usage(root).free >= 10 * 1024**3, "Keep at least 10 GiB free before collecting"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    directory = root / "raw/pilot" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    collector = Path(__file__).read_bytes()
    arxiv = Path(__file__).with_name("arxiv_source.py").read_bytes()
    collector_hash = hashlib.sha256(collector + arxiv).hexdigest()
    manifest = {"run_id": run_id, "started_at": now(), "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(), "collector_sha256": collector_hash, "tools": tools, "responses": [], "scope": "Current metadata snapshots and manually reviewable paper candidates; no historical adoption labels"}
    (directory / "config.original.json").write_bytes(config.read_bytes())
    (directory / "collector.py").write_bytes(collector)
    (directory / "arxiv_source.py").write_bytes(arxiv)
    (directory / "tools.json").write_text(encode({"tools": tools}) + "\n")
    for tool in tools:
        for source, url in [("pypi", f"https://pypi.org/pypi/{urllib.parse.quote(tool['pypi_package'])}/json"), ("github", f"https://api.github.com/repos/{tool['github_repo']}")]:
            result = fetch_json(url)
            if result["ok"]:
                try:
                    result["record"] = normalize(source, result["json"])
                except (KeyError, TypeError, ValueError) as error:
                    result["ok"] = False
                    result["error"] = {"type": "parse", "message": str(error)}
            row = save_response(root, run_id, tool["tool_id"], source, result)
            manifest["responses"].append(row)
            print(encode({"tool": tool["tool_id"], "source": source, "status": row["status"], "bytes": row["compressed_bytes"]}), flush=True)
            if source == "github":
                time.sleep(1)
        upper = datetime.now(timezone.utc).strftime("%Y%m%d2359")
        query = f"({tool['arxiv_query']}) AND submittedDate:[202301010000 TO {upper}]"
        result = fetch_candidate_arxiv_metadata(query, max_results=arxiv_max)
        result["record"] = {"query": query, "candidate_count": len(result.get("papers", [])) if result["ok"] else None, "requires_manual_review": True, "historical_feature_ready": False, "pagination": result.get("pagination"), "attempts": result.get("attempts")}
        row = save_response(root, run_id, tool["tool_id"], "arxiv", result)
        manifest["responses"].append(row)
        print(encode({"tool": tool["tool_id"], "source": "arxiv", "status": row["status"], "candidates": len(row["papers"])}), flush=True)
        (directory / "manifest.in-progress.json").write_text(encode(manifest) + "\n")
    statuses = Counter((row["source"], row["status"]) for row in manifest["responses"])
    failed = sum(row["status"] != "ok" for row in manifest["responses"])
    manifest.update({"finished_at": now(), "status": "completed" if not failed else "partial", "summary": {"selected_tools": len(tools), "successful_requests": len(manifest["responses"]) - failed, "failed_requests": failed, "source_status": {source: {status: statuses[source, status] for status in ("ok", "failed")} for source in ("github", "pypi", "arxiv")}, "paper_candidates": sum(len(row["papers"]) for row in manifest["responses"]), "raw_bytes": sum(row["raw_bytes"] for row in manifest["responses"]), "compressed_bytes": sum(row["compressed_bytes"] for row in manifest["responses"]), "historical_downloads": "pending_bigquery", "historical_github_activity": "pending_gh_archive"}})
    (directory / "manifest.json").write_text(encode(manifest) + "\n")
    (directory / "manifest.in-progress.json").unlink()
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] / "config/tools.pilot.json")
    parser.add_argument("--data-dir", type=Path, default=Path.home() / "6893/data")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--arxiv-max-results", type=int, default=5)
    parser.add_argument("--import-run", type=Path, help="Reimport an existing manifest without repeating HTTP requests")
    args = parser.parse_args()
    if not 1 <= args.limit <= 40 or not 1 <= args.arxiv_max_results <= 5:
        parser.error("--limit must be 1–40 and --arxiv-max-results 1–5")
    root = args.data_dir.expanduser().resolve()
    assert (root / "radar.duckdb").is_file(), "Initialize DuckDB before collection"
    with (root / ".pilot-collection.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = json.loads(args.import_run.read_text()) if args.import_run else collect(root, args.config, args.limit, args.arxiv_max_results)
        report = persist(root, manifest)
        (root / "pilot-latest.json").write_text(encode(report) + "\n")
        print(json.dumps(report, indent=2), flush=True)
        return 0 if report["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
