#!/usr/bin/env python3
"""Verify the latest c0 pilot without making new source API requests."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import urllib.request

ROOT = Path.home() / "6893"
sys.path.insert(0, str(ROOT / "pipeline"))
from collect_pilot import persist, validate_manifest, save_response
from query import connect


def main():
    latest = json.loads((ROOT / "data/pilot-latest.json").read_text())
    manifest = json.loads((ROOT / "data/raw/pilot" / latest["run_id"] / "manifest.json").read_text())
    validate_manifest(ROOT / "data", manifest)
    assert persist(ROOT / "data", manifest)["database_rows"] == latest["database_rows"]

    changed = deepcopy(manifest)
    changed["tools"][0]["notes"] += " verification change"
    try:
        persist(ROOT / "data", changed)
    except AssertionError as error:
        assert "immutable" in str(error)
    else:
        raise AssertionError("A changed manifest reused an existing run ID")

    corrupt = deepcopy(manifest)
    corrupt["responses"][0]["raw_sha256"] = "0" * 64
    try:
        validate_manifest(ROOT / "data", corrupt)
    except AssertionError as error:
        assert "checksum" in str(error)
    else:
        raise AssertionError("Corrupt source evidence was accepted")

    failed = save_response(ROOT / "data", latest["run_id"], "test", "arxiv", {
        "ok": False, "http": None, "request_url": "https://export.arxiv.org/api/query",
        "error": {"code": "network_error"},
    })
    assert failed["status"] == "failed" and failed["http_status"] is None and failed["raw_path"] is None

    with connect(data_dir=ROOT / "data") as connection:
        counts = {
            table: connection.execute(f"SELECT count(*) FROM pilot.{table} WHERE run_id = ?", [latest["run_id"]]).fetchone()[0]
            for table in ["runs", "tools", "source_responses", "paper_candidates"]
        }
        assert counts == latest["database_rows"]
        rows = connection.execute("SELECT source, record FROM pilot.source_responses WHERE run_id = ? AND status = ?", [latest["run_id"], "ok"]).fetchall()
        for source, record in rows:
            record = json.loads(record)
            if source in ("github", "pypi"):
                assert record["snapshot_only"] is True
            if source == "pypi":
                assert record["download_counts"] is None
            if source == "arxiv":
                assert record["requires_manual_review"] is True and record["historical_feature_ready"] is False
        for table, count in counts.items():
            path = ROOT / "data/processed/pilot" / latest["run_id"] / (table + ".parquet")
            assert connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] == count
            assert {row[0] for row in connection.execute("SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]).fetchall()} == {"ZSTD"}
        distinct_papers = connection.execute("SELECT count(DISTINCT paper_id) FROM pilot.paper_candidates WHERE run_id = ?", [latest["run_id"]]).fetchone()[0]

    with urllib.request.urlopen("http://127.0.0.1:8080/json/", timeout=5) as response:
        spark = json.load(response)
    assert spark["aliveworkers"] == 3
    report = {
        "status": "passed", "run_id": latest["run_id"], "database_rows": counts,
        "distinct_arxiv_papers": distinct_papers, "reimport_without_duplicates": True,
        "immutable_run_rejection": True, "corrupt_raw_rejection": True,
        "missing_source_is_not_zero": True, "snapshot_and_review_flags": True,
        "parquet_row_counts_and_zstd": True, "spark_alive_workers": 3,
    }
    (ROOT / "data/pilot-verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
