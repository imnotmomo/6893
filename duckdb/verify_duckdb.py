#!/usr/bin/env python3

import argparse
from contextlib import closing
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import uuid


FIXTURE_ROWS = [
    (1, "alpha", date(2026, 1, 5), 7, Decimal("0.1250"), True),
    (2, "工具-café", date(2026, 1, 12), None, None, False),
    (3, "beta", date(2026, 1, 19), 13, Decimal("-1.2500"), None),
]
FIXTURE_TYPES = ["INTEGER", "VARCHAR", "DATE", "BIGINT", "DECIMAL(10,4)", "BOOLEAN"]


def verify(condition, message):
    if not condition:
        raise RuntimeError(message)


def size_in_bytes(value):
    match = re.fullmatch(r"\s*([\d.]+)\s*([KMGTPE]?i?B)\s*", value, re.IGNORECASE)
    if not match:
        raise ValueError(f"Unrecognized size returned by DuckDB: {value!r}")
    unit = match.group(2).upper()
    exponent = "BKMGTPE".index(unit[0]) if unit != "B" else 0
    multiplier = 1024 if "I" in unit else 1000
    return float(match.group(1)) * multiplier ** exponent


def helper_query(helper, root, sql):
    return subprocess.run(
        [str(helper), "--data-dir", str(root), sql],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def inspect_helper(helper, root, expected_version):
    result = helper_query(
        helper,
        root,
        "SELECT version() AS version, current_schema() AS schema, "
        "current_setting('access_mode') AS access_mode, "
        "current_setting('memory_limit') AS memory_limit, "
        "current_setting('threads') AS threads, "
        "current_setting('temp_directory') AS temp_directory, "
        "current_setting('max_temp_directory_size') AS max_temp_directory_size",
    )
    verify(result.returncode == 0, f"Installed query helper failed: {result.stderr.strip()[:400]}")
    rows = list(csv.DictReader(io.StringIO(result.stdout)))
    verify(len(rows) == 1, "The installed helper did not return one settings row")
    settings = rows[0]
    verify(settings["version"].removeprefix("v") == expected_version, "Helper DuckDB version differs from the pinned version")
    verify(settings["schema"] == "main", "Helper default schema is not main")
    verify(settings["access_mode"].upper() == "READ_ONLY", "Helper did not open the database read-only")
    verify(int(settings["threads"]) == 2, "Helper thread setting is not 2")
    verify(
        math.isclose(size_in_bytes(settings["memory_limit"]), 1_000_000_000, rel_tol=0.02),
        "Helper memory setting is not approximately 1 GB",
    )
    verify(
        math.isclose(size_in_bytes(settings["max_temp_directory_size"]), 5_000_000_000, rel_tol=0.02),
        "Helper spill setting is not approximately 5 GB",
    )
    verify(
        Path(settings["temp_directory"]).expanduser().resolve() == root / "tmp",
        "Helper spill directory is not inside the configured data directory",
    )
    return settings


def verify_fixture(duckdb, root):
    with tempfile.TemporaryDirectory(prefix="duckdb-verify-", dir=root / "tmp") as temporary:
        directory = Path(temporary)
        database = directory / "fixture.duckdb"
        parquet = directory / "fixture.parquet"
        config = {
            "memory_limit": "1GB",
            "threads": 2,
            "temp_directory": str(directory / "spill"),
            "max_temp_directory_size": "5GB",
        }
        with closing(duckdb.connect(str(database), config=config)) as connection:
            connection.execute(
                "CREATE TABLE fixture (id INTEGER, tool VARCHAR, week DATE, "
                "downloads BIGINT, score DECIMAL(10,4), active BOOLEAN)"
            )
            connection.executemany("INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?)", FIXTURE_ROWS)
        verify(database.is_file() and database.stat().st_size > 0, "The fixture database was not persisted")

        with closing(duckdb.connect(str(database), read_only=True, config=config)) as connection:
            result = connection.execute("SELECT * FROM fixture ORDER BY id")
            verify([str(column[1]) for column in result.description] == FIXTURE_TYPES, "Persisted SQL types differ from the fixture")
            verify(result.fetchall() == FIXTURE_ROWS, "Reopened fixture rows differ, including nulls, dates, decimals, or Unicode")
            verify(
                connection.execute("SELECT count(*), sum(downloads) FROM fixture").fetchone() == (3, 20),
                "Persisted fixture count or sum is incorrect",
            )
            parquet_literal = "'" + str(parquet).replace("'", "''") + "'"
            connection.execute(
                f"COPY (SELECT * FROM fixture ORDER BY id) TO {parquet_literal} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            verify(parquet.is_file() and parquet.stat().st_size > 0, "Parquet export was not created")
            result = connection.execute("SELECT * FROM read_parquet(?) ORDER BY id", [str(parquet)])
            verify([str(column[1]) for column in result.description] == FIXTURE_TYPES, "Parquet SQL types differ from the fixture")
            verify(result.fetchall() == FIXTURE_ROWS, "Parquet round-trip rows differ from the fixture")
            try:
                codecs = sorted({
                    str(row[0]).upper()
                    for row in connection.execute(
                        "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(parquet)]
                    ).fetchall()
                })
            except duckdb.CatalogException as error:
                if "parquet_metadata" not in str(error) or "does not exist" not in str(error):
                    raise
                codecs = None
            if codecs is not None:
                verify(codecs == ["ZSTD"], f"Parquet metadata reports unexpected codecs: {codecs}")
            details = {
                "persistent_reopen": "passed",
                "typed_rows": len(FIXTURE_ROWS),
                "downloads_sum": 20,
                "parquet_round_trip": "passed",
                "parquet_codec": codecs if codecs is not None else "metadata function unavailable",
            }
    verify(not directory.exists(), "Temporary fixture files were not removed")
    details["temporary_files_removed"] = True
    return details


def verify_read_only(duckdb, helper, root):
    name = "__verification_must_fail_" + uuid.uuid4().hex
    database = root / "radar.duckdb"

    def object_count():
        with closing(duckdb.connect(str(database), read_only=True, config={
            "memory_limit": "1GB",
            "threads": 2,
            "temp_directory": str(root / "tmp"),
            "max_temp_directory_size": "5GB",
        })) as connection:
            return connection.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?", [name]
            ).fetchone()[0]

    verify(object_count() == 0, "The verification object unexpectedly already exists")
    result = helper_query(
        helper,
        root,
        f"BEGIN TRANSACTION; CREATE TABLE main.{name} (id INTEGER); ROLLBACK;",
    )
    absent = object_count() == 0
    verify(absent, "The installed helper left a verification object in the production database")
    verify(result.returncode != 0, "The default helper unexpectedly allowed a database write")
    verify(
        re.search(r"read[-_ ]only", result.stderr, re.IGNORECASE) is not None,
        f"The write failed for a reason other than read-only protection: {result.stderr.strip()[:400]}",
    )
    return {"write_rejected": True, "helper_exit_code": result.returncode, "production_object_absent": absent}


def main():
    parser = argparse.ArgumentParser(description="Verify the c0 DuckDB installation without adding production data")
    parser.add_argument("--data-dir", default=os.environ.get("RADAR_DATA_DIR", "~/6893/data"))
    parser.add_argument("--helper", type=Path, default=Path("/usr/local/bin/6893-duckdb"))
    parser.add_argument("--expected-version", default="2.0.0")
    args = parser.parse_args()
    root = Path(args.data_dir).expanduser().resolve()
    report = {
        "status": "failed",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "architecture": platform.machine(),
    }
    try:
        import duckdb

        report["duckdb_version"] = duckdb.__version__
        verify(duckdb.__version__ == args.expected_version, "DuckDB does not match the pinned version")
        verify((root / "radar.duckdb").is_file(), "The production database has not been initialized")
        verify((root / "tmp").is_dir(), "The configured temporary directory does not exist")
        helper = args.helper.expanduser().resolve()
        verify(helper.is_file() and os.access(helper, os.X_OK), "The installed query helper is missing or not executable")
        report["settings"] = inspect_helper(helper, root, args.expected_version)
        report["fixture"] = verify_fixture(duckdb, root)
        report["read_only"] = verify_read_only(duckdb, helper, root)
        report["status"] = "passed"
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"[:700]
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
