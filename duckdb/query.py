#!/usr/bin/env python3
"""Query the c0 project database; database writes require --write."""

import argparse
import csv
import os
from pathlib import Path
import sys

import duckdb


def connect(*, read_only=True, data_dir=None):
    root = Path(data_dir or os.environ.get("RADAR_DATA_DIR", "~/6893/data")).expanduser().resolve()
    database = root / "radar.duckdb"
    if read_only and not database.is_file():
        raise FileNotFoundError(f"Database not initialized: {database}. Run setup.sh on c0 first.")
    if not read_only:
        root.mkdir(parents=True, exist_ok=True)
        (root / "tmp").mkdir(exist_ok=True)
    return duckdb.connect(
        str(database),
        read_only=read_only,
        config={
            "memory_limit": "1GB",
            "threads": 2,
            "temp_directory": str(root / "tmp"),
            "max_temp_directory_size": "5GB",
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("sql", nargs="?", help="SQL to execute; defaults to database information")
    source.add_argument("--file", type=Path, help="Read SQL from a file")
    parser.add_argument("--write", action="store_true", help="Open this database for writing")
    parser.add_argument("--init", action="store_true", help="Initialize the database without replacing data")
    parser.add_argument("--data-dir", help="Override RADAR_DATA_DIR (default: ~/6893/data)")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum rows printed, from 1 to 10000")
    args = parser.parse_args()
    if not 1 <= args.limit <= 10000:
        parser.error("--limit must be from 1 to 10000")
    try:
        sql = args.file.read_text() if args.file else args.sql
        with connect(read_only=not (args.write or args.init), data_dir=args.data_dir) as connection:
            if args.init:
                connection.execute("CREATE SCHEMA IF NOT EXISTS main")
            connection.execute("SET schema = 'main'")
            result = connection.execute(sql or "SELECT version() AS duckdb_version, current_database() AS database, current_schema() AS schema")
            if result.description:
                writer = csv.writer(sys.stdout, lineterminator="\n")
                writer.writerow([column[0] for column in result.description])
                rows = result.fetchmany(args.limit + 1)
                writer.writerows(rows[:args.limit])
                if len(rows) > args.limit:
                    print(f"Output limited to {args.limit} rows. Use SQL COPY for a full export.", file=sys.stderr)
        return 0
    except (duckdb.Error, OSError) as error:
        print(f"DuckDB error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
