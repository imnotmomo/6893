# DuckDB on c0

These scripts support the project’s persistent DuckDB database on c0 at `/home/ubuntu/6893/data/radar.duckdb`.

- `setup.sh` installs the pinned package on c0 and initializes missing storage.
- `query.py` provides the `6893-duckdb` query helper.
- `verify_duckdb.py` checks the installation using temporary test fixtures, then removes them.

The project database is separate from the verification fixtures. Current metadata and paper candidates from the first ten-tool collection are stored in the `pilot` schema; initial historical batches and durable checkpoints are in `backfill`. Full historical coverage and the modeling table are not yet complete. See the [connection guide](../connection_guide.md#duckdb-storage) for usage.
