# EECS 6893 Final Project

**AI Technology Radar**

We are building a data spine that combines arXiv, GitHub, and PyPI signals to forecast AI-tool adoption and distinguish sustained growth from short-term attention.

## Cluster setup

Our Spark cluster runs on three Oracle Cloud VMs. **c0** (2 OCPUs, 12 GB RAM) hosts the master, driver, and main worker. **c1** and **c2** (1 GB RAM each) are small workers used for distributed demos; normal jobs run on c0.

DuckDB 2.0.0 runs on c0 as the project’s persistent analytical database. The repository’s `duckdb/` folder contains server setup, query, and verification scripts; verification uses temporary test data. Connect to c0 and run `6893-duckdb` to query the database.

See the [connection guide](connection_guide.md) for cluster access and job submission.
