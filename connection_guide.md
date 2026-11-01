# Connection Guide

[Back to README](README.md)

Connection and job submission instructions for the project cluster. Last verified on November 1, 2026: c0 was the active master, with all three workers registered as `ALIVE`.

## Cluster addresses

| Node | Public IP for SSH | Private IP | Resources | Role |
| --- | --- | --- | --- | --- |
| c0 | `129.159.41.246` | `10.0.0.132` | A1.Flex, Arm, 2 OCPUs / 12 GB RAM | Master, client driver, main worker |
| c1 | `170.9.29.30` | `10.0.0.228` | E2.1.Micro, AMD, 1 GB RAM | Small worker |
| c2 | `146.235.239.109` | `10.0.0.90` | E2.1.Micro, AMD, 1 GB RAM | Small worker |

The Spark master URL is `spark://10.0.0.132:7077`. This is a private address inside the Oracle VCN. Connect to c0 over SSH and submit jobs there. The previous master on c1 is disabled.

Runtime: Spark **3.5.9**, Java **17.0.21**, Python **3.11.18**. Spark is installed at `/opt/6893/spark`; its Python runtime is `/opt/6893/python/bin/python3.11`.

## Connect from your computer

The team shares the existing authorized SSH private key, `6893.key`. Each teammate should receive a copy through a private channel and save it at `~/.ssh/6893.key` on their own computer. Everyone connects as `ubuntu` with the same account permissions. No additional server configuration is needed. Keep the key out of Git; the repository ignores `*.key` files in every folder.

Run in your **local terminal**:

```sh
chmod 600 ~/.ssh/6893.key
ssh -o IdentitiesOnly=yes -i ~/.ssh/6893.key ubuntu@129.159.41.246
```

Optional worker connections:

```sh
# c1
ssh -o IdentitiesOnly=yes -i ~/.ssh/6893.key ubuntu@170.9.29.30

# c2
ssh -o IdentitiesOnly=yes -i ~/.ssh/6893.key ubuntu@146.235.239.109
```

c0's current OS hostname is `instance-20261101-1653`; its Oracle display name is `6893-c0`.

## Upload and run a job

Upload your Python script from your **local terminal**, replacing `my_job.py` with your local script:

```sh
scp -o IdentitiesOnly=yes -i ~/.ssh/6893.key \
  my_job.py ubuntu@129.159.41.246:~/6893/my_job.py
```

Then run this **inside your SSH session on c0**:

```sh
6893-spark-submit ~/6893/my_job.py
```

The installed wrapper loads the cluster's private configuration and uses a 2 GiB driver with one 6 GiB, two-core executor. Only c0's worker has enough memory for this profile; c1 and c2 stay registered but idle. Run one application at a time.

The smoke test is already installed on c0 and can check this profile:

```sh
6893-spark-submit ~/6893/smoke_test.py \
  --expected-hosts "$(hostname)" \
  --output ~/6893/smoke-result-c0.json
```

Download a result from your **local terminal**:

```sh
scp -o IdentitiesOnly=yes -i ~/.ssh/6893.key \
  ubuntu@129.159.41.246:~/6893/smoke-result-c0.json .
```

## Demonstrate all three workers

For a small distributed demonstration, run this **on c0**:

```sh
6893-spark-submit-demo ~/6893/smoke_test.py \
  --expected-hosts "$(hostname),6893-c1,6893-c2" \
  --output ~/6893/smoke-result-three-node.json
```

This helper uses one 512 MiB, one-core executor per host. It refuses to start while another Spark application is active, temporarily reduces c0's worker to one core, and restores its normal two-core allocation when it exits. The 1 GB micro VMs are suitable for tiny demonstration tasks; use the normal c0 profile for project processing.

## Open Spark web interfaces

In a separate **local terminal**, start this tunnel and leave it running:

```sh
ssh -N -o ExitOnForwardFailure=yes -o IdentitiesOnly=yes \
  -i ~/.ssh/6893.key \
  -L 127.0.0.1:8080:127.0.0.1:8080 \
  -L 127.0.0.1:8081:127.0.0.1:8081 \
  -L 127.0.0.1:4040:127.0.0.1:4040 \
  ubuntu@129.159.41.246
```

| Interface | Open on your computer |
| --- | --- |
| Spark master and worker registration | [http://127.0.0.1:8080](http://127.0.0.1:8080) |
| c0 worker | [http://127.0.0.1:8081](http://127.0.0.1:8081) |
| Application stages, SQL, and executors | [http://127.0.0.1:4040](http://127.0.0.1:4040), while a job is running |

Optional worker UI tunnels, each in its own local terminal:

```sh
# c1: open http://127.0.0.1:18081
ssh -N -o ExitOnForwardFailure=yes -o IdentitiesOnly=yes \
  -i ~/.ssh/6893.key \
  -L 127.0.0.1:18081:127.0.0.1:8081 ubuntu@170.9.29.30
```

```sh
# c2: open http://127.0.0.1:28081
ssh -N -o ExitOnForwardFailure=yes -o IdentitiesOnly=yes \
  -i ~/.ssh/6893.key \
  -L 127.0.0.1:28081:127.0.0.1:8081 ubuntu@146.235.239.109
```

Stop a tunnel with `Ctrl+C`. If a local port is occupied, change its local port number (for example, `127.0.0.1:18080:127.0.0.1:8080`) and open the corresponding local URL.

## Check services and connection problems

On **c0**:

```sh
systemctl status 6893-spark-master 6893-spark-worker --no-pager
sudo journalctl -u 6893-spark-master -u 6893-spark-worker -n 80 --no-pager
```

On **c1 or c2**:

```sh
systemctl status 6893-spark-worker --no-pager
sudo journalctl -u 6893-spark-worker -n 80 --no-pager
```

- `Permission denied (publickey)`: use `ubuntu`, check your key path and permissions, and confirm its public key is authorized on that VM.
- SSH timeout: check that the VM is running and its Oracle network rules allow SSH on TCP 22 from your client address.
- Missing workers or tasks that cannot connect: the relevant Oracle Security Lists/NSGs must allow **stateful TCP destination ports 7077–7081**, with source ports **All**, from each peer source CIDR: `10.0.0.132/32`, `10.0.0.228/32`, and `10.0.0.90/32`. The VM firewalls also restrict these ports to cluster peers.
- UI unavailable: keep the tunnel terminal open. The application UI on port 4040 exists only while an application is running.

The Spark UIs bind to loopback and use SSH tunnels. Keep Spark RPC and UI ports private. Authentication and network encryption are configured under `/etc/6893-spark`; its shared secret must stay out of Git, screenshots, and command examples.

## DuckDB storage

DuckDB 2.0.0 is installed on **c0** as an embedded database. It uses a local file and does not need a database server or network port.

**Project storage:** this is the persistent database on c0 for the project pipeline and dashboard. The repository’s `duckdb/` folder contains the scripts used on c0. Only `verify_duckdb.py` creates temporary test fixtures, which it removes after verification; it does not populate the project database.

- Database: `~/6893/data/radar.duckdb`
- Data folders: `~/6893/data/raw`, `processed`, and `published`
- Isolated Python environment: `~/6893/.venv-duckdb`
- Installed query helper: `~/6893/duckdb/query.py`

After connecting to **c0**, run:

```sh
6893-duckdb
6893-duckdb "SHOW TABLES;"
6893-duckdb "SELECT current_setting('memory_limit'), current_setting('threads');"
```

The database uses the standard `main` schema and starts without project tables; source data has not been ingested. Query output is CSV and limited to 1,000 rows by default (`--limit` accepts up to 10,000). Queries open the database read-only unless you explicitly pass `--write` for ingestion. Use `--file` to read SQL from a file.

The helper uses two threads, a 1 GB query memory limit, and a 5 GB temporary spill limit. These do not cap total process memory or the size of database files and exports. Temporary spill files go under `~/6893/data/tmp`.

For direct embedded access, use one writer process or multiple read-only processes. Close other database connections before a separate process writes. A dashboard that stays open during ingestion should read published Parquet snapshots.

The pinned ARM/Python 3.11 dependency and installer are in [duckdb/](duckdb/). To reproduce the setup, run this **on c0 from a clone of this repository**:

```sh
bash duckdb/setup.sh
```

The installer initializes missing storage without replacing existing data. Compression, collectors, and scheduled cleanup are separate pipeline work; installing DuckDB does not configure them.

## Data paths

c0’s root partition and ext4 filesystem were expanded on November 1, 2026. The disk is 93 GiB (about 100 GB decimal), with an 89.0 GiB root filesystem and about 83.9 GiB free immediately after expansion. c1 and c2 retain their existing disks; their root partitions already fill the available space.

The three VM disks are independent. Uploading a data file to c0 does not make it available to executors on c1 or c2. For jobs that use all three hosts, configure shared storage and its connector for distributed inputs and outputs. A local path such as `/tmp/output` can leave different Spark part files on different machines.

The smoke test generates data on executors and saves its small collected report on c0. Keep project extracts compact and leave disk space for Spark shuffle, spill, and logs.
