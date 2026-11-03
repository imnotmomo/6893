# AI Technology Radar

We are building a data spine that connects arXiv, GitHub, and PyPI signals to study sustained growth in AI tools. The forecasting target is Python package download activity, which is a proxy for demand rather than a count of users.

## Cluster setup

Our Spark cluster runs on three Oracle Cloud VMs. **c0** (2 OCPUs, 12 GB RAM) hosts the master, driver, main worker, persistent DuckDB database, and private dashboard. **c1** and **c2** (1 GB RAM each) are small workers for distributed demos; normal study jobs run on c0.

The repository's `duckdb/` folder contains setup, query, and verification scripts. Runtime data lives on c0. See the [connection guide](connection_guide.md) for SSH and dashboard access.

## Data and collection

The registry maps **30 AI tools** to their PyPI distributions and GitHub repositories. Collection covers January 2023 through completed available dates in 2026. See the [tool registry](config/tools.study.json) for package names and repository mappings.

The validated snapshot published on **November 2, 2026 (New York time)** contains **42,060 tool-day rows**, **6,000 calendar tool-weeks**, and **5,480 mature download labels**. Its daily panel runs through November 2 UTC. The primary models use download history, release activity, and a package-age control; other sources have separate roles below.

The weekly panel aggregates the underlying records into one row per tool and week. The full collection also keeps source objects, artifact metadata and the richer daily download dimensions.

### Source overview

| Source | Data retained | Use and status |
| --- | --- | --- |
| **PyPI artifact requests** | Daily totals plus version, artifact, Python, OS, installer, country and reported CI dimensions | Rich history is validated: **368,159,148** daily dimension records across all **1,402 UTC dates** through November 2, 2026, matching the frozen download totals exactly. Requests are not users. |
| **PyPI releases** | Visible versions, earliest artifact uploads, filenames and digests | Primary release features; deleted history may be absent. |
| **Immutable PyPI metadata** | **3,079,014** distinct metadata records, including artifact identities, requirements, extras and environment markers | Full history retrieved, with **73,866,993** parsed declaration edges. Conflicting claims and malformed requirements retain quality flags. |
| **Direct GitHub releases** | **5,383** visible release objects across all 30 repositories, including tags, full notes, flags and embedded assets | Supplementary repository-wide publications; event dates use `published_at`, and correspondence to the selected PyPI package is unverified. |
| **Git history and contributors** | Commits, parent links, author/committer dates and stable pseudonymous contributor IDs | All 30 repositories collected; retrospective development features remain separate from primary models. |
| **Direct GitHub issues/PRs** | **528,085** native PRs and **140,565** true issues, with object records, event fields and pagination receipts | All 30 repositories retrieved. Independent API views and unavailable objects retain quality flags; deleted history and intermediate states remain unknown. |
| **Direct GitHub stars** | API calendar-week counts and daily bins for surviving stars | All 30 histories retrieved. Removed stars and uncertified UTC boundaries limit interpretation. |
| **arXiv first versions** | Metadata for **5,819 papers**, **6,924** tool–paper candidate pairs and review evidence | Version-specific retrieval is complete for the discovered set; relevance and discovery coverage still need review. |
| **deps.dev** | Dated current selected-version requirements and resolved graphs | **29/30 tools** have complete observations; LangChain's provider gap stays explicit. Current graphs are not historical snapshots. |
| **GH Archive** | Captured event-type counts and distinct actors | Capture diagnostics only; upstream gaps exclude these counts from primary models. |

Git timestamps do not establish first public availability, and these sources do not repair missing GH Archive events. The [full data guide](full_scope_data.md) describes raw evidence, coverage and source constraints.

### Storage and collection pipeline

Compressed raw responses and checksum manifests are retained on **c0**. DuckDB stores collection records and job metadata; analysis exports include weekly CSV/Parquet datasets. The repository contains code, configuration, guides, and compact validation results. Runtime databases and raw datasets stay on the server.

The main worker dispatches resumable jobs every **15 minutes**. It adds newer completed UTC dates and rechecks the recent seven days once per daily cycle. Completed historical windows are reused. Six additional bounded workers handle rich exports, source refreshes and data-spine materialization. Their dispatch and refresh schedules are listed in the [backfill guide](backfill_guide.md#full-project-source-workers).

New rich-download queries now prefer two **unbilled BigQuery sandbox projects**, with paid fallback disabled. Existing jobs keep their original project and job ID. The cumulative BigQuery budget remains **$80**, with a **15 GiB free-space guard**. Fetching newer data does **not** automatically retrain models or publish a new dashboard forecasting snapshot; those are separate validated steps. See the [backfill guide](backfill_guide.md) for limits and progress checks.

## Project guides

- [Full project data](full_scope_data.md), [backfill and budget](backfill_guide.md), and [initial collection pilot](data_collection.md).
- [Study results, source quality, and reproduction](study_guide.md).
- [GitHub rebuild and comparison](study/GitHub_Rebuild_and_Sensitivity.md).
