# Data collection

The first manual pilot ran on c0 on November 1, 2026. It collected metadata for ten candidate tools: PyTorch, TensorFlow, Transformers, scikit-learn, XGBoost, LightGBM, LangChain, LlamaIndex, MLflow, and Ray.

## Current status

- 30 successful requests: ten each to GitHub, PyPI, and arXiv.
- 50 candidate tool–paper matches across 49 distinct papers, all requiring manual review.
- About 8.5 MB of source responses stored in about 1.8 MB of gzip files.
- DuckDB tables and Zstd Parquet exports are saved on c0.
- The same run can be reimported without duplicate rows; changed manifests require a new run ID.

These are current metadata snapshots and small candidate searches. They are not the 2023–2026 weekly training dataset. Current repository statistics and package requirements must not be assigned to earlier dates. PyPI's metadata API does not provide download counts. The separate historical backfill now retains package downloads, releases and captured GH Archive events; its successful retrieval does not establish upstream GitHub capture completeness.

The [pilot tool mapping](config/tools.pilot.json) is a starting cohort, not the final frozen universe. Package/repository mappings were checked against their APIs; [mapping notes](config/mapping-notes.md) describe remaining scope and alias issues. arXiv requests retrieve only the first five relevance-ranked candidates per tool, not complete mention histories.

## Schedule

The [historical backfill](backfill_guide.md) was activated on November 2, 2026. Cron dispatches bounded resumable work every 15 minutes. Daily seven-day replay windows are queued once per UTC day; this is separate from the dispatch interval. Weekly feature/model publication is a separate quality-gated stage:

| Source or output | Proposed cadence | Behavior |
| --- | --- | --- |
| GitHub repository metadata | Daily | Save a dated snapshot; skip unchanged payloads where supported. |
| GH Archive captures | Historical dispatcher | Retain requested completed-day captures and replays as diagnostics; all GH predictors are excluded from the corrected primary profile. |
| PyPI download aggregates | Daily | Query selected packages and dates; replay the recent seven days. |
| PyPI package metadata | Daily check | Retain changed metadata and new releases. |
| arXiv metadata | Daily | Retrieve new/updated candidates with pagination and a shared request limit. |
| Weekly features and forecasts | Weekly or manual rerun | Publish only after enabled-primary-source quality, grid, count, timing, model and Spark checks. |

Hourly scans are unnecessary for the eight-week forecasting target. The existing pilot command is a manual access/mapping check. Historical batches run for the registered tools with free dry runs, per-query and cumulative scan caps, and a disk-space guard. Corrected snapshot `20261103T012521Z-7fe27982`, created November 3 UTC / November 2 New York, contains complete requested grids of 42,060 daily rows through November 2 and 6,000 full feature weeks. GH capture validity and research relevance remain unvalidated.

Keep one collector process active, retain compressed source evidence and manifests, and delete redundant temporary files only after validation. Preserve the original config and collector source with checksums. A failed source remains explicitly missing; it does not become zero activity.

## Source-quality correction and supplementary GitHub sources

The corrected primary study uses PyPI artifact downloads and visible release counts. GH Archive capture degradation led to a post-audit correction: all GH numeric predictors are excluded for all dates; compromised columns are masked and original values remain as `observed_gh_*` diagnostics. A nonempty global archive table or a successful query is not a capture-completeness test. Earlier dates are unvalidated rather than certified complete. See the [source-quality notes](study_guide.md#source-quality-and-allowed-inputs) and shared [policy](study/source_quality.json).

Public direct GitHub collection is separate supplementary evidence. The aggregate star-history endpoint returns weekly counts of **currently surviving stars** grouped by creation time. It does not reconstruct removed stars or historical point-in-time totals. Raw Sunday-first bucket epochs and daily arrays are retained because the API does not guarantee UTC boundaries. Current-default-branch commit history describes commits reachable from that branch now; deleted branches and rewritten history can be absent. Neither source replaces the old event archive or is automatically eligible for primary modeling. Collection manifests record observation times, immutable compressed responses, checksums, repository numeric identity, pagination and source-specific quality checks.

arXiv retrieval success does not establish tool-paper relevance or historical version availability. Candidate matches require manual review and remain excluded from forecasts. Expected pre-artifact and feature-warmup missingness is documented separately from unexpected collection gaps.

The corrected published quality gate passed for enabled primary sources with no unexpected gaps. It checked all 56,450 eligible retained PyPI rows for artifact/exclusion arithmetic, 2,940 tool-days per source across 14 source-boundary and midyear weeks, and last-week releases/captured actors for all tools. Spark reproduced all 6,000 weekly totals with zero mismatches. These checks validate retained evidence and transformations, not GH event completeness. The public direct-star backfill remains resumable and API-limited; commit collection is separate supplementary work. Their live manifests, rather than this static document, report per-tool completion.

## Run and inspect on c0

The deployed files are under `~/6893/pipeline` and `~/6893/config`:

```sh
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/collect_pilot.py
6893-duckdb "SELECT source, status, count(*) AS requests FROM pilot.source_responses GROUP BY source, status ORDER BY source;"
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/verify_pilot.py
```

To reimport saved responses without HTTP requests, replace `RUN_ID` with a recorded run ID:

```sh
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/collect_pilot.py   --import-run ~/6893/data/raw/pilot/RUN_ID/manifest.json
```

| Location on c0 | Contents |
| --- | --- |
| `~/6893/data/raw/pilot/RUN_ID/` | Gzip source responses, original config, collector source, and manifest |
| `~/6893/data/processed/pilot/RUN_ID/` | Validated Zstd Parquet exports |
| `~/6893/data/radar.duckdb` | `pilot.runs`, `pilot.tools`, `pilot.source_responses`, and `pilot.paper_candidates` |
| `~/6893/data/pilot-latest.json` | Latest pilot summary |

Source files and runtime data stay out of Git. Each request records its URL, observation time, HTTP status, payload checksum, and retained raw path. The metadata snapshots are available to future processing, but are explicitly marked as snapshots rather than historical features.

See the [historical collection plan](historical_collection.md) for BigQuery access, bounded SQL, and the August 2026 PyPI logging change that must be handled consistently in the download target.

## Sources

- [PyPI metadata API](https://docs.pypi.org/api/json/)
- [PyPI download datasets](https://docs.pypi.org/api/bigquery/)
- [PyPI download-count change](https://blog.pypi.org/posts/2026-08-31-download-counts/)
- [GitHub repository metadata](https://docs.github.com/en/rest/repos/repos#get-a-repository)
- [GitHub aggregate star-history API and bucket limits](https://docs.github.com/en/rest/activity/starring?apiVersion=2026-03-10#get-repository-star-history)
- [GitHub explanation of surviving-star aggregation](https://github.blog/engineering/architecture-optimization/from-2500-requests-to-24-rebuilding-star-history-without-the-stargazers/)
- [arXiv API terms](https://info.arxiv.org/help/api/tou.html)
