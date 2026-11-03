# Historical backfill on c0

Activated November 2, 2026. The collector covers the registered tools from January 1, 2023 through completed available dates in 2026. It does not request future dates or copy complete global archives. The frozen study registry contains thirty tools. Matching completed per-tool history is reused with explicit provenance links; the expanded aggregate queries retain the same cumulative budget ledger.

## Running schedule

The Ubuntu user crontab on c0 contains:

```cron
*/15 * * * * /home/ubuntu/6893/pipeline/run-backfill.sh >> /home/ubuntu/6893/logs/backfill.log 2>&1
```

Every 15 minutes, the dispatcher processes the next bounded batch and resumes saved cursors and Google query jobs. It runs at most 60 tasks or about ten minutes per batch, with a twelve-minute outer timeout. A shared collection lock prevents overlapping runs, including manual pilot collection.

Historical work is queued in monthly arXiv windows, calendar-aligned BigQuery week windows, and per-tool release-metadata requests. Oversized windows split where necessary. After catch-up, completed-date planning fills gaps and a daily seven-day replay window checks recent changes. The 15-minute dispatch interval does not repeatedly download the whole history. Weekly feature building, model training and publication run separately through `~/6893/study/run_study.sh`, which binds one cutoff and requires source-quality, model and Spark gates.

## Full project source workers

Six additional workers are installed in the `EECS6893_FULL_SCOPE` crontab block. Existing primary collection and direct-star jobs remain separate. Dispatch frequency resumes bounded work; it does not refetch every source on every dispatch.

| Worker | Dispatch schedule on c0 | Work and refresh cadence |
| --- | --- | --- |
| `run-full-scope-downloads.sh` | Every 15 minutes | Resume rich package-download exports, reuse completed windows, then add newer completed UTC dates. |
| `run-full-scope-metadata.sh` | Minutes 2, 17, 32 and 47 | Complete immutable artifact metadata, validate dependency features and refresh the full metadata snapshot weekly. |
| `run-full-scope-materialize.sh` | Minutes 7 and 37 | Build changed rich features after full history coverage or at six-hour partial intervals; assemble changed source manifests at most hourly. |
| `run-github-activity.sh` | Minutes 9, 24, 39 and 54 | Bounded public issue/PR updates with saved cursors and API cooldowns; historical backfill completion is tracked separately. |
| `run-full-scope-public.sh` | Minutes 11, 26, 41 and 56 | Daily contributor history, weekly first-version research cycles and weekly dated deps.dev graphs, with per-invocation limits. |
| `run-github-releases.sh` | Minutes 13, 28, 43 and 58 | Weekly exhaustive visible release lists, bounded to one request per dispatch while stars are incomplete, four afterward, and 40 per UTC day; honor shared rate headers. |

All paid exports use the original **$80 cumulative authorization** and shared `backfill.budget` reservation ledger. New workers do not receive a separate budget. Unknown billed amounts keep their reservations, completed exports are reused, and the 15 GiB free-space guard still applies. The public refresh, release and materialization workers do not submit BigQuery queries.

Expanded PyPI execution is now free-first through `config/full_scope_execution.json`: `radar6893-free-20261103`, then `radar6893-free2-20261103`. Both projects have billing disabled and verified zero initial November usage. Each allocation is capped at 1 TiB, and paid fallback is disabled. Live billing metadata is checked before every new insert. Existing jobs remain pinned to their original project/job ID; `data/full_scope_query_routes.json` binds the logical query across account and output-folder changes. The original global ledger and conservative byte ceiling remain intact, including free scans. Monthly baselines require revalidation after a month change.

Personal gcloud credentials remain on the Mac; c0 uses only its preexisting dedicated collector service account. Read-only quota/billing evidence is stored under `data/full_scope/free_quota/`, not in credential files. See [Google's sandbox limits](https://docs.cloud.google.com/bigquery/docs/sandbox).

Validated source versions and spines publish through atomic pointers under `data/full_scope/` or their source directories. Partial inputs stay unknown or retain explicitly labeled partial observations. These workers preserve `data/study/latest`, its model outputs and the dashboard's forecasting snapshot.

Inspect the expanded collection after SSH login:

```sh
cat ~/6893/data/full_scope_download_status.json
cat ~/6893/data/full_scope_metadata_status.json
cat ~/6893/data/full_scope/latest/source_coverage.json
cat ~/6893/data/full_scope/materialize_state.json
cat ~/6893/data/full_scope/refresh_state.json
tail -n 20 ~/6893/logs/full-scope-downloads.log
tail -n 20 ~/6893/logs/full-scope-metadata.log
crontab -l
```

Reinstall only the marked six-worker block from the project checkout with `python3 pipeline/full_scope/install_full_scope_cron.py`. The installer preserves unrelated cron entries and configures daily rotation of `full-scope-*.log` with seven retained rotations and a 10 MB size threshold. This installation does not expand query authorization. See [full source coverage](full_scope_data.md) and the [implementation guide](pipeline/full_scope/FULL_SCOPE_SPINE.md).

## Sources and retained records

| Source | What is retained | Important boundary |
| --- | --- | --- |
| arXiv | Paginated title/abstract/author/date metadata for matching aliases, with raw XML and request provenance | Candidates need review. Current revised text is not automatically eligible as a historical predictor. PDFs are not downloaded. |
| PyPI releases | Visible artifact upload records for the requested dates, package/version/filename/digest, and compressed source JSON | Deleted releases may be absent. Current metadata is not an archived downstream-dependency graph. |
| PyPI downloads | Daily package download aggregates, with a consistent distribution-file filter | Download requests are a usage proxy. Missing source rows are not automatically zeros. |
| GH Archive | Observed captured event-type counts and distinct captured actors across requested windows, filtered by stable repository IDs | All GH predictors are excluded from primary modeling: capture degradation is known from at least May 24, 2025 and earlier capture remains unvalidated. Preserve `observed_gh_*`; a captured zero is not proof of inactivity. No exact commit or merged-PR counts are claimed. Never sum daily distinct actors into weekly uniques. |

All four sources have returned retained records. Corrected snapshot `20261103T012521Z-7fe27982` passed quality checks for enabled primary sources: PyPI artifact downloads and visible version releases. GH Archive remains capture-limited and research candidates remain quarantined. The ledger preserves overlapping responses; the builder selects compatible successful source/tool/date snapshots and does not sum repeated refreshes. This is retrospective event-time reconstruction, not proof of what a historical forecaster could have retrieved then.

The [source-quality notes](study_guide.md#source-quality-and-allowed-inputs) and [shared policy](study/source_quality.json) distinguish retrieval from source validity. Complete windows, checksums, nonempty global archive tables, and matching aggregate totals do not certify upstream event capture. The correction preserves old raw evidence and published snapshots and invents no corrected GH counts or global-volume adjustment.

## Storage and cost controls

- c0 has an approximately 89 GiB root filesystem. The worker pauses when less than **15 GiB** is free.
- Raw source evidence is gzip-compressed; published record exports use Zstd Parquet. Logs rotate daily or at 10 MB, with seven compressed rotations.
- The ordinary DuckDB database worker uses two threads, a 1 GB query-memory setting, and a 5 GB spill setting. The separate dependency-history and rich-download builders use one thread, a 3 GB query-memory limit, insertion-order preservation disabled, and up to 10 GiB of temporary spill while preserving 15 GiB free. Full rich normalization has a 120-minute inner deadline and 135-minute outer deadline. Query-memory settings do not cap all Python process memory or database/export sizes.
- The authorized BigQuery budget is **$80 total**. The cumulative reservation cap is **12 TiB**, equivalent to **$75** at the checked $6.25/TiB on-demand query price before free allowance. Each query also has a 50 GiB ceiling. This is a collector query-scan limit, not an account-wide billing budget.
- A free dry run precedes execution. Each job reserves its worst-case query cap before submission. Completed jobs use reported billed bytes when available; pending and uncertain jobs retain their reservations. Retries resume stable Google job IDs. Deliberate refreshes use distinct IDs.
- BigQuery scans the large public tables remotely and returns selected-tool aggregates. Scanned terabytes are not terabytes downloaded to c0.

First validated batches retained approximately 15.6 MB of raw responses as 3.2 MB of gzip objects, while about 83.7 GiB remained free. This is a measured initial batch size, not a full-history storage measurement.

At corrected publication (November 3 UTC / November 2 New York), the collection status recorded 1,612 raw objects: approximately 193 MB of source bodies compressed to 31 MB, with 82.49 GiB free. The conservative cumulative query estimate was **$34.5552** against the **$80** authorization; it is not an invoice and excludes unrelated charges. The source-quality/model correction reused retained data without requiring additional BigQuery scans.

The expanded rich archive completed on November 3 UTC: **221 exports / 368,159,148 records / 1,402 dates**, with no overlapping execution days or paid split parents. All 996 pre-switch budget entries were preserved; the 38 new sandbox jobs have billing-disabled receipts and remain within their separate 1 TiB allocations. The global scan guard totals **12,657,779,277,824 bytes** across 1,034 entries, below its 12 TiB ceiling. This includes free scans and is not a billed-dollar total. Full rich normalization passed with zero differences from the frozen daily totals. Its 1.07 GB Parquet represents 17.28 billion artifact requests, and c0 retained about **65 GiB free** after publication. See the [full data guide](full_scope_data.md) for the current source inventory and analysis status.

## Inspect progress

After connecting to c0:

```sh
cat ~/6893/data/backfill-status.json
tail -n 30 ~/6893/logs/backfill.log
crontab -l
6893-duckdb "SELECT source, state, count(*) AS windows FROM backfill.jobs GROUP BY source, state ORDER BY source, state;"
6893-duckdb "SELECT source, count(*) AS records FROM backfill.records GROUP BY source ORDER BY source;"
```

A collector status snapshot is written at the end of each batch. This reports retrieval state, not blanket source validity. While the writer briefly holds DuckDB’s file lock, retry a read query after it closes. Failed requests remain retry/blocked states, not zero observations. Budget exhaustion and missing requested windows remain explicit. Separate study publication requires the enabled-primary-source semantic gate; successful dispatch alone does not enable a predictor.

| Path on c0 | Contents |
| --- | --- |
| `~/6893/config/backfill.json` | Dates, scope, disk guard, and query caps |
| `~/6893/config/tools.backfill.json` | Tool mapping including verified repository IDs |
| `~/6893/data/raw/backfill/` | Gzip evidence, manifests, and retained registry versions |
| `~/6893/data/processed/backfill/records.parquet` | Observation ledger export; requires deduplication for features |
| `~/6893/data/radar.duckdb` | `backfill.jobs`, `objects`, `records`, and `budget` tables |
| `~/6893/data/backfill-status.json` | Last completed batch status |
| `~/6893/logs/backfill.log` | Dispatch and source diagnostics |

## Reproduce installation

Run this on c0 from a clone of the repository:

```sh
bash pipeline/install_backfill_cron.sh
```

The source code contains no Google credential. A dedicated service account with `roles/bigquery.jobUser` runs queries in `cs6893-cluster`. Its credential is stored only at `~/.config/6893/gcp-collector.json` with permissions 0600, and the private environment file is `~/.config/6893/backfill.env`. Configure a suitable private credential separately for a new machine; do not commit it.

To run one batch manually, use `~/6893/pipeline/run-backfill.sh`. The lock skips the run if another collector is active. To stop future dispatches, remove only the `EECS6893_BACKFILL` marked block using `crontab -e`; an already-running batch is separate.

See the [historical source notes](historical_collection.md) and [pilot guide](data_collection.md) for measurement changes and provenance constraints.

Sources: [BigQuery pricing](https://cloud.google.com/bigquery/pricing), [query cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs), [arXiv API terms](https://info.arxiv.org/help/api/tou.html).

The expansion uses paginated yearly arXiv windows for new mappings, replacing only untouched monthly tasks. Completed responses and partial cursors remain intact. `superseded` marks intentionally replaced tasks, while `reused` points to compatible completed history. Yearly responses retain the same page and response-size limits; oversized windows split. Current research metadata remains candidate material and is excluded from historical model features.

The research collector now waits at least ten seconds between request starts. HTTP 429 immediately returns to the dispatcher, which persists a project-wide cooldown of at least 120 seconds (or the longer Retry-After value) and ends the batch. Cron resumes from saved state; rate-limited or timed-out windows remain incomplete, never zero observations.

The full retained archive audit checked 1,612 of 1,612 raw responses and passed all 62 requested history groups for the audit endpoint **November 2, 2026 exclusive** (through November 1). The corrected study independently includes the next completed day: 42,060 daily rows through November 2, 6,000 full feature weeks, and 5,480 mature download labels. Its quality checks found no unexpected primary-source gaps; raw-body checks cover all 56,450 eligible PyPI rows plus 2,940 tool-days per source across 14 sampled weeks, and Spark found zero weekly mismatches. These are retrieval, integrity and transformation checks; GH capture validity and research relevance remain unresolved.

Future research jobs preserve the yearly bootstrap endpoint and collect subsequent dates in monthly catch-up windows with the seven-day refresh; completed years are not fetched again each day. Research retrieval completeness does not establish match relevance or historical feature eligibility. The client allows up to 60 seconds for a slow response while keeping request spacing and cooldowns.

Public direct-star and current-default-branch commit collection runs separately as supplementary work. The initial star backfill is complete for all thirty repositories; ongoing recent observations remain subject to public API request limits and per-cycle coverage checks. Star history counts currently surviving records by creation time, with Sunday-first buckets that are not guaranteed UTC-aligned. Current-default-branch commits can omit deleted branches and rewritten history. Neither source repairs the old archive or enters the primary model automatically. The corrected model evaluation and its post-audit sensitivity limits are documented in the [study guide](study_guide.md#published-corrective-study).

The GitHub activity dispatcher shares a total budget of four requests per 15-minute dispatch while stars are incomplete and ten afterward between independent issue and native-PR update streams. `activity_update_state.json` preserves issue cycles and missing-detail requests; `native_pr_update_state.json` preserves the independent updated-PR cycles. Actual Links, ordering and old-boundary completion checks must pass before coverage advances. Busy native lists can remain incomplete when offset pages change during traversal. The unpaid materializer publishes verified completed cycles to `data/full_scope/github_activity_updated`; affected features stay unknown while incomplete. Historical JSONP capture also follows actual provider Links; Search agreement and body-derived cursor probes do not certify issue completeness.
