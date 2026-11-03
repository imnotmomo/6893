# Full project data

The original AI Technology Radar goal connects research attention, development activity, package demand and downstream dependency declarations for the frozen **30-tool cohort**. The historical period starts January 1, 2023 and extends through completed available dates in 2026. New collection retains records and dimensions behind the weekly features, with separate source timestamps, checksums and coverage flags.

## Data inventory

| Source | Retained evidence | Intended role | Coverage boundary |
| --- | --- | --- | --- |
| PyPI artifact requests | Daily counts plus package version, artifact type, Python version/implementation, operating system, installer, country, reported CI and HTTP status | Demand forecasting, version migration and environment composition | Counts represent requests. Unknown CI/platform values remain unknown; requests do not identify installations or people. |
| Immutable PyPI metadata | Uploaded artifact identity, version, upload timestamp, digest, requirements, extras/markers, Python constraints, classifiers and project URLs | Release history, outbound dependencies and downstream declaration growth | Includes pre-2023 baselines and all observed versions of packages that declare a target, including later releases dropping it. Declarations do not measure installed users. |
| GitHub repository releases | Full visible release objects, IDs, tags, notes, draft/prerelease flags and embedded asset metadata | Repository-wide release-publication activity | Events use `published_at`; `created_at` can be the tag-commit date. Deleted history and exact correspondence to a selected PyPI distribution remain unknown. |
| Git history | Commit/parent IDs, author and committer times, stable pseudonymous contributor IDs and bot heuristics | Development activity and contributor growth, including real rolling distinct counts | Reconstructed from surviving current default-branch history. Commit timestamps do not prove first public availability. |
| GitHub issues/PRs | Public object IDs, creation/update/closure/merge timestamps, titles/bodies and dated snapshots of current metadata | Issue/PR creation and development activity | Deleted/private objects and past state changes cannot be reconstructed completely. Completeness is reported separately for each retrieval scope. |
| Direct GitHub stars | Provider calendar-week counts and daily bins | Supplementary attention series | Surviving stars omit removals. Provider calendar buckets are not certified UTC and are exported separately from the UTC feature panel. |
| arXiv | Candidate paper IDs, first-version titles/abstracts/authors/categories and publication dates, current metadata and match evidence | Research attention and cross-source convergence | All discovered candidates are retained. First-version retrieval does not establish discovery recall or semantic relevance; machine matches still require review. |
| deps.dev | Current selected-version requirements and resolved graph responses | Current dependency graph context | A present graph is a dated snapshot. Missing provider graphs remain explicit gaps. |
| GH Archive | Previously collected event-type and actor aggregates | Capture diagnostics | Known capture gaps exclude this source from primary model features. It does not repair missing GitHub events. |

## Why compact files can contain substantial activity

The weekly panel is a joined analysis table, not a copy of every original event. Its 6,000 rows are 30 tools times 200 completed weeks. Raw source evidence and richer normalized records live separately on c0.

The validated October 26–November 1, 2026 download pilot contains **2,630,153 joint-dimension aggregate records representing 250,654,368 artifact requests**. Its normalized Parquet file is **7,195,315 bytes**. Counts reconcile exactly with the existing daily download totals. Compression and aggregation explain the small file; the row count is not the number of downloads.

Immutable metadata history is fully retrieved: **3,093,989 source rows**, **3,079,014 distinct provider metadata records**, **79,656 package groups**, and **73,866,993 parsed declaration edges**. Record identity includes the full provider row; conflicting claims for one physical artifact stay separate. The source retains **2,062 conflicting metadata pairs** and **11,793 malformed requirements**, with the original fields and strings intact.

Capture is complete while quality remains `needs_review`. All 30 tools have 200 weekly rows. Existing parse uncertainty and an exhaustive conflict review leave 2,617 inbound-feature rows and 5,992 outbound-feature rows eligible; the eight unavailable outbound rows precede the first visible artifacts. Four additional inbound-family rows for W&B in June 2024 are masked because of a downstream package's conflicting declaration. Numerical observations remain retained separately from eligibility. The source manifest and audit files document these decisions.

The active research source contains **5,819 first-version papers** and **6,924 tool–paper candidate pairs**. Cross-matching the retained first-version texts against all 30 frozen alias sets added 15 candidates without new provider requests. The original responses, discovery queries and provider observation times are preserved. These are candidates for review; neither cross-matching nor human precision labels alone establish exhaustive historical discovery.

The full rich-download archive contains **368,159,148 daily joint-dimension aggregate records**, exported in **221 complete query results** covering every one of **1,402 UTC dates** from January 1, 2023 through November 2, 2026. Nineteen oversized parent plans were replaced by completed child windows without submitting the parent queries. Full-history validation passed with no missing query dates, unexpected post-artifact gaps or differences from the frozen daily totals. The normalized Parquet is **1,071,604,242 bytes (1.07 GB)** and represents **17,284,729,710 artifact requests**. Of 6,000 calendar tool-weeks, **5,990** have complete post-artifact coverage; ten ineligible tool-weeks include dates before the first visible artifacts. The pilot above is a separate measured example.

The final GitHub capture contains **528,085 unique native PR objects** from all 30 repository histories, plus **485,051 issue-list observations** across 4,866 actual-Link pages. Issue-list observations include PR wrappers and must not be added to the native PR count. Separate provider views disagree for some objects, and 3,515 unique identities have different retained issue/native PR creation times (3,447 native-created inside the study interval). Both raw clocks remain retained; PR creation features use the native PR clock. The normalized union retains 47 additional pull identities outside the native listing. Forty-six were created after the November 3 exclusive cutoff and have no in-window events. The remaining in-window object is TRL PR #7121: its matching native endpoint returned 404 on November 3. Its issue-wrapper evidence and error receipt remain retained; the cause is unknown. These identities do not inflate native-list measurements. PR event counts describe the retrieved native-visible view, not a complete census of every historical PR.

Direct-star histories are retrieved for all 30 repositories, with 6,000 provider-calendar-week rows exported separately from the UTC spine. Historical removals remain unavailable.

## Outputs and status

Runtime data stays on c0 under `~/6893/data/`; it is ignored by Git. The repository contains collectors, builders, tests, configuration and guides. The assembled weekly panel is a Parquet file that the existing DuckDB CLI can query directly; it does not require a separate server database or new persistent raw tables.

| Path on c0 | Contents |
| --- | --- |
| `data/raw/full_scope/bigquery/` | Frozen SQL/parameters, raw response pages, decoded JSONL, cloud job metadata and complete-export receipts |
| `data/raw/github_git_contributors/runs/` | Commit-level contributor evidence and weekly development features |
| `data/raw/research_v1/backfill/` | Version-specific paper responses, candidate evidence and review queues |
| `data/raw/github_direct_stars/backfill/` | Resumable star-history source and per-tool coverage |
| `data/raw/depsdev/` | Dated current dependency snapshots |
| `data/raw/github_direct_incremental/activity_update_state.json` | Versioned public issue-update cycles and queued direct PR details |
| `data/raw/github_direct_incremental/native_pr_update_state.json` | Independent native PR update cycles and per-cycle coverage |
| `data/raw/github_direct_incremental/activity_dispatch_state.json` | Shared issue/native request reservations and cooldowns |
| `data/full_scope/github_activity` | Verified immutable issue/PR baseline and per-feature masks |
| `data/full_scope/github_activity_updated` | Baseline plus completed, hash-verified public update cycles |
| `data/full_scope/github_releases` | Repository release publications and coverage, with 30 visible histories retrieved |
| `data/full_scope/dependency_history` | Published dependency declarations and weekly features |
| `data/full_scope/download_dimensions` | Rich download Parquet, weekly composition and validation results |
| `data/full_scope/latest` | Latest assembled source panel, data dictionary and source-coverage catalog |
| `data/full_scope_download_status.json` | Rich historical download dispatch progress |
| `data/full_scope_metadata_status.json` | Immutable metadata export progress |

The expanded historical backfill is resumable. The November 3 final rich-archive audit passed all 221 exports, with no pending dates, overlapping executions, duplicate job folders or paid split parents. Its proof is retained at `data/full_scope/audits/rich-raw-final-20261103T073842Z.json`. Consult each source manifest and the joined `source_coverage.json` before treating it as complete. Missing or incomplete data remains `NULL`; retained partial observations are labeled separately. The published wider spine has **6,000 rows and 383 columns**. Its **300,000 primary feature cells** match the frozen study exactly. The frozen study and its model outputs are preserved. New data collection does not automatically certify a source, retrain models or publish new results.

The joined source status remains `assembled_partial` where newer GitHub refresh coverage is incomplete or provider data is unavailable, including the current LangChain graph. Completed historical views remain retained with their own coverage. The Attention–Adoption Gap needs reviewed research matches and source-quality checks. Data retrieval alone does not establish a valid hype or durable-adoption label.

## Ongoing collection

The rich download worker adds completed UTC dates without buying overlapping completed historical windows again. Immutable metadata refreshes weekly after the initial full export. Existing primary collection adds completed dates and rechecks a recent window; contributor and first-version research refreshes run separately. The completed star backfill is retained; ongoing star collection refreshes recent observations within public API limits. Repository-release refreshes traverse every visible page, with a 40-request daily public quota and a lower per-dispatch cap while star backfill is active. Dispatch frequency is distinct from each source's refresh cadence.

GitHub issue backfill follows the actual opaque pagination Links exposed by GitHub's documented JSONP envelope. Search results and the earlier body-derived cursor attempts remain diagnostics; they cannot certify a complete issue list. The provider sort key can differ from the JSON `updated_at` value. Issue-object and PR-object creation timestamps can also differ, so both are retained and PR-opened counts use direct PR timestamps.

New public updates use independent issue and native-PR streams with versioned state. The issue stream follows actual Links and requests missing PR details when needed; the native PR stream independently checks recently updated PRs because issue listings can omit PR wrappers. Both share the same total request budget and cooldowns. Completed, hash-verified cycles can update their supported features; partial cycles leave the affected features unknown. On busy repositories, mutable offset pagination can prevent a native cycle from certifying coverage. The materializer retains that uncertainty and preserves the older refresh archive. The November 3 cron audit found issue cycles progressing while native cycles remained partial or failed duplicate-page guards; the affected newer coverage remains unknown. The completed historical baseline remains retained. The dated audit and single-object endpoint probe are bound by `data/full_scope/audits/github-reconciliation-20261103/manifest.json`.

The expanded PyPI worker uses [the free-first policy](config/full_scope_execution.json). New unsubmitted queries prefer the two unbilled sandbox projects in order, each with a verified November 2026 usage baseline and a 1 TiB allocation cap. Every new insert checks the provider's live billing-disabled status. Paid fallback is disabled. Existing reserved, submitted and completed jobs keep their original execution project, job ID and accounting; changing accounts does not rerun them.

The logical-query routing registry prevents another output folder or reordered named parameters from buying the same query in another project. Personal gcloud sign-in credentials stay on the Mac. c0 reuses its existing dedicated collector service account; no personal CLI credential or new key was copied to the cluster. The free allowance baseline is month-bound and must be reverified when the month changes. [Google's sandbox documentation](https://docs.cloud.google.com/bigquery/docs/sandbox) describes the provider's free limits.

All BigQuery execution shares the existing **$80 cumulative authorization** and reservation ledger; the configured conservative scan ceiling is described in [the budget guide](backfill_guide.md). Every paid job requires a dry run and a maximum-byte reservation. Unknown charges remain reserved. Collection pauses below **15 GiB free**. The original conservative global scan guard still counts sandbox scans before free allowances; its dollar equivalent is not an invoice. Query scan bytes are processed in BigQuery and are not bytes downloaded to c0.

After SSH login:

```sh
cat ~/6893/data/full_scope_download_status.json
cat ~/6893/data/full_scope_metadata_status.json
cat ~/6893/data/full_scope/latest/source_coverage.json
6893-duckdb "SELECT count(*) FROM read_parquet('/home/ubuntu/6893/data/full_scope/latest/full_scope_weekly.parquet');"
tail -n 20 ~/6893/logs/full-scope-downloads.log
crontab -l
```

See [the implementation guide](pipeline/full_scope/FULL_SCOPE_SPINE.md) for provenance checks and assembly commands, and [the study guide](study_guide.md) for the unchanged primary analysis.

GitHub documents `created_at` as the date of the commit used for the release; it is not the drafting or publication timestamp. Weekly repository-release counts use `published_at` only. [GitHub release timestamp semantics](https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release).
