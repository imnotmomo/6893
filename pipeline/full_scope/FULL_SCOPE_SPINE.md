# Full-scope weekly data spine

`assemble_full_scope.py` creates a reviewable joined dataset for the original AI Technology Radar scope. It uses the validated primary snapshot as the fixed tool/week calendar and retains its features and eight-week target under `primary__`. It does not modify that snapshot, retrain a model, or enable new primary predictors.

Run on c0 after the source builders have saved their hash-bound manifests:

```bash
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/full_scope/assemble_full_scope.py \
  --primary ~/6893/data/study/latest \
  --research ~/6893/data/raw/research_v1/latest \
  --contributors ~/6893/data/raw/github_git_contributors/runs/20261103T032641Z-914f77d3 \
  --dependencies ~/6893/data/full_scope/dependency_history \
  --downloads ~/6893/data/full_scope/download_dimensions \
  --stars ~/6893/data/raw/github_direct_stars/backfill \
  --depsdev ~/6893/data/raw/depsdev/latest \
  --output ~/6893/data/full_scope/runs/20261103-review
```

Use a fresh output directory for each assembly. Optionally supply `--github-api PATH` once that source has a completed, hash-bound weekly feature file. A raw-only archive is recorded as awaiting weekly features; seek pages do not become activity counts. Every input remains read-only. The command fails if a source checkpoint changes during the join; rerun after the collector saves a stable checkpoint.

## Source namespaces

| Prefix | Project role | Interpretation and limits |
| --- | --- | --- |
| `primary__` | Validated download/release features and future evaluation target | Existing primary model policy stays fixed; future targets are marked as labels in the dictionary. |
| `releases__` | Currently visible repository release publications | Counts use `published_at`; tag-commit dates and current asset download counts do not become historical release/adoption features. PyPI correspondence remains unverified. |
| `research__` | First-version paper mentions | Strict lexical/context matches are machine-classified candidates requiring relevance review. A complete candidate retrieval is not a complete historical discovery census. |
| `git__` | Commits, pseudonymous contributors and rolling development activity | Surviving current default-branch history, collected retrospectively. Commit dates do not prove when a repository or commit first became public. |
| `dependency__` | Published requirements and downstream declaration proxies | Inbound and outbound event-time eligibility are separate. A dependency declaration does not measure installed users. |
| `downloads__` | Package version and environment composition | Exact composition requires seven complete retrieved days. Artifact requests do not identify unique users. |
| `github_api__` | Optional issue/PR creation and recorded merge activity | Current visible records cannot certify deleted-object or historical visibility completeness. |

All joins use exact `tool_id` and Monday UTC `week_start` keys. Input cohort hashes must match the frozen 30-tool registry. Duplicate keys, unknown tools, altered files, invalid counts, or contradictory time boundaries stop assembly. Extra source weeks do not expand the primary calendar.

Two supplementary tables intentionally have no historical UTC count join. `supplementary_star_api_calendar.csv` preserves the provider's star-history calendar, whose UTC boundary alignment is uncertified. `supplementary_depsdev_current.csv` retains every requested tool's current selected-version graph observation, including explicit provider gaps such as a missing LangChain version. Their source manifests, available quality report and checksums are included; current graphs are never backdated into historical weekly predictors.

Missing or ineligible observations remain `NULL`; a missing file, incomplete week or unfinished backfill is never interpreted as zero. When a source provides partial counts, `observed_partial_*` or `observed_*` columns preserve those measurements while the corresponding analysis feature is `NULL`. Contributor partial calendar weeks are excluded from the join. Real zero counts from a retrieved eligible week remain zero.

Each source has `record_present`, `source_collection_complete`, `historical_primary_eligible`, source observation time and source manifest hash. Retrieval completion and scientific validity are separate: all new sources remain outside the existing primary models, including fully retrieved sources. Source coverage records distinguish missing inputs, unfinished retrieval, and sources awaiting weekly features.

## Outputs

- `full_scope_weekly.parquet` and `.csv`: wide panel on the original frozen calendar.
- `data_dictionary.json` and `.csv`: column types, provenance role, nonmissing counts and existing-model allowlist membership.
- `source_coverage.json`: source catalog, matched/missing row counts, limits and honest overall status.
- `tools.study.json`: exact frozen cohort.
- `manifest.json`: output hashes, primary manifest hash and assembler code hash.

The Attention–Adoption Gap remains `NULL` with an explicit deferred status. Research relevance and historical availability need review before comparing research, development and adoption as a scientific score. No automatic “hype” labels or unvalidated global normalizations are emitted.

## Bounded refresh and materialization

`refresh_full_scope.py` owns public-source collection only. Contributor collection is due daily, with a 120-second cap per tool, 15 GiB free-space floor and 2 GiB per-run storage ceiling. A weekly research cycle freezes the currently discovered candidate set and resumes at most eight first-version requests per invocation, sharing the existing arXiv rate lock and cooldown. Complete unchanged cycles perform no HTTP work. A new cycle re-fetches first-version metadata within those public request limits. Only a complete, provenance-verified research source moves `data/raw/research_v1/latest`; a partial cycle retains the previous pointer.

Current deps.dev observations refresh weekly into a new dated directory, bounded to 150 requests with at least one second between starts. A verified `complete_with_source_gaps` snapshot can publish `data/raw/depsdev/latest`; all 30 tool rows and explicit provider gaps are preserved. Such a snapshot remains current context and never becomes a historical graph feature.

Latest-source publication is monotonic: a stale state file adopts a newer validated source, and a rewritten checkpoint cannot make older observations appear newer. Research cutoffs must contain completed dates. A newer partial research cycle keeps its own resume state while the last complete source remains published.

Contributor and research history ends no later than January 1, 2027 exclusive. An unfinished first-version cycle resumes its frozen endpoint; completed capped cycles do not restart indefinitely.

`materialize_full_scope.py` never submits queries or invokes collectors. It fingerprints locally completed rich-query plans, receipts and source manifests. New rich feature versions publish when the frozen history is fully covered, or at a six-hour interval while coverage is partial. It adopts an equivalent existing first build and preserves the previous pointer on any build or validation failure. The wrapper updates `data/full_scope/download_dimensions` only after verifying weekly features and their frozen-primary binding. Dependency-history publication belongs to the separate metadata finalizer.

Changed source manifests trigger a new assembled spine at most hourly. The materializer prefers the verified research `latest` pointer and publishes `data/full_scope/latest` only after checking the new output hashes. `data/study/latest` is never changed. Both Python wrappers default to read-only plans:

The canonical `data/full_scope/github_activity` source is included automatically when available. Its per-feature completion flags preserve certified issue/PR streams even while other streams or repositories remain incomplete; an overall partial source does not erase independently complete counts.

```bash
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/full_scope/refresh_full_scope.py
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/full_scope/materialize_full_scope.py
```

The corresponding shell wrappers add process-group deadlines for cron review. Scheduling is a separate installation step; writing or running a plan does not install cron or authorize a paid query.

## Verification

```bash
~/6893/.venv-duckdb/bin/python -m unittest discover \
  -s ~/6893/pipeline/full_scope -p 'tests_assemble_full_scope.py' -v
```

Tests cover exact joins, missing-versus-zero behavior, source/cohort hashes, concurrent checkpoint changes, research candidate masking, independent dependency eligibility, partial contributor/download weeks, invalid values and future cutoffs. The Parquet integration test also verifies unchanged primary files, hashed output artifacts and the deferred gap.

Repository-release public collection is bounded to one request per dispatch while star backfill is partial, four after it finishes, and 40 per UTC day. A weekly cycle traverses the full visible list, verifies the frozen repository identity and a trailing head-ID anchor, and preserves failed attempts. Rate headers stop work without advancing a failed cursor. Complete cycles publish `data/full_scope/github_releases`; incomplete cycles preserve the prior snapshot. The canonical release source is included by the materializer automatically.

## GitHub source versions and newer observations

`github_jsonp_lists.py` validates safe JSONP envelopes, exact UTF-8 data-slice hashes, repository identity evidence and actual opaque provider Links. Serialized timestamps never generate cursors. Legacy Search and generated-cursor observations remain separately preserved and cannot certify a true-issue census.

`collect_github_activity_updates.py` uses `data/raw/github_direct_incremental/activity_update_state.json`, preserving the old refresh state. Completed actual-Link issue cycles supply changed issues and PR wrappers. A hash-bound baseline index reuses known PR creation times; unknown times or missing wrapper fields enqueue a bounded direct PR-detail request. Independent `native_pr_updates.py` cycles use `native_pr_update_state.json` and a descending native-PR updated prefix, ending only at an actual Link terminal or a full ordered page strictly older than the seven-day overlap boundary. Issue-wrapper coverage does not certify future PR coverage; malformed or nonmonotone native timestamps leave that stream unknown. The 128-page cap prevents an unbounded daily history scan.

`dispatch_github_activity.py` divides the same quarter-hour allowance between issue/detail and native streams: four public requests while star backfill is partial, ten afterward. An internal lock and persisted reservations prevent concurrent manual/cron dispatches or interrupted workers from exceeding that allowance. Both streams honor the shared cooldown. `github_activity_overlay.py` verifies their distinct source schemas and publishes only completed-cycle observations, with separate creation/merge/closure coverage. Pending details and partial native cycles leave affected exact features unknown. The materializer owns the atomic `data/full_scope/github_activity_updated` pointer and the ordinary assembled spine; it never changes the frozen primary study or retrains models.

An updated GitHub source is selected only while its hashed quality report references the current canonical baseline. Publishing a newer baseline makes the selector use that baseline until a matching overlay is rebuilt; older overlay files remain available for replay.

The rich-download production profile uses one DuckDB thread, a 3 GB query-memory limit, up to 10 GiB spill, and the existing 15 GiB free-space guard. Insertion-order preservation is disabled because row order is not a feature. Each original JSON export is parsed once into a temporary Parquet cache under the build's staging directory. The cache preserves JSON scalar types and original source paths for validation, per-query uniqueness and whole-date selection, then is deleted after the rich Parquet materializes; retained source bytes stay unchanged. On the same 2.63-million-row pilot, this reduced process CPU time from 21.96 to 16.50 seconds (24.9%). Concurrent workload differs between runs, so their wall times do not establish an equivalent speedup. Weekly composition Parquet is also reused for summaries and counts. The builder retains its bounded 120-minute deadline inside a 135-minute process-group deadline; memory/thread/spill caps are unchanged. Tests cover pandas/Parquet parity, rejected scalar types and duplicates, cache cleanup, source-vintage changes, cached-composition independence and deadline propagation. Resource changes alone do not trigger another partial build before its existing six-hour gate.

Rich-builder stderr logs emit bounded JSON progress records with a UTC timestamp, phase, query index and total. They mark raw validation, every tenth uniqueness query, Parquet/composition work, the release-date join and final quality/publication. They contain no row payloads; final stdout remains the one result JSON object.

Full native PR-history normalization must hold `data/.full-scope-materialize.lock`, the same lock used by rich materialization, so two large transformations do not overlap on c0. If the lock is busy, retain the previous source pointers and retry later. Older PR acquisition pages remain separate from the original current-page capture; complete native history can certify merge/last-reported-close counters while issue-wrapper disagreements stay diagnostic. All historical-primary flags remain false.
