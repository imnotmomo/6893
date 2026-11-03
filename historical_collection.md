# Historical collection plan

Corrected publication: November 3, 2026 UTC / November 2 New York, snapshot `20261103T012521Z-7fe27982`. Historical collection runs on c0 under a 15-minute resumable dispatcher. The thirty-tool dataset has complete requested day grids through November 2 UTC, with complete feature weeks through November 1. This is retrieval coverage, not proof that GH Archive captured the real activity stream. The corrected primary profile excludes every GitHub predictor. Research retrieval is complete for the frozen requested windows, while relevance and historical-version validation remain pending. Production queries are implemented in `pipeline/bigquery_sources.py`; the SQL below remains an illustrative planning example. See the [backfill guide](backfill_guide.md) for the active scope and $80 query cap and the [source-quality notes](study_guide.md#source-quality-and-allowed-inputs) for allowed inputs.

## Access status

- Google access was refreshed. The collector uses the existing `cs6893-cluster` project and a dedicated service account with BigQuery job-creation permission. BigQuery access and the US dataset location were verified through free dry runs and initial completed query jobs. Credentials remain private on c0.
- The total authorized query budget is $80. The collector reserves at most 12 TiB cumulatively, with a 50 GiB per-query ceiling, dry-run validation, and stable retry job IDs. Free-tier availability is not assumed. No new GCP VM or Dataproc cluster was created.

## Sources and historical limits

| Source | Collection route | Limits relevant to this project |
| --- | --- | --- |
| PyPI downloads | `bigquery-public-data.pypi.file_downloads`; aggregate bounded dates and the verified package list remotely, then save small daily/weekly outputs | Artifact requests are the primary demand proxy, not unique users or retained usage. Caches, mirrors, automation and missing records affect counts. Consistent artifact filtering is applied throughout the retained study history. |
| PyPI release metadata | Retained project JSON, counting each visible version once at its earliest artifact upload | The implemented series is retrospective visible-version history and may omit deleted artifacts. The immutable BigQuery `distribution_metadata` table is a possible independent extension, not the current collector's implemented route. Current metadata does not establish historical repository identity or downstream usage. |
| GH Archive | Retained selected-repository aggregates from `githubarchive.day.YYYYMMDD` BigQuery tables | Values are observed captured events and captured actors. Capture degradation is documented from at least May 24, 2025; earlier capture is unvalidated. Preserve `observed_gh_*`, mask compromised model columns and exclude every GH predictor from the primary profile. PushEvents are not commit counts. Complete date grids and nonempty source tables do not validate capture. |
| GitHub REST snapshots | Mapping verification and current repository metadata | The general Events API returns at most 300 events within 30 days, so it cannot backfill 2023–2026. Today's stars, contributors, dependency graph, or repository metadata must not be copied into earlier weekly feature rows. |
| Direct aggregate star history | Public `GET /repos/{owner}/{repo}/stargazers/history`, stored in a separate source/run | Counts creation times of currently surviving stars; removed stars are absent. It is not historical net growth or a point-in-time vintage. Weeks and Sunday-first daily arrays are not guaranteed UTC-aligned. Retain raw epochs, observation time, repository ID and pagination proofs; keep supplementary. |
| Current-default-branch commits | Public commit history reachable from the repository's current default branch, retained separately | Rewritten history, deleted branches and branch changes create survivorship limits. Commit timestamps do not establish historical API availability or the full repository event stream. Keep supplementary; do not substitute these counts for GH Archive events. |

**Applied PyPI measurement rule:** on August 24, 2026, logging changed to exclude metadata and other non-artifact requests; older rows were not rewritten. The implemented download series counts filenames ending in `.whl`, `.tar.gz`, or `.zip` consistently across history and retains excluded-request counts. Quality checks verify `downloads + excluded_requests = logged_objects` in retained raw rows and include weeks around the change. This controls that known counting-rule difference without proving invariant user behavior or eliminating the need for sensitivity checks.

## Cadence and first backfill

1. Freeze package/repository mappings with stable IDs and source evidence. The initial bounded pilot expanded to the frozen thirty-tool cohort; future expansions still need a small manual access/quality check and reviewed dry-run estimates before broader queries.
2. Recommended steady-state collection is **daily**, using UTC event dates and a seven-day replay window to replace changed source/date keys. GH Archive publishes hourly, but downloading every hour is unnecessary for a weekly forecast. Use hourly ingestion only if a later product requirement needs it.
3. Build Monday–Sunday weekly features with a two-day feature-availability lag and the enabled-primary-source quality gate. Keep partial weeks and immature labels ineligible. Use `~/6893/study/run_study.sh` so features, model and publisher share one `as_of`. The corrected publication includes the complete October 26–November 1 feature week; source-bucket timing for supplementary direct GitHub data is separate and must not be silently recast as exact Monday–Sunday UTC bins.
4. Save retrieval time, event time, source URL/query, parameters, raw-object checksum or query job ID, row counts, coverage status, and software version. Retain snapshots rather than silently rewriting previously published forecasts. Late corrections mean retrospective values may differ from what was available at an earlier forecast date.
5. Train only when the entire eight-week outcome window has elapsed. Do not infer earlier dependency snapshots from present-day deps.dev results, fill pre-launch weeks with zeros, or label missing-source days as zero downloads/events.

## PyPI weekly SQL draft

The official PyPI example verifies `project`, `timestamp`, and `file.filename`. The draft reports partial-week status rather than claiming every output is a complete weekly target. Start/end are inclusive/exclusive UTC dates. It has not been validated by a BigQuery dry run.

```sql
WITH daily AS (
  SELECT
    project AS package,
    DATE(timestamp, 'UTC') AS download_date,
    COUNT(*) AS logged_objects,
    COUNTIF(REGEXP_CONTAINS(file.filename, r'\.(whl|tar\.gz|zip)$'))
      AS distribution_downloads
  FROM `bigquery-public-data.pypi.file_downloads`
  WHERE timestamp >= TIMESTAMP(@start_date, 'UTC')
    AND timestamp < TIMESTAMP(@end_date, 'UTC')
    AND project IN UNNEST(@packages)
  GROUP BY package, download_date
)
SELECT
  package,
  DATE_TRUNC(download_date, WEEK(MONDAY)) AS week_start_utc,
  DATE_TRUNC(download_date, WEEK(MONDAY)) + 7 AS week_end_exclusive_utc,
  SUM(distribution_downloads) AS distribution_downloads,
  SUM(logged_objects - distribution_downloads) AS excluded_object_requests,
  COUNT(*) AS days_with_logged_requests,
  @start_date <= DATE_TRUNC(download_date, WEEK(MONDAY))
    AND @end_date >= DATE_TRUNC(download_date, WEEK(MONDAY)) + 7
    AS requested_full_week
FROM daily
GROUP BY package, week_start_utc, week_end_exclusive_utc, requested_full_week
ORDER BY package, week_start_utc;
```

`days_with_logged_requests` is not a source-validity guarantee. The implemented builder joins a complete package/day grid and distinguishes observed/eligible zero counts, pre-artifact dates and missing retrieval. Before a first retained artifact, downloads and releases remain missing rather than zero; rolling features and labels have additional documented warmup and availability requirements. The target includes all filtered artifact requests; a pip-only or mirror-excluding sensitivity analysis would be a separate documented series.

The retained GH Archive queries use date-pruned tables, stable repository filters and observed event identifiers. Deduplication, complete requested windows and raw-to-weekly agreement can validate transformations, but cannot recover events the upstream archive missed. Do not derive corrected counts by dividing by global archive volumes, infer commits from PushEvents, or claim merged-PR counts without validated payload-specific extraction. Source-project capture reports and the shared policy are linked from the study guide; all GH numeric inputs remain disabled in primary modeling for every period.

A passed archive audit means the retained request/response evidence, checksums and requested windows passed their checks. It does not mean every source is suitable for modeling. The corrected `data_quality.json` passed for enabled primary sources with no unexpected gaps; it checked 56,450 raw PyPI rows and 2,940 tool-days per source across 14 sampled weeks. Spark reproduced 6,000 weekly totals with zero mismatches. Direct GitHub collectors retain separate supplementary quality manifests; their API-limited backfills are resumable and are not automatically complete for all tools. arXiv candidates still need relevance and version review. Earlier combined-source model claims are superseded; the corrected [study results](study_guide.md#published-corrective-study) are explicitly a post-audit sensitivity analysis.

## Dry-run and cost controls

After saving the SQL block as `pypi_weekly.sql`, this command validates and estimates it without executing the query. The two package names are illustrative and must match the verified manifest. Supply a real project explicitly; this does not change the local active configuration.

```sh
bq --project_id=YOUR_EXISTING_PROJECT query \
  --use_legacy_sql=false \
  --dry_run \
  --maximum_bytes_billed=1073741824 \
  --parameter='start_date:DATE:2026-10-19' \
  --parameter='end_date:DATE:2026-10-20' \
  --parameter='packages:ARRAY<STRING>:["transformers","torch"]' \
  < pypi_weekly.sql
```

Keep an explicit `--maximum_bytes_billed` on any later execution; 1 GiB above is a conservative example cap, not an estimate or spending authorization. If the estimate exceeds the chosen cap, stop and revise the window/query. A package filter or `LIMIT` does not guarantee a small scan. Dry runs are free; actual query costs and free-tier availability still depend on the account and project.

## Sources

- [PyPI BigQuery tables and immutable metadata](https://docs.pypi.org/api/bigquery/)
- [PyPA download schema, partition filtering, and caveats](https://packaging.python.org/en/latest/guides/analyzing-pypi-package-downloads/)
- [PyPI August 2026 logging change and artifact-only SQL](https://blog.pypi.org/posts/2026-08-31-download-counts/)
- [GH Archive access, history, update cadence, and date-pruned queries](https://www.gharchive.org/)
- [GH Archive maintained BigQuery schema](https://github.com/igrigorik/gharchive.org/blob/master/bigquery/schema.js)
- [GitHub Events API retention and polling](https://docs.github.com/en/rest/activity/events)
- [GitHub aggregate star-history API](https://docs.github.com/en/rest/activity/starring?apiVersion=2026-03-10#get-repository-star-history)
- [GitHub surviving-star aggregation semantics](https://github.blog/engineering/architecture-optimization/from-2500-requests-to-24-rebuilding-star-history-without-the-stargazers/)
- [BigQuery dry runs](https://docs.cloud.google.com/bigquery/docs/running-queries#dry-run)
- [BigQuery array query parameters](https://docs.cloud.google.com/bigquery/docs/parameterized-queries#use_arrays_in_parameterized_queries)
- [BigQuery maximum bytes billed and scan-cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
