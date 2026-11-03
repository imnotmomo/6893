# Running the AI Technology Radar study

The frozen cohort contains thirty AI Python distributions and their matched GitHub repositories. The pipeline reconstructs weekly artifact requests, retained captured GitHub events and visible release activity from 2023 through completed available dates in 2026. The corrected primary analysis uses PyPI downloads, release counts and tool-age controls. GitHub captures and current arXiv candidates remain supplementary or quarantined as described below.

## Private dashboard

Connect from your computer using the shared project key:

```sh
ssh -i ~/.ssh/6893.key -N -L 8501:127.0.0.1:8501 ubuntu@129.159.41.246
```

Then open [the dashboard](http://localhost:8501). Keep the SSH command running. Spark's existing tunnels and connection instructions remain in `connection_guide.md`.

The dashboard reads a published snapshot. It does not fetch sources, run paid queries or fit models when a teammate changes a control. Snapshot publication updates `~/6893/data/study/latest` atomically so the UI sees a complete dataset/model run.

## Target and evaluation

A feature week starts Monday. The model uses that complete week and preceding weeks, then waits two days for source availability. It skips the week containing that cutoff and predicts the median of the following eight complete weeks. The regression target is the difference between `log1p` of this future median and `log1p` of the preceding eight-week median. A sustained-growth outcome requires six of the eight target weeks above 110% of the preceding median.

The 2025 chronological backtests select the model; 2026 backtests evaluate it. Monthly refits use only training outcomes whose entire future window and two-day availability lag have elapsed. Imputation and scaling are fitted within each training period. The corrected primary comparison uses persistence, a training-median baseline, past trend and models with validated PyPI download/release inputs on matching evaluation rows. It excludes every GitHub numeric predictor, including missingness indicators. The selection rule uses only 2025 outcomes available by January 1, 2026: 1,260 rows across 42 origins, rather than all 52 descriptive 2025 origins.

The reported outcome is package artifact demand. Downloads do not identify users, retention or causal adoption. The selected cohort is conditional on currently discoverable tools, and historical event-time reconstruction is not a real-time data vintage. Current metadata, stars, dependencies and unreviewed revised abstracts must not be backdated into model features.

A projection generated today can have an outcome period that has already begun because data arrives with a lag. Such rows retain the actual generation time and an explicit outcome-period-started flag; they are not claimed as forecasts issued earlier. Historical backtest dates remain separate from published future projections.

## Source quality and allowed inputs

Successful requests, checksums, complete date grids and nonempty global archive tables establish retrieval properties. They do not establish that an upstream service captured all real events. The [source-quality policy](study/source_quality.json) is shared by the feature builder, model and publisher. Each corrected snapshot carries a hash-bound `data_quality.json`; publication requires its enabled-primary-source checks to pass.

- **PyPI downloads:** consistently count only `.whl`, `.tar.gz` and `.zip` artifact requests across the August 24, 2026 logging change. Retain excluded-request arithmetic for auditing. These remain request counts, not unique users or installations.
- **PyPI releases:** count one retained visible version at its earliest artifact upload, regardless of how many wheels or source files it has. Deleted history and retrospective metadata remain limitations.
- **GH Archive:** source-project reports and retained volume diagnostics show capture degradation. May 24, 2025 is the first known multi-event boundary; earlier coverage is also unvalidated. Preserve `observed_gh_*` captured counts, mask compromised `gh_*` columns and exclude all GitHub predictors from the primary analysis for every period. A captured zero does not prove zero repository activity. No corrected counts or global-volume scaling are invented.
- **Research:** retained arXiv results are current revised metadata and unreviewed tool-paper candidates. Retrieval completion does not establish relevance, precision or historical availability. Research and dependency predictors remain excluded.

Public direct GitHub sources are collected separately as supplementary evidence. The aggregate star-history API reports creation dates of **currently surviving stars**; removed stars are absent, so it is not a historical net-star or point-in-time series. Its Sunday-first API buckets are not guaranteed to align with UTC. Preserve the original epochs and daily arrays; any conservative alignment delay is an explicit assumption, not a historical availability guarantee. Current-default-branch commit history contains commits reachable from that branch at collection time and can omit deleted branches or rewritten history. It is not the complete historical repository event stream. Neither source silently repairs GH Archive or becomes a primary predictor.

Expected missingness is explicit: pre-artifact days and partial first weeks stay missing; rolling and lagged features need their stated warmup; labels require all eight outcome weeks and the availability lag. These differ from unexpected source gaps. In the corrected snapshot, the 64 daily and 10 weekly missing values in each primary count series are explained by PEFT and LlamaIndex first-artifact dates. There are no unexpected primary-source gaps.

## Reproduce on c0

Use `~/6893/.venv-duckdb/bin/python` for the study. The deployment freezes its resolved dependencies separately from the original collector lock file.

```sh
~/6893/study/run_study.sh
```

This publishing wrapper freezes one `as_of` and binds the same cutoff, cohort, policy and feature hashes through feature building, modeling and publication. Do not run separate builder/model commands with independently chosen cutoffs. Each run retains settings, folds, predictions, metrics, quality reports and input hashes in a fresh versioned directory. The publisher changes `latest` only after source-quality, model and Spark checks pass; it preserves prior snapshots.

Collection continues through the existing 15-minute dispatcher. A one-time study backfill uses bounded batches and four concurrent BigQuery network requests, with one serialized database writer and the same global reservation ledger. The authorized query budget is $80 cumulatively; the conservative scan ceiling is 12 TiB ($75 before free allowances at the checked price), leaving headroom. This is not an account-wide billing budget or an invoice. Collection stops with fewer than 15 GiB free.

Raw responses and runtime databases remain on c0 and are ignored by Git. Keep private keys and Google service-account credentials outside the repository.

## Published corrective study

Snapshot `20261103T012521Z-7fe27982` was published with `as_of = 2026-11-03T01:25:21.455306Z` (November 2 in New York). It contains 30 tools, 42,060 daily rows through November 2 UTC, 6,000 complete Monday–Sunday feature rows through October 26–November 1, and 5,480 mature download labels. The 2026 evaluation covers feature weeks January 5–August 17: 990 predictions per model across 33 origins. Later labels are not mature. These grid and label counts do not certify GitHub capture validity.

The source audit invalidated the use of GH Archive as a trusted historical predictor. Earlier combined-source improvement claims are superseded as primary results. The corrected run passed enabled-primary-source, model and Spark gates. It is a **post-audit corrective sensitivity analysis** because 2026 outcomes had already been examined; it is not an untouched confirmatory test.

The frozen 2025 selection rule chose `histgb_dl_release`. Its 2026 MAE is **0.120729** log-growth units, compared with **0.134710** for the training-median baseline and **0.170619** for persistence. Precision at five is **0.6848**, against sustained-growth prevalence **0.4697**. Downloads-only boosting is slightly better on 2026 MAE (**0.120019**), but the chosen model is not switched using those outcomes. Release additions have mixed incremental value: Ridge improves from 0.144517 to 0.144053, while boosting worsens from 0.120019 to 0.120729. The results do not establish a general benefit from extra sources or from GitHub.

Spark independently reproduced all 6,000 weekly PyPI/release and **observed captured** GitHub totals with **zero mismatches**, using eight input partitions and a two-core executor on c0. The raw checks verified arithmetic for **56,450** retained PyPI rows, compared **2,940 tool-days per source across 14 sampled weeks**, and checked last-week release-version and captured-actor counts for all 30 tools. Agreement validates retained-body consistency, transformations and execution; it proves neither upstream event completeness nor multi-host speedup.

The corrected publication's conservative cumulative query estimate is **$34.5552** before free allowances, within the authorized **$80** limit. It is a dated scan-cost estimate, not an invoice or an account-wide bill. The quality/model correction reused retained data without requiring new BigQuery scans. Version `20261102T215227Z-b9b371b3` remains intact as the pre-correction snapshot. The corrected snapshot retains **6,909** tool-paper candidate pairs requiring relevance and version review. Successful retrieval checks do not establish GH capture completeness or research feature validity.

The supplementary public star-history backfill is resumable and subject to public API request limits; it is not yet claimed as complete for all tools. Separate current-default-branch commit collection is also supplementary. Read their current manifests for per-tool completion rather than treating their state as part of this published model run.

The [compact result evidence](study/results/metrics.json) is saved in this repository. Runtime datasets and the active model snapshot remain on c0. The compact evidence directory is an export, not the active database; its full run manifest refers to the complete c0 snapshot.

## Direct GitHub rebuild

The [GitHub supplement](study/GitHub_Rebuild_and_Sensitivity.md) records the completed 30-repository commit-history rebuild and matched retrospective experiment. These commit features did not show reliable added predictive value in this comparison. The primary model remains unchanged. Direct star-history collection resumes every 15 minutes within public API limits; commit observations refresh daily at 05:10 UTC. Neither job automatically changes the forecasting models.
