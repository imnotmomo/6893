# AI Technology Radar dashboard

A read-only Streamlit view of the published study snapshot. It does not run collectors, Spark jobs, queries, training, or external APIs when a page or control changes.

## Run privately

Use a Python environment containing `streamlit`, `pandas`, and `pyarrow`. `plotly` improves the chart presentation; native Streamlit charts provide a fallback.

```sh
RADAR_STUDY_DIR="$HOME/6893/data/study/latest" \
  python -m streamlit run app.py \
  --server.address=127.0.0.1 --server.port=8501 \
  --browser.gatherUsageStats=false
```

Alternatively pass the directory as a script argument after `--`:

```sh
python -m streamlit run app.py --server.address=127.0.0.1 --server.port=8501 \
  --browser.gatherUsageStats=false -- --data-dir /absolute/path/to/study/latest
```

Reach the loopback service through the existing SSH tunnel. No authentication system or public deployment is provided by this dashboard; do not expose its port publicly. The explicit Streamlit setting above also disables usage-statistics collection.

## Published inputs

| File | Required columns or contents |
| --- | --- |
| `weekly_features.parquet` | `tool_id`, `week_start`; optional `dl_downloads`, `gh_event_count`, `gh_push_events`, `gh_issue_events`, `gh_pr_events`, `gh_active_actors`, `release_count`, `research_candidate_count`, `research_eligible_count` |
| `predictions.parquet` | `tool_id`, `week_start`, `model`, `prediction_log_growth`, `target_log_growth`; `split` distinguishes `validation_2025` and `holdout_2026` |
| `latest_forecasts.parquet` | Optional future forecasts with tool, origin week, model, prediction, and unobserved/NULL targets. `forecasts.parquet` is accepted as a secondary filename. |
| `metrics.json` | `results` list with model/split, `mae`, `median_weekly_spearman`, `precision_at_5`, sample counts; optional `best_model`, `target_semantics`, `periods` |
| `study_status.json` | Snapshot ID/time, source coverage, cohort, budget and limitations. Accepted coverage keys: `source_coverage`, `coverage`, or `sources`. |
| `data_quality.json` | Corrected primary profile, enabled/excluded source groups, semantic checks, raw spot checks and hashes bound to the exact feature files and cohort. |
| `source_quality.json` | Shared `capture_quality_v1` policy; all GH and research predictors excluded from the primary profile. |

Prediction aliases `y_pred`/`y_true` and `model_name` are accepted. Date and source-column aliases are limited to common schema equivalents in `app.py`; the dashboard does not guess new metrics. Optional prediction fields include baseline/predicted/observed median downloads, feature/training cutoffs, outcome availability, target-window bounds and forecast horizon.

The corrected feature files preserve GitHub counts as `observed_gh_*` diagnostics. Compromised `gh_*` event/actor columns are masked, and **all** GH predictors are excluded from the primary model, including earlier unvalidated dates and missingness flags. A complete retrieval grid or matching Spark total does not certify upstream capture. The UI must retain this warning and must not present a captured zero as proof of zero activity. Read the [source-quality notes](../study_guide.md#source-quality-and-allowed-inputs) for the policy and expected pre-artifact/warmup missingness.

Supplementary direct GitHub star histories count currently surviving stars by creation time, not historical net stars or point-in-time totals. Preserve their original Sunday-first API buckets because UTC alignment is not guaranteed. Current-default-branch commit history represents commits reachable from that branch now and can omit deleted branches or rewritten history. These separate sources do not silently replace GH Archive or enter the primary profile. Current arXiv candidates still require relevance and historical-version review.

The model target is `ln(1 + F) - ln(1 + B)`, where `B` is the past eight-week median and `F` the future eight-week median. Exponentiating that target does not directly give `F/B`. The ranking displays percentage change only when the published baseline and predicted median counts support the calculation with positive baselines; otherwise it displays log-growth units. The evaluation MAE remains in log-growth units. Precision@5 is the share of the five highest predicted-growth tools that meet the sustained-growth label, averaged across eligible weeks. That label requires at least six future weeks above 110% of the past median.

Timing is anchored to feature week `t` (Monday): features are available at `t + 9 days`; week `t + 1` is skipped; the eight complete target weeks are `t + 2` through `t + 9`. The target starts at `t + 14 days`, ends exclusively at `t + 70 days`, and becomes available as a label two days later. The UI calls `week_start` the feature week and displays the published target-window dates rather than describing the target as the next eight calendar weeks.

## Views and interpretation

- **Radar:** defaults to the separately published future-forecast artifact when available. Otherwise it shows a clearly labeled historical held-out backtest. Actual outcomes never appear in the future-forecast table. Published model IDs remain dynamic; no confidence values or adoption categories are invented.
- **Tool details:** weekly downloads and releases, explicitly labeled captured GitHub diagnostics when retained, normalized comparisons, and historical prediction checks. Missing values remain gaps. Current arXiv snapshot metadata is never converted into historical weekly counts.
- **Evaluation:** published baselines and source ablations, evaluation-period selection, sample sizes, target definition and model-selection record.
- **Data evidence:** retrieval coverage, source-validity policy, missing observations, model timing, collection budget and limitations. Coverage is reported from the published artifacts rather than inferred from a chart. Retrieval success and capture validity are separate fields.

CSV downloads contain the currently displayed ranking, metrics, or tool-history rows. A JSON download exposes the published study status. These controls export existing data and perform no collection.

On c0, reproduce and publish through `~/6893/study/run_study.sh`. It binds one `as_of`, cohort, source policy and feature hashes through feature construction, model evaluation and publication. The publisher requires enabled-primary-source quality, model and Spark checks before atomically changing `latest`; prior snapshots remain intact. Corrected snapshot `20261103T012521Z-7fe27982` passed these checks with 42,060 daily rows, 6,000 weekly rows, 5,480 mature labels, no unexpected primary gaps and zero Spark mismatches. Its `histgb_dl_release` selection follows frozen 2025 validation; 2026 MAE is 0.120729 versus 0.134710 for the training-median baseline and 0.170619 for persistence. Earlier combined-source claims remain superseded. Because 2026 outcomes were already examined, the correction is a post-audit sensitivity analysis, not an untouched confirmatory test.

The app resolves `latest` once per rerun and checks file signatures before and after loading, avoiding a mixed snapshot during publication. Missing or unreadable artifacts produce explicit messages rather than synthetic charts or zero-filled results. The refresh button rereads the published snapshot; caching is keyed by the resolved path and file modification signature.
