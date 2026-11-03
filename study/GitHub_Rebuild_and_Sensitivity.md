# GitHub rebuild and forecasting sensitivity

The independent Git history collector works for all 30 mapped tools. The matched forecasting experiment found **no reliable added predictive value from these Git features**: both 2026 mean errors increased slightly, and both paired block intervals include zero. The primary model and published primary snapshot remain unchanged.

## What was rebuilt

The completed source retains 691,885 unique surviving reachable commit records, including 545,621 first-parent commits, from the frozen current default-branch histories. Compressed commit metadata occupies 44.2 MB. Weekly study features count commits by UTC committer date and include reachable and first-parent counts, logarithms, and trailing four-week summaries. Incomplete weeks and unavailable history remain missing.

Each tool retains its collected branch tip, graph checks, author and committer timestamps, timestamp anomaly counts, and file hashes. The collector checks non-shallow reachable history and the first-parent chain. These checks establish consistency of the retained current graph.

This source does not recover GitHub PushEvents, deleted branches, rewritten commits, or the time a commit first became public. Commit timestamps can be backdated or reordered. Historical public availability therefore remains **unknown**, and the source stays outside the primary forecasting profile. Current direct repository and stargazer collection proceeds separately under public API limits; incomplete star snapshots are not a complete historical star series.

## Matched experiment

Four fixed models compare downloads plus releases with the same inputs plus Git features, separately for Ridge and histogram gradient boosting. They use the original 20 monthly cutoffs, at least 52 training weeks, training labels available by each cutoff, and preprocessing fitted on training rows only. Hyperparameters are unchanged; no 2026 tuning or winner promotion is performed.

All eleven compared series share evaluation rows: four matched refits and seven unchanged published primary references. The 2026 subset contains 990 tool-weeks, 30 tools and 33 origin weeks. This is a posthoc retrospective sensitivity analysis of previously examined 2026 outcomes, not an untouched confirmatory test.

| Estimator | MAE without Git | MAE with Git | Relative error change | 95% interval for paired MAE difference |
| --- | ---: | ---: | ---: | --- |
| Ridge | 0.14405335 | 0.14447872 | +0.2953% | [-0.00024578, +0.00085012] |
| Boosted trees | 0.12072902 | 0.12107140 | +0.2836% | [-0.00030843, +0.00092129] |

Positive differences mean worse error after adding Git. Intervals use 1,000 paired moving-block resamples with eight-week blocks, remain descriptive for this selected cohort, and account for overlapping outcomes without establishing historical data availability. These results support retaining downloads and releases as the primary feature sources; they do not establish that all developer activity signals lack value.

## Validation and provenance

Independent read-only QA verified all 14 experiment output hashes, 28,050 prediction rows on common keys, all 20 training cutoffs, 660 per-tool error rows, all four source comparisons and their block intervals. The 17,850 published reference predictions and their timing fields exactly match the original primary values. The primary manifest hash still matches its value recorded before the experiment.

The separate output profile is `retrospective_git_sensitivity`, with `primary_eligible=false`, `historical_public_availability=unknown`, and no selected winner. Detailed evidence is retained in the sensitivity bundle's `metrics.json`, `ablations.json`, `folds.json`, and `provenance_quality.json`; the independent check is `independent_git_validation.json`.

Git source manifest SHA256: `88dc6a5520e70cd24ddfda98b774f610340e91ff86de99b220ed4525df519403`.

Unchanged primary manifest SHA256: `31851ae4ed3844f1ebdf405fade7e8332c60e96b4992f79cb935be81cf63c3eb`.
