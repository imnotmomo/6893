#!/usr/bin/env python3
"""Plot saved AI Technology Radar results without fitting or selecting a model.

Run after the feature and model artifacts have been combined in one snapshot:
    python plot_study.py --results-dir /absolute/path/to/snapshot

Forecast-comparison PNGs use actual previously examined 2026 outcomes only; two additional plots
show saved eligibility flags and block intervals. Model choice is read from the
published frozen-validation selection record, never inferred from held-out MAE.
CSV exports retain the saved input rows for the report builder on the Mac.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


NAMES = {
    "persistence": "No-change baseline",
    "training_median": "Past-label median baseline",
    "trend8": "Eight-week trend",
    "ridge_dl": "Ridge: downloads",
    "ridge_dl_release": "Ridge: downloads + releases",
    "histgb_dl": "Boosted trees: downloads",
    "histgb_dl_release": "Boosted trees: downloads + releases",
}

INK, TEAL, ORANGE, GREY = "#23363D", "#117D80", "#C36A38", "#8B959A"


def utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("A required timestamp is missing")
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def named(model):
    return NAMES.get(str(model), str(model).replace("_", " "))


def _required(frame, columns, artifact):
    missing = sorted(set(columns) - set(frame))
    if missing:
        raise ValueError(f"{artifact} is missing columns: {missing}")


def _finite(frame, columns, artifact):
    for column in columns:
        values = pd.to_numeric(frame[column], errors="raise").to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError(f"{artifact} contains missing or nonfinite {column}")


def load_results(root):
    files = {name: root / name for name in ["metrics.json", "predictions.parquet", "weekly_features.parquet", "data_quality.json", "source_quality.json"]}
    absent = [name for name, path in files.items() if not path.is_file()]
    if absent:
        raise FileNotFoundError("Required saved study artifacts are missing: " + ", ".join(absent))
    metrics = json.loads(files["metrics.json"].read_text())
    if not isinstance(metrics, dict):
        raise ValueError("metrics.json must contain a JSON object")
    if (metrics.get("profile") != "validated_primary"
            or metrics.get("analysis_type") != "post_audit_corrective_sensitivity"
            or metrics.get("previously_examined_2026") is not True
            or set(metrics.get("excluded_feature_groups", [])) != {"gh", "research"}):
        raise ValueError("Plots require the corrected primary post-audit sensitivity artifacts.")
    gate = metrics.get("quality_gate", {})
    quality = json.loads(files["data_quality.json"].read_text())
    if quality.get("validation_status") != "passed" or gate.get("validation_status") != "passed":
        raise ValueError("Plots require passed source-quality validation.")
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    if (gate.get("quality_report_sha256") != sha(files["data_quality.json"])
            or gate.get("source_policy_sha256") != sha(files["source_quality.json"])
            or gate.get("weekly_features_sha256") != sha(files["weekly_features.parquet"])):
        raise ValueError("Plot source-quality artifacts or feature bytes differ from the modeled artifacts.")
    selection = metrics.get("selection", {})
    best = metrics.get("best_model")
    if (not isinstance(selection, dict)
            or selection.get("status") != "selected_from_frozen_validation"
            or not best or selection.get("best_model") != best):
        raise ValueError("A consistent saved frozen-validation model selection is required; this script will not select one")
    cutoff = utc(selection.get("label_availability_cutoff"))
    if cutoff > utc("2026-01-01") or utc(selection.get("maximum_label_available_at")) > cutoff:
        raise ValueError("Selection labels extend beyond the pre-2026 frozen cutoff")
    predictions = pd.read_parquet(files["predictions.parquet"])
    features = pd.read_parquet(files["weekly_features.parquet"])
    _required(predictions, ["tool_id", "week_start", "model", "split", "prediction_log_growth", "target_log_growth", "target_sustained"], "predictions.parquet")
    _required(features, ["tool_id", "week_start"], "weekly_features.parquet")
    if predictions.empty or features.empty:
        raise ValueError("Feature and prediction artifacts must contain observed rows")
    if predictions.duplicated(["model", "tool_id", "week_start"]).any():
        raise ValueError("Duplicate model/tool/week predictions")
    if features.duplicated(["tool_id", "week_start"]).any():
        raise ValueError("Duplicate feature tool/week rows")
    predictions["week_start"] = pd.to_datetime(predictions["week_start"], errors="raise", utc=True)
    holdout = predictions.loc[predictions["split"] == "holdout_2026"].copy()
    if holdout.empty:
        raise ValueError("No saved 2026 held-out predictions are available")
    _finite(holdout, ["prediction_log_growth", "target_log_growth", "target_sustained"], "2026 holdout")
    reference = None
    for model, frame in holdout.groupby("model"):
        labels = frame.set_index(["tool_id", "week_start"])[["target_log_growth", "target_sustained"]].sort_index()
        if reference is None:
            reference = labels
        elif not labels.index.equals(reference.index) or not np.allclose(labels.to_numpy(dtype=float), reference.to_numpy(dtype=float)):
            raise ValueError("Held-out model comparisons do not use identical rows and labels")
    summary = pd.DataFrame(metrics.get("results", []))
    _required(summary, ["split", "model", "mae"], "metrics.json results")
    summary = summary.loc[summary["split"] == "holdout_2026"].copy()
    if summary.empty or summary["model"].duplicated().any():
        raise ValueError("Expected one saved 2026 metric row per model")
    _finite(summary, ["mae"], "saved holdout metrics")
    if (set(summary["model"]) != set(holdout["model"]) or best not in set(summary["model"])
            or set(summary["model"]) != set(NAMES)):
        raise ValueError("Metric model IDs, held-out predictions, and frozen selection disagree")
    actual_mae = holdout.assign(error=lambda f: (f["prediction_log_growth"] - f["target_log_growth"]).abs()).groupby("model")["error"].mean()
    for record in summary.to_dict("records"):
        if not np.isclose(float(record["mae"]), actual_mae.loc[record["model"]], rtol=1e-10, atol=1e-12):
            raise ValueError("Saved MAE disagrees with actual predictions for " + record["model"])
    return metrics, predictions, features, holdout, summary, best, files


def _style():
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 10,
        "axes.titlesize": 12, "axes.labelsize": 10, "axes.edgecolor": "#A4ADB1",
        "axes.labelcolor": INK, "text.color": INK, "xtick.color": INK,
        "ytick.color": INK, "axes.spines.top": False, "axes.spines.right": False,
        "legend.frameon": False, "savefig.facecolor": "white", "figure.facecolor": "white",
    })


def _save(figure, path, footer):
    wrapped = "\n".join(textwrap.fill(line, width=135) for line in footer.splitlines())
    figure.text(0.015, 0.018, wrapped, fontsize=8, color="#56666D", ha="left", va="bottom")
    figure.savefig(path, dpi=220, bbox_inches="tight", pad_inches=0.15)
    plt.close(figure)


def _bar_plot(frame, best, path, title, footer, short_labels=None):
    frame = frame.copy().reset_index(drop=True)
    values = frame["mae"].to_numpy(dtype=float)
    labels = [(short_labels or {}).get(model, named(model)) + (" *" if model == best else "") for model in frame["model"]]
    figure, axis = plt.subplots(figsize=(8.8, max(3.5, 0.47 * len(frame) + 1.5)))
    colors = [TEAL if model == best else GREY for model in frame["model"]]
    axis.barh(np.arange(len(frame)), values, color=colors, height=0.68)
    axis.set_yticks(np.arange(len(frame)), labels)
    axis.invert_yaxis()
    axis.set_xlabel("Mean absolute error (natural-log growth units; lower is better)")
    axis.set_title(title, loc="left", pad=13)
    axis.set_axisbelow(True)
    axis.grid(axis="x", color="#E3E7E9", linewidth=0.6)
    maximum = max(float(values.max()), 0.01)
    axis.set_xlim(0, maximum * 1.19)
    for index, value in enumerate(values):
        axis.text(value + maximum * 0.018, index, f"{value:.3f}", va="center", fontsize=9)
    figure.tight_layout(rect=(0, 0.09, 1, 1))
    _save(figure, path, footer)


def plot_all(root):
    root = Path(root).expanduser().resolve()
    metrics, predictions, features, holdout, summary, best, inputs = load_results(root)
    figures = root / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    _style()
    unique_rows = holdout[["tool_id", "week_start"]].drop_duplicates()
    period = f"{unique_rows.week_start.min():%Y-%m-%d} to {unique_rows.week_start.max():%Y-%m-%d}"
    cohort = f"{len(unique_rows):,} common tool-weeks; {unique_rows.tool_id.nunique()} tools; {unique_rows.week_start.nunique()} feature weeks"
    note = "* Selected from 2025 labels mature by 2026-01-01 UTC. Post-audit sensitivity rerun of previously examined 2026; not an untouched confirmatory test."
    footer = f"2026 feature-week origins: {period}. {cohort}.\n{note}"
    ordered = summary.sort_values(["mae", "model"], kind="mergesort")
    _bar_plot(ordered, best, figures / "model_holdout_mae.png", "Corrected 2026 forecasting error: seven primary models", footer)
    ablation_models = ["ridge_dl", "ridge_dl_release", "histgb_dl", "histgb_dl_release"]
    missing = sorted(set(ablation_models) - set(summary.model))
    if missing:
        raise ValueError("Matched source ablations are missing: " + ", ".join(missing))
    ablations = summary.set_index("model").loc[ablation_models].reset_index()
    _bar_plot(ablations, best, figures / "source_ablation.png",
              "Release-signal ablation within each fixed algorithm", footer,
              {"ridge_dl": "Ridge: downloads", "ridge_dl_release": "Ridge: downloads + releases",
               "histgb_dl": "HistGB: downloads", "histgb_dl_release": "HistGB: downloads + releases"})
    holdout["absolute_error"] = (holdout.prediction_log_growth - holdout.target_log_growth).abs()
    weekly = holdout.groupby(["model", "week_start"], as_index=False).absolute_error.mean()
    chosen = list(dict.fromkeys([best, "persistence", "training_median", "trend8"]))
    if not set(chosen) <= set(weekly.model):
        raise ValueError("Saved baselines and selected-model weekly predictions are required")
    figure, axis = plt.subplots(figsize=(8.8, 4.7))
    for model in chosen:
        frame = weekly.loc[weekly.model == model].sort_values("week_start")
        selected = model == best
        label = named(model) + (" *" if selected else "")
        axis.plot(frame.week_start, frame.absolute_error, label=label,
                  color=TEAL if selected else {"persistence": INK, "training_median": GREY, "trend8": ORANGE}[model],
                  linewidth=2.0 if selected else 1.5,
                  linestyle="-" if selected else {"persistence": "--", "training_median": "-.", "trend8": ":"}[model])
    axis.set_title("Weekly 2026 sensitivity error: selected model and baselines", loc="left", pad=12)
    axis.set_ylabel("Mean absolute error (log-growth units)")
    axis.set_xlabel("Feature week (Monday UTC)")
    axis.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=7))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y", tz=timezone.utc))
    axis.grid(axis="y", color="#E3E7E9", linewidth=0.6)
    axis.legend(loc="upper left", fontsize=9)
    figure.tight_layout(rect=(0, 0.12, 1, 1))
    _save(figure, figures / "weekly_error.png", footer)
    selected = holdout.loc[holdout.model == best].copy()
    errors = selected.groupby("tool_id", as_index=False).absolute_error.mean().rename(columns={"absolute_error": "mae"})
    examples = errors.sort_values(["mae", "tool_id"], ascending=[False, True], kind="mergesort").head(3)
    figure, axes = plt.subplots(len(examples), 1, figsize=(8.8, max(3.2, 2.35 * len(examples))), sharex=True, squeeze=False)
    for axis, record in zip(axes[:, 0], examples.to_dict("records")):
        frame = selected.loc[selected.tool_id == record["tool_id"]].sort_values("week_start")
        axis.plot(frame.week_start, frame.target_log_growth, color=INK, linewidth=1.7, label="Observed eight-week log growth")
        axis.plot(frame.week_start, frame.prediction_log_growth, color=TEAL, linewidth=1.7, linestyle="--", label="Saved prediction")
        axis.axhline(0, color="#9AA5AA", linewidth=0.6)
        axis.set_title(f"{record['tool_id']}  |  held-out MAE {record['mae']:.3f}", loc="left", fontsize=10)
        axis.set_ylabel("Log growth")
        axis.grid(axis="y", color="#E3E7E9", linewidth=0.6)
    axes[0, 0].legend(loc="best", fontsize=8, ncol=2)
    axes[-1, 0].set_xlabel("Feature week (Monday UTC)")
    axes[-1, 0].xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=7))
    axes[-1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%b %Y", tz=timezone.utc))
    figure.suptitle("Post-audit diagnostics: largest errors of " + named(best), x=0.015, ha="left", fontsize=12)
    figure.tight_layout(rect=(0, 0.09, 1, 0.96))
    _save(figure, figures / "example_forecasts.png", "Examples are the three highest per-tool held-out MAEs (tool_id breaks ties), selected for diagnosis, not representativeness.\nOutcome: ln(1 + future median downloads) minus ln(1 + past median downloads); these are historical backtests.")
    _required(features, ["prediction_eligible", "coverage_complete"], "weekly_features.parquet availability flags")
    availability = features[["week_start", "prediction_eligible", "coverage_complete"]].copy()
    availability["week_start"] = pd.to_datetime(availability.week_start, errors="raise", utc=True)
    for flag in ["prediction_eligible", "coverage_complete"]:
        if not availability[flag].isin([True, False, 0, 1]).all():
            raise ValueError("Availability flag contains missing or non-boolean values: " + flag)
        availability[flag] = availability[flag].astype(int)
    availability = availability.groupby("week_start", as_index=False)[["prediction_eligible", "coverage_complete"]].sum()
    figure, axis = plt.subplots(figsize=(8.8, 4.5))
    axis.step(availability.week_start, availability.prediction_eligible, where="mid", color=TEAL,
              linewidth=1.8, label="Feature-ready: eight past download weeks")
    axis.step(availability.week_start, availability.coverage_complete, where="mid", color=INK,
              linewidth=1.6, linestyle="--", label="Mature label: past + full outcome window")
    axis.set_ylim(bottom=0)
    axis.set_ylabel("Eligible tools per feature week")
    axis.set_xlabel("Feature week (Monday UTC)")
    axis.set_title("Panel availability at the saved study cutoff", loc="left", pad=12)
    axis.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=7))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y", tz=timezone.utc))
    axis.grid(axis="y", color="#E3E7E9", linewidth=0.6)
    axis.legend(loc="upper left", fontsize=9)
    figure.tight_layout(rect=(0, 0.11, 1, 1))
    _save(figure, figures / "panel_availability.png", "Counts come directly from prediction_eligible and coverage_complete in the saved weekly panel.\nFeature readiness does not imply that the eight-week outcome is already observable; mature rows are not a final held-out sample count.")
    bootstrap = metrics.get("confidence_intervals", {})
    intervals = [record for record in bootstrap.get("results", [])
                 if record.get("split") == "holdout_2026" and record.get("status") == "computed"]
    figure, axis = plt.subplots(figsize=(8.8, max(3.5, 0.47 * len(intervals) + 1.6)))
    if intervals:
        interval_frame = pd.DataFrame(intervals).sort_values("model", kind="mergesort").reset_index(drop=True)
        _finite(interval_frame, ["mean_weekly_mae_difference", "ci_low", "ci_high"], "saved block intervals")
        if (interval_frame.ci_low > interval_frame.ci_high).any():
            raise ValueError("A saved bootstrap interval has reversed bounds")
        for index, record in enumerate(interval_frame.to_dict("records")):
            color = TEAL if record["model"] == best else GREY
            axis.plot([record["ci_low"], record["ci_high"]], [index, index], color=color, linewidth=2.0)
            axis.scatter([record["mean_weekly_mae_difference"]], [index], color=color, s=28, zorder=3)
        axis.set_yticks(np.arange(len(interval_frame)), [named(model) + (" *" if model == best else "") for model in interval_frame.model])
        axis.invert_yaxis()
        axis.axvline(0, color=INK, linewidth=1.0, linestyle="--")
        axis.set_xlabel("Mean weekly MAE difference versus persistence (negative favors the model)")
        axis.grid(axis="x", color="#E3E7E9", linewidth=0.6)
    else:
        axis.axis("off")
        axis.text(0.5, 0.5, "No computed 2026 block intervals are available in the saved results.",
                  ha="center", va="center", transform=axis.transAxes)
    axis.set_title("Paired weekly errors: descriptive 95% block intervals", loc="left", pad=12)
    figure.tight_layout(rect=(0, 0.13, 1, 1))
    _save(figure, figures / "block_intervals.png",
          f"Saved paired moving-block percentile intervals; {bootstrap.get('block_weeks', 'unreported')} weeks per block, {bootstrap.get('draws', 'unreported')} draws.\nIntervals address overlapping outcomes and remain descriptive for this selected cohort. No IID-row confidence claim. {note}")
    # These are direct exports; no rows are sampled, filled, or synthesized.
    features.to_csv(root / "weekly_features.csv", index=False)
    predictions.to_csv(root / "predictions.csv", index=False)
    output_files = [figures / name for name in ["model_holdout_mae.png", "source_ablation.png", "weekly_error.png", "example_forecasts.png", "panel_availability.png", "block_intervals.png"]]
    output_files += [root / "weekly_features.csv", root / "predictions.csv"]
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "results_dir": str(root),
        "selection_source": "metrics.json selection", "selected_model": best,
        "profile": "validated_primary", "analysis_type": "post_audit_corrective_sensitivity",
        "previously_examined_2026": True, "excluded_feature_groups": ["gh", "research"],
        "selection_label_availability_cutoff": metrics["selection"]["label_availability_cutoff"],
        "holdout_models": sorted(summary.model), "common_holdout_tool_weeks": len(unique_rows),
        "examples_rule": "three largest per-tool MAEs for the frozen selected model; tool_id ascending breaks ties",
        "example_tools": examples.to_dict("records"),
        "spearman_note": "Undefined Spearman values remain unavailable in saved metrics; no zero replacement or confidence score is plotted.",
        "inputs": {name: sha(path) for name, path in inputs.items()},
        "outputs": {str(path.relative_to(root)): sha(path) for path in output_files},
    }
    (figures / "figure_manifest.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = plot_all(args.results_dir)
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}), file=sys.stderr)
        return 1
    print(json.dumps({"status": "complete", "selected_model": result["selected_model"], "outputs": list(result["outputs"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
