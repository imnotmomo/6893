"""Run fixed forecasting models with calendar folds and label-availability checks.

The outcome has eight full weeks after one skipped lag week: for feature week t,
features close at t+9 days, outcomes cover t+2 through t+9, and labels mature no
earlier than t+72 days. The corrected primary study excludes all GitHub and
research features. The 2026 period was previously examined: this rerun is a
post-audit corrective sensitivity analysis, not an untouched confirmatory test.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import shutil
import tempfile

import numpy as np
import pandas as pd


SEED = 6893
GROUPS = ("dl", "gh", "release", "research")
FORBIDDEN_FEATURE_TOKEN = re.compile(
    r"(?:^|_)(?:target|label|future|forward|outcome|next|prediction|predicted|post)(?:_|$)"
)
BASE_MODELS = ("persistence", "training_median", "trend8")
PROFILE = "validated_primary"
ANALYSIS_TYPE = "post_audit_corrective_sensitivity"
SOURCE_QUALITY_POLICY = "capture_quality_v1"
EXCLUDED_FEATURE_GROUPS = ("gh", "research")
MODEL_IDS = BASE_MODELS + ("ridge_dl", "ridge_dl_release", "histgb_dl", "histgb_dl_release")
ANALYSIS_NOTE = (
    "Post-audit corrective sensitivity rerun after inspecting the original 2026 results. "
    "The original GitHub-containing results are superseded as primary evidence. "
    "Model selection still uses only 2025-origin labels available by 2026-01-01 UTC; "
    "hyperparameters are frozen and no 2026 tuning is performed. The 2026 period "
    "is not an untouched confirmatory test."
)

SELECTION_CUTOFF = pd.Timestamp("2026-01-01", tz="UTC")
CORE_FEATURE_ALLOWLIST = {
    "dl_downloads", "dl_log_downloads", "dl_log_change_1w", "dl_log_change_4w",
    "dl_log_change_8w", "dl_log_median_4w", "dl_missing",
    *(f"dl_lag_{lag}" for lag in range(8)),
    "release_count", "release_count_4w", "release_missing", "tool_age_weeks",
}
TARGET_SEMANTICS = {
    "formula": "ln(1 + F_i,t) - ln(1 + B_i,t)",
    "unit": "natural_log_change_in_one_plus_median_weekly_downloads",
    "baseline_window": "median artifact downloads in feature weeks t-7 through t",
    "outcome_window": "median artifact downloads in eight full weeks t+2 through t+9",
    "feature_cutoff": "feature week Monday plus 9 days",
    "outcome_gap_weeks": 1,
    "forecast_horizon_weeks": 8,
    "target_window_start": "feature week Monday plus 14 days",
    "target_window_end_exclusive": "feature week Monday plus 70 days",
    "minimum_label_available_at": "target window end plus 2 days",
    "sustained_label": "at least six outcome weeks exceed 1.10 times B_i,t",
    "precision_at_5_definition": (
        "For each week with at least five eligible tools, rank predicted growth "
        "descending and average target_sustained among the first five; then average "
        "across weeks. Ties use tool_id ascending. This is not top-five overlap."
    ),
    "spearman_definition": (
        "Median of defined cross-sectional weekly Spearman correlations. "
        "A constant prediction or target has undefined correlation, not zero."
    ),
    "relative_change_note": (
        "expm1(prediction_log_growth) is the change in 1+median downloads. "
        "Use predicted_median_downloads / baseline_median_downloads - 1 for an "
        "exact download percentage only when the baseline is positive."
    ),
    "forecast_timing_note": (
        "Latest forecasts are generated now from the latest eligible feature week. "
        "Their training cutoff is the actual as-of time. Check target-window bounds "
        "and outcome_period_has_started rather than treating them as backdated forecasts."
    ),
}


def utc_timestamp(value):
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("A required date is missing.")
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def _flag(series, name):
    valid = series.isna() | series.isin([True, False, 0, 1])
    if not bool(valid.all()):
        raise ValueError(f"{name} must contain booleans, zero/one, or missing values.")
    return series.fillna(False).astype(bool)


def normalize_frame(frame):
    required = {
        "tool_id", "week_start", "feature_cutoff", "target_log_growth",
        "target_sustained", "label_available_at", "coverage_complete",
    }
    absent = sorted(required - set(frame.columns))
    if absent:
        raise ValueError(f"Missing required columns: {absent}")
    result = frame.copy()
    if result["tool_id"].isna().any():
        raise ValueError("tool_id cannot be missing.")
    result["tool_id"] = result["tool_id"].astype(str)
    if result["tool_id"].str.strip().eq("").any():
        raise ValueError("tool_id cannot be empty.")
    for column in ["week_start", "feature_cutoff", "label_available_at"]:
        result[column] = pd.to_datetime(result[column], utc=True, errors="raise")
    if result[["week_start", "feature_cutoff"]].isna().any().any():
        raise ValueError("week_start and feature_cutoff cannot be missing.")
    if (result["week_start"].dt.dayofweek != 0).any():
        raise ValueError("week_start must be a Monday.")
    if (result["week_start"] != result["week_start"].dt.normalize()).any():
        raise ValueError("week_start must be midnight UTC.")
    if result.duplicated(["tool_id", "week_start"]).any():
        raise ValueError("The input has duplicate tool_id/week_start keys.")
    result["target_window_start"] = result["week_start"] + pd.Timedelta(days=14)
    result["target_window_end_exclusive"] = result["week_start"] + pd.Timedelta(days=70)
    expected_cutoff = result["week_start"] + pd.Timedelta(days=9)
    if (result["feature_cutoff"] != expected_cutoff).any():
        raise ValueError("feature_cutoff must equal week_start plus 9 days for this frozen protocol.")
    known_availability = result["label_available_at"].notna()
    too_early = result["label_available_at"] < (
        result["target_window_end_exclusive"] + pd.Timedelta(days=2)
    )
    if (known_availability & too_early).any():
        raise ValueError("A label is marked available before its full outcome window and two-day lag.")
    for column in ["target_log_growth", "target_sustained"]:
        result[column] = pd.to_numeric(result[column], errors="raise")
    finite_targets = result["target_log_growth"].dropna().to_numpy(dtype=float)
    if not np.isfinite(finite_targets).all():
        raise ValueError("Observed target_log_growth values must be finite.")
    known_sustained = result["target_sustained"].dropna()
    if not known_sustained.isin([0, 1, True, False]).all():
        raise ValueError("Observed target_sustained values must be zero or one.")
    result["coverage_complete"] = _flag(result["coverage_complete"], "coverage_complete")
    if "prediction_eligible" in result:
        result["prediction_eligible"] = _flag(result["prediction_eligible"], "prediction_eligible")
    else:
        result["prediction_eligible"] = result["coverage_complete"]
    return result.sort_values(["week_start", "tool_id"]).reset_index(drop=True)


def select_feature_groups(frame, include_research=False):
    """Use an exact reviewed allowlist, never a prefix as permission to predict."""
    if include_research:
        raise ValueError("Research is excluded from the corrected validated_primary profile.")
    selected = {group: [] for group in GROUPS}
    selected["controls"] = []
    rejected = {}
    for column in frame.columns:
        name = column[8:] if column.startswith("feature_") else column
        if name.startswith(("gh_", "observed_gh_", "research_")):
            rejected[column] = "excluded source group in corrected primary profile; GitHub capture is unvalidated and research lacks point-in-time review"
            continue
        group = next((group for group in ("dl", "release") if name.startswith(group + "_")), None)
        if name == "tool_age_weeks":
            group = "controls"
        if group is None:
            continue
        if FORBIDDEN_FEATURE_TOKEN.search(name.lower()) or name not in CORE_FEATURE_ALLOWLIST:
            rejected[column] = "not in the exact frozen historical feature allowlist"
            continue
        if not pd.api.types.is_numeric_dtype(frame[column]):
            raise ValueError(f"Allowlisted feature {column!r} is not numeric.")
        # Reject duplicate aliases rather than counting the same feature twice.
        if name in {c[8:] if c.startswith("feature_") else c for c in selected[group]}:
            raise ValueError(f"Duplicate aliases for allowlisted feature {name!r}.")
        selected[group].append(column)
    for columns in selected.values():
        columns.sort()
    for required_group in ("dl", "release"):
        if not selected[required_group]:
            raise ValueError(f"No safe numeric {required_group}_ features were supplied.")
    return selected, rejected


def lag_columns(frame):
    columns = []
    for lag in range(8):
        names = [f"dl_lag_{lag}", f"feature_dl_lag_{lag}"]
        found = next((name for name in names if name in frame), None)
        if found is None:
            raise ValueError(f"The trend baseline requires dl_lag_{lag} (or its feature_ alias).")
        columns.append(found)
    return columns


def valid_lag_mask(frame, columns):
    values = frame[columns].to_numpy(dtype=float, na_value=np.nan)
    return pd.Series(np.isfinite(values).all(axis=1) & (values >= 0).all(axis=1), index=frame.index)


def historical_research_mask(frame, include_research):
    if not include_research:
        return pd.Series(True, index=frame.index)
    return _flag(frame["research_history_eligible"], "research_history_eligible")


def select_train_rows(frame, cutoff, forecast_week=None, include_research=False):
    cutoff = utc_timestamp(cutoff)
    mask = (
        frame["coverage_complete"]
        & frame["prediction_eligible"]
        & frame["target_log_growth"].notna()
        & frame["target_sustained"].notna()
        & frame["label_available_at"].notna()
        & (frame["label_available_at"] <= cutoff)
        & (frame["feature_cutoff"] <= cutoff)
        & valid_lag_mask(frame, lag_columns(frame))
        & historical_research_mask(frame, include_research)
    )
    if forecast_week is not None:
        mask &= frame["week_start"] < utc_timestamp(forecast_week)
    return frame.loc[mask].copy()


def trend8_prediction(frame, columns=None):
    columns = columns or lag_columns(frame)
    values = frame[columns].to_numpy(dtype=float, na_value=np.nan)
    if not (np.isfinite(values).all() and (values >= 0).all()):
        raise ValueError("Trend baseline received incomplete or negative historical downloads.")
    chronological_logs = np.log1p(values[:, ::-1])
    past_times = np.arange(-7.0, 1.0)
    centered = past_times - past_times.mean()
    slopes = chronological_logs @ centered / np.dot(centered, centered)
    future_times = np.arange(2.0, 10.0)
    predicted_logs = chronological_logs.mean(axis=1)[:, None] + slopes[:, None] * (
        future_times - past_times.mean()
    )[None, :]
    predicted_logs = np.maximum(predicted_logs, 0.0)
    middle = np.sort(predicted_logs, axis=1)[:, 3:5]
    future_log_median = np.logaddexp(middle[:, 0], middle[:, 1]) - np.log(2.0)
    return future_log_median - np.log1p(np.median(values, axis=1))


def build_pipeline(kind, n_jobs=2):
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    if kind == "ridge":
        estimator = Ridge(alpha=10.0, solver="lsqr", tol=1e-6)
    elif kind == "histgb":
        estimator = HistGradientBoostingRegressor(
            learning_rate=0.05, max_iter=120, max_leaf_nodes=8, max_depth=3,
            min_samples_leaf=20, l2_regularization=10.0,
            early_stopping=False, random_state=SEED,
        )
    elif kind == "xgboost":
        from xgboost import XGBRegressor
        estimator = XGBRegressor(
            objective="reg:squarederror", n_estimators=150, max_depth=3,
            learning_rate=0.05, min_child_weight=10, reg_lambda=10.0,
            subsample=1.0, colsample_bytree=1.0, tree_method="hist",
            n_jobs=n_jobs, random_state=SEED,
        )
    else:
        raise ValueError(f"Unknown estimator: {kind}")
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
        ("scaler", StandardScaler()),
        ("model", estimator),
    ])


@contextmanager
def limited_threads(n_jobs):
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=n_jobs):
        yield


def model_recipes(groups, boosted_model="histgb", include_research=False):
    if include_research or boosted_model != "histgb":
        raise ValueError("The corrected primary comparison requires frozen Ridge and HistGB models without research.")
    if groups.get("gh") or groups.get("research"):
        raise ValueError("Excluded source groups cannot enter primary model recipes.")
    def columns(*names):
        result = sorted(set(groups["controls"] + sum((groups[name] for name in names), [])))
        for column in result:
            canonical = column[8:] if column.startswith("feature_") else column
            if canonical not in CORE_FEATURE_ALLOWLIST:
                raise ValueError(f"Recipe feature {column!r} is outside the exact primary allowlist.")
        return result
    return {
        "ridge_dl": {"kind": "ridge", "features": columns("dl")},
        "ridge_dl_release": {"kind": "ridge", "features": columns("dl", "release")},
        "histgb_dl": {"kind": "histgb", "features": columns("dl")},
        "histgb_dl_release": {"kind": "histgb", "features": columns("dl", "release")},
    }


def _numeric_features(frame, features):
    return frame[features].astype(float).replace([np.inf, -np.inf], np.nan)


def _fit_recipes(train, recipes, n_jobs):
    target = train["target_log_growth"].to_numpy(dtype=float)
    if not len(target) or not np.isfinite(target).all():
        raise ValueError("Training requires finite, available labels.")
    fitted = {"training_median": float(np.median(target))}
    with limited_threads(n_jobs):
        for name, recipe in recipes.items():
            pipeline = build_pipeline(recipe["kind"], n_jobs)
            pipeline.fit(_numeric_features(train, recipe["features"]), train["target_log_growth"].to_numpy(dtype=float))
            fitted[name] = pipeline
    return fitted


def _predict_all(rows, recipes, fitted, n_jobs):
    predictions = {
        "persistence": np.zeros(len(rows), dtype=float),
        "training_median": np.full(len(rows), fitted["training_median"], dtype=float),
        "trend8": trend8_prediction(rows),
    }
    with limited_threads(n_jobs):
        for name, recipe in recipes.items():
            predictions[name] = fitted[name].predict(_numeric_features(rows, recipe["features"]))
    baseline = np.median(rows[lag_columns(rows)].to_numpy(dtype=float), axis=1)
    lower_bound = -np.log1p(baseline)
    for name, values in predictions.items():
        values = np.maximum(np.asarray(values, dtype=float), lower_bound)
        if not np.isfinite(values).all():
            raise ValueError(f"Model {name} returned non-finite predictions.")
        predictions[name] = values
    return predictions


def split_name(week):
    year = utc_timestamp(week).year
    return "validation_2025" if year == 2025 else "holdout_2026" if year == 2026 else f"backtest_{year}"


def _median_counts(baseline, log_growth):
    log_count = np.log1p(baseline) + np.asarray(log_growth, dtype=float)
    safe = log_count <= np.log(np.finfo(float).max)
    result = np.full(len(log_count), np.nan)
    result[safe] = np.maximum(0.0, np.expm1(log_count[safe]))
    return result


def prediction_records(rows, predictions, train_cutoff, train, generated_at, future=False):
    baseline = np.median(rows[lag_columns(rows)].to_numpy(dtype=float), axis=1)
    keys = [
        "tool_id", "week_start", "feature_cutoff", "label_available_at",
        "target_window_start", "target_window_end_exclusive",
    ]
    result = []
    for name, values in predictions.items():
        records = rows[keys].copy().reset_index(drop=True)
        records["model"] = name
        records["prediction_log_growth"] = np.asarray(values, dtype=float)
        records["target_log_growth"] = np.nan if future else rows["target_log_growth"].to_numpy(dtype=float)
        records["target_sustained"] = pd.array(
            [None] * len(rows) if future else rows["target_sustained"].astype(int).tolist(), dtype="Int8"
        )
        records["split"] = "future_unobserved" if future else rows["week_start"].map(split_name).to_numpy()
        records["forecast_type"] = "future_unobserved" if future else "retrospective_backtest"
        records["profile"] = PROFILE
        records["analysis_type"] = ANALYSIS_TYPE
        records["previously_examined_2026"] = True
        records["train_cutoff"] = utc_timestamp(train_cutoff)
        records["train_row_count"] = len(train)
        records["train_week_count"] = train["week_start"].nunique()
        records["train_last_week"] = train["week_start"].max()
        records["training_target_median"] = float(train["target_log_growth"].median())
        records["forecast_horizon_weeks"] = 8
        records["outcome_gap_weeks"] = 1
        records["generated_at"] = utc_timestamp(generated_at)
        records["outcome_period_has_started"] = records["target_window_start"] <= utc_timestamp(generated_at)
        records["baseline_median_downloads"] = baseline
        records["predicted_median_downloads"] = _median_counts(baseline, values)
        records["observed_future_median_downloads"] = (
            np.nan if future else _median_counts(baseline, rows["target_log_growth"].to_numpy(dtype=float))
        )
        result.append(records)
    return add_prediction_ranks(pd.concat(result, ignore_index=True))


def add_prediction_ranks(predictions):
    result = predictions.copy()
    result["prediction_rank"] = 0
    result["is_top5"] = False
    for _, group in result.groupby(["model", "week_start"], sort=False):
        order = group.sort_values(["prediction_log_growth", "tool_id"], ascending=[False, True], kind="mergesort").index
        result.loc[order, "prediction_rank"] = np.arange(1, len(order) + 1)
        if len(order) >= 5:
            result.loc[order[:5], "is_top5"] = True
    result["prediction_rank"] = result["prediction_rank"].astype(int)
    return result.sort_values(["week_start", "model", "prediction_rank"]).reset_index(drop=True)


def assert_same_evaluation_rows(predictions):
    if predictions.empty:
        raise ValueError("Evaluation requires observed predictions and known labels.")
    for column in ("prediction_log_growth", "target_log_growth", "target_sustained"):
        if not np.isfinite(pd.to_numeric(predictions[column], errors="raise").to_numpy(dtype=float, na_value=np.nan)).all():
            raise ValueError("Evaluation requires finite predictions and known labels; future rows cannot be scored.")
    if not predictions["target_sustained"].isin([0, 1]).all():
        raise ValueError("Evaluation target_sustained must be zero or one.")
    if predictions.duplicated(["model", "tool_id", "week_start"]).any():
        raise ValueError("Duplicate model/tool/week predictions.")
    reference = None
    for _, group in predictions.groupby("model", sort=True):
        indexed = group.set_index(["tool_id", "week_start"])[["target_log_growth", "target_sustained"]].sort_index()
        if reference is None:
            reference = indexed
        elif not indexed.index.equals(reference.index) or not np.allclose(
            indexed.astype(float).to_numpy(), reference.astype(float).to_numpy(), equal_nan=True
        ):
            raise ValueError("Model comparisons do not use identical evaluation rows and labels.")


def weekly_spearman(target, prediction):
    target_rank = pd.Series(np.asarray(target, dtype=float)).rank(method="average").to_numpy()
    prediction_rank = pd.Series(np.asarray(prediction, dtype=float)).rank(method="average").to_numpy()
    if len(target_rank) < 2 or np.ptp(target_rank) == 0 or np.ptp(prediction_rank) == 0:
        return np.nan
    return float(np.corrcoef(target_rank, prediction_rank)[0, 1])


def calculate_metrics(predictions):
    assert_same_evaluation_rows(predictions)
    weekly_rows = []
    for (split, model, week), group in predictions.groupby(["split", "model", "week_start"], sort=True):
        ordered = group.sort_values(["prediction_log_growth", "tool_id"], ascending=[False, True], kind="mergesort")
        error = ordered["prediction_log_growth"].to_numpy(dtype=float) - ordered["target_log_growth"].to_numpy(dtype=float)
        weekly_rows.append({
            "split": split, "model": model, "week_start": week,
            "n_tools": len(group), "mae": float(np.mean(np.abs(error))),
            "spearman": weekly_spearman(ordered["target_log_growth"], ordered["prediction_log_growth"]),
            "precision_at_5": float(ordered.head(5)["target_sustained"].mean()) if len(group) >= 5 else np.nan,
            "sustained_prevalence": float(ordered["target_sustained"].mean()),
            "positive_growth_prevalence": float((ordered["target_log_growth"] > 0).mean()),
        })
    weekly = pd.DataFrame(weekly_rows)
    results = []
    for (split, model), group in predictions.groupby(["split", "model"], sort=True):
        weeks = weekly[(weekly["split"] == split) & (weekly["model"] == model)]
        correlations = weeks["spearman"].dropna()
        precision = weeks["precision_at_5"].dropna()
        results.append({
            "split": split, "model": model,
            "mae": float(np.abs(group["prediction_log_growth"] - group["target_log_growth"]).mean()),
            "median_weekly_spearman": float(correlations.median()) if len(correlations) else None,
            "precision_at_5": float(precision.mean()) if len(precision) else None,
            "mean_weekly_sustained_prevalence": float(weeks["sustained_prevalence"].mean()),
            "mean_weekly_positive_growth_prevalence": float(weeks["positive_growth_prevalence"].mean()),
            "precision_lift_over_prevalence": (
                float((weeks.loc[weeks["precision_at_5"].notna(), "precision_at_5"]
                       - weeks.loc[weeks["precision_at_5"].notna(), "sustained_prevalence"]).mean())
                if len(precision) else None
            ),
            "n_predictions": len(group), "n_weeks": group["week_start"].nunique(),
            "n_tools": group["tool_id"].nunique(),
            "n_spearman_weeks": len(correlations), "n_precision_weeks": len(precision),
        })
    tools = predictions.assign(
        absolute_error=lambda f: (f["prediction_log_growth"] - f["target_log_growth"]).abs(),
        prediction_error=lambda f: f["prediction_log_growth"] - f["target_log_growth"],
    ).groupby(["split", "model", "tool_id"], as_index=False).agg(
        n_predictions=("target_log_growth", "size"),
        mae=("absolute_error", "mean"),
        mean_prediction_error=("prediction_error", "mean"),
    )
    return results, weekly, tools


def select_from_frozen_validation(predictions):
    """Choose once using only validation labels mature before the holdout year."""
    eligible = predictions.loc[
        (predictions["split"] == "validation_2025")
        & (predictions["label_available_at"] <= SELECTION_CUTOFF)
    ].copy()
    selection_metrics = calculate_metrics(eligible)[0] if len(eligible) else []
    ordered = sorted(selection_metrics, key=lambda item: (
        item["mae"],
        -item["median_weekly_spearman"] if item["median_weekly_spearman"] is not None else np.inf,
        item["model"],
    ))
    keys = eligible[["tool_id", "week_start"]].drop_duplicates()
    return {
        "split": "validation_2025", "metric": "mae",
        "label_availability_cutoff": SELECTION_CUTOFF,
        "rule": "Only 2025-origin validation predictions whose labels are available by 2026-01-01 UTC; full-year validation metrics are descriptive.",
        "best_model": ordered[0]["model"] if ordered else None,
        "value": ordered[0]["mae"] if ordered else None,
        "status": "selected_from_frozen_validation" if ordered else "mature_validation_period_unavailable",
        "n_rows_per_model": len(keys),
        "n_weeks": keys["week_start"].nunique(),
        "n_tools": keys["tool_id"].nunique(),
        "first_feature_week": keys["week_start"].min() if len(keys) else None,
        "last_feature_week": keys["week_start"].max() if len(keys) else None,
        "maximum_label_available_at": eligible["label_available_at"].max() if len(eligible) else None,
        "results": selection_metrics,
    }


def measurement_boundary_metrics(predictions):
    boundary = utc_timestamp("2026-08-24")
    held_out = predictions.loc[predictions["split"] == "holdout_2026"].copy()
    segments = {
        "outcome_before_logging_change": held_out["target_window_end_exclusive"] <= boundary,
        "outcome_spans_logging_change": (
            (held_out["target_window_start"] < boundary)
            & (held_out["target_window_end_exclusive"] > boundary)
        ),
        "outcome_after_logging_change": held_out["target_window_start"] >= boundary,
    }
    results = []
    for name, mask in segments.items():
        subset = held_out.loc[mask]
        if subset.empty:
            continue
        rows, _, _ = calculate_metrics(subset)
        for row in rows:
            row["outcome_period"] = name
            row["first_feature_week"] = subset["week_start"].min()
            row["last_feature_week"] = subset["week_start"].max()
            results.append(row)
    return {
        "boundary": boundary,
        "grouping_axis": "future eight-week outcome window, not forecast origin",
        "note": (
            "All periods use the same artifact-only download definition. The past "
            "baseline window may precede the boundary even for an after-boundary "
            "outcome. This diagnostic does not establish a causal logging-change effect."
        ),
        "empty_segments": [name for name, mask in segments.items() if not bool(mask.any())],
        "results": results,
    }


def paired_block_bootstrap(weekly_metrics, reference="persistence", draws=1000, block_weeks=8):
    results = []
    for split, period in weekly_metrics.groupby("split", sort=True):
        baseline = period.loc[period["model"] == reference, ["week_start", "mae"]].rename(columns={"mae": "reference_mae"})
        for model, rows in period.loc[period["model"] != reference].groupby("model", sort=True):
            joined = rows[["week_start", "mae"]].merge(baseline, on="week_start", how="inner", validate="one_to_one").sort_values("week_start").reset_index(drop=True)
            differences = (joined["mae"] - joined["reference_mae"]).to_numpy(dtype=float)
            starts = []
            for start in range(max(0, len(joined) - block_weeks + 1)):
                dates = joined["week_start"].iloc[start:start + block_weeks]
                if all(dates.iloc[j] - dates.iloc[0] == pd.Timedelta(weeks=j) for j in range(block_weeks)):
                    starts.append(start)
            record = {
                "split": split, "model": model, "reference": reference,
                "statistic": "unweighted mean of paired weekly MAE differences, model minus reference",
                "mean_weekly_mae_difference": float(differences.mean()) if len(differences) else None,
                "n_weeks": len(differences), "block_weeks": block_weeks,
                "draws": draws, "valid_moving_blocks": len(starts),
                "ci_level": 0.95, "ci_low": None, "ci_high": None,
                "status": "insufficient_contiguous_weeks",
            }
            if len(differences) >= 2 * block_weeks and len(starts) >= 2:
                block_values = np.asarray([differences[start:start + block_weeks] for start in starts])
                rng = np.random.default_rng(SEED)
                sampled_blocks = rng.integers(0, len(starts), size=(draws, int(np.ceil(len(differences) / block_weeks))))
                samples = block_values[sampled_blocks].reshape(draws, -1)[:, :len(differences)].mean(axis=1)
                record.update(
                    ci_low=float(np.quantile(samples, 0.025)),
                    ci_high=float(np.quantile(samples, 0.975)),
                    status="computed",
                )
            results.append(record)
    return {
        "method": "paired moving-block percentile bootstrap of weekly MAE differences",
        "block_weeks": block_weeks, "draws": draws, "seed": SEED,
        "interpretation": "negative differences favor the model over persistence",
        "note": "Eight-week blocks address overlapping outcomes; intervals remain descriptive for this selected cohort and are not an IID-row calculation.",
        "results": results,
    }


def source_ablation_metrics(metric_results, weekly_metrics, tool_errors):
    """Compare added release signals within each fixed estimator on matched rows."""
    results, tool_rows = [], []
    metrics = pd.DataFrame(metric_results)
    for algorithm in ("ridge", "histgb"):
        baseline, augmented = f"{algorithm}_dl", f"{algorithm}_dl_release"
        intervals = paired_block_bootstrap(weekly_metrics, reference=baseline)
        for split, period in metrics.groupby("split", sort=True):
            indexed = period.set_index("model")
            if baseline not in indexed.index or augmented not in indexed.index:
                raise ValueError("Matched download/release ablation models are missing.")
            first, second = indexed.loc[baseline], indexed.loc[augmented]
            if first["n_predictions"] != second["n_predictions"] or first["n_weeks"] != second["n_weeks"]:
                raise ValueError("Source ablation does not use identical row counts.")
            interval = next(record for record in intervals["results"]
                            if record["split"] == split and record["model"] == augmented)
            results.append({
                "split": split, "algorithm": algorithm,
                "baseline_model": baseline, "augmented_model": augmented,
                "baseline_sources": ["dl", "controls"],
                "augmented_sources": ["dl", "release", "controls"],
                "baseline_mae": float(first["mae"]), "augmented_mae": float(second["mae"]),
                "mae_difference": float(second["mae"] - first["mae"]),
                "relative_mae_change": float(second["mae"] / first["mae"] - 1) if first["mae"] else None,
                "n_predictions": int(first["n_predictions"]), "n_weeks": int(first["n_weeks"]),
                "mean_weekly_mae_difference": interval["mean_weekly_mae_difference"],
                "ci_low": interval["ci_low"], "ci_high": interval["ci_high"],
                "ci_status": interval["status"], "block_weeks": interval["block_weeks"],
            })
        first = tool_errors.loc[tool_errors["model"] == baseline].drop(columns="model")
        second = tool_errors.loc[tool_errors["model"] == augmented].drop(columns="model")
        paired = first.merge(second, on=["split", "tool_id"], suffixes=("_dl", "_dl_release"), validate="one_to_one")
        if len(paired) != len(first) or len(paired) != len(second):
            raise ValueError("Per-tool source ablations require identical tools.")
        paired["algorithm"] = algorithm
        paired["mae_difference"] = paired["mae_dl_release"] - paired["mae_dl"]
        tool_rows.extend(paired.to_dict("records"))
    return {
        "profile": PROFILE, "analysis_type": ANALYSIS_TYPE,
        "comparison": "Within-estimator downloads versus downloads plus release signals; identical evaluation rows and fixed hyperparameters",
        "interpretation": "negative MAE differences favor adding releases; this is descriptive post-audit sensitivity analysis",
        "excluded_feature_groups": list(EXCLUDED_FEATURE_GROUPS),
        "results": results,
    }, pd.DataFrame(tool_rows)


def _raw_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_quality_gate(input_path, as_of, quality_report=None, quality_policy=None):
    """Fail closed unless audited feature bytes and exclusion policy match this run."""
    input_path = Path(input_path)
    if not input_path.is_file():
        raise ValueError("The corrected primary gate requires one audited weekly_features Parquet file.")
    report_path = Path(quality_report) if quality_report else input_path.parent / "data_quality.json"
    policy_path = Path(quality_policy) if quality_policy else input_path.parent / "source_quality.json"
    if not report_path.is_file() or not policy_path.is_file():
        raise ValueError("A passed data_quality.json report and source_quality.json policy are required.")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    for artifact in (report, policy):
        if artifact.get("profile") != PROFILE or artifact.get("source_policy") != SOURCE_QUALITY_POLICY:
            raise ValueError("Quality artifacts do not describe the validated_primary capture-quality policy.")
        if set(artifact.get("excluded_feature_groups", [])) != set(EXCLUDED_FEATURE_GROUPS):
            raise ValueError("Quality artifacts must exclude all GitHub and research features.")
    if report.get("validation_status") != "passed":
        raise ValueError("Source quality validation has not passed.")
    if set(policy.get("enabled_feature_groups", [])) != {"dl", "release", "controls"}:
        raise ValueError("Quality policy enables an unreviewed feature group.")
    gh = policy.get("sources", {}).get("gharchive", {})
    if gh.get("primary_eligible") is not False or gh.get("capture_validated") is not False:
        raise ValueError("Quality policy must explicitly mark GH capture as unvalidated and excluded.")
    if utc_timestamp(report.get("as_of")) != utc_timestamp(as_of):
        raise ValueError("Quality report and model as-of cutoffs differ.")
    hashes = report.get("input_hashes", {})
    expected = hashes.get("weekly_features.parquet")
    if not expected or expected != _raw_sha256(input_path):
        raise ValueError("Quality report does not match the weekly feature bytes.")
    policy_hash = _raw_sha256(policy_path)
    if (report.get("source_policy_sha256") != policy_hash
            or hashes.get("source_quality.json") != policy_hash):
        raise ValueError("Quality report does not match the policy bytes.")
    if not re.fullmatch(r"[0-9a-f]{64}", str(report.get("cohort_sha256", ""))):
        raise ValueError("Quality report must bind the audited cohort registry.")
    return report, policy, {
        "quality_report_sha256": _raw_sha256(report_path),
        "source_policy_sha256": policy_hash,
        "weekly_features_sha256": expected,
        "cohort_sha256": report["cohort_sha256"],
    }


def _input_fingerprint(path):
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    digest = hashlib.sha256()
    for file in files:
        digest.update(str(file.relative_to(path) if path.is_dir() else file.name).encode())
        with file.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


def _json_default(value):
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (np.integer, np.bool_)):
        return value.item()
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot encode {type(value).__name__}")


def write_json(path, value):
    def clean(item):
        if isinstance(item, dict):
            return {key: clean(value) for key, value in item.items()}
        if isinstance(item, (list, tuple)):
            return [clean(value) for value in item]
        if isinstance(item, (float, np.floating)) and not np.isfinite(item):
            return None
        return item
    path.write_text(json.dumps(clean(value), indent=2, default=_json_default, allow_nan=False) + "\n", encoding="utf-8")


def _versions():
    result = {"python": platform.python_version()}
    for name in ["numpy", "pandas", "scikit-learn", "joblib", "pyarrow", "xgboost"]:
        try:
            result[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            result[name] = None
    return result


def run_study(input_path, output_dir, min_train_weeks=52, test_start="2025-01-06",
              test_end="latest_mature", as_of=None, boosted_model="histgb",
              include_research=False, n_jobs=2, quality_report=None, quality_policy=None,
              supersedes_snapshot="20261102T215227Z-b9b371b3"):
    import joblib

    input_path, output_dir = Path(input_path), Path(output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError("Use a new output directory to preserve prior study artifacts.")
    if min_train_weeks < 2 or n_jobs < 1:
        raise ValueError("min_train_weeks must be at least two and n_jobs at least one.")
    as_of = utc_timestamp(as_of or datetime.now(timezone.utc))
    generated_at = utc_timestamp(datetime.now(timezone.utc))
    quality, policy, quality_binding = validate_quality_gate(input_path, as_of, quality_report, quality_policy)
    test_start = utc_timestamp(test_start)
    if test_start < utc_timestamp("2025-01-01"):
        raise ValueError("The frozen validation/test protocol starts in 2025.")
    frame = normalize_frame(pd.read_parquet(input_path))
    groups, rejected_features = select_feature_groups(frame, include_research)
    recipes = model_recipes(groups, boosted_model, include_research)
    valid_lags = valid_lag_mask(frame, lag_columns(frame))
    research_mask = historical_research_mask(frame, include_research)
    mature = (
        frame["coverage_complete"] & frame["prediction_eligible"] & valid_lags & research_mask
        & frame["target_log_growth"].notna() & frame["target_sustained"].notna()
        & frame["label_available_at"].notna() & (frame["label_available_at"] <= as_of)
        & (frame["feature_cutoff"] <= as_of)
    )
    if not mature.any():
        raise ValueError("No fully covered, mature labeled rows are available at the as-of cutoff.")
    latest_mature = frame.loc[mature, "week_start"].max()
    end_requested = str(test_end)
    if end_requested.lower().replace("-", "_") in {"latest_mature", "latestmature"}:
        test_end = latest_mature
    else:
        test_end = min(utc_timestamp(test_end), latest_mature)
    test_end = min(test_end, utc_timestamp("2026-12-31"))
    evaluation = frame.loc[
        mature & (frame["week_start"] >= test_start) & (frame["week_start"] <= test_end)
    ].copy()
    if evaluation.empty:
        raise ValueError("No mature evaluation rows remain in the requested 2025/2026 interval.")
    evaluation["refit_month"] = evaluation["week_start"].dt.strftime("%Y-%m")
    fold_records, prediction_parts = [], []
    for month, test in evaluation.groupby("refit_month", sort=True):
        first_week = test["week_start"].min()
        cutoff = test.loc[test["week_start"] == first_week, "feature_cutoff"].min()
        train = select_train_rows(frame, cutoff, first_week, include_research)
        fold = {
            "fold_id": month, "train_cutoff": cutoff,
            "train_row_count": len(train), "train_week_count": train["week_start"].nunique(),
            "train_last_week": train["week_start"].max() if len(train) else None,
            "maximum_training_label_available_at": train["label_available_at"].max() if len(train) else None,
            "evaluation_weeks": sorted(test["week_start"].unique()), "evaluation_row_count": len(test),
        }
        if train["week_start"].nunique() < min_train_weeks or len(train) < 20:
            fold["status"] = "skipped_insufficient_training"
            fold_records.append(fold)
            continue
        fitted = _fit_recipes(train, recipes, n_jobs)
        fold["training_target_median"] = fitted["training_median"]
        predictions = _predict_all(test, recipes, fitted, n_jobs)
        prediction_parts.append(prediction_records(test, predictions, cutoff, train, generated_at))
        fold["status"] = "evaluated"
        fold_records.append(fold)
        print(f"Evaluated {month}: {len(train)} training rows, {len(test)} common test rows.", flush=True)
    if not prediction_parts:
        raise ValueError("Every forecast month failed the minimum-training-history requirement.")
    predictions = pd.concat(prediction_parts, ignore_index=True)
    assert_same_evaluation_rows(predictions)
    metric_results, weekly_metrics, tool_errors = calculate_metrics(predictions)
    boundary_metrics = measurement_boundary_metrics(predictions)
    bootstrap = paired_block_bootstrap(weekly_metrics)
    ablations, ablation_tools = source_ablation_metrics(metric_results, weekly_metrics, tool_errors)
    selection = select_from_frozen_validation(predictions)
    best_model = selection["best_model"]
    if best_model is None or set(predictions["model"]) != set(MODEL_IDS):
        raise ValueError("The primary comparison requires all seven models and a mature frozen-validation selection.")
    eligible_forecast = (
        frame["prediction_eligible"] & valid_lags & research_mask & (frame["feature_cutoff"] <= as_of)
    )
    latest_week = frame.loc[eligible_forecast, "week_start"].max() if eligible_forecast.any() else None
    final_train = select_train_rows(frame, as_of, include_research=include_research)
    if final_train["week_start"].nunique() < min_train_weeks or len(final_train) < 20:
        raise ValueError("Insufficient mature data for the final as-of model fit.")
    final_models = _fit_recipes(final_train, recipes, n_jobs)
    future_rows = frame.loc[eligible_forecast & (frame["week_start"] == latest_week)].copy()
    future_rows = future_rows.loc[
        (future_rows["target_window_end_exclusive"] > as_of)
        & (future_rows["label_available_at"].isna() | (future_rows["label_available_at"] > as_of))
    ]
    if len(future_rows):
        forecasts = prediction_records(
            future_rows, _predict_all(future_rows, recipes, final_models, n_jobs),
            as_of, final_train, generated_at, future=True,
        )
        forecast_status = "published_unobserved_outcome"
    else:
        forecasts = predictions.iloc[:0].copy()
        forecast_status = "no_eligible_future_outcome_window"
    periods = {}
    for split, rows in predictions.groupby("split"):
        periods[split] = {
            "first_feature_week": rows["week_start"].min(),
            "last_feature_week": rows["week_start"].max(),
            "n_feature_weeks": rows["week_start"].nunique(),
        }
    config = {
        "schema_version": 2, "generated_at": generated_at, "as_of": as_of,
        "profile": PROFILE, "analysis_type": ANALYSIS_TYPE,
        "previously_examined_2026": True, "analysis_note": ANALYSIS_NOTE,
        "supersedes_snapshot": supersedes_snapshot,
        "excluded_feature_groups": list(EXCLUDED_FEATURE_GROUPS),
        "source_quality_policy": SOURCE_QUALITY_POLICY,
        "quality_gate": {"validation_status": "passed", **quality_binding},
        "raw_series_quality": {
            "dl": "artifact-only PyPI downloads; interpretation and measurement-boundary caveats retained",
            "release": "release metadata available at historical feature cutoff",
            "gh": "observed capture is incomplete or unvalidated; all GH predictors excluded on all dates",
            "research": "not reviewed for point-in-time eligibility; excluded",
        },
        "input": str(input_path.resolve()), "input_sha256": _input_fingerprint(input_path),
        "versions": _versions(), "seed": SEED, "n_jobs": n_jobs,
        "protocol": {
            "refit": "monthly; train once at the first eligible forecast cutoff of each month",
            "tuning": "none; original fixed hyperparameters retained, no tuning on previously examined 2026 outcomes",
            "evaluation_status": "post-audit sensitivity rerun of previously examined 2026, not an untouched confirmatory test",
            "training_median_baseline": "constant median target_log_growth from the current fold's available training labels only; MAE-optimal training constant before common physical lower-bound clipping",
            "model_selection": "lowest MAE among 2025-origin validation labels available by 2026-01-01 UTC; exact ties use higher median weekly Spearman then model name; full 2025 metrics are descriptive and later labels do not influence selection",
            "selection_label_availability_cutoff": SELECTION_CUTOFF,
            "training_rule": "label_available_at <= model training cutoff and complete historical coverage",
            "preprocessing": "median imputation plus missing indicators and standard scaling fit on each training fold only",
            "prediction_constraint": "growth is bounded below by -ln(1+past-eight-week median) so downloads cannot be negative",
            "min_train_weeks": min_train_weeks,
        },
        "target_semantics": TARGET_SEMANTICS,
        "feature_groups": groups, "rejected_feature_columns": rejected_features,
        "core_feature_allowlist": sorted(CORE_FEATURE_ALLOWLIST),
        "include_research": include_research,
        "research_policy": "excluded from corrected primary profile; no override",
        "tool_age_note": "control derived from earliest retained visible artifact upload, not certified project birth",
        "model_ids": list(MODEL_IDS),
        "recipes": recipes,
        "estimator_parameters": {
            name: build_pipeline(recipe["kind"], n_jobs).named_steps["model"].get_params()
            for name, recipe in recipes.items()
        },
        "row_audit": {
            "input_rows": len(frame), "input_tools": frame["tool_id"].nunique(),
            "complete_coverage_rows": int(frame["coverage_complete"].sum()),
            "valid_past_eight_download_weeks": int(valid_lags.sum()),
            "mature_covered_rows_at_as_of": int(mature.sum()),
            "evaluation_rows_before_training_minimum": len(evaluation),
            "evaluation_rows_per_model": len(predictions) // (len(BASE_MODELS) + len(recipes)),
        },
    }
    metrics = {
        "schema_version": 2, "generated_at": generated_at, "as_of": as_of,
        "profile": PROFILE, "analysis_type": ANALYSIS_TYPE,
        "previously_examined_2026": True, "analysis_note": ANALYSIS_NOTE,
        "supersedes_snapshot": supersedes_snapshot,
        "excluded_feature_groups": list(EXCLUDED_FEATURE_GROUPS),
        "source_quality_policy": SOURCE_QUALITY_POLICY,
        "quality_gate": {"validation_status": "passed", **quality_binding},
        "target_semantics": TARGET_SEMANTICS, "periods": periods,
        "source_ablations": ablations,
        "results": metric_results, "best_model": best_model,
        "selection": selection,
        "confidence_intervals": bootstrap,
        "uncertainty_note": "Paired eight-week block intervals are computed from actual weekly errors; insufficient series receive no interval.",
        "measurement_boundary_diagnostic": boundary_metrics,
        "latest_forecasts": {"status": forecast_status, "feature_week": latest_week, "rows": len(forecasts)},
    }
    folds = {
        "schema_version": 1, "as_of": as_of, "min_train_weeks": min_train_weeks,
        "test_start": test_start, "test_end_requested": end_requested,
        "test_end_effective": test_end, "folds": fold_records,
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".model-study-", dir=output_dir.parent))
    try:
        predictions.to_parquet(staging / "predictions.parquet", index=False)
        forecasts.to_parquet(staging / "latest_forecasts.parquet", index=False)
        forecasts.to_parquet(staging / "future_predictions.parquet", index=False)
        pd.DataFrame(metric_results).to_csv(staging / "metrics.csv", index=False)
        weekly_metrics.to_csv(staging / "per_week_metrics.csv", index=False)
        tool_errors.to_csv(staging / "per_tool_errors.csv", index=False)
        pd.DataFrame(boundary_metrics["results"]).to_csv(staging / "measurement_boundary_metrics.csv", index=False)
        pd.DataFrame(bootstrap["results"]).to_csv(staging / "block_bootstrap_vs_persistence.csv", index=False)
        write_json(staging / "measurement_boundary_metrics.json", boundary_metrics)
        write_json(staging / "block_bootstrap_vs_persistence.json", bootstrap)
        write_json(staging / "source_ablation_metrics.json", ablations)
        pd.DataFrame(ablations["results"]).to_csv(staging / "source_ablation_metrics.csv", index=False)
        ablation_tools.to_csv(staging / "source_ablation_per_tool.csv", index=False)
        report_source = Path(quality_report) if quality_report else input_path.parent / "data_quality.json"
        policy_source = Path(quality_policy) if quality_policy else input_path.parent / "source_quality.json"
        (staging / "data_quality.json").write_bytes(report_source.read_bytes())
        (staging / "source_quality.json").write_bytes(policy_source.read_bytes())
        write_json(staging / "metrics.json", metrics)
        write_json(staging / "folds.json", folds)
        write_json(staging / "model_config.json", config)
        write_json(staging / "feature_groups.json", groups)
        model_dir = staging / "final_models"
        model_dir.mkdir()
        for name, pipeline in final_models.items():
            if name not in recipes:
                continue
            joblib.dump({
                "model": pipeline, "features": recipes[name]["features"],
                "train_cutoff": as_of.isoformat(), "target_semantics": TARGET_SEMANTICS,
                "train_row_count": len(final_train), "versions": config["versions"],
            }, model_dir / f"{name}.joblib", compress=3)
        write_json(staging / "final_models.json", {
            "train_cutoff": as_of, "train_row_count": len(final_train),
            "train_week_count": final_train["week_start"].nunique(),
            "maximum_training_label_available_at": final_train["label_available_at"].max(),
            "best_model_selected_from_2025": best_model, "models": list(recipes),
            "baselines": {"persistence": 0.0, "training_median": final_models["training_median"],
                          "trend8": "fixed OLS extrapolation of eight historical log-download counts"},
        })
        manifest = {
            "schema_version": 2, "status": "complete", "generated_at": generated_at,
            "profile": PROFILE, "analysis_type": ANALYSIS_TYPE,
            "previously_examined_2026": True, "analysis_note": ANALYSIS_NOTE,
            "supersedes_snapshot": supersedes_snapshot,
            "excluded_feature_groups": list(EXCLUDED_FEATURE_GROUPS),
            "source_quality_policy": SOURCE_QUALITY_POLICY,
            "quality_gate": {"validation_status": "passed", **quality_binding},
            "model_ids": list(MODEL_IDS),
            "prediction_rows": len(predictions), "future_prediction_rows": len(forecasts),
            "best_model": best_model, "as_of": as_of,
            "files": {str(path.relative_to(staging)): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in sorted(staging.rglob("*")) if path.is_file()},
        }
        write_json(staging / "study_manifest.json", manifest)
        if output_dir.exists():
            output_dir.rmdir()
        staging.rename(output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--min-train-weeks", type=int, default=52)
    parser.add_argument("--test-start", default="2025-01-06")
    parser.add_argument("--test-end", default="latest_mature")
    parser.add_argument("--as-of", help="UTC evaluation/label-availability cutoff; defaults to actual current time.")
    parser.add_argument("--boosted-model", choices=["histgb"], default="histgb")
    parser.add_argument("--include-research", action="store_true")
    parser.add_argument("--n-jobs", type=int, default=2)
    parser.add_argument("--quality-report", type=Path, help="Passed report; defaults to data_quality.json beside input.")
    parser.add_argument("--quality-policy", type=Path, help="Policy bound by the report; defaults to source_quality.json beside input.")
    parser.add_argument("--supersedes-snapshot", default="20261102T215227Z-b9b371b3")
    args = parser.parse_args()
    try:
        manifest = run_study(
            args.input, args.output_dir, args.min_train_weeks,
            args.test_start, args.test_end, args.as_of,
            args.boosted_model, args.include_research, args.n_jobs,
            args.quality_report, args.quality_policy, args.supersedes_snapshot,
        )
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}), flush=True)
        return 1
    print(json.dumps(manifest, indent=2, default=_json_default), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
