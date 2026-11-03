import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from contextlib import nullcontext

import numpy as np
import pandas as pd

import model_study as study


HAS_SKLEARN = importlib.util.find_spec("sklearn") is not None
HAS_PARQUET = importlib.util.find_spec("pyarrow") is not None
HAS_JOBLIB = importlib.util.find_spec("joblib") is not None


def row(tool, week, target=0.1):
    start = study.utc_timestamp(week)
    result = {
        "tool_id": tool, "week_start": start,
        "feature_cutoff": start + pd.Timedelta(days=9),
        "label_available_at": start + pd.Timedelta(days=72),
        "target_log_growth": target, "target_sustained": 1,
        "coverage_complete": True, "prediction_eligible": True,
        "gh_event_count": 4.0, "gh_missing": False,
        "release_count": 1.0, "tool_age_weeks": 40.0,
    }
    result.update({f"dl_lag_{lag}": 100.0 for lag in range(8)})
    return result


def fixture_frame(weeks=78, tools=6):
    dates = pd.date_range("2025-01-06", periods=weeks, freq="W-MON", tz="UTC")
    records = []
    for tool in range(tools):
        counts = np.asarray([
            100 + 25 * tool + 2 * week + 15 * np.sin(week / 5 + tool)
            for week in range(weeks)
        ])
        for week, date in enumerate(dates):
            record = row(f"tool_{tool}", date)
            for lag in range(8):
                record[f"dl_lag_{lag}"] = counts[week - lag] if week >= lag else np.nan
            eligible = week >= 7
            mature = eligible and week + 9 < weeks
            baseline = np.median(counts[week-7:week+1]) if eligible else np.nan
            outcomes = counts[week+2:week+10]
            record.update(
                target_log_growth=(np.log1p(np.median(outcomes)) - np.log1p(baseline)) if mature else np.nan,
                target_sustained=int(np.sum(outcomes > 1.1 * baseline) >= 6) if mature else np.nan,
                coverage_complete=mature,
                prediction_eligible=eligible,
                dl_downloads=counts[week],
                dl_log_downloads=np.log1p(counts[week]),
                gh_event_count=float((week + tool) % 9),
                release_count=float(week % (tool + 3) == 0),
                tool_age_weeks=float(week + 20),
                research_current_mentions=float(week * 1000),
                dl_future_total=float(week * 99999),
                baseline_median_downloads=baseline,
            )
            records.append(record)
    return pd.DataFrame(records)


def quality_fixture(root, as_of):
    policy = {
        "source_policy": study.SOURCE_QUALITY_POLICY, "profile": study.PROFILE,
        "enabled_feature_groups": ["dl", "release", "controls"],
        "excluded_feature_groups": ["gh", "research"],
        "sources": {"gharchive": {"primary_eligible": False, "capture_validated": False}},
    }
    policy_path = root / "source_quality.json"
    policy_path.write_text(json.dumps(policy))
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    report = {
        "source_policy": study.SOURCE_QUALITY_POLICY, "profile": study.PROFILE,
        "validation_status": "passed", "as_of": study.utc_timestamp(as_of).isoformat(),
        "excluded_feature_groups": ["gh", "research"], "cohort_sha256": "a" * 64,
        "source_policy_sha256": sha(policy_path),
        "input_hashes": {"weekly_features.parquet": sha(root / "input.parquet"),
                         "source_quality.json": sha(policy_path)},
    }
    (root / "data_quality.json").write_text(json.dumps(report))
    return report, policy


def metric_fixture():
    rows = []
    for week in ["2026-05-04", "2026-07-06", "2026-08-10"]:
        start = study.utc_timestamp(week)
        for model in ["perfect", "persistence"]:
            for tool in range(6):
                rows.append({
                    "tool_id": f"t{tool}", "week_start": start,
                    "target_window_start": start + pd.Timedelta(days=14),
                    "target_window_end_exclusive": start + pd.Timedelta(days=70),
                    "split": "holdout_2026", "model": model,
                    "target_log_growth": tool / 10,
                    "prediction_log_growth": tool / 10 if model == "perfect" else 0.0,
                    "target_sustained": int(tool >= 3),
                })
    return pd.DataFrame(rows).sample(frac=1, random_state=6893)


class DataAndLeakageTests(unittest.TestCase):
    def test_label_cutoff_excludes_not_yet_available_outcomes(self):
        frame = study.normalize_frame(pd.DataFrame([
            row("available", "2023-01-02"),
            row("not_yet_available", "2023-01-09", target=99999),
        ], index=[77, 8]))
        train = study.select_train_rows(frame, "2023-03-20", "2023-03-20")
        self.assertEqual(train["tool_id"].tolist(), ["available"])
        self.assertLessEqual(train["label_available_at"].max(), study.utc_timestamp("2023-03-20"))

    def test_frozen_gap_and_label_maturity_are_enforced(self):
        frame = study.normalize_frame(pd.DataFrame([row("a", "2023-01-02")]))
        self.assertEqual(frame.iloc[0]["target_window_start"], study.utc_timestamp("2023-01-16"))
        self.assertEqual(frame.iloc[0]["target_window_end_exclusive"], study.utc_timestamp("2023-03-13"))
        bad = pd.DataFrame([row("a", "2023-01-02")])
        bad["label_available_at"] = study.utc_timestamp("2023-03-14")
        with self.assertRaisesRegex(ValueError, "label is marked available"):
            study.normalize_frame(bad)

    def test_research_and_future_columns_are_not_predictors(self):
        frame = pd.DataFrame([row("a", "2023-01-02")])
        frame["research_current_mentions"] = 1000.0
        frame["dl_future_total"] = 1000.0
        frame["dl_unreviewed_current_snapshot"] = 1000.0
        frame["baseline_median_downloads"] = 100.0
        frame["future_median_downloads"] = 1000.0
        groups, rejected = study.select_feature_groups(frame)
        predictors = sum(groups.values(), [])
        self.assertEqual(groups["research"], [])
        self.assertEqual(groups["gh"], [])
        self.assertIn("gh_event_count", rejected)
        self.assertIn("research_current_mentions", rejected)
        for forbidden in ["dl_future_total", "dl_unreviewed_current_snapshot", "tool_id", "target_log_growth", "baseline_median_downloads", "future_median_downloads"]:
            self.assertNotIn(forbidden, predictors)
        with self.assertRaisesRegex(ValueError, "Research is excluded"):
            study.select_feature_groups(frame, include_research=True)

    def test_primary_allowlist_is_independent_of_prefix_and_needs_no_gh(self):
        frame = pd.DataFrame([row("a", "2023-01-02")]).drop(columns=["gh_event_count", "gh_missing"])
        frame["observed_gh_event_count"] = 1e12
        frame["feature_gh_push_events"] = 1e12
        frame["feature_dl_magic_signal"] = 1e12
        frame["research_past_mentions"] = 1e12
        groups, rejected = study.select_feature_groups(frame)
        self.assertEqual(groups["gh"], [])
        self.assertEqual(groups["research"], [])
        self.assertIn("observed_gh_event_count", rejected)
        self.assertIn("feature_gh_push_events", rejected)
        self.assertIn("feature_dl_magic_signal", rejected)
        recipes = study.model_recipes(groups)
        self.assertEqual(set(recipes), set(study.MODEL_IDS) - set(study.BASE_MODELS))
        groups["dl"].append("gh_event_count")
        with self.assertRaisesRegex(ValueError, "exact primary allowlist"):
            study.model_recipes(groups)

    def test_quality_gate_requires_pass_policy_hash_and_same_feature_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # The gate hashes bytes; no Parquet engine is needed for this unit test.
            (root / "input.parquet").write_bytes(b"audited feature fixture")
            with self.assertRaisesRegex(ValueError, "passed data_quality"):
                study.validate_quality_gate(root / "input.parquet", "2026-11-02")
            report, _ = quality_fixture(root, "2026-11-02")
            study.validate_quality_gate(root / "input.parquet", "2026-11-02")
            with self.assertRaisesRegex(ValueError, "as-of cutoffs differ"):
                study.validate_quality_gate(root / "input.parquet", "2026-11-03")
            report["validation_status"] = "failed"
            (root / "data_quality.json").write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, "has not passed"):
                study.validate_quality_gate(root / "input.parquet", "2026-11-02")
            report["validation_status"] = "passed"
            (root / "data_quality.json").write_text(json.dumps(report))
            (root / "input.parquet").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "feature bytes"):
                study.validate_quality_gate(root / "input.parquet", "2026-11-02")
            quality_fixture(root, "2026-11-02")
            (root / "source_quality.json").write_text((root / "source_quality.json").read_text() + " ")
            with self.assertRaisesRegex(ValueError, "policy bytes"):
                study.validate_quality_gate(root / "input.parquet", "2026-11-02")

    def test_trend_baseline_uses_only_past_counts_and_skips_lag_week(self):
        constant = pd.DataFrame([row("a", "2023-01-02")])
        self.assertAlmostEqual(study.trend8_prediction(constant)[0], 0.0)
        intercept, slope = 5.0, 0.1
        past = np.expm1(intercept + slope * np.arange(-7, 1))
        future = np.expm1(intercept + slope * np.arange(2, 10))
        frame = constant.copy()
        for lag in range(8):
            frame[f"dl_lag_{lag}"] = past[-1-lag]
        expected = np.log1p(np.median(future)) - np.log1p(np.median(past))
        self.assertAlmostEqual(study.trend8_prediction(frame)[0], expected, places=10)

    def test_mae_constant_baseline_is_training_median_not_mean_or_test_statistic(self):
        train = pd.DataFrame({"target_log_growth": [0.0, 0.2, 30.0]})
        with patch.object(study, "limited_threads", side_effect=lambda _: nullcontext()):
            fitted = study._fit_recipes(train, {}, 1)
        self.assertEqual(fitted["training_median"], 0.2)
        rows = pd.DataFrame([row("a", "2023-01-02", target=99999.0)])
        with patch.object(study, "limited_threads", side_effect=lambda _: nullcontext()):
            predictions = study._predict_all(rows, {}, fitted, 1)
        self.assertEqual(predictions["training_median"][0], 0.2)

    @unittest.skipUnless(HAS_SKLEARN, "scikit-learn is required for preprocessing checks")
    def test_preprocessing_never_learns_future_values(self):
        train = pd.DataFrame({"dl_count": [1.0, np.nan, 3.0], "gh_missing": [0.0, 1.0, 0.0]})
        pipeline = study.build_pipeline("ridge")
        pipeline.fit(train, np.asarray([0.0, 0.1, 0.2]))
        medians = pipeline.named_steps["imputer"].statistics_.copy()
        means = pipeline.named_steps["scaler"].mean_.copy()
        pipeline.predict(pd.DataFrame({"dl_count": [1e12, np.nan], "gh_missing": [0.0, 1.0]}))
        np.testing.assert_array_equal(pipeline.named_steps["imputer"].statistics_, medians)
        np.testing.assert_array_equal(pipeline.named_steps["scaler"].mean_, means)
        self.assertEqual(medians[0], 2.0)


class EvaluationTests(unittest.TestCase):
    def test_metrics_align_labels_and_use_sustained_hits(self):
        predictions = metric_fixture()
        results, weeks, _ = study.calculate_metrics(predictions)
        perfect = next(item for item in results if item["model"] == "perfect")
        persistence = next(item for item in results if item["model"] == "persistence")
        self.assertEqual(perfect["mae"], 0.0)
        self.assertAlmostEqual(perfect["median_weekly_spearman"], 1.0)
        self.assertAlmostEqual(perfect["precision_at_5"], 0.6)
        self.assertAlmostEqual(perfect["mean_weekly_sustained_prevalence"], 0.5)
        self.assertIsNone(persistence["median_weekly_spearman"])
        self.assertEqual(persistence["n_spearman_weeks"], 0)
        with self.assertRaisesRegex(ValueError, "identical evaluation rows"):
            study.calculate_metrics(predictions.iloc[1:])

    def test_future_or_unknown_targets_cannot_be_scored(self):
        frame = metric_fixture()
        frame.loc[frame.index[0], "target_log_growth"] = np.nan
        with self.assertRaisesRegex(ValueError, "known labels"):
            study.calculate_metrics(frame)
        frame = metric_fixture()
        frame.loc[frame.index[0], "prediction_log_growth"] = np.inf
        with self.assertRaisesRegex(ValueError, "finite predictions"):
            study.calculate_metrics(frame)

    def test_boundary_diagnostics_follow_outcome_dates(self):
        result = study.measurement_boundary_metrics(metric_fixture())
        self.assertEqual(result["empty_segments"], [])
        self.assertEqual({item["outcome_period"] for item in result["results"]}, {
            "outcome_before_logging_change", "outcome_spans_logging_change", "outcome_after_logging_change"
        })

    def test_selection_excludes_late_2025_origins_with_2026_labels(self):
        rows = []
        for date, good_model in [("2025-06-02", "early_winner"), ("2025-12-01", "late_winner")]:
            for model in ["early_winner", "late_winner"]:
                for tool in range(6):
                    record = row(f"t{tool}", date, target=float(tool))
                    record.update(model=model, split="validation_2025", prediction_log_growth=float(tool) if model == good_model else 1000.0)
                    rows.append(record)
        selection = study.select_from_frozen_validation(pd.DataFrame(rows))
        self.assertEqual(selection["best_model"], "early_winner")
        self.assertEqual(selection["n_rows_per_model"], 6)
        self.assertEqual(selection["n_weeks"], 1)
        self.assertLessEqual(selection["maximum_label_available_at"], study.SELECTION_CUTOFF)

    def test_block_bootstrap_is_paired_and_requires_contiguous_history(self):
        rows = []
        for date in pd.date_range("2026-01-05", periods=16, freq="W-MON", tz="UTC"):
            rows.extend([
                {"split": "holdout_2026", "model": "persistence", "week_start": date, "mae": 0.2},
                {"split": "holdout_2026", "model": "better", "week_start": date, "mae": 0.1},
            ])
        result = study.paired_block_bootstrap(pd.DataFrame(rows))
        estimate = result["results"][0]
        self.assertEqual(estimate["status"], "computed")
        self.assertAlmostEqual(estimate["ci_low"], -0.1)
        self.assertAlmostEqual(estimate["ci_high"], -0.1)
        gapped = pd.DataFrame(rows)
        gapped["week_start"] = gapped["week_start"].map(lambda date: study.utc_timestamp("2026-01-05") + 2 * (date - study.utc_timestamp("2026-01-05")))
        unavailable = study.paired_block_bootstrap(gapped)["results"][0]
        self.assertEqual(unavailable["status"], "insufficient_contiguous_weeks")
        self.assertIsNone(unavailable["ci_low"])

    def test_source_ablation_is_paired_within_each_algorithm(self):
        rows = []
        for date in pd.date_range("2026-01-05", periods=16, freq="W-MON", tz="UTC"):
            for model in study.MODEL_IDS:
                for tool in range(6):
                    error = {"ridge_dl": .2, "ridge_dl_release": .1,
                             "histgb_dl": .3, "histgb_dl_release": .4}.get(model, .5)
                    rows.append({"tool_id": f"t{tool}", "week_start": date,
                                 "split": "holdout_2026", "model": model,
                                 "target_log_growth": .1 * tool, "target_sustained": int(tool > 2),
                                 "prediction_log_growth": .1 * tool + error})
        metrics, weekly, tools = study.calculate_metrics(pd.DataFrame(rows))
        ablations, per_tool = study.source_ablation_metrics(metrics, weekly, tools)
        ridge = next(record for record in ablations["results"] if record["algorithm"] == "ridge")
        histgb = next(record for record in ablations["results"] if record["algorithm"] == "histgb")
        self.assertAlmostEqual(ridge["mae_difference"], -.1)
        self.assertAlmostEqual(ridge["ci_low"], -.1)
        self.assertAlmostEqual(ridge["ci_high"], -.1)
        self.assertAlmostEqual(histgb["mae_difference"], .1)
        self.assertEqual(len(per_tool), 12)
        self.assertEqual(set(per_tool["algorithm"]), {"ridge", "histgb"})

    @unittest.skipUnless(HAS_SKLEARN and HAS_PARQUET and HAS_JOBLIB, "model integration needs sklearn, pyarrow, and joblib")
    def test_full_pipeline_preserves_folds_and_unobserved_forecasts(self):
        with tempfile.TemporaryDirectory(prefix="6893-model-fixture-") as temporary:
            root = Path(temporary)
            frame = fixture_frame()
            frame.to_parquet(root / "input.parquet", index=False)
            as_of = study.utc_timestamp(frame["feature_cutoff"].max())
            quality_fixture(root, as_of)
            manifest = study.run_study(
                root / "input.parquet", root / "results", min_train_weeks=8,
                test_start="2025-07-07", as_of=as_of, n_jobs=1,
            )
            self.assertEqual(manifest["status"], "complete")
            self.assertEqual(manifest["profile"], "validated_primary")
            self.assertEqual(manifest["analysis_type"], "post_audit_corrective_sensitivity")
            self.assertTrue(manifest["previously_examined_2026"])
            self.assertEqual(set(manifest["model_ids"]), set(study.MODEL_IDS))
            folds = json.loads((root / "results/folds.json").read_text())
            for fold in folds["folds"]:
                if fold["status"] == "evaluated":
                    self.assertLessEqual(study.utc_timestamp(fold["maximum_training_label_available_at"]), study.utc_timestamp(fold["train_cutoff"]))
            metrics = json.loads((root / "results/metrics.json").read_text())
            self.assertEqual(metrics["selection"]["split"], "validation_2025")
            validation = metrics["selection"]["results"]
            self.assertAlmostEqual(metrics["selection"]["value"], min(item["mae"] for item in validation))
            self.assertLessEqual(study.utc_timestamp(metrics["selection"]["maximum_label_available_at"]), study.SELECTION_CUTOFF)
            config = json.loads((root / "results/model_config.json").read_text())
            self.assertFalse(config["include_research"])
            self.assertEqual(config["feature_groups"]["research"], [])
            self.assertEqual(config["feature_groups"]["gh"], [])
            self.assertEqual(set(config["recipes"]), {"ridge_dl", "ridge_dl_release", "histgb_dl", "histgb_dl_release"})
            for recipe in config["recipes"].values():
                self.assertFalse(any("gh_" in column or "research_" in column for column in recipe["features"]))
            ablations = json.loads((root / "results/source_ablation_metrics.json").read_text())
            self.assertEqual({record["algorithm"] for record in ablations["results"]}, {"ridge", "histgb"})
            predictions = pd.read_parquet(root / "results/predictions.parquet")
            study.assert_same_evaluation_rows(predictions)
            self.assertEqual(set(predictions["model"]), set(study.MODEL_IDS))
            # Each month uses only the available fold target median, never evaluation labels.
            normalized = study.normalize_frame(frame)
            for cutoff, group in predictions.loc[predictions["model"] == "training_median"].groupby("train_cutoff"):
                first_week = group["week_start"].min()
                train = study.select_train_rows(normalized, cutoff, first_week)
                baseline = group["baseline_median_downloads"].to_numpy(dtype=float)
                expected = np.maximum(float(train["target_log_growth"].median()), -np.log1p(baseline))
                np.testing.assert_allclose(group["prediction_log_growth"], expected)
            forecasts = pd.read_parquet(root / "results/latest_forecasts.parquet")
            self.assertGreater(len(forecasts), 0)
            self.assertTrue(forecasts["target_log_growth"].isna().all())
            self.assertTrue(forecasts["target_sustained"].isna().all())
            self.assertTrue((forecasts["forecast_type"] == "future_unobserved").all())
            self.assertTrue((forecasts["train_cutoff"] == as_of).all())
            self.assertTrue((forecasts["target_window_start"] > forecasts["feature_cutoff"]).all())
            final = json.loads((root / "results/final_models.json").read_text())
            self.assertLessEqual(study.utc_timestamp(final["maximum_training_label_available_at"]), as_of)


if __name__ == "__main__":
    unittest.main()
