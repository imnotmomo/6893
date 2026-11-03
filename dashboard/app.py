"""Read-only views of an atomically published AI Technology Radar study."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
except ImportError:
    go = None


INK = "#183438"
TEAL = "#187c70"
ORANGE = "#ce623c"
GOLD = "#b58a37"
PAPER = "#f5f3eb"
RULE = "#dcded4"
COLORS = [TEAL, INK, ORANGE, GOLD, "#6e84a0", "#80766d"]
PRIMARY_MODELS = {"persistence", "training_median", "trend8", "ridge_dl", "ridge_dl_release", "histgb_dl", "histgb_dl_release"}
MODEL_NAMES = {
    "persistence": "No-change baseline",
    "training_median": "Past-training median baseline",
    "trend8": "Eight-week trend",
    "ridge_dl": "Ridge · downloads",
    "ridge_dl_release": "Ridge · downloads + releases",
    "histgb_dl": "Boosted trees · downloads",
    "histgb_dl_release": "Boosted trees · downloads + releases",
    "ridge_gh": "Ridge · GitHub",
    "ridge_dl_gh": "Ridge · downloads + GitHub",
    "ridge_dl_gh_release": "Ridge · downloads + GitHub + releases",
    "histgb_dl_gh_release": "Boosted trees · combined sources",
    "xgboost_dl_gh_release": "XGBoost · combined sources",
}
SIGNALS = {
    "downloads": ("Package downloads", TEAL),
    "gh_event_count": ("GitHub events", INK),
    "gh_push_events": ("Push events", "#507f95"),
    "gh_issue_events": ("Issue events", GOLD),
    "gh_pr_events": ("Pull request events", ORANGE),
    "gh_active_actors": ("Active GitHub actors", "#677a54"),
    "release_count": ("Package releases", ORANGE),
    "research_candidate_count": ("Candidate research matches", GOLD),
    "research_eligible_count": ("Date-eligible research matches", "#677a54"),
}
ARCHIVE_SIGNALS = {
    "observed_gh_event_count": ("Archived observed events", INK),
    "observed_gh_push_events": ("Archived observed push events", "#507f95"),
    "observed_gh_issue_events": ("Archived observed issue events", GOLD),
    "observed_gh_pr_events": ("Archived observed pull-request events", ORANGE),
    "observed_gh_watch_events": ("Archived observed watch events", "#6e84a0"),
    "observed_gh_active_actors": ("Archived observed actors", "#677a54"),
}
FILES = ("weekly_features.parquet", "predictions.parquet", "latest_forecasts.parquet",
         "forecasts.parquet", "metrics.json", "study_status.json", "data_quality.json", "source_ablation_metrics.json")
SUPPLEMENTARY_SOURCES = {
    "git": ("github_git_history/latest", "manifest.json", "weekly_git_history.csv"),
    "stars": ("github_direct_stars/backfill", "collection_manifest.json", "github_direct_stars_weekly.csv"),
}


def primary_ready(status, metrics, quality):
    """Old retrieval-only snapshots must never inherit a source-validity claim."""
    return (status.get("primary_analysis_ready") is True
            and status.get("publication_status") == "validated_primary"
            and status.get("spark_validation", {}).get("ok") is True
            and status.get("spark_validation", {}).get("mismatch_rows") == 0
            and quality.get("validation_status") == "passed"
            and quality.get("profile") == "validated_primary"
            and metrics.get("profile") == "validated_primary"
            and metrics.get("best_model") in PRIMARY_MODELS
            and metrics.get("analysis_type") == "post_audit_corrective_sensitivity"
            and {"gh", "research"}.issubset(set(metrics.get("excluded_feature_groups", [])))
            and quality.get("sources", {}).get("gharchive", {}).get("primary_eligible") is False
            and all(quality.get("sources", {}).get(source, {}).get("status") == "primary_enabled"
                    for source in ("pypi", "pypi_releases")))


def model_name(value):
    return MODEL_NAMES.get(str(value), str(value).replace("_", " "))


def date_label(value):
    try:
        return pd.Timestamp(value).strftime("%b %d, %Y") if pd.notna(value) else "Not reported"
    except (ValueError, TypeError):
        return str(value)


def number(value, decimals=0):
    try:
        return f"{float(value):,.{decimals}f}" if pd.notna(value) else "Unknown"
    except (ValueError, TypeError):
        return "Unknown"


def _first(mapping, keys, default=None):
    for key in keys:
        if mapping.get(key) is not None:
            return mapping[key]
    return default


def _signature(path):
    try:
        stat = path.stat()
        return stat.st_mtime_ns, stat.st_size, stat.st_ino
    except FileNotFoundError:
        return None


@st.cache_data(show_spinner=False, max_entries=12)
def _read_artifact(path, signature):
    path = Path(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("Status file is larger than the dashboard read limit")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def load_snapshot(path):
    root = Path(path).expanduser().resolve()
    signatures = {name: _signature(root / name) for name in FILES}
    artifacts, errors = {}, {}
    for name, signature in signatures.items():
        if signature is None:
            continue
        try:
            artifacts[name] = _read_artifact(str(root / name), signature)
        except Exception as error:
            errors[name] = f"{type(error).__name__}: {error}"
    if any(_signature(root / name) != signature for name, signature in signatures.items()):
        return root, {}, {"snapshot": "The snapshot changed while loading. Refresh after publication finishes."}
    return root, artifacts, errors


def _read_supplementary(root, kind, signatures):
    """Read the exact CSV bytes whose hash the separate collector records."""
    root=Path(root).resolve(strict=True)
    _,manifest_name,csv_name=SUPPLEMENTARY_SOURCES[kind]
    manifest_path=(root/manifest_name).resolve(strict=True)
    csv_path=(root/csv_name).resolve(strict=True)
    if not manifest_path.is_relative_to(root) or not csv_path.is_relative_to(root):
        raise ValueError("A supplementary artifact points outside its collection directory")
    if manifest_path.stat().st_size>8*1024**2 or csv_path.stat().st_size>32*1024**2:
        raise ValueError("Supplementary collection exceeds the dashboard read limit")
    manifest=json.loads(manifest_path.read_bytes())
    if not isinstance(manifest,dict):raise ValueError("Invalid supplementary manifest")
    raw=csv_path.read_bytes()
    if kind=="git":
        if manifest.get("source")!="github_git_default_branch" or manifest.get("status")!="complete":
            raise ValueError("The Git collection is not a completed independent source run")
        expected=manifest.get("files",{}).get(csv_name)
        completed_ids={row["tool_id"] for row in manifest.get("tools",[]) if row.get("status")=="complete"}
        requested=int(manifest.get("requested_tools",0));completed=int(manifest.get("completed_tools",0))
        if requested!=completed or len(manifest.get("tools",[]))!=requested:
            raise ValueError("The Git collection has incomplete tool coverage")
    else:
        if manifest.get("source")!="github_direct_stars" or manifest.get("status") not in {"passed","partial"}:
            raise ValueError("The direct-star collection has no usable collection status")
        expected=manifest.get("weekly_csv_sha256")
        completed_ids={tid for tid,row in manifest.get("tools",{}).items() if row.get("status")=="passed"}
        requested=len(manifest.get("tool_ids",[]));completed=int(manifest.get("tools_passed",0))
        if not completed_ids<=set(manifest.get("tool_ids",[])):
            raise ValueError("The direct-star manifest contains an unexpected tool")
    if not isinstance(expected,str) or not re.fullmatch(r"[0-9a-f]{64}",expected) or hashlib.sha256(raw).hexdigest()!=expected:
        raise ValueError("The supplementary CSV does not match its manifest hash")
    if requested<1 or completed<0 or completed>requested or len(completed_ids)!=completed:
        raise ValueError("Supplementary completion counts are inconsistent")
    frame=pd.read_csv(io.BytesIO(raw))
    if len(frame)>100000:raise ValueError("Supplementary table exceeds the dashboard row limit")
    required={"tool_id","primary_eligible"}
    required|={"week_start_utc","git_reachable_commit_count","git_first_parent_commit_count"} if kind=="git" else {"api_week_start_epoch","stars","historical_vintage"}
    if not required<=set(frame):raise ValueError("Supplementary CSV is missing required fields")
    if frame.tool_id.isna().any() or set(frame.tool_id.astype(str))!=completed_ids:
        raise ValueError("Supplementary CSV tool coverage differs from its manifest")
    if not frame.primary_eligible.astype(str).str.lower().isin(["false","0"]).all():
        raise ValueError("Supplementary data must remain excluded from primary models")
    if kind=="stars" and not frame.historical_vintage.astype(str).str.lower().isin(["false","0"]).all():
        raise ValueError("Direct stars cannot certify historical public vintages")
    date_column="week_start_utc" if kind=="git" else "api_week_start_epoch"
    if frame.duplicated(["tool_id",date_column]).any():raise ValueError("Duplicate supplementary tool/week keys")
    frame["week_start"]=pd.to_datetime(frame[date_column],utc=True,errors="raise",**({"unit":"s"} if kind=="stars" else {})).dt.tz_localize(None)
    if frame.week_start.isna().any():raise ValueError("Missing supplementary week anchor")
    count_columns=["git_reachable_commit_count","git_first_parent_commit_count"] if kind=="git" else ["stars"]
    for name in count_columns:
        values=pd.to_numeric(frame[name],errors="raise")
        valid=values.dropna()
        if ((valid<0)|(valid%1!=0)).any():raise ValueError("Invalid supplementary count")
        frame[name]=values
    if any(_signature(root/name)!=signature for name,signature in signatures):
        raise ValueError("The supplementary collection changed while loading; refresh after it finishes")
    return {"state":"ready","manifest":manifest,"frame":frame,"requested":requested,
            "completed":completed,"partial":completed<requested or manifest.get("status")=="partial"}


@st.cache_data(show_spinner=False,max_entries=12)
def _cached_supplementary(root,kind,signatures):
    return _read_supplementary(root,kind,signatures)


def load_supplementary(base,kind):
    """Missing collections are optional and never become primary page errors."""
    try:
        base=Path(base).expanduser().resolve(strict=True)
        relative,manifest_name,csv_name=SUPPLEMENTARY_SOURCES[kind]
        root=(base/relative).resolve(strict=True)
        if not root.is_relative_to(base):raise ValueError("Collection points outside the configured source directory")
        signatures=tuple((name,_signature(root/name)) for name in (manifest_name,csv_name))
        if any(signature is None for _,signature in signatures):return {"state":"absent"}
        return _cached_supplementary(str(root),kind,signatures)
    except FileNotFoundError:return {"state":"absent"}
    except Exception as error:return {"state":"unverified","message":str(error)}


def _aliases(frame, aliases):
    frame = frame.copy()
    for canonical, candidates in aliases.items():
        if canonical not in frame:
            candidate = next((name for name in candidates if name in frame), None)
            if candidate:
                frame[canonical] = frame[candidate]
    for name in ("week_start", "feature_cutoff", "train_cutoff", "target_window_start",
                 "target_window_end_exclusive", "label_available_at", "generated_at"):
        if name in frame:
            frame[name] = pd.to_datetime(frame[name], errors="coerce", utc=True).dt.tz_localize(None)
    if "tool_id" in frame:
        frame["tool_id"] = frame["tool_id"].astype("string")
    return frame


def features_frame(frame):
    return _aliases(frame, {
        "tool_id": ["tool", "package"], "week_start": ["week", "week_start_utc"],
        "downloads": ["dl_downloads", "distribution_downloads"],
        "gh_event_count": ["gh_events", "github_events"],
        "gh_active_actors": ["github_active_actors"],
        "research_eligible_count": ["eligible_count"],
    })


def predictions_frame(frame):
    return _aliases(frame, {
        "tool_id": ["tool", "package"], "week_start": ["as_of_week", "forecast_origin", "origin_week"],
        "model": ["model_name"], "prediction": ["prediction_log_growth", "y_pred"],
        "actual": ["target_log_growth", "y_true"],
    })


def _usable(frame, columns):
    return not frame.empty and all(name in frame for name in columns)


def _numeric(frame, columns):
    frame = frame.copy()
    for name in columns:
        if name in frame:
            frame[name] = pd.to_numeric(frame[name], errors="coerce").replace([float("inf"), -float("inf")], float("nan"))
    return frame


def holdout_rows(frame):
    if frame.empty:
        return frame
    if "split" in frame:
        split = frame["split"].astype(str).str.lower()
        mask = split.str.contains("holdout|test|backtest", regex=True) & ~split.str.contains("train", regex=False)
        if mask.any():
            return frame[mask].copy()
        validation = split.str.contains("validation", regex=False)
        return frame[validation].copy()
    if "actual" in frame:
        return frame[pd.to_numeric(frame["actual"], errors="coerce").notna()].copy()
    return pd.DataFrame()


def forecast_rows(artifacts, predictions):
    for name in ("latest_forecasts.parquet", "forecasts.parquet"):
        if name in artifacts and not artifacts[name].empty:
            frame = predictions_frame(artifacts[name])
            if "actual" in frame and pd.to_numeric(frame["actual"], errors="coerce").notna().any():
                continue
            return frame
    if "split" in predictions:
        mask = predictions["split"].astype(str).str.lower().isin(["forecast", "future", "future_unobserved"])
        candidate = predictions[mask].copy()
        if not candidate.empty and ("actual" not in candidate or candidate["actual"].isna().all()):
            return candidate
    return pd.DataFrame()


def metric_rows(metrics):
    rows = _first(metrics, ["results", "models", "metrics"], [])
    if isinstance(rows, list):
        frame = pd.DataFrame(rows)
    elif isinstance(rows, dict):
        frame = pd.DataFrame([{"model": model, **value} for model, value in rows.items() if isinstance(value, dict)])
    else:
        frame = pd.DataFrame()
    return _numeric(frame, ["mae", "median_weekly_spearman", "spearman", "precision_at_5",
                            "precision_at_k", "n_predictions", "n_weeks", "n_tools"])


def _published_best(metrics):
    best = metrics.get("best_model")
    if isinstance(best, dict):
        best = _first(best, ["model", "name", "model_id", "selected_model"])
    return best


def best_model(metrics, options):
    best = _published_best(metrics)
    return best if best in options else options[0]


def _download(frame, label, key, filename):
    st.download_button(label, frame.to_csv(index=False).encode("utf-8"), filename,
                       "text/csv", key=key, width="content")


def _table(frame, names=None):
    display = frame.rename(columns=names or {})
    st.dataframe(display, hide_index=True, width="stretch",
                 height=min(560, max(110, len(frame) * 35 + 40)))


def _layout(figure, height=330, percent=False):
    figure.update_layout(height=height, margin=dict(l=12, r=16, t=16, b=12),
                         paper_bgcolor=PAPER, plot_bgcolor=PAPER,
                         font=dict(family="Optima, Candara, sans-serif", color=INK, size=13),
                         legend=dict(orientation="h", y=1.14, x=0), hovermode="x unified")
    figure.update_xaxes(showgrid=False, linecolor=RULE)
    figure.update_yaxes(gridcolor=RULE, zerolinecolor=RULE, ticksuffix="%" if percent else "")
    return figure


def _line_chart(frame, columns, labels=None, colors=None, normalized=False, x_label=None):
    labels, colors = labels or {}, colors or {}
    data = _numeric(frame, columns).sort_values("week_start")
    if normalized:
        for column in columns:
            valid = data.loc[data[column].notna() & data[column].gt(0), column]
            data[column] = data[column] / valid.iloc[0] * 100 if len(valid) else float("nan")
    if go is None:
        fallback=data.set_index("week_start")[columns].rename(columns=labels)
        if x_label:fallback.index.name=x_label
        st.line_chart(fallback, width="stretch")
        return
    figure = go.Figure()
    for index, column in enumerate(columns):
        figure.add_trace(go.Scatter(x=data["week_start"], y=data[column], name=labels.get(column, column),
                                   mode="lines", connectgaps=False,
                                   line=dict(color=colors.get(column, COLORS[index % len(COLORS)]), width=2.6)))
    figure.update_yaxes(title_text="Index · first positive observation = 100" if normalized else None)
    if x_label:figure.update_xaxes(title_text=x_label)
    st.plotly_chart(_layout(figure), width="stretch", config={"displayModeBar": False})


def _bar_chart(frame, value, label, category="tool_id", secondary=None):
    if go is None:
        columns = [value] + ([secondary] if secondary else [])
        st.bar_chart(frame.set_index(category)[columns], width="stretch")
        return
    figure = go.Figure()
    figure.add_trace(go.Bar(x=frame[category], y=frame[value], name=label, marker_color=TEAL))
    if secondary:
        figure.add_trace(go.Bar(x=frame[category], y=frame[secondary], name="Observed outcome", marker_color=INK))
    figure.update_layout(barmode="group")
    figure.update_yaxes(title_text=label)
    st.plotly_chart(_layout(figure, height=340), width="stretch", config={"displayModeBar": False})


def _model_select(frame, metrics, key):
    options = sorted(frame["model"].dropna().astype(str).unique())
    if not options:
        return None
    selected = best_model(metrics, options)
    return st.selectbox("Model", options, index=options.index(selected), format_func=model_name, key=key)


def _target_caption(metrics):
    semantics = metrics.get("target_semantics")
    if isinstance(semantics, str):
        st.caption(semantics)
    elif isinstance(semantics, dict):
        description = _first(semantics, ["description", "definition", "formula", "target"])
        if description:
            st.caption(str(description))
    st.caption("Log-growth target: ln(1 + future median downloads) − ln(1 + past eight-week median downloads). "
               "Download counts measure requests, not unique users.")
    st.caption("The target covers eight complete weeks after one skipped week following the feature week. "
               "Use the published feature cutoff and target-window dates when interpreting a forecast.")


def _rank_values(frame):
    frame = _numeric(frame, ["prediction", "actual", "baseline_median_downloads", "predicted_median_downloads", "observed_future_median_downloads"])
    frame["display_prediction"] = frame["prediction"]
    if "actual" in frame:
        frame["display_actual"] = frame["actual"]
    label = "Predicted eight-week log growth"
    if {"baseline_median_downloads", "predicted_median_downloads"}.issubset(frame.columns):
        baseline = frame["baseline_median_downloads"]
        if baseline.gt(0).all() and frame["predicted_median_downloads"].notna().all():
            frame["display_prediction"] = (frame["predicted_median_downloads"] / baseline - 1) * 100
            if "observed_future_median_downloads" in frame:
                frame["display_actual"] = (frame["observed_future_median_downloads"] / baseline - 1) * 100
            else:
                frame.drop(columns=["display_actual"], errors="ignore", inplace=True)
            label = "Change in median downloads (%)"
    return frame, label


def radar_page(features, predictions, forecasts, metrics):
    st.subheader("The radar")
    st.write("Compare the tools with the strongest published growth predictions, then inspect the evidence behind them.")
    backtests = holdout_rows(predictions)
    started = "outcome_period_has_started" in forecasts and forecasts["outcome_period_has_started"].fillna(False).astype(bool).any()
    future_label = "Current projection" if started else "Future forecast"
    modes = ([future_label] if not forecasts.empty else []) + (["Historical backtest"] if not backtests.empty else [])
    if not modes:
        st.info("Predictions have not been published yet. Feature coverage is available under Tool details and Data evidence.")
        return
    mode = st.radio("Prediction view", modes, horizontal=True, label_visibility="collapsed")
    future = mode == future_label
    frame = forecasts if future else backtests
    if not _usable(frame, ["tool_id", "week_start", "model", "prediction"]):
        st.warning("The published prediction file is missing a tool, origin week, model, or prediction column.")
        return
    if future:
        st.info("Full eight-week outcomes are not yet observed. These projections are separate from historical evaluation.")
    else:
        st.warning("Historical backtest · the outcomes shown have already occurred. This is not a live forecast.")
    controls = st.columns([2, 2, 1])
    with controls[0]:
        model = _model_select(frame, metrics, "radar_model")
    frame = frame[frame["model"].eq(model)].dropna(subset=["week_start"])
    if frame.empty:
        st.info("This model has no valid published prediction origins.")
        return
    origins = sorted(frame["week_start"].unique(), reverse=True)
    with controls[1]:
        origin = st.selectbox("Feature week", origins, format_func=date_label, key="radar_origin_" + str(future))
    current = frame[frame["week_start"].eq(origin)].copy()
    with controls[2]:
        top_n = st.selectbox("Show", [5, 10, 20, "All"], index=1, key="radar_topn")
    current = _numeric(current, ["prediction"]).dropna(subset=["prediction"]).sort_values(
        ["prediction", "tool_id"], ascending=[False, True], kind="mergesort"
    )
    if top_n != "All":
        current = current.head(int(top_n))
    if current.empty:
        st.info("No finite predictions are available for this origin.")
        return
    current, value_label = _rank_values(current)
    if future:
        if "generated_at" in current and current["generated_at"].notna().any():
            issued = pd.to_datetime(current["generated_at"], utc=True).max()
            st.caption("Generated " + issued.strftime("%b %d, %Y at %H:%M UTC") + ". The feature week is the data window, not the issuance date.")
        if "outcome_period_has_started" in current and current["outcome_period_has_started"].fillna(False).astype(bool).any():
            st.warning("The target period had already begun when this projection was generated. It was not issued on the earlier feature-week date.")
    stats = st.columns(3)
    stats[0].metric("Tools in this view", str(current["tool_id"].nunique()))
    stats[1].metric("Feature week", pd.Timestamp(origin).strftime("%b %d, %Y"))
    horizon = current["forecast_horizon_weeks"].dropna().iloc[0] if "forecast_horizon_weeks" in current and current["forecast_horizon_weeks"].notna().any() else None
    stats[2].metric("Forecast horizon", f"{number(horizon)} weeks" if horizon is not None else "Not reported")
    if {"target_window_start", "target_window_end_exclusive"}.issubset(current.columns):
        first = current["target_window_start"].min()
        last = current["target_window_end_exclusive"].max()
        if pd.notna(first) and pd.notna(last):
            st.caption("Target window: " + date_label(first) + " – " + date_label(last - pd.Timedelta(days=1)) + ".")
    _bar_chart(current, "display_prediction", value_label,
               secondary="display_actual" if not future and "display_actual" in current else None)
    st.caption("Source: published future-forecast artifact." if future else "Source: predictions.parquet · held-out historical predictions.")
    _target_caption(metrics)
    columns = [name for name in ["tool_id", "week_start", "display_prediction", "display_actual", "baseline_median_downloads",
                                "predicted_median_downloads", "observed_future_median_downloads", "target_sustained",
                                "feature_cutoff", "train_cutoff"] if name in current]
    if future:
        columns = [name for name in columns if name not in ("target_sustained", "display_actual", "observed_future_median_downloads")]
    _table(current[columns], {"tool_id": "Tool", "week_start": "Feature week", "display_prediction": value_label,
                             "display_actual": "Observed change (%)" if "%" in value_label else "Observed log growth",
                             "baseline_median_downloads": "Past median downloads", "predicted_median_downloads": "Predicted future median",
                             "observed_future_median_downloads": "Observed future median", "target_sustained": "Observed sustained growth",
                             "feature_cutoff": "Feature cutoff", "train_cutoff": "Training cutoff"})
    _download(current, "Download this ranking", "radar_csv", "radar_ranking.csv")


def tool_page(features, predictions, metrics):
    st.subheader("Follow a tool")
    if not _usable(features, ["tool_id", "week_start"]):
        st.info("Weekly feature history has not been published yet.")
        return
    options = sorted(features["tool_id"].dropna().unique())
    if not options:
        st.info("No tool histories are available in this snapshot.")
        return
    tool = st.selectbox("Tool", options, key="detail_tool")
    frame = features[features["tool_id"].eq(tool)].dropna(subset=["week_start"]).sort_values("week_start").copy()
    frame = frame[[name for name in frame if not name.startswith(("gh_", "observed_gh_", "research_"))]]
    if frame.empty:
        st.info("This tool has no dated weekly observations.")
        return
    minimum, maximum = frame["week_start"].min().date(), frame["week_start"].max().date()
    bounds = st.date_input("Observation weeks", (minimum, maximum), min_value=minimum, max_value=maximum)
    if isinstance(bounds, (tuple, list)) and len(bounds) == 2:
        frame = frame[frame["week_start"].between(pd.Timestamp(bounds[0]), pd.Timestamp(bounds[1]))]
    if frame.empty:
        st.info("There are no published observations in this date range.")
        return
    st.caption("Source: weekly_features.parquet · " + str(tool) + " · feature weeks "
               + date_label(frame["week_start"].min()) + " – " + date_label(frame["week_start"].max()) + ".")
    latest = frame.iloc[-1]
    tiles = st.columns(2)
    for tile, column in zip(tiles, ["downloads", "release_count"]):
        tile.metric(SIGNALS[column][0] + " · latest week", number(latest.get(column)))
    if "downloads" in frame:
        st.markdown("#### Downloads over time")
        _line_chart(frame, ["downloads"], {"downloads": "Distribution download requests"}, {"downloads": TEAL})
    extra = [name for name in ["release_count"] if name in frame]
    if extra:
        st.markdown("#### Package release events")
        _line_chart(frame, extra, {name: SIGNALS[name][0] for name in extra}, {name: SIGNALS[name][1] for name in extra})
        st.caption("Release dates come from retained distribution uploads in current PyPI metadata. This does not reconstruct deleted artifacts or earlier metadata versions.")
    else:
        st.caption("A historical release series has not been published for this view.")
    st.caption("Capture-limited GitHub observations and quarantined research metadata are available under Data evidence; neither enters the primary models.")
    comparable = [name for name in ["downloads", "release_count"] if name in frame]
    if len(comparable) > 1 and st.checkbox("Compare normalized trends", value=False):
        _line_chart(frame, comparable, {name: SIGNALS[name][0] for name in comparable},
                    {name: SIGNALS[name][1] for name in comparable}, normalized=True)
        st.caption("Each series is divided by its own first positive observation in the selected range. Missing values remain gaps; this comparison does not show causality.")
    with st.expander("Inspect the weekly observations"):
        _table(frame)
    _download(frame, "Download this tool history", "tool_csv", re.sub(r"[^a-zA-Z0-9_-]", "_", str(tool)) + "_weekly.csv")
    test = holdout_rows(predictions)
    if _usable(test, ["tool_id", "model", "week_start", "prediction", "actual"]):
        test = test[test["tool_id"].eq(tool)]
        if not test.empty:
            st.markdown("#### Historical prediction check")
            model = _model_select(test, metrics, "detail_model")
            test = test[test["model"].eq(model)]
            _line_chart(test, ["prediction", "actual"], {"prediction": "Predicted log growth", "actual": "Observed log growth"},
                        {"prediction": TEAL, "actual": INK})
            st.caption("Source: predictions.parquet. Held-out predictions plotted by feature week; outcomes are known historical observations.")


def evaluation_page(predictions, metrics, ablations):
    st.subheader("Do release events improve the download-history baseline?")
    st.write("Compare the published baselines and source ablations on the same evaluation period.")
    frame = metric_rows(metrics)
    if not _usable(frame, ["model"]):
        st.info("Evaluation metrics have not been published yet.")
        return
    if "split" in frame:
        splits = sorted(frame["split"].dropna().astype(str).unique())
        if splits:
            default = next((value for value in splits if "holdout" in value.lower()), splits[0])
            split = st.selectbox("Evaluation period", splits, index=splits.index(default))
            frame = frame[frame["split"].eq(split)].copy()
    frame["Model"] = frame["model"].map(model_name)
    st.caption("Source: metrics.json · published evaluation results.")
    options = sorted(frame["model"].dropna().astype(str).unique())
    if options:
        selected = _published_best(metrics)
        if selected in options:
            st.caption("Published selection: " + model_name(selected) + ". Selection uses labels mature before 2026; the already-examined 2026 period is a corrective sensitivity analysis.")
        else:
            st.caption("A selected model has not been published for this view. Compare validation and holdout results separately.")
    columns = [name for name in ["mae", "median_weekly_spearman", "spearman", "precision_at_5"] if name in frame]
    for column in columns:
        names = {"mae": "Mean absolute error · lower is better", "median_weekly_spearman": "Median weekly Spearman rank correlation · higher is better",
                 "spearman": "Spearman rank correlation · higher is better", "precision_at_5": "Precision@5 · higher is better"}
        st.markdown("#### " + names[column])
        plotted = frame.dropna(subset=[column]).sort_values(column, ascending=column == "mae")
        if plotted.empty:
            st.caption("This metric is unavailable for the selected period.")
        else:
            _bar_chart(plotted, column, names[column], category="Model")
    _target_caption(metrics)
    st.caption("Precision@5 is the mean share of each eligible week’s five highest growth predictions that actually met the sustained-growth label. "
               "It is not overlap with the five highest realized growth rates. Sustained growth requires at least six of eight future weeks above 110% of the past eight-week median.")
    _table(frame, {"model": "Model ID", "mae": "MAE (log-growth units)", "median_weekly_spearman": "Median weekly Spearman",
                   "precision_at_5": "Precision@5", "n_predictions": "Predictions", "n_weeks": "Weeks", "n_tools": "Tools"})
    _download(frame, "Download these metrics", "metrics_csv", "evaluation_metrics.csv")
    matched = pd.DataFrame(ablations.get("results", []))
    if not matched.empty and ablations.get("profile") == "validated_primary":
        if "split" in matched and "split" in frame and not frame.empty:
            matched = matched[matched.split.eq(frame.split.iloc[0])]
        st.markdown("#### Matched release-source comparisons")
        st.caption("Each comparison holds the algorithm and download inputs fixed and adds release events. Negative MAE difference favors the release-augmented model. This separates a source comparison from a change of algorithm.")
        columns = [name for name in ["algorithm", "baseline_model", "augmented_model", "baseline_mae", "augmented_mae", "mae_difference", "relative_mae_change", "ci_low", "ci_high", "ci_status", "n_predictions", "n_weeks"] if name in matched]
        _table(matched[columns])
        st.caption("Confidence intervals use consecutive week blocks; see the saved comparison record for block length and status. These corrective comparisons use previously examined 2026 outcomes when that period is selected.")
        _download(matched, "Download matched comparisons", "matched_csv", "source_ablation_metrics.csv")
    if "best_model" in metrics:
        with st.expander("Selection record and target definition"):
            selection = metrics.get("selection", {})
            if isinstance(selection, dict):
                cutoff = selection.get("label_availability_cutoff")
                if cutoff:
                    st.caption("Selection freeze: only validation labels available by "
                               + date_label(cutoff) + " UTC are eligible. Full-year validation metrics are descriptive.")
                counts = [("n_rows_per_model", "rows per model"), ("n_weeks", "feature weeks"), ("n_tools", "tools")]
                details = [number(selection[key]) + " " + label for key, label in counts if selection.get(key) is not None]
                if details:
                    st.caption("Selection subset: " + ", ".join(details) + ".")
            st.json({key: metrics[key] for key in ["best_model", "selection", "target_semantics", "periods"] if key in metrics})


def _coverage_table(status):
    coverage = _first(status, ["source_coverage", "coverage", "sources"], {})
    if isinstance(coverage, list):
        return pd.json_normalize(coverage)
    if isinstance(coverage, dict):
        return pd.json_normalize([{"source": name, **(value if isinstance(value, dict) else {"status": value})}
                                  for name, value in coverage.items()])
    return pd.DataFrame()


def supplementary_github_section():
    st.markdown("#### Independent GitHub collections · supplementary")
    st.caption("These separately collected sources do not change the published study, its source-quality gate, model inputs or rankings. Viewing them makes no network requests and does not fit a model.")
    base=os.environ.get("RADAR_GITHUB_DATA_DIR",str(Path.home()/"6893/data/raw"))
    for kind,title in [("git","Surviving default-branch Git history"),("stars","Direct GitHub surviving-star history")]:
        with st.expander(title):
            result=load_supplementary(base,kind)
            if result["state"]=="absent":
                st.info("This optional collection has not been published here yet.")
                continue
            if result["state"]!="ready":
                st.info("Unverified supplementary data are hidden. "+result.get("message","The manifest and CSV could not be verified."))
                continue
            manifest,frame=result["manifest"],result["frame"]
            st.metric("Tools completed / requested",str(result["completed"])+" / "+str(result["requested"]))
            if result["partial"]:
                st.warning("Partial collection: only the completed tools listed here are available. Missing tools have not been filled with zeros.")
            else:
                st.caption("Requested tool coverage is complete for this separate collection; historical public availability remains uncertified.")
            if kind=="git":
                st.write("Retrospective commits reachable from the current default-branch tip, with the surviving first-parent chain shown separately. These counts do not reconstruct GitHub PushEvents or certify when commits first became public.")
                st.caption("Current branch selection, rewritten history, deleted branches and creator-supplied commit dates limit interpretation. Nulls before the first retained commit remain unknown; a zero later is a count in this surviving graph, not proof of no development.")
                columns=["git_reachable_commit_count","git_first_parent_commit_count"]
                labels={columns[0]:"Surviving reachable commits",columns[1]:"Surviving first-parent commits"}
                axis="UTC committer-date week"
            else:
                st.write("Stars created in each API calendar week that still exist at collection time. Unstars remove historical records, so this is a surviving-star reconstruction rather than historical net star growth.")
                st.caption("GitHub does not guarantee UTC boundaries for these API weeks or their daily bins. The plotted epoch anchor is rendered as a date for display; it is not a certified UTC observation window. Current partial API weeks are excluded by the collector.")
                columns=["stars"];labels={"stars":"Surviving stars · API week"};axis="GitHub API calendar week · UTC boundaries uncertified"
            if not frame.empty:
                selected=st.selectbox("Supplementary tool",sorted(frame.tool_id.astype(str).unique()),key="supplementary_"+kind)
                current=frame[frame.tool_id.astype(str).eq(selected)].sort_values("week_start")
                _line_chart(current,columns,labels,{columns[0]:TEAL,**({columns[1]:ORANGE} if len(columns)>1 else {})},x_label=axis)
                st.caption("CSV hash verified against its collection manifest. This verifies artifact integrity, not a historical data vintage.")
                _download(current,"Download supplementary tool history","supplementary_csv_"+kind,selected+"_"+kind+"_supplementary.csv")
            else:
                st.info("No completed tool histories are available in the saved CSV yet.")
            st.caption("Separate collection record")
            st.json({key:manifest[key] for key in ["source","profile","status","created_at","as_of","history_start","history_end_exclusive","requested_tools","completed_tools","tools_passed","measurement","source_semantics","limitations","primary_eligible","historical_public_availability"] if key in manifest},expanded=False)


def evidence_page(features, predictions, metrics, status, quality, root, errors):
    st.subheader("Data evidence")
    st.write("Coverage, timing, and the limits that determine what a forecast can support.")
    st.markdown("#### Source validity and model eligibility")
    st.caption("Retrieval coverage asks whether requested archive responses arrived and matched their hashes. Source validity asks what those responses can measure. Passing retrieval checks does not certify complete GitHub activity.")
    source_rows = []
    for source, detail in quality.get("sources", {}).items():
        if isinstance(detail, dict):
            source_rows.append({"Source": source, "Source assessment": detail.get("status", "not reported"),
                                "Primary models": "Enabled" if detail.get("status") == "primary_enabled" else "Excluded"})
    if source_rows:
        _table(pd.DataFrame(source_rows))
    else:
        st.warning("This snapshot predates semantic source-quality validation. Its saved coverage status is retrieval evidence only.")
    st.warning("GH Archive observations are capture-limited. All GitHub features are excluded from the primary study, including earlier periods whose capture completeness has not been independently established. An archived count of zero does not establish repository inactivity.")
    st.markdown("Source-project issue reports document a decline beginning **May 24, 2025** ([#310](https://github.com/igrigorik/gharchive.org/issues/310)), further WatchEvent capture problems ([#320](https://github.com/igrigorik/gharchive.org/issues/320)), and a sharp feed drop on **October 27, 2026** ([#326](https://github.com/igrigorik/gharchive.org/issues/326)). These reports support a source limitation; they do not supply a calibrated correction factor for this cohort.")
    st.caption("Research candidates remain quarantined pending relevance and version-availability review. Current paper text and dependency metadata cannot be backdated into historical features.")
    predecessor = status.get("supersedes")
    if predecessor:
        st.caption("This correction supersedes an earlier snapshot. The earlier artifacts remain unchanged for audit; their GitHub-based performance claim is not a validated primary result.")
        with st.expander("Correction lineage"):
            st.json(predecessor)
    coverage = _coverage_table(status)
    if not coverage.empty:
        st.markdown("#### Retrieved panel coverage")
        _table(coverage)
    else:
        st.info("Source coverage has not been published in study_status.json yet.")
    if _usable(features, ["tool_id", "week_start"]):
        st.markdown("#### The observed panel")
        a, b, c = st.columns(3)
        a.metric("Tools with weekly observations", str(features["tool_id"].nunique()))
        b.metric("Published tool-weeks", number(len(features)))
        c.metric("Latest feature week", date_label(features["week_start"].max()))
        signals = [name for name in ["downloads", "release_count"] if name in features]
        if signals:
            missing = pd.DataFrame({"Signal": [SIGNALS[name][0] for name in signals],
                                    "Missing tool-weeks": [int(features[name].isna().sum()) for name in signals],
                                    "Observed tool-weeks": [int(features[name].notna().sum()) for name in signals]})
            _table(missing)
        st.caption("A missing value is unknown. It is not automatically converted to zero. Counts shown here describe the published panel, not complete platform coverage.")
    if _usable(features, ["tool_id", "week_start"]):
        with st.expander("Capture-limited archived GitHub observations"):
            archive = features.copy()
            # Legacy snapshots used unqualified names for the same saved counts.
            # This alias is for this explicitly quarantined evidence view only.
            for observed in ARCHIVE_SIGNALS:
                legacy = observed.removeprefix("observed_")
                if observed not in archive and legacy in archive:
                    archive[observed] = archive[legacy]
            available = [name for name in ARCHIVE_SIGNALS if name in archive]
            if available:
                tool = st.selectbox("Archived tool", sorted(archive.tool_id.dropna().unique()), key="archive_tool")
                selected = st.multiselect("Archived observed counts", available, default=available[:1],
                                          format_func=lambda name: ARCHIVE_SIGNALS[name][0])
                if selected:
                    _line_chart(archive[archive.tool_id.eq(tool)].sort_values("week_start"), selected,
                                {name: ARCHIVE_SIGNALS[name][0] for name in selected},
                                {name: ARCHIVE_SIGNALS[name][1] for name in selected})
                st.caption("These are counts present in retained archive responses, not complete development activity. Gaps and zeros cannot distinguish inactivity from capture loss. Push events are not exact commits; actors are not necessarily humans. No correction multiplier is applied.")
            else:
                st.info("Archived observed GitHub counts are not included in this snapshot.")
    supplementary_github_section()
    cohort = status.get("cohort")
    if cohort is not None:
        with st.expander("Published tool cohort"):
            if isinstance(cohort, list) and cohort and isinstance(cohort[0], dict):
                _table(pd.json_normalize(cohort))
            elif isinstance(cohort, (dict, list)):
                st.json(cohort)
            else:
                st.write(cohort)
    if not predictions.empty:
        timing = []
        for name, label in [("feature_cutoff", "Feature cutoff"), ("train_cutoff", "Training cutoff"), ("label_available_at", "Outcome available at")]:
            if name in predictions and predictions[name].notna().any():
                timing.append({"Timing field": label, "Earliest": date_label(predictions[name].min()), "Latest": date_label(predictions[name].max())})
        if timing:
            st.markdown("#### Backtest cutoffs")
            _table(pd.DataFrame(timing))
            st.caption("Cutoffs vary by prediction origin. Future outcomes become labels only after their full window has elapsed.")
    st.markdown("#### What the signals mean")
    st.markdown("- **Downloads:** distribution requests, affected by automation and caches; they are not unique users.\n"
                "- **Archived GitHub observations:** capture-limited saved counts, excluded from primary forecasting.\n"
                "- **Research:** candidate text matches; they require a relevance check before being treated as library citations.\n"
                "- **Dependencies:** current metadata cannot stand in for an earlier historical snapshot.")
    limits = _first(status, ["limitations", "limit_flags", "warnings"], [])
    if limits:
        st.markdown("#### Snapshot limitations")
        if isinstance(limits, list):
            for limit in limits:
                st.write(limit)
        elif isinstance(limits, dict):
            st.json(limits)
        else:
            st.write(limits)
    budget = _first(status, ["budget", "query_budget", "billing"])
    if budget is not None:
        with st.expander("Published collection budget"):
            st.json(budget) if isinstance(budget, (dict, list)) else st.write(budget)
    with st.expander("Snapshot record"):
        st.caption("Read-only snapshot directory: " + str(root))
        st.json(status if status else {"status": "not_published_yet"})
        if errors:
            st.json({"read_errors": errors})
    if quality:
        with st.expander("Semantic source-quality checks"):
            st.json(quality)
    if status:
        st.download_button("Download study status", json.dumps(status, indent=2, default=str).encode(),
                           "study_status.json", "application/json", key="status_json")


def _style():
    st.markdown("""<style>
      :root { --paper:#f5f3eb; --ink:#183438; --teal:#187c70; }
      .stApp { background:var(--paper); color:var(--ink); }
      [data-testid="stMainBlockContainer"] { max-width:1280px; padding:2.5rem 3rem 4rem; }
      [data-testid="stSidebar"] { background:#17363a; border-right:0; }
      [data-testid="stSidebar"] * { color:#f2f0e6; }
      [data-testid="stSidebar"] [data-baseweb="select"] * { color:#183438; }
      [data-testid="stSidebar"] hr { border-color:#446064; }
      [data-testid="stSidebar"] pre, [data-testid="stSidebar"] pre *,
      [data-testid="stSidebar"] code, [data-testid="stSidebar"] code * { color:#183438; }
      [data-testid="stSidebar"] .stButton>button,
      [data-testid="stSidebar"] .stButton>button * { color:#155e56; }
      h1,h2,h3,h4 { color:var(--ink); font-family:Georgia,'Palatino Linotype',serif; letter-spacing:-.035em; }
      h1 { font-size:3.1rem !important; line-height:1.04 !important; font-weight:400 !important; max-width:850px; }
      h2 { font-weight:400 !important; }
      p,div,label,button,input,select { font-family:Optima,Candara,'Segoe UI',sans-serif; }
      [data-testid="stMetric"] { border-top:2px solid #187c70; padding:1rem 0 1.1rem; }
      [data-testid="stMetricLabel"] { font-size:.78rem; text-transform:uppercase; letter-spacing:.07em; }
      [data-testid="stMetricValue"] { font-family:Georgia,serif; font-size:1.8rem; color:#183438; }
      [data-testid="stAlert"] { border-radius:2px; border-left:3px solid #ce623c; }
      .stButton>button,.stDownloadButton>button { border-radius:2px; border:1px solid #187c70; color:#155e56; background:#f5f3eb; }
      .stButton>button:hover,.stDownloadButton>button:hover { border-color:#155e56; background:#e7eee5; color:#155e56; }
      [data-testid="stDataFrame"] { border:1px solid #dcded4; }
      .eyebrow { color:#687d78; font-size:.72rem; letter-spacing:.16em; text-transform:uppercase; margin-bottom:.7rem; }
      .hero-rule { border-top:1px solid #bac9bf; margin:1.4rem 0 1.1rem; }
      .brand { font-family:Georgia,serif; font-size:2rem; line-height:1.02; letter-spacing:-.04em; margin:0 0 .8rem; }
      .brand-note { font-size:.69rem; text-transform:uppercase; letter-spacing:.15em; color:#b2cac2 !important; }
      @media(max-width:800px) { [data-testid="stMainBlockContainer"] { padding:1.5rem 1rem 3rem; } h1 { font-size:2.3rem !important; } }
      @media(prefers-reduced-motion:no-preference) { .eyebrow { animation:arrive .5s ease both; } @keyframes arrive { from { opacity:0; transform:translateY(5px); } to { opacity:1; transform:none; } } }
    </style>""", unsafe_allow_html=True)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--data-dir", default=os.environ.get("RADAR_STUDY_DIR", "~/6893/data/study/latest"))
    args, _ = parser.parse_known_args()
    st.set_page_config(page_title="AI Technology Radar", page_icon="◉", layout="wide", initial_sidebar_state="expanded")
    _style()
    root, artifacts, errors = load_snapshot(args.data_dir)
    features = features_frame(artifacts.get("weekly_features.parquet", pd.DataFrame()))
    predictions = predictions_frame(artifacts.get("predictions.parquet", pd.DataFrame()))
    forecasts = forecast_rows(artifacts, predictions)
    metrics, status = artifacts.get("metrics.json", {}), artifacts.get("study_status.json", {})
    quality = artifacts.get("data_quality.json", {})
    ready = primary_ready(status, metrics, quality)
    if not ready:
        # Preserve the immutable old snapshot, but do not present suspect models
        # as the current validated study while its correction is being built.
        predictions, forecasts, metrics = pd.DataFrame(), pd.DataFrame(), {}
    else:
        for frame in (predictions, forecasts):
            if "model" in frame:
                frame.drop(frame.index[~frame.model.isin(PRIMARY_MODELS)], inplace=True)
    with st.sidebar:
        st.markdown('<p class="brand-note">Research · development · uptake</p><div class="brand">AI Technology<br>Radar</div>', unsafe_allow_html=True)
        st.markdown("A study of sustained package downloads.")
        st.divider()
        page = st.radio("Explore", ["Radar", "Tool details", "Evaluation", "Data evidence"], label_visibility="collapsed")
        st.divider()
        snapshot = _first(status, ["snapshot_id", "run_id", "study_id"], root.name)
        st.caption("Published snapshot")
        st.code(str(snapshot), language=None)
        published = _first(status, ["published_at", "generated_at", "created_at"])
        if published:
            st.caption("Published " + date_label(published))
        st.caption("Reads saved study results. Changing a view does not collect data or train a model.")
        if st.button("Refresh published snapshot", width="stretch"):
            st.rerun()
    st.markdown('<div class="eyebrow">EECS 6893 · Multi-source adoption study</div>', unsafe_allow_html=True)
    st.title("AI Technology Radar")
    st.write("Can download history and release activity help forecast sustained package-download growth?")
    if not ready:
        st.warning("Source-quality correction pending. GH Archive has documented capture gaps; the previous GitHub-based model results and its reported 29% improvement are superseded and are not validated primary findings. Rankings and evaluation remain hidden until a corrected snapshot passes source, model, and Spark checks.")
    else:
        st.caption("Primary models use download history, release events and an age control. GitHub archive counts and unreviewed research candidates are excluded.")
        st.warning("Post-audit corrective sensitivity study: the 2026 outcomes had already been examined before this correction. These results are not an untouched confirmatory test.")
    st.markdown('<div class="hero-rule"></div>', unsafe_allow_html=True)
    if not artifacts:
        st.info("The study snapshot has not been published yet. This dashboard will show the saved results when publication completes.")
        with st.expander("Expected publication location"):
            st.code(str(root), language=None)
            st.write("Expected files: weekly_features.parquet, predictions.parquet, metrics.json, study_status.json. Future forecasts are optional.")
    if errors:
        st.warning("Some published files could not be read. Available results remain visible; Data evidence contains the read errors.")
    if page == "Radar":
        radar_page(features, predictions, forecasts, metrics)
    elif page == "Tool details":
        tool_page(features, predictions, metrics)
    elif page == "Evaluation":
        evaluation_page(predictions, metrics, artifacts.get("source_ablation_metrics.json", {}) if ready else {})
    else:
        evidence_page(features, predictions, metrics, status, quality, root, errors)


if __name__ == "__main__":
    main()
