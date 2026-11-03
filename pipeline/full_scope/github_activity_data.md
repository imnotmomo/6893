# GitHub issue and pull-request observations

The canonical feature directory is `~/6893/data/full_scope/github_activity`, an atomic symlink to one immutable directory under `github_activity_runs/`. The source archive lives under `~/6893/data/raw/github_direct_activity/backfill/`. Its frozen collection-state file lists every retained response path, collection time and compressed/decoded SHA256. Upload response files first and the frozen state last. A running collector's mutable state is never the published feature source.

The archive retains complete public object payloads; JSONP envelopes retain unchanged data arrays and only allowlisted provider metadata. `normalized_records.jsonl.gz` is a compact deduplicated index of the timestamps and account IDs needed for analysis; its small size does not describe the size of the underlying response archive. Canonical object identity is frozen numeric repository ID plus issue/PR number. A Search issue ID and a direct PR ID can describe the same pull request and must not be counted twice.

`build_github_activity.py` reads a frozen state and makes no API calls. It verifies the cohort, source identities and raw-file hashes before evaluating each acquisition stream independently. Direct PR creation counts use the complete native PR-created cohort, with its original offset pages and verified study-start boundary. Search totals and retained Search pages describe a different provider index; equality with Search is not required to preserve an independently complete native PR series. Census totals above 1,000 remain totals; the one sample row is not the census.

True-issue observations use the actual-Link JSONP profile in `github_jsonp_lists.py`. A fixed callback is parsed as JSON, never executed. The retained safe envelope contains the unchanged `data` array and an allowlist of provider metadata. Every page binds the exact UTF-8 array slice and checksum, full-envelope checksum, request URL, registry and numeric repository identity. Pagination follows the provider's actual opaque `next` Link, including a nonempty terminal page when no next relation exists. Serialized `updated_at` values do not generate cursors or override provider Links. The filtered issue view and the independent native PR view can disagree: a complete no-since spaCy issue-list probe still omitted a PR preserved by the native endpoint. Both views and the reconciliation evidence remain archived.

The main `github_activity_weekly.csv` contains full Monday–Sunday UTC weeks. Each feature has a separate `_complete` flag. Incomplete scientific counts are NULL; `observed_partial_*` columns retain diagnostic lower bounds. Exact account counts remain unknown when a creation author ID is missing. The shared `ghost` placeholder, confirmed in retained responses as account ID 10137, is also unknown for exact creator counts; its reported ID remains available for audit. The combined author measure uses a distinct union across issue and PR creators. Account IDs identify public accounts, not certified people.

The native all-history profile in `github_pr_history.py` reuses the original current-cohort pages, adds separately planned older pages, and verifies an actual empty tail. If a short cached boundary gained records, a targeted boundary refresh must preserve its prior prefix and close the offset gap. A complete native history can independently certify in-window PR merge and last-reported-close counts, including PRs created before 2023. Capture completion and per-feature eligibility are validated separately. Direct PR timestamps take priority for native PR measurements; issue-object and PR-object creation clocks are preserved separately. Closure measures use the last reported `closed_at` in the newest retained observation; they are not a complete close/reopen event log. Leading and trailing partial calendar weeks are retained separately in `github_activity_partial_weeks.csv`, with `week_complete=false` and explicit effective interval bounds.

Native older-history records can contain a reported close before their PR-object creation. These original dates remain in the normalized record and raw body with explicit chronology diagnostics. An anomalous event inside the study window disables the corresponding native event measurement; an event entirely before 2023 does not invalidate unrelated study-week counts. The native updated collector uses the same explicit policy if such an old PR changes later, so a pre-study anomaly does not stall unrelated current observations. Capture completion and the other event/author measurements keep their separate gates. Future events and other source chronology contradictions still fail validation.

`source_quality.json` and `github_activity_quality.json` contain the same quality record. The manifest binds both reports, normalized records, weekly tables, frozen state, code and registry. Capture completion is separate from per-feature eligibility. Historical public visibility remains incomplete and `primary_eligible` is always false. The two-day feature-availability lag is explicitly an assumption. The JSONP safe envelope retains status, Links, ETag, Date, selected API version and rate metadata when the provider reports them; unrelated OAuth actor, client and expiry metadata are excluded. Full-envelope hashes preserve the acquisition binding without retaining those fields. Earlier raw-only connector responses have their original, more limited receipts and do not gain invented headers.

`github_issues_cursor.py`, `github_visible_cursor.py` and `github_visible_lists.py` are retained legacy diagnostic/proof readers, not supported live enumeration methods. The archived Flax counterexample disproves the generated-updated-cursor assumption: a provider Link's sort key differs from the same row's serialized timestamp. All such generated-cursor completion claims are disqualified, including a generated chain that reached an empty page. Do not change the frozen encoder bytes or discard its observations; use the actual-Link profile for enumeration.

Newer public observations have separate schemas and archives under `data/raw/github_direct_incremental`. `collect_github_activity_updates.py` follows actual HTTP issue Links, reuses hash-bound baseline PR-creation facts, and queues bounded PR-detail requests for unknown records. Issue-wrapper completeness cannot certify the future PR population. `native_pr_updates.py` independently polls the native PR endpoint in descending updated order. It completes only at a genuine no-next Link or a full, validated page whose native timestamps are all strictly older than its seven-day overlap boundary. Ordering contradictions, missing pages, malformed Links and the 128-page cap preserve unknown coverage instead of advancing a watermark.

`dispatch_github_activity.py` shares one quarter-hour allowance across both workers: four requests while star backfill remains partial, ten after it finishes. It holds an internal dispatch lock, persists request reservations before invoking either worker, honors both streams' rate cooldowns, and preserves uncertain reservations after interruption. Native polling does not add another request allowance. Only completed cycles enter `github_activity_overlay.py`; partial raw pages remain diagnostics. The unpaid materializer verifies the independent receipts, builds an immutable overlay and updates `data/full_scope/github_activity_updated` after validation. The baseline and frozen primary study remain unchanged.

Busy repositories can change their native updated-offset pages between checkpoints. Such cycles remain unknown when duplicate, order or boundary guards fail; a scheduled run does not imply perfect newer-data coverage. Additional authenticated read capacity may be needed later to shorten bounded capture intervals. The current worker keeps its existing public request allowance and makes no authentication changes.

Example replay on c0:

```sh
PYTHONPATH="$HOME/6893/pipeline:$HOME/6893/pipeline/full_scope" \
  "$HOME/6893/.venv-duckdb/bin/python" \
  "$HOME/6893/pipeline/full_scope/build_github_activity.py" \
  --source-root "$HOME/6893/data/raw/github_direct_activity/backfill" \
  --state-snapshot /absolute/path/to/frozen-collection-state.json \
  --registry "$HOME/6893/config/tools.study.json" \
  --output /absolute/path/to/a/fresh/immutable/source-run
```

Publish a new canonical symlink only after the source passes the joined-spine provenance adapter. Keep prior source runs and their hashes available for reproducibility.

`prepare_github_snapshot.py` freezes one state read, verifies every referenced response checksum and emits a reusable `raw_files.txt`, frozen registry/state and `transfer_manifest.json`. It makes no network requests and prints the exact rsync command. For a later bulk snapshot, run:

```sh
python prepare_github_snapshot.py \
  --source-root /absolute/path/to/github-api \
  --output-root /absolute/path/to/transfer-bundles \
  --remote ubuntu@129.159.41.246:/home/ubuntu/6893/data/raw/github_direct_activity/backfill \
  --ssh-key "$HOME/.ssh/6893.key"
```

The helper also freezes optional `spacy_census_reconciliation.json`, `connector_rate_limit_receipts.jsonl` and `cursor_conformance.json` evidence when present. These independent runtime files receive their own captured checksums and lineage file list; they are copied from the immutable transfer bundle, never from the changing live files. Changed evidence produces a new bundle even when the request-state bytes are unchanged.

Referenced cursor/identity/protocol proofs are followed transitively through a strict relative-path allowlist. Provider receipts, their gzip bodies, the separate uint64 boundary proof, and the hash-bound proof-reference correction mapping are included. Source-relative proof names remain available to the feature gate, with separate immutable lineage copies. Known correction mappings are validated against each original raw receipt and observation; the helper uses them only to resolve references while preserving the original state, raw bodies and correction bytes without rewriting them.

Run the emitted raw and ancillary rsync commands, upload the frozen registry and transfer manifest, then transfer the frozen state as a pending file. Verify destination hashes and rename the frozen state into place last. Repeated transfers use checksums and copy only missing or changed response bytes. Runtime evidence and transfer bundles stay outside Git. The helper does not upload or publish anything itself.
