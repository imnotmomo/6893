# First version research enrichment

`collect_research_v1.py` enriches retained arXiv candidate papers with explicitly requested `v1` metadata. It preserves titles, abstracts, author names and affiliations, publication/version dates, categories, links and available DOI/comment/journal metadata.

Run on c0:

```sh
~/6893/.venv-duckdb/bin/python ~/6893/pipeline/collect_research_v1.py \
  --output ~/6893/data/raw/research_v1/backfill \
  --max-requests 60 --max-seconds 600
```

By default, the first invocation freezes candidate pairs from the retained `backfill.records` table. `--candidates <CSV>` can instead freeze a previously exported candidate list. Resume the same output directory to reuse verified completed responses. A changed registry or candidate input requires a new output directory. Use a fresh dated output for later candidate universes.

The collector takes the existing `.pilot-collection.lock`, waits ten seconds before its first request and retains the lock until ten seconds after the final response. HTTP429 persists the shared `.arxiv-cooldown.json` and stops immediately. Requests and processing remain bounded; collection stops below15GiB free.

Outputs include immutable compressed batch XML, the frozen inputs, response/query/checksum metadata, `papers_v1.jsonl`, `tool_paper_v1.jsonl`, `research_v1_weekly.csv`, and `research_v1_quality.json`. `collection_manifest.json` binds current outputs and all retained raw response hashes. Partial retrieval and unavailable IDs stay explicit.

The full review queue includes every candidate pair. `research_review_sample.csv` selects up to five strict and five other retrieved candidates per tool using deterministic SHA ordering; human label/reviewer fields are deliberately blank.

Strict aliases and explicit library-context phrases are machine rules. A match is not a verified library citation, proof of use, or manual review. The weekly file separates candidate counts, unresolved retrieval, and a two-day availability assumption. All rows remain `historical_feature_eligible=false` until relevance and historical-discovery limitations are assessed. Retrieving v1 does not recover papers missed by the original current-text search.

Tests use hand-authored Atom feeds and perform no network requests:

```sh
python -m unittest discover -s . -p 'tests_research_v1.py' -v
```
