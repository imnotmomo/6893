# Frozen conditional study cohort

This registry freezes a 30-tool scope for the AI Technology Radar study as of 2026-11-02. It preserves the ten pilot tool IDs and their repository IDs and adds twenty tools. The cohort is conditional on successfully observed, correctly mapped source coverage; missing data must not be manufactured as zero. Changes after freezing require a new registry hash and a documented amendment.

## Selection rationale and limits

Selection uses functional role coverage across frameworks, classical ML/explainability, LLM/generative tooling, MLOps/optimization, interchange/inference, and NLP; the additions have a currently public canonical Python distribution and GitHub repository and visible artifact availability by the end of 2023. No tools were selected based on forecast accuracy, observed 2023–2026 growth, a growing/declining label, or model results. This is a deliberately assembled ecosystem cohort, not a random sample or a representative census of all AI tools. Present-day availability, name familiarity, ecosystem clustering, and continued discoverability can produce selection and survivorship bias. Category/age coverage does not remove these biases.

The category counts are:

- `classical_ml_and_explainability`: 5
- `deep_learning_frameworks`: 6
- `interchange_and_inference`: 2
- `llm_and_generative_tooling`: 9
- `mlops_and_optimization`: 6
- `nlp`: 2

All twenty additions were checked through the public PyPI project JSON API and GitHub repository API during this run. The original ten retain their already verified pilot/backfill mappings; their metadata was not fetched again in this cohort-expansion run. Per-tool evidence URLs are included in the JSON. Current canonical ownership is not proof of historical ownership or package semantics. Earliest visible upload is a retained-artifact observation, not a certified package creation date; deleted artifacts and ownership changes can invalidate birth-date assumptions.

## Verified repository redirects

| Tool | Current canonical repository | Stable repository ID | Verified legacy path |
|---|---|---:|---|
| JAX | `jax-ml/jax` | 154739597 | `google/jax` |
| PyTorch Lightning | `Lightning-AI/pytorch-lightning` | 178626720 | `Lightning-AI/lightning` |
| Sentence Transformers | `huggingface/sentence-transformers` | 198616978 | `UKPLab/sentence-transformers` |
| SHAP | `shap/shap` | 74505259 | `slundberg/shap` |
| DVC | `treeverse/dvc` | 83878269 | `iterative/dvc` |
| Gensim | `piskvorky/gensim` | 1349775 | `RaRe-Technologies/gensim` |

The current DVC owner is `treeverse`; the current Gensim API canonical path is `piskvorky/gensim`. PyPI's older `RaRe-Technologies/gensim` link resolves to the same ID. PyTorch Lightning's PyPI `Lightning-AI/lightning` link also resolves to the same ID as `Lightning-AI/pytorch-lightning`. Match GH Archive by stable numeric repository ID; retain names for audit.

## Research matching rules

- `arxiv_query` generates candidates from title/abstract only. It does not declare tool use, relevance, or historical feature eligibility. The date window is added by the collector.
- `research_exact_aliases` contains literal case-insensitive phrase candidates, not regexes or automatic ground-truth labels. Match phrases with sensible Unicode word boundaries, normalize whitespace, and record the phrase and title/abstract evidence span. Never silently turn a generic word into an accepted tool mention.
- `research_ambiguous=true` requires manual/contextual review. Ray, Transformers, JAX, Flax, Lightning, Accelerate, Datasets, Diffusers, Sentence Transformers, PEFT, TRL, SHAP, and DVC need special care. A contextual API query can still return unrelated matches when its words occur in different sentences.
- For Ray, never count bare “ray”; for Hugging Face tools, require the appropriate library/repository context, not merely generic datasets, transformers, acceleration, diffusion, or parameter-efficient fine-tuning. SHAP method papers alone do not prove use of the Python library.
- For unambiguous branded algorithms/standards such as XGBoost, LightGBM, CatBoost, and ONNX, research mentions are explicitly attention signals, not evidence of Python installation. spaCy uses case-insensitive matching despite its stylistic capitalization; use token boundaries.
- Alias lists are intentionally conservative and may miss punctuation variants or less explicit mentions. Report precision/recall limitations and review labeled samples across tools rather than treating zero retained mentions as zero research attention. Preserve unmatched/rejected candidates and reasons when available.

## Historical validity and target boundaries

Retain paper canonical ID, version, published and updated timestamps, fetched-at timestamp, query and mapping hash, evidence span, review decision, and exact-text availability date. Current revised abstracts must not be placed into older feature weeks using only the first publication date. Until version availability is established, keep `historical_feature_eligible=false`.

PEFT's earliest currently visible PyPI artifact was uploaded on 2023-01-19. Earlier package-target dates are unestablished; exclude ineligible pre-availability weeks instead of imputing fake downloads. Hugging Face Datasets has old visible PyPI uploads from 2015 while its current repository was created in 2020; the first old package artifact cannot establish the Hugging Face library's birth. Inspect coverage within the actual 2023-onward window.

Each target remains exactly the named PyPI distribution: do not silently combine torch variants, tensorflow variants, keras embedded in tensorflow, jaxlib, lightning, onnxruntime-gpu, mlflow-skinny, llama-index-core, langchain-core, model packages, or other ecosystem distributions. Monorepo activity and one-package downloads are different units; retain this limitation in interpretation. Package splitting, renames, release-policy changes and installer automation can affect the target.

Aggregate visible release artifacts into unique package/version release events; one wheel is not one release. GH PushEvents are not commit counts, and active event actors are not necessarily code contributors. New actors means first observed in the study window unless earlier history is checked.

Only fully mature eight-week outcomes belong in retrospective scoring. Freeze cohort, transforms, missingness rules and chronological evaluation protocol before examining model performance. Keep present-day cohort selection limits visible in the report.
