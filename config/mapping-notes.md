# Pilot mapping notes

This is a ten-tool collection pilot, not the frozen final study universe. Every row remains `pilot_candidate_awaiting_team_approval`. Current PyPI project metadata and canonical GitHub repository responses were checked on November 1, 2026. That verifies the present package/repository relationship; it does not approve the cohort or establish a complete historical entity mapping.

The `arxiv_query` values are unvalidated title/abstract search candidates, not ground-truth paper labels. Collectors should URL-encode these expressions, add the desired date restriction separately, and deduplicate by arXiv paper/version identity. Review sample hits and missed aliases before using counts as research-attention features. Transformers and Ray deliberately avoid their ambiguous bare names. Method mentions for XGBoost and LightGBM are not proof of Python-library use.

Use the exact listed PyPI distribution for this pilot. Do not automatically sum similarly named packages, renamed packages, or split ecosystem components. In particular, LangChain and LlamaIndex repository activity covers more than their single selected distribution. LlamaIndex's earlier GPT Index history needs a separate date-aligned decision before a 2023 backfill.

GitHub's public repository API returned these stable IDs:

| Tool | Canonical repository | Repository ID |
| --- | --- | --- |
| PyTorch | `pytorch/pytorch` | 65600975 |
| TensorFlow | `tensorflow/tensorflow` | 45717250 |
| Hugging Face Transformers | `huggingface/transformers` | 155220641 |
| scikit-learn | `scikit-learn/scikit-learn` | 843222 |
| XGBoost | `dmlc/xgboost` | 16587283 |
| LightGBM | `lightgbm-org/LightGBM` | 64991887 |
| LangChain | `langchain-ai/langchain` | 552661142 |
| LlamaIndex | `run-llama/llama_index` | 560704231 |
| MLflow | `mlflow/mlflow` | 136202695 |
| Ray | `ray-project/ray` | 71932349 |

The [legacy LightGBM URL](https://api.github.com/repos/microsoft/LightGBM) resolves to repository ID 64991887, now `lightgbm-org/LightGBM`. The [legacy GPT Index URL](https://api.github.com/repos/jerryjliu/gpt_index) resolves to repository ID 560704231, now `run-llama/llama_index`. Resolve historical GH Archive events by stable repository ID where available; filtering only today's owner/name can lose pre-rename events. Current redirects do not establish the dates on which a historical name was in use.

The evidence URLs in each JSON row are public primary sources. The pilot collector should preserve its own timestamped metadata responses, extraction cutoffs, and checksums so a later metadata change does not silently alter provenance. Missing or ambiguous research coverage must not automatically be treated as a verified zero.
