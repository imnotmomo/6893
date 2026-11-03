# Study code

This folder contains the weekly dataset builder, chronological model study, plotting and validated snapshot publisher. Runtime data lives on c0 under `~/6893/data/study/`; it is not the repository database.

Run `~/6893/study/run_study.sh --wait-for-core` on c0 to require full thirty-tool source coverage, train/evaluate the frozen models, produce figures, run the actual Spark weekly correctness comparison, and atomically publish a new snapshot. Failures preserve the previous published snapshot. Research candidate collection may continue independently and does not enable historical research predictors.

See the [study guide](../study_guide.md) for target timing, limitations and private dashboard access. `protocol_frozen.json` records the analysis choices and source hashes before actual model results were examined. `requirements-study.lock.txt` records the resolved c0 environment.

Checks on c0:

```sh
cd ~/6893/study
~/6893/.venv-duckdb/bin/python -m unittest -v tests_features.py tests_model_study.py
PYTHONPATH="$HOME/6893/pipeline:$HOME/6893/study" ~/6893/.venv-duckdb/bin/python -m unittest -v tests_parallel_bq.py
```

The concurrent query regression lives in `pipeline/tests_parallel_bq.py` in the repository and is copied into the c0 test workspace during deployment. It uses mocked network calls and isolated real DuckDB fixtures.

The final November 2 collection audit passed all 62 source history groups and all 1,488 retained objects. `results/` contains compact exported evidence for the published study, not the runtime database. The full manifest refers to the complete c0 snapshot. `results/verification.json` records 37 passing unit checks, the actual Spark check, and browser verification.
