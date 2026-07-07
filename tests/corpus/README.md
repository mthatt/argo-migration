# Test corpus

Real-world Argo Workflows manifests used by `tests/test_corpus.py` to measure
converter coverage. Every file here must parse and generate syntactically
valid Prefect code, except those listed in `baseline.json` (known failures).

The corpus pass-rate is the progress metric for the v2 rebuild — see
`REBUILD_PLAN.md` at the repo root. New failures break the build; files that
start passing must be removed from the baseline (run
`python tests/update_corpus_baseline.py`).

## Sources

- `argo-examples/` — the `examples/` directory of
  [argoproj/argo-workflows](https://github.com/argoproj/argo-workflows)
  at commit `37d0dd7fae84ae9a895e63af9be55d51062e0e98`, Apache License 2.0
  (see `argo-examples/LICENSE`). Subdirectory paths are flattened into
  filenames with `__`.

Add sanitized client manifests as additional subdirectories, each with a
README noting provenance.
