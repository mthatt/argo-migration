# argo2prefect v2 — Rebuild Plan

A ground-up rebuild of `argo2prefect` with two goals: **robustness** (correct
output on real-world Argo fleets, not just curated examples) and **client
experience** (an assessment-first workflow that produces deliverables a
migration engagement can be run from).

The v1 pipeline shape (YAML → typed IR → code generator) is sound and is
retained. Each stage is rebuilt with wider Argo coverage, and the whole tool is
re-anchored around a measurable corpus so progress and regressions are visible.

---

## Verdict on v1

### Worth keeping

- **Three-stage pipeline** (parser → IR → generator): clean separation, easy to
  unit test.
- **"Warn, don't guess" philosophy**: `# TODO` + warning instead of
  silently-wrong code is exactly right for client work.
- **PEP 723 script metadata** (`uv run flow.py` just works) — great first-run
  experience.
- **Deploy artifacts** (`prefect.yaml` + `DEPLOY.md`) and their honest guidance
  about Managed pool limits.
- The existing test suite and examples become the v2 regression floor.

### Correctness gaps

1. **No cross-file template resolution.** Each manifest converts in isolation,
   so `templateRef` into a shared `WorkflowTemplate` becomes a
   `NotImplementedError` stub. Real fleets centralize logic in shared
   templates — this is the single biggest gap.
2. **`depends` is not parsed.** Only `dependencies` is read. DAGs using
   `depends: "A && (B.Succeeded || C.Failed)"` silently lose all their edges —
   no warning, wrong output.
3. **Output parameters are broken.** `tasks.X.outputs.parameters.NAME`
   resolves to the whole `X_fut.result()` regardless of the named output;
   `valueFrom.path`/`jsonPath` outputs are dropped. Correct only by accident
   for `outputs.result` (stdout).
4. **Secret/configmap env vars render a fake placeholder** — `valueFrom` env
   becomes the literal string `{{env.valueFrom.NAME}}` in generated code
   instead of a Prefect Secret block or a loud TODO.
5. **Retry semantics reduced to a count.** `retryStrategy.limit` only;
   `backoff` (→ `retry_delay_seconds`), `retryPolicy`, and
   `activeDeadlineSeconds` (→ `timeout_seconds`) are dropped despite direct
   Prefect equivalents.
6. **Missing features with clean Prefect equivalents:** `onExit`/lifecycle
   hooks (→ `on_failure`/`on_completion`), `withSequence` (→ `.map(range())`),
   `synchronization` (→ global concurrency limits), `parallelism` (→ task
   runner limits), `memoize` (→ `cache_key_fn`), multiple cron schedules and
   `concurrencyPolicy`, `{{=expr}}` expr-lang, sprig functions.
7. **Runtime backends shell out** to stringly-built `docker run` /
   `kubectl apply` via `ShellOperation`: quoting hazards, no log streaming,
   failed k8s Jobs leak. `prefect-docker` / `prefect-kubernetes` are the
   idiomatic replacements.

### Client-UX gaps

- **No fleet assessment.** The first deliverable of an engagement is "you have
  240 workflows; 180 convert cleanly, 45 need review, 15 are manual — here's
  the breakdown." Today warnings scroll by on stderr.
- **No validation of output** — generated code is not even `ast.parse`-checked,
  nor formatted.
- Silent overwrites, no dry-run, no config file, no JSON output for CI, plain
  argparse output with no progress for a 200-file conversion.

### Engineering gaps

- No CI, no lint/format/type-check configuration.
- No real-world test corpus (upstream argo-workflows ships ~200 example
  manifests — a free acceptance suite).
- Deprecated license metadata in `pyproject.toml`; Python 3.9 floor (EOL
  October 2025).

---

## Phases

### Phase 0 — Scope + corpus (foundation) ✅ complete

> Status: corpus vendored (211 manifests; 204/206 workflow manifests pass the
> parse → generate → valid-syntax pipeline, 2 known `{{=expr}}` failures in
> the baseline), harness + golden snapshots in place, CI live, packaging
> modernized (Python ≥3.10, SPDX license, ruff + mypy clean).

Make the rebuild measurable before changing behavior.

- Vendor a test corpus under `tests/corpus/`: the upstream
  argo-workflows examples (Apache-2.0, provenance documented), plus sanitized
  client manifests as they become available.
- Corpus harness: every manifest must (a) parse without crashing, (b) generate
  code that `ast.parse`s, (c) contain no silent feature drops. Files that fail
  are tracked in a **baseline** file: known failures are allowed, *new*
  failures break the build, and newly-passing files must be promoted. Corpus
  pass-rate is the progress metric for every later phase.
- Golden snapshots for the curated `examples/` so generator output changes are
  always explicit diffs.
- CI (GitHub Actions): pytest on Python 3.10–3.13, ruff lint + format check,
  mypy.
- Packaging hygiene: SPDX license expression, drop deprecated classifier,
  raise floor to Python 3.10, add dev tooling (ruff, mypy, types-PyYAML).

### Phase 1 — Project loader + linker (biggest correctness win) ✅ complete

> Status: directory conversion now loads all manifests as one linked project
> (`argo2prefect.project`). `templateRef` resolves across files (and across
> documents in one file), `workflowTemplateRef` inherits entrypoint +
> arguments, and WorkflowTemplate/ClusterWorkflowTemplate manifests emit once
> into `shared_templates.py`, imported by per-workflow modules. Pulled
> forward from Phase 2: `depends` expressions now contribute dependency
> edges (previously silently dropped); non-trivial gating is flagged with a
> TODO. On the corpus converted as one project, every named templateRef
> resolves (was 10 stubbed modules; the 3 remaining stubs are nameless
> Argo `inline:` templates, a Phase 2 feature).

- Replace file-at-a-time conversion with a **project model**: load all
  manifests in scope, build a registry of `WorkflowTemplate` /
  `ClusterWorkflowTemplate`, resolve `templateRef`s (including
  cross-namespace), then generate.
- Shared templates emit as a shared Python module that per-workflow files
  import — mirroring how the client organized their Argo code.

### Phase 2 — IR + parser v2

- Model the missing semantics: `depends` expressions, `withSequence`, full
  `retryStrategy`, timeouts, `onExit`/hooks, `synchronization`, `memoize`,
  CronWorkflow extras (multiple schedules, `concurrencyPolicy`, `suspend`),
  artifact storage specs (S3/GCS/HTTP identified, not just counted).
- Pydantic IR models: validation, precise error locations, self-documenting
  schema.
- A real tokenizer for `{{...}}` / `{{=...}}` expressions with a
  sprig-function translation table for the common cases; everything else
  flagged with a stable TODO id.

### Phase 3 — Generator v2

- Emitter registry (template-kind × runtime backend) instead of one
  monolithic module.
- Native `prefect-docker` / `prefect-kubernetes` backends replacing
  shell-outs.
- Named output parameters via small typed result objects instead of raw
  stdout strings.
- Parameter type inference (int/bool/JSON defaults instead of
  everything-is-str).
- Every generated file post-processed with `ruff format` and verified with
  `ast.parse`.
- Structured TODOs with stable ids (`# TODO(A2P-107): ...`) the report can
  link to.

### Phase 4 — Client-facing layer

- `argo2prefect assess ./manifests`: no code generated; fleet report with
  workflow count, feature histogram, per-workflow fidelity grade
  (auto / review / manual), estimated effort. Markdown + HTML + JSON.
- `argo2prefect convert`: progress output, dry-run, no-clobber by default,
  `--json` for CI, and a consolidated `MIGRATION_REPORT.md` with file:line
  links for every TODO.
- `argo2prefect verify`: import each generated flow so output is provably
  loadable before the client touches it.
- Docs rewritten around the assess → convert → verify → deploy journey, plus
  a feature-coverage matrix (Argo feature → Prefect equivalent → automation
  status) that doubles as engagement collateral.

### Phase 5 — Release

- PyPI publish workflow, changelog, versioned docs, examples gallery.

---

## Sequencing rules

- v2 is built in this repo; the existing test suite and examples stay green
  throughout — "from scratch" internals with today's behavior as the
  acceptance floor.
- Corpus pass-rate is reported in CI on every PR; it may only go up.
- Phases 0–1 first (they fix what is most likely to fail on a real client
  fleet); Phase 4's `assess` command is the next priority after — it turns the
  tool from a code generator into a migration methodology.
