# Changelog

All notable changes to `argo2prefect`. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/) (0.x: minor bumps may include breaking changes,
called out explicitly).

## [0.2.0] — 2026-07-07

First public release: the v2 rebuild (see `REBUILD_PLAN.md`).

### Added
- **`assess` command** — grades every workflow (automatic / review / manual)
  by running the real conversion pipeline in memory; Markdown + JSON + HTML
  fleet reports with per-workflow TODO counts and effort estimates.
- **`verify` command** — imports every generated module in a subprocess to
  prove the output loads before anyone runs it.
- **Project linker** — directories convert as one linked project:
  `templateRef` and `workflowTemplateRef` resolve across files;
  `WorkflowTemplate` / `ClusterWorkflowTemplate` manifests emit once into a
  shared `shared_templates.py` module.
- **Argo semantics**: `depends` expressions, full `retryStrategy` (backoff →
  constant/exponential delays), `activeDeadlineSeconds` → `timeout_seconds`,
  `onExit` → state hooks, `synchronization` → concurrency guards, `memoize` →
  cache policy, `withSequence`, `inline:` templates, CronWorkflow multiple
  schedules + suspend, artifact storage identification (s3/gcs/azure/…).
- **Expression engine**: `{{= expr-lang }}` translation; Argo bare-word
  string-comparison semantics; translated conditions guaranteed to be valid
  Python (untranslatable gates become explicit `if False` + warning).
- `MIGRATION_REPORT.md` on convert: every `TODO(A2P-###)` as a checklist
  with file:line anchors; `--dry-run`; no-clobber by default (`--force`).
- Stable TODO codes (`A2P-1xx`) on every follow-up item; `COVERAGE.md`
  feature matrix.

### Changed
- **Docker runtime** now uses the Docker SDK (was: shelling out to
  `docker run`); scripts stream into the container command with no temp files.
- **Kubernetes runtime** now uses the official Kubernetes client (was:
  `kubectl` shell-outs); Jobs are polled to completion and always deleted.
- Main-flow signatures are typed (int/float inferred from defaults).
- Generated modules are formatted with `ruff format`.
- Python floor raised to 3.10.

### Breaking
- Named output parameters (`tasks.X.outputs.parameters.NAME`) now raise a
  `NotImplementedError` with mapping guidance instead of silently
  substituting stdout (`outputs.result` is unchanged). Silently-wrong data
  was judged worse than a loud failure; see `TODO(A2P-105)`.
- Converting a directory now links it as one project; files consisting only
  of `WorkflowTemplate`s land in `shared_templates.py` rather than getting
  standalone flow files.

## [0.1.0] — 2026-06-30

Internal baseline: single-file conversion of Workflow / WorkflowTemplate /
ClusterWorkflowTemplate / CronWorkflow manifests to Prefect 3 flows, with
`inspect`, `prefect.yaml` + `DEPLOY.md` emission, and shell/docker/kubernetes
(kubectl) runtimes.
