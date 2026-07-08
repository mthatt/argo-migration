# argo2prefect

Migrate [Argo Workflows](https://argo-workflows.readthedocs.io/) manifests into
runnable [Prefect 3](https://docs.prefect.io/) Python flows.

`argo2prefect` parses your Argo YAML (`Workflow`, `WorkflowTemplate`,
`ClusterWorkflowTemplate`, `CronWorkflow`), builds a typed intermediate
representation, and generates clean, idiomatic Prefect code — DAG dependencies,
parallel steps, loops, parameters, schedules, retries and all. Where Argo has no
faithful Prefect equivalent, the tool emits a clear `# TODO` and a warning rather
than guessing, so you always know what still needs a human.

---

## Why

Argo Workflows are declared in Kubernetes YAML and run as pods. Prefect flows are
plain Python, run anywhere, and come with first-class observability, retries,
caching and a UI. Moving a large fleet of Argo manifests by hand is slow and
error-prone. This tool does 80–90% of the work and flags the rest.

## Installation

The lowest-friction way is [`uv`](https://docs.astral.sh/uv/) — no manual venv,
and it bootstraps Python for you:

```bash
# Run it without installing anything (recommended):
uvx argo2prefect assess ./argo-manifests

# Or install it as a persistent CLI tool:
uv tool install argo2prefect
```

Prefer `pip`/`pipx`? Both work too:

```bash
pipx install argo2prefect          # isolated CLI
pip install argo2prefect           # into the current environment
```

From source (this repo):

```bash
uvx --from . argo2prefect --help        # run straight from a checkout
pip install -e ".[dev,generated]"       # editable install + test/runtime deps
```

Requires Python 3.10+.

## The migration journey

A migration runs in four steps — **assess → convert → verify → deploy** — and
the CLI has a command for each:

```bash
# 1. ASSESS: how big is this migration? No code is written. Every workflow is
#    graded automatic / review / manual by running the real conversion
#    pipeline in memory, with a fleet report in Markdown + JSON + HTML.
argo2prefect assess ./argo-manifests -o ./assessment

# 2. CONVERT: the directory is converted as ONE linked project — templateRef /
#    workflowTemplateRef resolve across files, WorkflowTemplate manifests are
#    emitted once into shared_templates.py, and MIGRATION_REPORT.md
#    consolidates every remaining TODO with file:line anchors.
argo2prefect convert ./argo-manifests -o ./prefect_flows

# 3. VERIFY: prove every generated module imports before anyone runs it.
argo2prefect verify ./prefect_flows

# 4. DEPLOY: emit a Prefect Cloud prefect.yaml + step-by-step runbook.
argo2prefect convert ./argo-manifests -o ./prefect_flows --force \
  --emit-prefect-yaml --source-repo https://github.com/acme/flows
```

Also useful:

```bash
argo2prefect convert workflow.yaml                  # single manifest to stdout
argo2prefect convert ./manifests -o ./flows --dry-run   # see what would be written
argo2prefect inspect examples/argo/cron-backup.yaml # quick manifest summary
```

Existing output files are never overwritten unless you pass `--force`.

See [COVERAGE.md](COVERAGE.md) for the full Argo-feature → Prefect-equivalent
matrix and each feature's automation status.

`inspect` prints a quick summary:

```
CronWorkflow: nightly-backup
  entrypoint: backup
  schedule:   0 2 * * * (America/New_York)
  parameters: database_url
  templates:  1
    - backup [container] (image: postgres:16)
```

### Running the generated flow

Every generated file carries a [PEP 723](https://peps.python.org/pep-0723/)
header listing exactly what it needs (Prefect, plus the Docker or Kubernetes
SDK depending on `--runtime`), so it is **self-bootstrapping** — `uv` reads
it, installs the deps into an isolated env, and runs the flow. No
`pip install` step:

```bash
uv run flow.py            # one-off local run (uses default parameters)
uv run flow.py --serve    # deploy on the workflow's schedule (Prefect worker)
```

Without `uv`, install the deps from the file's PEP 723 header yourself, e.g.
for the default docker runtime:

```bash
pip install "prefect>=3,<4" "docker>=7"
python flow.py            # one-off run;  add --serve to deploy on a schedule
```

## Deploying to Prefect Cloud

`--serve` above is great for a quick local test, but it ties the schedule to a
process on your machine. For a real handoff, `argo2prefect` can generate a
[**Prefect Cloud**](https://docs.prefect.io/) deployment config so your flows
run on a schedule *in the client's workspace* — with a step-by-step runbook.

Add `--emit-prefect-yaml` to any `convert`:

```bash
argo2prefect convert ./argo-manifests -o ./prefect_flows \
  --emit-prefect-yaml \
  --work-pool my-managed-pool \
  --source-repo https://github.com/acme/flows
```

Alongside the `*_flow.py` files this writes two things into the output folder:

- **`prefect.yaml`** — one deployment per workflow (entrypoint, schedule,
  parameters), wired to a work pool and a `pull` step that fetches the code.
- **`DEPLOY.md`** — the exact commands to get from zero to scheduled runs.

### The end-to-end client experience

1. **Convert** with `--emit-prefect-yaml` (above).
2. **Log in** — this, not `prefect.yaml`, is what binds the deployments to their
   account and workspace:
   ```bash
   prefect cloud login -k <API_KEY>
   ```
3. **Create the work pool** (one time):
   ```bash
   prefect work-pool create my-managed-pool --type prefect:managed
   ```
4. **Deploy** every flow:
   ```bash
   prefect deploy --all
   ```

The flows now appear in the Prefect Cloud UI, each on its schedule.

### Start on Managed, then pick the right pool

The default work pool type is **Prefect Managed** (`prefect:managed`): Prefect
hosts the compute, so your client stands up **no worker and no infrastructure** —
the fastest path to a first green run. Managed compute has CPU/memory/time
limits, though, and has **no Docker daemon or Kubernetes cluster access**, so
both `prefect.yaml` and `DEPLOY.md` explicitly encourage switching to a pool
that matches the workload once things work:

| Workload | Recommended pool | Worker/infra |
| -------- | ---------------- | ------------ |
| Light Python (get started) | `prefect:managed` *(default)* | none — Prefect-hosted |
| Closest to Argo | `kubernetes` | in-cluster worker |
| Serverless containers | `ecs:push` / `cloud-run:push` / `azure-container-instance:push` | none (cloud-credentials block) |
| Simple self-hosted | `process` / `docker` | a worker you run |

Switching is a one-line edit to `work_pool.name` in `prefect.yaml` plus a
`prefect work-pool create`, then `prefect deploy --all` again.

### Code delivery (important)

A Cloud worker runs off-machine, so it must **fetch your flow code**. Pass
`--source-repo <git-url>` to emit a `git_clone` pull step (recommended). Without
it, `prefect.yaml` falls back to a local directory and both files flag — with a
`# TODO` and a checklist item — that a Managed/serverless pool cannot read it.

> **Flags:** `--emit-prefect-yaml`, `--work-pool NAME` (default `managed-pool`),
> `--work-pool-type TYPE` (default `prefect:managed`), `--source-repo URL`.
> Emitting the config requires a file/directory output (`-o`).

## Execution runtimes

Argo runs every template in a container on Kubernetes. When generating Prefect
code you choose how that work should execute with `--runtime`:

| Runtime              | How container/script templates run                                  | Needs on the worker  |
| -------------------- | ------------------------------------------------------------------- | -------------------- |
| `docker` *(default)* | Docker SDK: `containers.run(...)` with env, auto-remove, combined logs | a Docker daemon |
| `shell`              | the command/script runs directly on the Prefect worker host (image ignored) | the interpreter/CLI on PATH |
| `kubernetes`         | official Kubernetes client: a `Job` is created, polled to completion, logs returned, and **always deleted** | kubeconfig or in-cluster credentials |

The kubernetes runtime reads its namespace from `$A2P_NAMESPACE` (default
`default`). `resource` templates always use `kubectl`, `http` templates use
Python's stdlib `urllib`, and `suspend` templates map to `time.sleep` (fixed
duration) or a `# TODO` for indefinite pauses.

## Mapping reference

The highlights (the complete matrix with per-feature automation status is
[COVERAGE.md](COVERAGE.md)):

| Argo concept                                   | Prefect output                                              |
| ---------------------------------------------- | ----------------------------------------------------------- |
| `container` / `script` / `resource` / `http`   | `@task` function                                            |
| `dag` / `steps` template                       | `@flow` subflow                                             |
| `spec.entrypoint`                              | top-level `@flow` (the served deployment)                   |
| `dag.tasks[].dependencies` / `depends`         | `.submit()` + `wait_for=[...]` (status gates flagged)       |
| `templateRef` / `workflowTemplateRef`          | import from the generated `shared_templates.py`             |
| `steps` (list of groups)                       | sequential groups, parallel `.submit()` within a group      |
| `withItems` / `withParam` / `withSequence`     | `.map()` / `unmapped(...)`                                  |
| `when` (incl. `{{= expr-lang }}`)              | `if` guard (flagged for review)                             |
| `inputs.parameters`                            | task/flow function arguments                                |
| `arguments.parameters` (workflow-level)        | typed main-flow parameters + shared `WORKFLOW_PARAMETERS` dict |
| `{{inputs.parameters.x}}`                      | the local `x` argument                                      |
| `{{workflow.parameters.x}}`                    | `WORKFLOW_PARAMETERS['x']`                                   |
| `{{tasks.X.outputs.result}}`                   | upstream future `X_fut` (or `X_fut.result()` when embedded) |
| `{{tasks.X.outputs.parameters.NAME}}`          | loud helper that **raises until you map it** (`A2P-105`)    |
| `{{item}}` / `{{item.key}}`                    | the mapped loop variable                                    |
| `retryStrategy` (limit + backoff)              | `@task(retries=, retry_delay_seconds=/exponential_backoff)` |
| `activeDeadlineSeconds`                        | `timeout_seconds=` on the task/flow                         |
| `onExit`                                       | `on_completion=` + `on_failure=` state hooks                |
| `synchronization` (mutex/semaphore)            | `with concurrency(...)` guard (+ limit-creation guidance)   |
| `memoize`                                      | `cache_policy=INPUTS` + `cache_expiration=`                 |
| `CronWorkflow.schedule(s)` / `.timezone` / `.suspend` | `flow.serve(cron=...)` / schedules in `prefect.yaml` (paused if suspended) |

### Example

Input (`examples/argo/dag-diamond.yaml`), a classic diamond DAG:

```yaml
templates:
  - name: diamond
    dag:
      tasks:
        - { name: A, template: echo, arguments: { parameters: [{ name: message, value: "{{workflow.parameters.greeting}} from A" }] } }
        - { name: B, template: echo, dependencies: [A], arguments: { parameters: [{ name: message, value: "B saw: {{tasks.A.outputs.result}}" }] } }
        - { name: C, template: echo, dependencies: [A], arguments: { parameters: [{ name: message, value: "C" }] } }
        - { name: D, template: echo, dependencies: [B, C], arguments: { parameters: [{ name: message, value: "D" }] } }
```

Output (see `examples/prefect/dag-diamond_flow.py`):

```python
@flow(name="diamond")
def diamond():
    """Argo dag template 'diamond'."""
    # A -> template 'echo'
    a_fut = echo.submit(message=f"{WORKFLOW_PARAMETERS['greeting']} from A")
    # B -> template 'echo'
    b_fut = echo.submit(
        message=f"B saw: {a_fut.result()}", wait_for=[_f for _f in [a_fut] if _f is not None]
    )
    # C -> template 'echo'
    c_fut = echo.submit(message="C", wait_for=[_f for _f in [a_fut] if _f is not None])
    # D -> template 'echo'
    d_fut = echo.submit(message="D", wait_for=[_f for _f in [b_fut, c_fut] if _f is not None])
    return None
```

Committed reference conversions live in [`examples/prefect/`](examples/prefect/).

## What needs manual review

The tool is honest about its limits. Every follow-up item carries a stable
`TODO(A2P-###)` code in the generated code, is collected in the file's
docstring, and lands in `MIGRATION_REPORT.md` with a `file:line` anchor.
The recurring ones:

- **Artifacts** (`inputs/outputs.artifacts`): storage is identified
  (s3/gcs/azure/http/git/…) and each artifact gets an anchored TODO with its
  location, but fetching/publishing is yours to wire — Prefect uses
  results/storage blocks instead of Argo's artifact repository.
- **Named output parameters** (`outputs.parameters` with `valueFrom`): Argo
  read these from files inside the container; only stdout is captured. A
  reference to one generates a helper that **raises with mapping guidance**
  until you port it — a loud failure instead of silently-wrong data.
- **`when` conditions**: translated (including common `{{= expr-lang }}`) and
  marked for review. Anything untranslatable — regex `=~`, unknown functions —
  becomes an explicit `if False` with a warning, never invalid code.
- **Secret/configmap env vars** (`valueFrom`): flagged with a placeholder;
  wire up Prefect Secret blocks or worker env yourself.
- **Volumes / `volumeClaimTemplates`**: configure on your Prefect work pool.
- **`templateRef` to a manifest *outside* the conversion input**,
  `containerSet`, `data`, and plugin templates: emitted as stubs that raise
  `NotImplementedError`. (`templateRef` *within* the input resolves via the
  shared module — include the whole fleet in one run.)

Workflow parameters are shared through a module-level `WORKFLOW_PARAMETERS` dict
that the entry-point flow updates at runtime; this is correct for in-process task
runners (the default). If you switch to a distributed task runner, pass
parameters explicitly instead.

## Project layout

```
src/argo2prefect/
  models.py        # typed intermediate representation (IR)
  parser.py        # Argo YAML  -> IR
  project.py       # multi-manifest loading + cross-file templateRef linking
  expressions.py   # {{...}} / {{= expr-lang }} expression translation
  naming.py        # Argo names -> valid Python identifiers
  generator.py     # IR -> Prefect 3 Python source (per-runtime backends)
  assess.py        # fleet grading + assessment / migration reports
  todos.py         # stable TODO(A2P-###) code catalogue
  deploy.py        # deployment plans -> Prefect Cloud prefect.yaml + DEPLOY.md
  cli.py           # assess / convert / verify / inspect commands
examples/
  argo/            # sample Argo manifests
  prefect/         # committed reference conversions (regenerated from the tool)
tests/
  corpus/          # 200+ real-world manifests (upstream argo-workflows examples)
  golden/          # pinned generator output for the curated examples
  ...              # parser, expression, generator, CLI and end-to-end tests
```

## Development

```bash
uv run --extra dev --extra generated pytest   # with uv
# or
pip install -e ".[dev,generated]" && pytest
```

The end-to-end tests generate flows, import them and actually execute them with
Prefect (using host-available `echo`/`sh`, no Docker or cluster needed); they are
skipped if Prefect is not installed. One test additionally runs a generated flow
via `uv run` to verify the PEP 723 self-bootstrapping path (skipped if `uv` is not
on `PATH`).

## License

MIT
