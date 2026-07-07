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
# Run it without installing anything (recommended). Once published to PyPI:
uvx argo2prefect convert ./argo-manifests -o ./prefect_flows

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

Requires Python 3.9+.

## Quick start

```bash
# Convert a single manifest and print to stdout
argo2prefect convert examples/argo/dag-diamond.yaml

# Convert to a file, executing containers via `docker run`
argo2prefect convert examples/argo/dag-diamond.yaml -o flow.py --runtime docker

# Convert every *.yaml/*.yml in a directory into an output folder.
# The directory is converted as ONE linked project: templateRef /
# workflowTemplateRef resolve across files, and WorkflowTemplate /
# ClusterWorkflowTemplate manifests are emitted once into a shared
# shared_templates.py module that the per-workflow files import.
argo2prefect convert ./argo-manifests -o ./prefect_flows

# Convert AND emit a Prefect Cloud deployment config + runbook
argo2prefect convert ./argo-manifests -o ./prefect_flows \
  --emit-prefect-yaml --source-repo https://github.com/acme/flows

# Inspect a manifest without generating code
argo2prefect inspect examples/argo/cron-backup.yaml
```

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
header, so it is **self-bootstrapping** — `uv` reads it, installs Prefect into an
isolated env, and runs the flow. No `pip install` step:

```bash
uv run flow.py            # one-off local run (uses default parameters)
uv run flow.py --serve    # deploy on the workflow's schedule (Prefect worker)
```

Without `uv`, install the runtime deps yourself and use plain Python:

```bash
pip install "prefect>=3,<4" prefect-shell
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
limits, though, and **cannot run Docker or `kubectl`**, so both `prefect.yaml`
and `DEPLOY.md` explicitly encourage switching to a pool that matches the
workload once things work:

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

| Runtime              | How container/script templates run                                  | Needs                |
| -------------------- | ------------------------------------------------------------------- | -------------------- |
| `docker` *(default)* | `docker run --rm <image> <cmd>` via `prefect-shell`                 | Docker on the worker |
| `shell`              | the command/script runs directly on the Prefect worker host        | the interpreter/CLI on PATH |
| `kubernetes`         | renders a Kubernetes `Job` manifest and `kubectl apply`/`wait`/`logs` | `kubectl` + cluster access |

`resource` templates always use `kubectl`, `http` templates use Python's stdlib
`urllib`, and `suspend` templates map to `time.sleep` (fixed duration) or a
`# TODO` for indefinite pauses.

## Mapping reference

| Argo concept                                   | Prefect output                                              |
| ---------------------------------------------- | ----------------------------------------------------------- |
| `container` / `script` / `resource` / `http`   | `@task` function                                            |
| `dag` / `steps` template                       | `@flow` subflow                                             |
| `spec.entrypoint`                              | top-level `@flow` (the served deployment)                   |
| `dag.tasks[].dependencies`                     | `.submit()` + `wait_for=[...]`                              |
| `steps` (list of groups)                       | sequential groups, parallel `.submit()` within a group      |
| `withItems` / `withParam`                      | `.map()` / `unmapped(...)`                                  |
| `when`                                         | `if` guard (flagged for review)                             |
| `inputs.parameters`                            | task/flow function arguments                                |
| `arguments.parameters` (workflow-level)        | main flow parameters + shared `WORKFLOW_PARAMETERS` dict     |
| `{{inputs.parameters.x}}`                      | the local `x` argument                                      |
| `{{workflow.parameters.x}}`                    | `WORKFLOW_PARAMETERS['x']`                                   |
| `{{tasks.X.outputs.result}}`                   | upstream future `X_fut` (or `X_fut.result()` when embedded) |
| `{{item}}` / `{{item.key}}`                    | the mapped loop variable                                    |
| `retryStrategy.limit`                          | `@task(retries=N)`                                          |
| `CronWorkflow.schedule` / `.timezone`          | `flow.serve(cron=..., timezone=...)`                        |

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
    a_fut = echo.submit(message=f"{WORKFLOW_PARAMETERS['greeting']} from A")
    b_fut = echo.submit(message=f"B saw: {a_fut.result()}", wait_for=[_f for _f in [a_fut] if _f is not None])
    c_fut = echo.submit(message="C", wait_for=[_f for _f in [a_fut] if _f is not None])
    d_fut = echo.submit(message="D", wait_for=[_f for _f in [b_fut, c_fut] if _f is not None])
    return None
```

Committed reference conversions live in [`examples/prefect/`](examples/prefect/).

## What needs manual review

The tool is honest about its limits. It emits warnings (collected in the
generated file's docstring) for things that need a human:

- **Artifacts** (`inputs/outputs.artifacts`): Prefect uses results/storage blocks
  instead of Argo's artifact repository — wire these up yourself.
- **Named output parameters** (`outputs.parameters` with `valueFrom`): generated
  tasks return their stdout; references to named outputs resolve to that stdout.
- **`when` conditions**: translated best-effort and marked with a `# TODO`. Regex
  (`=~`) conditions are left for you to port.
- **Volumes / `volumeClaimTemplates`**: configure on your Prefect work pool.
- **`templateRef` across files**, `containerSet`, `data` templates: emitted as
  stubs that raise `NotImplementedError`.

Workflow parameters are shared through a module-level `WORKFLOW_PARAMETERS` dict
that the entry-point flow updates at runtime; this is correct for in-process task
runners (the default). If you switch to a distributed task runner, pass
parameters explicitly instead.

## Project layout

```
src/argo2prefect/
  models.py        # typed intermediate representation (IR)
  parser.py        # Argo YAML  -> IR
  expressions.py   # {{...}} expression translation
  naming.py        # Argo names -> valid Python identifiers
  generator.py     # IR -> Prefect 3 Python source
  deploy.py        # deployment plans -> Prefect Cloud prefect.yaml + DEPLOY.md
  cli.py           # `argo2prefect` command-line interface
examples/
  argo/            # sample Argo manifests
  prefect/         # committed reference conversions
tests/             # parser, expression, generator, CLI and end-to-end tests
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
