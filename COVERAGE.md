# Argo → Prefect feature coverage

What `argo2prefect` converts automatically, what it converts with a flagged
review item, and what stays manual. Statuses:

- ✅ **automatic** — faithful Prefect equivalent, no follow-up.
- 🔍 **review** — converted, with a stable `TODO(A2P-###)` at the exact spot a
  human should read.
- ✋ **manual** — no faithful equivalent; an explicit stub raises until
  implemented (never silently wrong output).

Grades in `argo2prefect assess` come from running this exact pipeline, so this
table and the tool cannot drift apart.

## Workflow structure

| Argo feature | Prefect equivalent | Status |
|---|---|---|
| `Workflow` / `WorkflowTemplate` / `ClusterWorkflowTemplate` / `CronWorkflow` | `@flow` modules | ✅ |
| `container` template | `@task` (Docker SDK / K8s Job / host shell, per `--runtime`) | ✅ |
| `script` template | `@task` running the inline source | ✅ |
| `dag` + `dependencies` | `.submit()` + `wait_for=` | ✅ |
| `dag` + `depends` (plain `&&`) | dependency edges via `wait_for=` | ✅ |
| `depends` with status gates (`.Failed`, `\|\|`) | edges preserved | 🔍 A2P-102 |
| `steps` (sequential groups, parallel within) | `.submit()` + group barriers | ✅ |
| `templateRef` across files | import from generated `shared_templates.py` | ✅ |
| `workflowTemplateRef` (spec-level) | inherits entrypoint + arguments | ✅ |
| `inline:` templates | hoisted into named templates | ✅ |
| Unresolvable references | explicit stub | ✋ A2P-108 |
| `http` template | `urllib.request` task | ✅ |
| `resource` template | `kubectl <action>` task | ✅ |
| `suspend` (with duration) | `time.sleep` task | ✅ |
| `suspend` (indefinite) | flagged | 🔍 A2P-107 |
| `containerSet` / `data` / plugin templates | stub | ✋ A2P-112 |

## Parameters, loops, conditions

| Argo feature | Prefect equivalent | Status |
|---|---|---|
| workflow `arguments.parameters` | typed flow parameters (int/float inferred) | ✅ |
| `inputs.parameters` (+ defaults) | task parameters | ✅ |
| `withItems` / `withParam` | `.map()` / `unmapped()` | ✅ |
| `withSequence` | `.map()` over generated sequence | ✅ |
| `when` (string comparisons, `&&`/`\|\|`) | `if` guard, Argo string semantics | 🔍 A2P-101 |
| `when` with `{{= expr-lang }}` | translated (refs, literals, asInt/asFloat, common sprig) | 🔍 A2P-101 |
| untranslatable conditions | explicit `if False` | 🔍 A2P-110 |
| `{{tasks.X.outputs.result}}` (stdout) | upstream future `.result()` | ✅ |
| named output parameters (`valueFrom` files) | loud helper, raises until mapped | ✋ A2P-105 |
| `{{workflow.name}}` / `{{workflow.uid}}` | `prefect.runtime.flow_run` | ✅ |

## Behavior

| Argo feature | Prefect equivalent | Status |
|---|---|---|
| `retryStrategy.limit` | `retries=` | ✅ |
| `retryStrategy.backoff` (duration/factor) | `retry_delay_seconds` / `exponential_backoff` | ✅ |
| `retryPolicy` ≠ OnFailure, `maxDuration` | flagged in header | 🔍 |
| `activeDeadlineSeconds` (template + workflow) | `timeout_seconds=` | ✅ |
| `onExit` exit handler | `on_completion=` + `on_failure=` state hooks | ✅ |
| task/step-level `onExit` | flagged | 🔍 |
| `synchronization` mutex/semaphore | `with concurrency(...)` + limit-creation guidance | 🔍 |
| `memoize` | `cache_policy=INPUTS` + `cache_expiration` | 🔍 |
| CronWorkflow `schedule(s)` + `timezone` | deployment schedules in `prefect.yaml` | ✅ |
| CronWorkflow `suspend` | `paused=True` / `active: false` schedules | ✅ |
| CronWorkflow `concurrencyPolicy` / `startingDeadlineSeconds` | flagged | 🔍 |
| artifacts (s3/gcs/azure/http/git/…) | identified with location, anchored TODOs | ✋ A2P-103/104 |
| volumes / volumeClaimTemplates | work-pool infrastructure concern | 🔍 |
| secret/configmap env (`valueFrom`) | flagged marker | 🔍 |

## Execution runtimes

| `--runtime` | How container/script work runs | Needs on the worker |
|---|---|---|
| `docker` (default) | Docker SDK: `containers.run(..., remove=True)`, combined logs | Docker daemon |
| `kubernetes` | official K8s client: Job created, polled, logs fetched, always deleted | cluster credentials |
| `shell` | `prefect-shell` on the worker host (image ignored) | the commands themselves |
