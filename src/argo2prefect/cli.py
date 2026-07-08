"""Command-line interface for argo2prefect.

Examples
--------
Convert a single manifest to stdout::

    argo2prefect convert workflow.yaml

Convert to a file, running containers via docker::

    argo2prefect convert workflow.yaml -o flow.py --runtime docker

Convert every manifest in a directory into an output folder::

    argo2prefect convert ./argo-manifests -o ./prefect_flows

Inspect what a manifest contains without generating code::

    argo2prefect inspect workflow.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__
from .assess import (
    assess_project,
    render_html,
    render_json,
    render_markdown,
    render_migration_report,
)
from .deploy import DeployOptions, render_deploy_md, render_prefect_yaml
from .generator import (
    DeploymentPlan,
    GeneratorOptions,
    format_code,
    generate_module,
    generate_project,
)
from .parser import ParseError, parse_workflows
from .project import load_project, load_project_from_text


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 1
    try:
        return args.handler(args)
    except ParseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="argo2prefect",
        description="Migrate Argo Workflows manifests into Prefect 3 flows.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    convert = sub.add_parser("convert", help="Convert Argo YAML to Prefect Python.")
    convert.add_argument(
        "input",
        help="Path to an Argo manifest, a directory of manifests, or '-' for stdin.",
    )
    convert.add_argument(
        "-o",
        "--output",
        help="Output .py file, output directory (for directory input), or '-' for stdout.",
    )
    convert.add_argument(
        "--runtime",
        choices=["shell", "docker", "kubernetes"],
        default="docker",
        help="How generated tasks execute container/script work (default: docker).",
    )
    convert.add_argument(
        "--no-serve",
        action="store_true",
        help="Omit the __main__ run/serve block entirely.",
    )
    convert.add_argument(
        "--emit-prefect-yaml",
        action="store_true",
        help="Also write a Prefect Cloud prefect.yaml + DEPLOY.md deployment runbook.",
    )
    convert.add_argument(
        "--work-pool",
        default="managed-pool",
        help="Work pool name referenced in prefect.yaml (default: managed-pool).",
    )
    convert.add_argument(
        "--work-pool-type",
        default="prefect:managed",
        help="Work pool type for the DEPLOY.md create command (default: prefect:managed).",
    )
    convert.add_argument(
        "--source-repo",
        help="Git URL the Prefect Cloud worker clones to fetch flow code "
        "(recommended for Managed/serverless pools).",
    )
    convert.add_argument(
        "--no-script-metadata",
        action="store_true",
        help="Omit the PEP 723 header used by `uv run flow.py` to auto-install deps.",
    )
    convert.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress the migration warning summary on stderr.",
    )
    convert.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be written without writing anything.",
    )
    convert.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files (refused by default).",
    )
    convert.set_defaults(handler=_cmd_convert)

    assess = sub.add_parser(
        "assess",
        help="Grade a fleet of manifests (automatic/review/manual) without writing code.",
    )
    assess.add_argument("input", help="Manifest, directory of manifests, or '-' for stdin.")
    assess.add_argument(
        "-o",
        "--output",
        help="Directory to write ASSESSMENT.md / .json / .html into (default: markdown to stdout).",
    )
    assess.add_argument(
        "--runtime",
        choices=["shell", "docker", "kubernetes"],
        default="docker",
        help="Runtime assumed during assessment (default: docker).",
    )
    assess.set_defaults(handler=_cmd_assess)

    verify = sub.add_parser(
        "verify",
        help="Import every generated flow module in a directory to prove it loads.",
    )
    verify.add_argument("directory", help="Directory of generated *.py flow modules.")
    verify.set_defaults(handler=_cmd_verify)

    inspect = sub.add_parser("inspect", help="Summarise a manifest without generating code.")
    inspect.add_argument("input", help="Path to an Argo manifest or '-' for stdin.")
    inspect.set_defaults(handler=_cmd_inspect)

    return parser


def _cmd_convert(args: argparse.Namespace) -> int:
    options = GeneratorOptions(
        runtime=args.runtime,
        serve=not args.no_serve,
        script_metadata=not args.no_script_metadata,
    )

    to_file = bool(args.output and args.output != "-")
    if args.emit_prefect_yaml and not to_file:
        print(
            "error: --emit-prefect-yaml needs a file/directory output; pass -o <path>.",
            file=sys.stderr,
        )
        return 2

    if args.input != "-" and Path(args.input).is_dir():
        return _convert_directory(Path(args.input), args, options)

    text = _read_input(args.input)
    code, plans = _convert_text(text, options, args)

    if to_file:
        out_path = Path(args.output)
        if args.dry_run:
            print(f"[dry-run] would write {out_path}", file=sys.stderr)
            return 0
        if out_path.exists() and not args.force:
            print(
                f"error: {out_path} already exists; pass --force to overwrite.",
                file=sys.stderr,
            )
            return 2
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(code, encoding="utf-8")
        print(f"Wrote {out_path}", file=sys.stderr)
        if args.emit_prefect_yaml:
            for plan in plans:
                plan.entrypoint_file = out_path.name
            _emit_deploy_artifacts(out_path.parent, plans, args)
    else:
        sys.stdout.write(code)
    return 0


def _convert_directory(src: Path, args: argparse.Namespace, options: GeneratorOptions) -> int:
    """Convert a directory as one linked project.

    All manifests are loaded up front so ``templateRef`` /
    ``workflowTemplateRef`` resolve across files: WorkflowTemplate and
    ClusterWorkflowTemplate manifests are emitted once into
    ``shared_templates.py`` and the per-workflow modules import from it.
    """
    project = load_project([src])
    if not project.files and not project.skipped:
        print(f"error: no .yaml/.yml files found in {src}", file=sys.stderr)
        return 2

    for name, reason in project.skipped:
        print(f"skip {name}: {reason}", file=sys.stderr)

    try:
        out = generate_project(project, options)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    out_dir = Path(args.output) if args.output and args.output != "-" else src

    if args.dry_run:
        for filename in out.files:
            print(f"[dry-run] would write {out_dir / filename}", file=sys.stderr)
        print(f"[dry-run] would write {out_dir / 'MIGRATION_REPORT.md'}", file=sys.stderr)
        if args.emit_prefect_yaml:
            print(f"[dry-run] would write {out_dir / 'prefect.yaml'} + DEPLOY.md", file=sys.stderr)
        return 0

    clobbered = [f for f in out.files if (out_dir / f).exists()]
    if clobbered and not args.force:
        listing = ", ".join(clobbered[:5]) + ("..." if len(clobbered) > 5 else "")
        print(
            f"error: output file(s) already exist in {out_dir}: {listing}\n"
            "Pass --force to overwrite, or -o <fresh-dir>.",
            file=sys.stderr,
        )
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    for filename, code in out.files.items():
        out_path = out_dir / filename
        out_path.write_text(code, encoding="utf-8")
        print(f"Wrote {out_path}", file=sys.stderr)

    report_path = out_dir / "MIGRATION_REPORT.md"
    report_path.write_text(render_migration_report(out.files, out.warnings), encoding="utf-8")
    print(f"Wrote {report_path} (consolidated TODO checklist)", file=sys.stderr)

    libraries = len(project.libraries)
    if libraries:
        print(
            f"Linked {libraries} shared template librar{'y' if libraries == 1 else 'ies'} "
            "into shared_templates.py.",
            file=sys.stderr,
        )
    if not args.quiet:
        _print_warnings([wf for file in project.files for wf in file.workflows])
        for warning in out.warnings:
            print(f"  - {warning}", file=sys.stderr)
    print(
        f"Converted {len(out.files)} module(s) from {len(project.files)} manifest file(s).",
        file=sys.stderr,
    )
    if args.emit_prefect_yaml and out.plans:
        _emit_deploy_artifacts(out_dir, out.plans, args)
    return 0


def _convert_text(
    text: str, options: GeneratorOptions, args: argparse.Namespace
) -> tuple[str, list[DeploymentPlan]]:
    workflows = parse_workflows(text)
    code, plans = generate_module(workflows, options)
    if not args.quiet:
        _print_warnings(workflows)
    return format_code(code), plans


def _emit_deploy_artifacts(
    out_dir: Path, plans: list[DeploymentPlan], args: argparse.Namespace
) -> None:
    opts = DeployOptions(
        work_pool=args.work_pool,
        work_pool_type=args.work_pool_type,
        source_repo=args.source_repo,
        runtime=args.runtime,
        project_name=out_dir.name or "argo-flows",
    )
    yaml_path = out_dir / "prefect.yaml"
    md_path = out_dir / "DEPLOY.md"
    yaml_path.write_text(render_prefect_yaml(plans, opts), encoding="utf-8")
    md_path.write_text(render_deploy_md(plans, opts), encoding="utf-8")
    print(f"Wrote {yaml_path}", file=sys.stderr)
    print(f"Wrote {md_path} (Prefect Cloud deployment runbook)", file=sys.stderr)
    if opts.runtime_needs_richer_pool and not args.quiet:
        print(
            f"note: --runtime {args.runtime} cannot run on the default Managed work "
            "pool; see DEPLOY.md to switch pools.",
            file=sys.stderr,
        )


def _cmd_assess(args: argparse.Namespace) -> int:
    options = GeneratorOptions(runtime=args.runtime, serve=False)
    if args.input == "-":
        project = load_project_from_text(_read_input("-"))
    else:
        path = Path(args.input)
        if not path.exists():
            print(f"error: {path} does not exist", file=sys.stderr)
            return 2
        project = load_project([path])
    if not project.files:
        print("error: no workflow manifests found to assess.", file=sys.stderr)
        return 2

    assessment = assess_project(project, options)

    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, renderer in (
            ("ASSESSMENT.md", render_markdown),
            ("ASSESSMENT.json", render_json),
            ("ASSESSMENT.html", render_html),
        ):
            (out_dir / name).write_text(renderer(assessment), encoding="utf-8")
            print(f"Wrote {out_dir / name}", file=sys.stderr)
    else:
        sys.stdout.write(render_markdown(assessment))

    counts = assessment.counts
    print(
        f"Assessed {len(assessment.workflows)} workflow(s): "
        f"{counts['automatic']} automatic, {counts['review']} review, "
        f"{counts['manual']} manual.",
        file=sys.stderr,
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    """Import every generated module in a directory, proving it loads.

    Runs each import in a subprocess with the directory on sys.path (so
    shared_templates imports resolve) and the __main__ guard intact (so
    nothing executes).
    """
    import subprocess

    directory = Path(args.directory)
    modules = sorted(p for p in directory.glob("*.py") if not p.name.startswith("_"))
    if not modules:
        print(f"error: no .py modules found in {directory}", file=sys.stderr)
        return 2

    failures = 0
    for module in modules:
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys, importlib\n"
                    f"sys.path.insert(0, {str(directory.resolve())!r})\n"
                    f"importlib.import_module({module.stem!r})\n"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode == 0:
            print(f"ok   {module.name}")
        else:
            failures += 1
            reason = (proc.stderr or proc.stdout).strip().splitlines()
            detail = reason[-1] if reason else "unknown import error"
            print(f"FAIL {module.name}: {detail}")
            if "No module named" in detail:
                print(
                    "     hint: install the generated flows' runtime deps first, e.g. "
                    'pip install "prefect>=3,<4" prefect-shell docker',
                    file=sys.stderr,
                )
    total = len(modules)
    print(f"{total - failures}/{total} module(s) import cleanly.", file=sys.stderr)
    return 1 if failures else 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    workflows = parse_workflows(_read_input(args.input))
    for wf in workflows:
        print(f"{wf.kind}: {wf.display_name}")
        if wf.entrypoint:
            print(f"  entrypoint: {wf.entrypoint}")
        if wf.schedule:
            tz = f" ({wf.timezone})" if wf.timezone else ""
            print(f"  schedule:   {wf.schedule}{tz}")
        if wf.arguments:
            names = ", ".join(p.name for p in wf.arguments)
            print(f"  parameters: {names}")
        print(f"  templates:  {len(wf.templates)}")
        for template in wf.templates:
            detail = _template_detail(template)
            print(f"    - {template.name} [{template.kind.value}]{detail}")
        if wf.warnings:
            print("  warnings:")
            for warning in wf.warnings:
                print(f"    ! {warning}")
    return 0


def _template_detail(template) -> str:
    if template.kind.value == "dag":
        return f" ({len(template.dag_tasks)} tasks)"
    if template.kind.value == "steps":
        steps = sum(len(group) for group in template.step_groups)
        return f" ({steps} steps in {len(template.step_groups)} groups)"
    if template.container and template.container.image:
        return f" (image: {template.container.image})"
    if template.script and template.script.image:
        return f" (script: {template.script.interpreter})"
    return ""


def _read_input(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8")


def _print_warnings(workflows) -> None:
    # The generator collects the authoritative warning set; re-run lightly here is
    # avoided by surfacing parser-level warnings (generation warnings are embedded
    # in the file header).
    notes = [w for wf in workflows for w in wf.warnings]
    if notes:
        print("Parser notes:", file=sys.stderr)
        for note in notes:
            print(f"  - {note}", file=sys.stderr)
        print(
            "See the generated file's docstring for the full migration checklist.",
            file=sys.stderr,
        )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
