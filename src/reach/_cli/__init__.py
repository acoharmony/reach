"""Command-line entry point for REACH package workflows (``aco``)."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from collections.abc import Sequence

from reach._pipes._audit_tables import audit_copay_table_names, audit_giftcard_table_names
from reach._pipes._oversight_tables import oversight_table_names
from reach._pipes._registry import PipeRegistry
from reach._tables import reach_environment

ENV = reach_environment()
DEFAULT_NOTES_TABLE = "uat_sandbox.gov_programs.reach_clinical_oversight_patient_notes"
DISCUSSION_GROUP_CLINICIAN_NPIS = ("1568005569", "1922454586", "1417337247")
DEFAULT_GIFTCARDS_SOURCE_PATH = f"{ENV.bronze_volume_root}/reach_audit_giftcards/reach_giftcards_2026.csv"


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _split_cli_values(values: Sequence[str]) -> tuple[str, ...]:
    parts: list[str] = []
    for value in values:
        parts.extend(_split_csv(value))
    return tuple(parts)


def _pipe_selection(value: str) -> tuple[str, ...]:
    aliases = {
        "all": ("reach_audit_copay", "reach_audit_giftcards"),
        "copay": ("reach_audit_copay",),
        "cost-sharing": ("reach_audit_copay",),
        "cost_sharing": ("reach_audit_copay",),
        "giftcards": ("reach_audit_giftcards",),
        "gift_cards": ("reach_audit_giftcards",),
        "reach_audit_copay": ("reach_audit_copay",),
        "reach_audit_giftcards": ("reach_audit_giftcards",),
    }
    selected: list[str] = []
    for token in _split_csv(value):
        try:
            selected.extend(aliases[token])
        except KeyError as exc:
            valid = ", ".join(sorted(aliases))
            raise SystemExit(f"Unknown reach_audit pipe selector {token!r}. Use one of: {valid}.") from exc
    deduped: list[str] = []
    for pipe in selected or list(aliases["all"]):
        if pipe not in deduped:
            deduped.append(pipe)
    return tuple(deduped)


def _audit_task_parameters(pipe_name: str, args: argparse.Namespace) -> list[str]:
    params = [pipe_name, "--uat-catalog-schema", args.uat_catalog_schema]
    if pipe_name == "reach_audit_copay":
        params.extend(["--performance-year", str(args.performance_year)])
    if pipe_name == "reach_audit_giftcards":
        params.extend(["--source-csv-path", args.source_csv_path])
    for output in args.output:
        params.extend(["--output", output])
    if args.through_output:
        params.extend(["--through-output", args.through_output])
    if args.allow_empty:
        params.append("--allow-empty")
    return params


def _submit_payload(
    *,
    run_name: str,
    tasks: list[dict[str, object]],
    workspace_wheel_path: str,
    environment_key: str,
    environment_version: str,
) -> dict[str, object]:
    return {
        "run_name": run_name,
        "tasks": tasks,
        "environments": [
            {
                "environment_key": environment_key,
                "spec": {
                    "environment_version": environment_version,
                    "dependencies": [workspace_wheel_path],
                },
            }
        ],
    }


def _run_databricks_submit(payload: dict[str, object], args: argparse.Namespace) -> int:
    command = [args.databricks_cli, "jobs", "submit", "-o", "json", "--json", json.dumps(payload, sort_keys=True)]
    if args.no_wait:
        command.append("--no-wait")
    else:
        command.extend(["--timeout", args.databricks_timeout])
    if args.databricks_profile:
        command.extend(["--profile", args.databricks_profile])
    print("Submitting Databricks run:", shlex.join(command))
    return subprocess.run(command, check=False).returncode


def _print_pipe_tables(pipe_names: Sequence[str], *, uat_catalog_schema: str) -> None:
    for pipe_name in pipe_names:
        tables = audit_copay_table_names(uat_catalog_schema) if pipe_name == "reach_audit_copay" else audit_giftcard_table_names(uat_catalog_schema)
        print(pipe_name)
        for output_name, table_name in tables.items():
            print(f"  {output_name}: {table_name}")


def reach_audit(args: argparse.Namespace) -> int:
    """Run the REACH audit copay/giftcard pipes."""
    pipe_names = _pipe_selection(args.pipe)
    if (args.output or args.through_output) and len(pipe_names) != 1:
        raise SystemExit("Use --output or --through-output only when --pipe selects a single audit pipe.")
    if args.dry_run:
        _print_pipe_tables(pipe_names, uat_catalog_schema=args.uat_catalog_schema)
        return 0
    if args.local:
        from pyspark.sql import SparkSession

        from reach._pipes._audit_runner import run_audit_pipe

        spark = SparkSession.builder.getOrCreate()
        for pipe_name in pipe_names:
            result = run_audit_pipe(
                spark,
                pipe_name,
                performance_year=args.performance_year,
                source_csv_path=args.source_csv_path,
                uat_catalog_schema=args.uat_catalog_schema,
                outputs=_split_cli_values(args.output),
                through_output=args.through_output,
                require_non_empty=not args.allow_empty,
            )
            print(json.dumps({"pipe_name": result["pipe_name"], "row_counts": list(result["row_counts"]), "run_summary_table": result["run_summary_table"]}, indent=2, sort_keys=True))
        return 0

    tasks = [
        {
            "task_key": pipe_name,
            "python_wheel_task": {
                "package_name": "reach",
                "entry_point": "reach-audit-pipe",
                "parameters": _audit_task_parameters(pipe_name, args),
            },
            "environment_key": "reach",
        }
        for pipe_name in pipe_names
    ]
    payload = _submit_payload(
        run_name="reach_audit_pipe",
        tasks=tasks,
        workspace_wheel_path=args.workspace_wheel_path,
        environment_key="reach",
        environment_version=args.environment_version,
    )
    return _run_databricks_submit(payload, args)


def _oversight_task_parameters(args: argparse.Namespace) -> list[str]:
    params = ["--notes-table", args.notes_table, "--uat-catalog-schema", args.uat_catalog_schema]
    if args.all_statuses:
        params.append("--all-statuses")
    for clinician_npi in args.clinician_npi:
        params.extend(["--clinician-npi", clinician_npi])
    for output in args.output:
        params.extend(["--output", output])
    if args.through_output:
        params.extend(["--through-output", args.through_output])
    if args.allow_empty:
        params.append("--allow-empty")
    return params


def reach_oversight(args: argparse.Namespace) -> int:
    """Run the REACH clinical oversight pipe."""
    if args.dry_run:
        tables = oversight_table_names(args.uat_catalog_schema)
        print("reach_oversight")
        for output_name, table_name in tables.items():
            print(f"  {output_name}: {table_name}")
        return 0
    if args.local:
        from pyspark.sql import SparkSession

        from reach._pipes._oversight_runner import run_reach_oversight_pipe

        spark = SparkSession.builder.getOrCreate()
        clinician_npis = tuple(args.clinician_npi) or DISCUSSION_GROUP_CLINICIAN_NPIS
        result = run_reach_oversight_pipe(
            spark,
            notes_table=args.notes_table,
            active_only=not args.all_statuses,
            clinician_npis=clinician_npis,
            uat_catalog_schema=args.uat_catalog_schema,
            outputs=_split_cli_values(args.output),
            through_output=args.through_output,
            require_non_empty=not args.allow_empty,
        )
        print(json.dumps({"pipe_name": result["pipe_name"], "row_counts": list(result["row_counts"]), "run_summary_table": result["run_summary_table"]}, indent=2, sort_keys=True))
        return 0

    payload = _submit_payload(
        run_name="reach_oversight_pipe",
        tasks=[
            {
                "task_key": "reach_oversight",
                "python_wheel_task": {
                    "package_name": "reach",
                    "entry_point": "reach-oversight-pipe",
                    "parameters": _oversight_task_parameters(args),
                },
                "environment_key": "reach",
            }
        ],
        workspace_wheel_path=args.workspace_wheel_path,
        environment_key="reach",
        environment_version=args.environment_version,
    )
    return _run_databricks_submit(payload, args)


def list_pipes(_args: argparse.Namespace) -> int:
    """List registered REACH pipes."""
    for name in PipeRegistry.list_names():
        metadata = PipeRegistry.metadata(name)
        description = metadata.get("description", "")
        print(f"{name}: {description}" if description else name)
    return 0


def _add_remote_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--local", action="store_true", help="Run in the current Spark process instead of submitting Databricks Jobs.")
    parser.add_argument("--workspace-wheel-path", default=ENV.wheel_workspace_path)
    parser.add_argument("--environment-version", default=ENV.serverless_environment_version)
    parser.add_argument("--databricks-cli", default="databricks")
    parser.add_argument("--databricks-profile", default="")
    parser.add_argument("--databricks-timeout", default="90m")
    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--dry-run", action="store_true")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aco", description="REACH package command line.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser("reach_audit", help="Run REACH audit copay and giftcard pipes.")
    audit.add_argument("--pipe", default="all", help="Comma-separated: all, copay, giftcards, reach_audit_copay, reach_audit_giftcards.")
    audit.add_argument("--performance-year", type=int, default=2026)
    audit.add_argument("--source-csv-path", default=DEFAULT_GIFTCARDS_SOURCE_PATH)
    audit.add_argument("--uat-catalog-schema", default=ENV.uat_catalog_schema)
    audit.add_argument("--output", action="append", default=[], help="Only for one selected pipe; may be repeated or comma-separated.")
    audit.add_argument("--through-output", default="", help="Only for one selected pipe.")
    audit.add_argument("--allow-empty", action="store_true")
    _add_remote_options(audit)
    audit.set_defaults(func=reach_audit)

    oversight = subparsers.add_parser("reach_oversight", help="Run REACH clinical oversight pipe.")
    oversight.add_argument("--notes-table", default=DEFAULT_NOTES_TABLE)
    oversight.add_argument("--all-statuses", action="store_true", help="Do not restrict to active-engaged patients.")
    oversight.add_argument("--clinician-npi", action="append", default=[], help="NPI; may be repeated.")
    oversight.add_argument("--uat-catalog-schema", default=ENV.uat_catalog_schema)
    oversight.add_argument("--output", action="append", default=[], help="May be repeated or comma-separated.")
    oversight.add_argument("--through-output", default="")
    oversight.add_argument("--allow-empty", action="store_true")
    _add_remote_options(oversight)
    oversight.set_defaults(func=reach_oversight)

    pipes = subparsers.add_parser("pipes", help="List registered REACH pipes.")
    pipes.set_defaults(func=list_pipes)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the REACH CLI."""
    args = _parser().parse_args(argv)
    raise SystemExit(args.func(args))


__all__ = ["main"]
