"""Databricks-friendly runner for the REACH clinical oversight pipe."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from reach._notes._clinical_oversight import DEFAULT_NOTES_TABLE, DISCUSSION_GROUP_CLINICIAN_NPIS, build_review_workbook
from reach._pipes._builder import PipeStage, select_pipe_stages
from reach._pipes._materialize import ensure_schema, materialize_frames
from reach._pipes._oversight_tables import OVERSIGHT_STAGE_OUTPUTS, oversight_run_summary_table, oversight_table_names
from reach._pipes._registry import PipeRegistry
from reach._tables import reach_environment

PIPE_NAME = "clinical-oversight"


def _parse_outputs(values: Sequence[str]) -> tuple[str, ...]:
    outputs: list[str] = []
    for value in values:
        outputs.extend(part.strip() for part in value.split(",") if part.strip())
    return tuple(outputs)


def _parse_npis(values: Sequence[str]) -> tuple[str, ...]:
    npis: list[str] = []
    for value in values:
        npis.extend(part.strip() for part in value.split(",") if part.strip())
    return tuple(npis)


def _normalize_stage_selectors(selectors: Iterable[str]) -> tuple[str, ...]:
    stage_by_output = {output_name: stage_name for stage_name, output_name in OVERSIGHT_STAGE_OUTPUTS.items()}
    return tuple(stage_by_output.get(selector, selector) for selector in selectors)


def _selected_output_names(stages: Iterable[PipeStage], stage_outputs: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(stage_outputs[stage.name] for stage in stages if stage.name in stage_outputs)


def _selected_stages(*, outputs: Sequence[str] = (), through_output: str = "") -> tuple[PipeStage, ...]:
    stages = PipeRegistry.get(PIPE_NAME)()
    selected_outputs = _normalize_stage_selectors(outputs)
    selected_through_output = _normalize_stage_selectors((through_output,))[0] if through_output else ""
    return select_pipe_stages(stages, outputs=selected_outputs, through_output=selected_through_output)


def _summary_frame(spark: SparkSession, rows: Sequence[dict[str, Any]]) -> DataFrame:
    enriched = [
        {
            "pipe_name": "reach_oversight",
            "output_name": str(row["output_name"]),
            "output_table": str(row["output_table"]),
            "row_count": int(row["row_count"]),
            "run_started_at": str(row["run_started_at"]),
        }
        for row in rows
    ]
    return spark.createDataFrame(enriched).withColumn("run_recorded_at", F.current_timestamp())


def _write_run_summary(spark: SparkSession, summary: DataFrame, *, uat_catalog_schema: str) -> str:
    table_name = oversight_run_summary_table(uat_catalog_schema)
    ensure_schema(spark, table_name)
    summary.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    return table_name


def run_reach_oversight_pipe(
    spark: SparkSession,
    *,
    notes_table: str = DEFAULT_NOTES_TABLE,
    active_only: bool = True,
    clinician_npis: Sequence[str] = DISCUSSION_GROUP_CLINICIAN_NPIS,
    uat_catalog_schema: str | None = None,
    outputs: Sequence[str] = (),
    through_output: str = "",
    require_non_empty: bool = True,
) -> dict[str, Any]:
    """Build and persist clinical oversight dashboard/worklist outputs."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    selected_stages = _selected_stages(outputs=outputs, through_output=through_output)
    selected_outputs = _selected_output_names(selected_stages, OVERSIGHT_STAGE_OUTPUTS)
    table_names = oversight_table_names(schema)
    frames = build_review_workbook(spark, notes_table=notes_table, active_only=active_only, clinician_npis=clinician_npis)
    rows = materialize_frames(spark, frames, table_names, selected_outputs=selected_outputs, require_non_empty=False)
    summary = _summary_frame(spark, rows)
    run_summary_table = _write_run_summary(spark, summary, uat_catalog_schema=schema)
    empty = [str(row["output_table"]) for row in rows if int(row["row_count"]) == 0]
    if require_non_empty and empty:
        raise RuntimeError("REACH oversight pipe wrote empty required table(s): " + ", ".join(empty))
    return {
        "pipe_name": "reach_oversight",
        "notes_table": notes_table,
        "active_only": bool(active_only),
        "clinician_npis": tuple(clinician_npis),
        "uat_catalog_schema": schema,
        "tables": tuple(row["output_table"] for row in rows),
        "row_counts": tuple(rows),
        "summary": summary,
        "run_summary_table": run_summary_table,
        "outputs": frames,
    }


def _jsonable_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pipe_name": result["pipe_name"],
        "uat_catalog_schema": result["uat_catalog_schema"],
        "tables": list(result["tables"]),
        "row_counts": list(result["row_counts"]),
        "run_summary_table": result["run_summary_table"],
        "notes_table": result["notes_table"],
        "active_only": result["active_only"],
        "clinician_npis": list(result["clinician_npis"]),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the REACH clinical oversight pipe and persist Delta outputs.")
    parser.add_argument("--notes-table", default=DEFAULT_NOTES_TABLE)
    parser.add_argument("--all-statuses", action="store_true", help="Do not restrict the worklist to active-engaged patients.")
    parser.add_argument("--clinician-npi", action="append", default=[], help="Clinician NPI. May be repeated or comma-separated.")
    parser.add_argument("--uat-catalog-schema", default=reach_environment().uat_catalog_schema)
    parser.add_argument("--output", action="append", default=[], help="Stage name, output table, or logical output name. May be repeated or comma-separated.")
    parser.add_argument("--through-output", default="", help="Run through this stage name or output table.")
    parser.add_argument("--allow-empty", action="store_true", help="Permit selected output tables with zero rows.")
    return parser


def main() -> None:
    """Console entry point for the clinical oversight pipe."""
    args = _parser().parse_args()
    spark = SparkSession.builder.getOrCreate()
    clinician_npis = _parse_npis(args.clinician_npi) or DISCUSSION_GROUP_CLINICIAN_NPIS
    result = run_reach_oversight_pipe(
        spark,
        notes_table=args.notes_table,
        active_only=not args.all_statuses,
        clinician_npis=clinician_npis,
        uat_catalog_schema=args.uat_catalog_schema,
        outputs=_parse_outputs(args.output),
        through_output=args.through_output,
        require_non_empty=not args.allow_empty,
    )
    print(json.dumps(_jsonable_result(result), indent=2, sort_keys=True))


__all__ = ["PIPE_NAME", "main", "run_reach_oversight_pipe"]
