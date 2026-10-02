"""Databricks-friendly runners for REACH audit pipes."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from reach._notes._copay_waiver import build_copay_waiver_workbook
from reach._notes._giftcards import REACH_AUDIT_GIFTCARDS_SOURCE_PATH, build_reach_audit_giftcards_workbook
from reach._pipes._audit_tables import (
    COPAY_STAGE_OUTPUTS,
    GIFTCARD_STAGE_OUTPUTS,
    audit_copay_table_names,
    audit_giftcard_table_names,
    audit_run_summary_table,
)
from reach._pipes._builder import PipeStage, select_pipe_stages
from reach._pipes._materialize import ensure_schema, materialize_frames
from reach._pipes._registry import PipeRegistry
from reach._tables import reach_environment

AUDIT_PIPE_NAMES = ("reach_audit_copay", "reach_audit_giftcards")


def _parse_outputs(values: Sequence[str]) -> tuple[str, ...]:
    outputs: list[str] = []
    for value in values:
        outputs.extend(part.strip() for part in value.split(",") if part.strip())
    return tuple(outputs)


def _stage_outputs_for_pipe(pipe_name: str) -> Mapping[str, str]:
    if pipe_name == "reach_audit_copay":
        return COPAY_STAGE_OUTPUTS
    if pipe_name == "reach_audit_giftcards":
        return GIFTCARD_STAGE_OUTPUTS
    available = ", ".join(AUDIT_PIPE_NAMES)
    raise ValueError(f"Unknown REACH audit pipe {pipe_name!r}. Available audit pipes: {available}.")


def _normalize_stage_selectors(pipe_name: str, selectors: Iterable[str]) -> tuple[str, ...]:
    stage_outputs = _stage_outputs_for_pipe(pipe_name)
    stage_by_output = {output_name: stage_name for stage_name, output_name in stage_outputs.items()}
    return tuple(stage_by_output.get(selector, selector) for selector in selectors)


def _selected_output_names(stages: Iterable[PipeStage], stage_outputs: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(stage_outputs[stage.name] for stage in stages if stage.name in stage_outputs)


def _summary_frame(spark: SparkSession, rows: Sequence[dict[str, Any]], *, pipe_name: str) -> DataFrame:
    enriched = [
        {
            "pipe_name": pipe_name,
            "output_name": str(row["output_name"]),
            "output_table": str(row["output_table"]),
            "row_count": int(row["row_count"]),
            "run_started_at": str(row["run_started_at"]),
        }
        for row in rows
    ]
    return spark.createDataFrame(enriched).withColumn("run_recorded_at", F.current_timestamp())


def _write_run_summary(spark: SparkSession, pipe_name: str, summary: DataFrame, *, uat_catalog_schema: str) -> str:
    table_name = audit_run_summary_table(pipe_name, uat_catalog_schema)
    ensure_schema(spark, table_name)
    summary.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    return table_name


def _selected_stages(pipe_name: str, *, outputs: Sequence[str] = (), through_output: str = "") -> tuple[PipeStage, ...]:
    stages = PipeRegistry.get(pipe_name)()
    selected_outputs = _normalize_stage_selectors(pipe_name, outputs)
    selected_through_output = _normalize_stage_selectors(pipe_name, (through_output,))[0] if through_output else ""
    return select_pipe_stages(stages, outputs=selected_outputs, through_output=selected_through_output)


def run_reach_audit_copay_pipe(
    spark: SparkSession,
    *,
    performance_year: int,
    uat_catalog_schema: str | None = None,
    outputs: Sequence[str] = (),
    through_output: str = "",
    require_non_empty: bool = True,
) -> dict[str, Any]:
    """Build and persist the REACH copay audit pipe outputs."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    selected_stages = _selected_stages("reach_audit_copay", outputs=outputs, through_output=through_output)
    selected_outputs = _selected_output_names(selected_stages, COPAY_STAGE_OUTPUTS)
    table_names = audit_copay_table_names(schema)
    frames = build_copay_waiver_workbook(
        spark,
        performance_year=performance_year,
        active_engaged_only=False,
    )
    rows = materialize_frames(spark, frames, table_names, selected_outputs=selected_outputs, require_non_empty=False)
    summary = _summary_frame(spark, rows, pipe_name="reach_audit_copay")
    run_summary_table = _write_run_summary(spark, "reach_audit_copay", summary, uat_catalog_schema=schema)
    empty = [str(row["output_table"]) for row in rows if int(row["row_count"]) == 0]
    if require_non_empty and empty:
        raise RuntimeError("REACH audit pipe wrote empty required table(s): " + ", ".join(empty))
    return {
        "pipe_name": "reach_audit_copay",
        "performance_year": int(performance_year),
        "uat_catalog_schema": schema,
        "tables": tuple(row["output_table"] for row in rows),
        "row_counts": tuple(rows),
        "summary": summary,
        "run_summary_table": run_summary_table,
        "outputs": frames,
    }


def run_reach_audit_giftcards_pipe(
    spark: SparkSession,
    *,
    source_csv_path: str = REACH_AUDIT_GIFTCARDS_SOURCE_PATH,
    uat_catalog_schema: str | None = None,
    outputs: Sequence[str] = (),
    through_output: str = "",
    require_non_empty: bool = True,
) -> dict[str, Any]:
    """Build and persist the REACH gift-card audit pipe outputs."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    selected_stages = _selected_stages("reach_audit_giftcards", outputs=outputs, through_output=through_output)
    selected_outputs = _selected_output_names(selected_stages, GIFTCARD_STAGE_OUTPUTS)
    table_names = audit_giftcard_table_names(schema)
    frames = build_reach_audit_giftcards_workbook(
        spark,
        source_csv_path=source_csv_path,
        target_table=table_names["giftcard_distribution"],
    )
    rows = materialize_frames(spark, frames, table_names, selected_outputs=selected_outputs, require_non_empty=False)
    summary = _summary_frame(spark, rows, pipe_name="reach_audit_giftcards")
    run_summary_table = _write_run_summary(spark, "reach_audit_giftcards", summary, uat_catalog_schema=schema)
    empty = [str(row["output_table"]) for row in rows if int(row["row_count"]) == 0]
    if require_non_empty and empty:
        raise RuntimeError("REACH audit pipe wrote empty required table(s): " + ", ".join(empty))
    return {
        "pipe_name": "reach_audit_giftcards",
        "source_csv_path": source_csv_path,
        "uat_catalog_schema": schema,
        "tables": tuple(row["output_table"] for row in rows),
        "row_counts": tuple(rows),
        "summary": summary,
        "run_summary_table": run_summary_table,
        "outputs": frames,
    }


def run_audit_pipe(
    spark: SparkSession,
    pipe_name: str,
    *,
    performance_year: int = 2026,
    source_csv_path: str = REACH_AUDIT_GIFTCARDS_SOURCE_PATH,
    uat_catalog_schema: str | None = None,
    outputs: Sequence[str] = (),
    through_output: str = "",
    require_non_empty: bool = True,
) -> dict[str, Any]:
    """Run one registered REACH audit pipe."""
    if pipe_name == "reach_audit_copay":
        return run_reach_audit_copay_pipe(
            spark,
            performance_year=performance_year,
            uat_catalog_schema=uat_catalog_schema,
            outputs=outputs,
            through_output=through_output,
            require_non_empty=require_non_empty,
        )
    if pipe_name == "reach_audit_giftcards":
        return run_reach_audit_giftcards_pipe(
            spark,
            source_csv_path=source_csv_path,
            uat_catalog_schema=uat_catalog_schema,
            outputs=outputs,
            through_output=through_output,
            require_non_empty=require_non_empty,
        )
    available = ", ".join(AUDIT_PIPE_NAMES)
    raise ValueError(f"Unknown REACH audit pipe {pipe_name!r}. Available audit pipes: {available}.")


def _jsonable_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pipe_name": result["pipe_name"],
        "uat_catalog_schema": result["uat_catalog_schema"],
        "tables": list(result["tables"]),
        "row_counts": list(result["row_counts"]),
        "run_summary_table": result["run_summary_table"],
    }


def _parser(default_pipe_name: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a REACH audit pipe and persist its Delta outputs.")
    if default_pipe_name is None:
        parser.add_argument("pipe_name", choices=AUDIT_PIPE_NAMES)
    parser.add_argument("--performance-year", type=int, default=2026)
    parser.add_argument("--source-csv-path", default=REACH_AUDIT_GIFTCARDS_SOURCE_PATH)
    parser.add_argument("--uat-catalog-schema", default=reach_environment().uat_catalog_schema)
    parser.add_argument("--output", action="append", default=[], help="Stage name, output table, or logical output name. May be repeated or comma-separated.")
    parser.add_argument("--through-output", default="", help="Run through this stage name or output table.")
    parser.add_argument("--allow-empty", action="store_true", help="Permit selected output tables with zero rows.")
    return parser


def _main(default_pipe_name: str | None = None) -> None:
    args = _parser(default_pipe_name).parse_args()
    pipe_name = default_pipe_name or args.pipe_name
    spark = SparkSession.builder.getOrCreate()
    result = run_audit_pipe(
        spark,
        pipe_name,
        performance_year=args.performance_year,
        source_csv_path=args.source_csv_path,
        uat_catalog_schema=args.uat_catalog_schema,
        outputs=_parse_outputs(args.output),
        through_output=args.through_output,
        require_non_empty=not args.allow_empty,
    )
    print(json.dumps(_jsonable_result(result), indent=2, sort_keys=True))


def main() -> None:
    """Console entry point for selecting any REACH audit pipe."""
    _main()


def copay_main() -> None:
    """Console entry point for the REACH copay audit pipe."""
    _main("reach_audit_copay")


def giftcards_main() -> None:
    """Console entry point for the REACH gift-card audit pipe."""
    _main("reach_audit_giftcards")


__all__ = [
    "AUDIT_PIPE_NAMES",
    "copay_main",
    "giftcards_main",
    "main",
    "run_audit_pipe",
    "run_reach_audit_copay_pipe",
    "run_reach_audit_giftcards_pipe",
]
