"""Delta table materialization helpers for REACH pipe outputs."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

_DELTA_INVALID_COLUMN_CHARS_PATTERN = re.compile(r"[ ,;{}()\n\t=]+")


def ensure_schema(spark_session: Any, table_name: str) -> None:
    """Create the catalog schema for a three-part Unity Catalog table name."""
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a three-part Unity Catalog table name, got {table_name!r}.")
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}.{parts[1]}")


def delta_safe_column_names(columns: Sequence[Any]) -> tuple[str, ...]:
    """Normalize column names for Delta persistence and deduplicate collisions."""
    safe_columns: list[str] = []
    seen: dict[str, int] = {}
    for index, column in enumerate(columns, start=1):
        normalized = _DELTA_INVALID_COLUMN_CHARS_PATTERN.sub("_", str(column).strip())
        normalized = re.sub(r"_+", "_", normalized).strip("_").lower()
        if not normalized:
            normalized = f"column_{index}"
        if normalized[0].isdigit():
            normalized = f"column_{normalized}"
        count = seen.get(normalized, 0) + 1
        seen[normalized] = count
        safe_columns.append(normalized if count == 1 else f"{normalized}_{count}")
    return tuple(safe_columns)


def with_delta_safe_column_names(frame: Any) -> Any:
    """Return a DataFrame renamed to Delta-safe columns."""
    columns = tuple(getattr(frame, "columns", ()) or ())
    if not columns:
        return frame
    safe_columns = delta_safe_column_names(columns)
    if tuple(str(column) for column in columns) == safe_columns:
        return frame
    return frame.toDF(*safe_columns)


def materialize_frames(
    spark_session: Any,
    outputs: Mapping[str, Any],
    output_tables: Mapping[str, str],
    *,
    selected_outputs: Sequence[str] = (),
    require_non_empty: bool = True,
) -> tuple[dict[str, Any], ...]:
    """Write selected DataFrame outputs to Delta tables and return row-count records."""
    wanted = tuple(selected_outputs or output_tables.keys())
    run_started_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    empty: list[str] = []
    for output_name in wanted:
        if output_name not in output_tables:
            available = ", ".join(sorted(output_tables))
            raise ValueError(f"Unknown output {output_name!r}. Available outputs: {available}.")
        if output_name not in outputs:
            raise KeyError(f"Pipe did not build required output {output_name!r}.")
        table_name = output_tables[output_name]
        frame = outputs[output_name]
        if not hasattr(frame, "write"):
            raise TypeError(f"Pipe output {output_name!r} is not a Spark DataFrame-like object.")
        ensure_schema(spark_session, table_name)
        write_frame = with_delta_safe_column_names(frame)
        write_frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        row_count = int(spark_session.table(table_name).count())
        if row_count == 0:
            empty.append(table_name)
        rows.append(
            {
                "output_name": output_name,
                "output_table": table_name,
                "row_count": row_count,
                "run_started_at": run_started_at,
            }
        )
    if require_non_empty and empty:
        raise RuntimeError("REACH audit pipe wrote empty required table(s): " + ", ".join(empty))
    return tuple(rows)


__all__ = ["delta_safe_column_names", "ensure_schema", "materialize_frames", "with_delta_safe_column_names"]
