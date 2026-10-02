"""Internal helpers for copay-waiver transforms."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from pyspark.sql import Column, DataFrame, functions as F

from reach._expressions._common import blank_to_null, date_literal


def _py_start(performance_year: int):
    return date_literal(date(int(performance_year), 1, 1))


def _py_end(performance_year: int):
    return date_literal(date(int(performance_year), 12, 31))


def _require_columns(frame: DataFrame, columns: Iterable[str], *, context: str) -> tuple[str, ...]:
    required = tuple(columns)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{context} is missing required column(s): {', '.join(missing)}.")
    return required


def _select_required(frame: DataFrame, columns: Iterable[str], *, context: str) -> DataFrame:
    return frame.select(*_require_columns(frame, columns, context=context))


def _mixed_date_value(value: Column) -> Column:
    raw = F.trim(F.coalesce(value.cast("string"), F.lit("")))
    return F.coalesce(
        F.when(raw.rlike(r"^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$"), F.to_date(raw, "M/d/yyyy")),
        F.when(raw.rlike(r"^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$"), F.to_date(raw, "yyyy-M-d")),
    )


def _with_alignment_window(frame: DataFrame, *, performance_year: int) -> DataFrame:
    _require_columns(frame, ("reach_alignment_start_dt", "reach_alignment_end_dt"), context="REACH alignment window")
    start_dt = _mixed_date_value(F.col("reach_alignment_start_dt"))
    end_dt = _mixed_date_value(F.col("reach_alignment_end_dt"))
    return frame.withColumn("reach_alignment_start_dt", start_dt).withColumn("reach_alignment_end_dt", F.coalesce(end_dt, _py_end(performance_year)))


def _service_in_alignment_window_expr(service_date: Column | str) -> Column:
    service_dt = F.to_date(F.col(service_date) if isinstance(service_date, str) else service_date)
    return (
        service_dt.isNotNull()
        & F.col("reach_alignment_start_dt").isNotNull()
        & (service_dt >= F.col("reach_alignment_start_dt"))
        & (service_dt <= F.col("reach_alignment_end_dt"))
    )


def _hcmpi_key(column: str = "hcmpi"):
    return blank_to_null(F.col(column).cast("string"))


def _with_hcmpi_key(df: DataFrame, column: str = "hcmpi") -> DataFrame:
    _require_columns(df, (column,), context="HCMPI key source")
    return df.withColumn("hcmpi", _hcmpi_key(column)).where(F.col("hcmpi").isNotNull())


def _numeric_prefix_key(column: str):
    raw = F.trim(F.coalesce(F.col(column).cast("string"), F.lit("")))
    return F.regexp_extract(raw, r"^([0-9]+)", 1)


def _comparison_group_expr():
    return F.when(F.col("copays_waived_flag"), F.lit("copays_waived")).otherwise(F.lit("not_copays_waived"))


def _population_group_expr():
    return (
        F.when(~F.col("no_secondary_or_tertiary_coverage_flag"), F.lit("has_secondary_or_tertiary_coverage"))
        .when(F.col("copays_waived_flag"), F.lit("no_secondary_copays_waived"))
        .when(F.col("alignment_period_copay_due_without_secondary_amt") > 0, F.lit("no_secondary_copay_due_not_waived"))
        .otherwise(F.lit("no_secondary_no_copay_due"))
    )


def _metric_amount(metric: str, value_col: str):
    return F.sum(F.when(F.col("Metric") == F.lit(metric), F.col(value_col).cast("double")).otherwise(F.lit(0.0)))
