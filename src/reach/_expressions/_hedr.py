"""Spark expressions for REACH Health Equity Data Reporting eligibility."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import Column, functions as F

from reach._expressions._alignment import living_beneficiary_expr
from reach._expressions._common import as_column, date_literal, trueish
from reach._expressions._registry import register_expression

COMMON_HEDR_DATA_COLUMNS = (
    "race",
    "ethnicity",
    "language_preference",
    "preferred_language",
    "gender_identity",
    "sexual_orientation",
    "disability_status",
)


def reach_month_columns_for_py(schema: Sequence[str], performance_year: int, *, through_month: int = 10) -> tuple[str, ...]:
    """Return ``ym_YYYYMM_reach`` columns through the specified month."""
    out: list[str] = []
    for col in schema:
        if not col.startswith(f"ym_{performance_year}") or not col.endswith("_reach"):
            continue
        token = col.split("_")[1]
        month = int(token[4:6])
        if month <= through_month:
            out.append(col)
    return tuple(sorted(out))


def reach_month_count_expr(schema: Sequence[str], performance_year: int, *, through_month: int = 10) -> Column:
    """Count true REACH months through the HEDR October cutoff."""
    month_cols = reach_month_columns_for_py(schema, performance_year, through_month=through_month)
    if month_cols:
        total = F.lit(0)
        for col in month_cols:
            total = total + trueish(col).cast("int")
        return total
    year_col = f"months_in_reach_{performance_year}"
    if year_col in schema:
        return F.coalesce(as_column(year_col).cast("int"), F.lit(0))
    if "months_in_reach" in schema:
        return F.coalesce(as_column("months_in_reach").cast("int"), F.lit(0))
    reach_col = f"reach_months_{performance_year}"
    if reach_col in schema:
        return F.coalesce(as_column(reach_col).cast("int"), F.lit(0))
    return F.lit(0)


@register_expression("hedr_denominator", concern="hedr")
def reach_hedr_denominator_expr(
    performance_year: int,
    *,
    schema: Sequence[str],
    october_first_date: date | None = None,
    death_date_col: str = "death_date",
) -> Column:
    """True when a beneficiary has 6+ REACH months accumulated by October 1."""
    october_first = october_first_date or date(performance_year, 10, 1)
    expr = living_beneficiary_expr(list(schema), death_date_col=death_date_col)
    expr = expr & (reach_month_count_expr(schema, performance_year, through_month=10) >= F.lit(6))
    if "first_reach_date" in schema:
        expr = expr & F.to_date(as_column("first_reach_date")).isNotNull()
        expr = expr & (F.to_date(as_column("first_reach_date")) <= date_literal(october_first))
    if "last_reach_date" in schema:
        last_reach = F.to_date(as_column("last_reach_date"))
        expr = expr & (last_reach.isNull() | (last_reach >= date_literal(october_first)))
    if "observable_start" in schema and "observable_end" in schema:
        expr = expr & (F.to_date(as_column("observable_start")) <= date_literal(october_first))
        expr = expr & (F.to_date(as_column("observable_end")) >= date_literal(october_first))
    return expr


def complete_data_expr(required_data_columns: Sequence[str], *, schema: Sequence[str]) -> Column:
    """True when all required HEDR data fields exist and are populated."""
    if not required_data_columns:
        required_data_columns = tuple(col for col in COMMON_HEDR_DATA_COLUMNS if col in schema)
    if not required_data_columns:
        return F.lit(False)
    expr = F.lit(True)
    for col in required_data_columns:
        if col not in schema:
            expr = expr & F.lit(False)
        else:
            text = F.trim(F.coalesce(as_column(col).cast("string"), F.lit("")))
            expr = expr & (F.length(text) > 0)
    return expr


@register_expression("hedr_numerator", concern="hedr")
def reach_hedr_numerator_expr(
    performance_year: int,
    *,
    schema: Sequence[str],
    required_data_columns: Sequence[str] = (),
    october_first_date: date | None = None,
    death_date_col: str = "death_date",
) -> Column:
    """True when a denominator member has all required HEDR data reported."""
    return reach_hedr_denominator_expr(
        performance_year,
        schema=schema,
        october_first_date=october_first_date,
        death_date_col=death_date_col,
    ) & complete_data_expr(required_data_columns, schema=schema)


@register_expression("hedr_status", concern="hedr")
def hedr_status_expr(denominator_col: str = "hedr_denominator", numerator_col: str = "hedr_numerator") -> Column:
    """Human-readable HEDR status from numerator/denominator flags."""
    return (
        F.when(as_column(numerator_col), F.lit("Complete"))
        .when(as_column(denominator_col), F.lit("Incomplete"))
        .otherwise(F.lit("Ineligible"))
    )


def missing_data_fields_expr(required_data_columns: Sequence[str], *, schema: Sequence[str]) -> Column:
    """Comma-delimited list of missing HEDR fields for denominator misses."""
    pieces: list[Column] = []
    for col in required_data_columns:
        if col not in schema:
            pieces.append(F.lit(col))
        else:
            text = F.trim(F.coalesce(as_column(col).cast("string"), F.lit("")))
            pieces.append(F.when(F.length(text) == 0, F.lit(col)))
    if not pieces:
        return F.lit(None).cast("string")
    return F.concat_ws(", ", *pieces)


def calculate_reach_hedr_rate(denominator_count: int, numerator_count: int) -> float:
    """Return the HEDR reporting rate as a percentage."""
    if denominator_count == 0:
        return 0.0
    return numerator_count / denominator_count * 100.0
