"""Spark expressions for current REACH alignment and review queues."""

from __future__ import annotations

import re
from datetime import date

from pyspark.sql import Column, functions as F

from reach._expressions._common import as_column, blank_to_null, date_literal, normalize_mbi, trueish
from reach._expressions._registry import register_expression

_ALIGNMENT_MONTH_COLUMN = re.compile(r"^ym_(\d{6})_(reach|mssp)$", re.IGNORECASE)


def alignment_year_months(schema: list[str] | tuple[str, ...], *, program: str | None = None) -> tuple[str, ...]:
    """Return sorted YYYYMM values present in wide alignment month columns."""
    program_key = program.lower() if program else None
    observed = set()
    for name in schema:
        match = _ALIGNMENT_MONTH_COLUMN.match(name)
        if not match:
            continue
        year_month, observed_program = match.groups()
        if program_key is None or observed_program.lower() == program_key:
            observed.add(year_month)
    return tuple(sorted(observed))


def latest_alignment_year_month(schema: list[str] | tuple[str, ...], *, program: str | None = None) -> str:
    """Return the latest YYYYMM represented by wide alignment month columns."""
    year_months = alignment_year_months(schema, program=program)
    if not year_months:
        suffix = f" for {program.upper()}" if program else ""
        raise ValueError(f"No ym_YYYYMM alignment columns found{suffix}.")
    return year_months[-1]


@register_expression("living_beneficiary", concern="alignment")
def living_beneficiary_expr(schema: list[str] | None = None, death_date_col: str = "death_date") -> Column:
    """Return true for living beneficiaries when a death-date column is available."""
    schema = schema or []
    if death_date_col not in schema:
        return F.lit(True)
    return F.to_date(as_column(death_date_col)).isNull()


@register_expression("current_reach_observable", concern="alignment")
def current_reach_observable_expr(
    *,
    reference_date: date | None = None,
    schema: list[str] | None = None,
    observable_start_col: str = "observable_start",
    observable_end_col: str = "observable_end",
    last_reach_date_col: str = "last_reach_date",
    ever_reach_col: str = "ever_reach",
    death_date_col: str = "death_date",
) -> Column:
    """Identify currently REACH-attributed rows from temporal alignment columns."""
    schema = schema or []
    ref = date_literal(reference_date or date.today())
    expr = living_beneficiary_expr(schema, death_date_col=death_date_col)
    if observable_start_col in schema and observable_end_col in schema:
        expr = expr & (F.to_date(as_column(observable_start_col)) <= ref) & (F.to_date(as_column(observable_end_col)) >= ref)
    if last_reach_date_col in schema:
        expr = expr & F.to_date(as_column(last_reach_date_col)).isNotNull() & (F.to_date(as_column(last_reach_date_col)) <= ref)
    if ever_reach_col in schema:
        expr = expr & trueish(ever_reach_col)
    return expr


@register_expression("current_alignment_file", concern="alignment")
def current_alignment_file_expr(filename_col: str = "source_filename") -> Column:
    """True for CMS current-alignment BAR files, not reconciliation files."""
    return F.coalesce(as_column(filename_col).cast("string"), F.lit("")).rlike(r"\.ALGC")


@register_expression("aligned_bar_row", concern="alignment")
def aligned_bar_row_expr(status_col: str = "AlignmentStatusCD") -> Column:
    """True for aligned beneficiary rows in a BAR-like source."""
    return F.upper(F.trim(F.coalesce(as_column(status_col).cast("string"), F.lit("")))) == F.lit("AL")


@register_expression("active_patient_status", concern="alignment")
def active_patient_status_expr(status_col: str = "lc_status_current") -> Column:
    """True for active/current census statuses while avoiding inactive variants."""
    status = F.upper(F.trim(F.coalesce(as_column(status_col).cast("string"), F.lit(""))))
    active_word = status.rlike(r"(^|[^A-Z])ACTIVE([^A-Z]|$)")
    current_word = status.rlike(r"(^|[^A-Z])CURRENT([^A-Z]|$)")
    inactive_word = status.like("%INACTIVE%") | status.like("%NOT ACTIVE%")
    return (active_word | current_word) & ~inactive_word


@register_expression("active_engaged_patient_status", concern="alignment")
def active_engaged_patient_status_expr(status_col: str = "lc_status_current") -> Column:
    """True only for the active-engaged lifecycle status used by oversight review."""
    status = F.lower(F.trim(F.coalesce(as_column(status_col).cast("string"), F.lit(""))))
    normalized = F.regexp_replace(F.regexp_replace(status, r"\s*-\s*", " - "), r"\s+", " ")
    return normalized == F.lit("active - engaged")


@register_expression("current_reach_bar_row", concern="alignment")
def current_reach_bar_row_expr(
    *,
    schema: list[str] | None = None,
    filename_col: str = "source_filename",
    status_col: str = "AlignmentStatusCD",
    program_col: str = "program",
    death_date_col: str = "death_date",
) -> Column:
    """True for living aligned rows from current REACH BAR snapshots."""
    schema = schema or []
    expr = living_beneficiary_expr(schema, death_date_col=death_date_col) & current_alignment_file_expr(filename_col) & aligned_bar_row_expr(status_col)
    if program_col in schema:
        program = F.upper(F.trim(F.coalesce(as_column(program_col).cast("string"), F.lit(""))))
        expr = expr & (program == F.lit("REACH"))
    return expr


@register_expression("ny_reach", concern="alignment")
def ny_reach_expr(
    *,
    reference_date: date | None = None,
    schema: list[str] | None = None,
    office_col: str = "office_location",
) -> Column:
    """True for current REACH beneficiaries assigned to a New York office."""
    schema = schema or []
    expr = current_reach_observable_expr(reference_date=reference_date, schema=schema)
    if office_col in schema:
        office = F.upper(F.trim(F.coalesce(as_column(office_col).cast("string"), F.lit(""))))
        expr = expr & office.isin("NY", "NEW YORK")
    return expr


@register_expression("alignment_window_overlap", concern="alignment")
def alignment_window_overlap_expr(start_col: str, end_col: str, window_start, window_end) -> Column:
    """True when an alignment date span overlaps an inclusive window."""
    start_date = F.to_date(as_column(start_col))
    end_date = F.coalesce(F.to_date(as_column(end_col)), date_literal(window_end))
    return start_date.isNotNull() & (start_date <= date_literal(window_end)) & (end_date >= date_literal(window_start))


@register_expression("patient_key", concern="alignment")
def patient_key_expr(hcmpi_col: str = "hcmpi", mbi_col: str = "MedicareBeneficiaryID") -> Column:
    """Stable review key: prefer HCMPI and fall back to normalized MBI."""
    return F.coalesce(blank_to_null(hcmpi_col), normalize_mbi(mbi_col))
