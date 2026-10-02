"""Spark expressions shared by REACH quality-measure transforms."""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, functions as F

from reach._expressions._common import as_column, date_literal, safe_divide
from reach._expressions._high_needs import HOSPITAL_INPATIENT_BILL_TYPE_PREFIX
from reach._expressions._registry import register_expression

LOWER_IS_BETTER_MEASURES = frozenset({"ACR", "NQF1789", "UAMCC", "NQF2888", "DAH_LOST_DAYS"})


@register_expression("quality_inpatient_claim", concern="quality")
def inpatient_claim_expr(claim_type_col: str = "claim_type", bill_type_col: str = "bill_type_code") -> Column:
    """True for institutional inpatient rows used by ACR/UAMCC admission logic."""
    claim_type = F.lower(F.trim(F.coalesce(as_column(claim_type_col).cast("string"), F.lit(""))))
    bill_type = F.trim(F.coalesce(as_column(bill_type_col).cast("string"), F.lit("")))
    return (claim_type == F.lit("institutional")) & bill_type.startswith(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX)


@register_expression("quality_measure_rate", concern="quality")
def measure_rate_expr(numerator_col: str = "numerator", denominator_col: str = "denominator", scale: float = 1.0) -> Column:
    """Generic rate expression with null-safe denominator handling."""
    return safe_divide(numerator_col, denominator_col) * F.lit(float(scale))


def measure_direction_expr(measure_id_col: str = "measure_id") -> Column:
    """Return ``lower_is_better`` or ``higher_is_better`` for known REACH measures."""
    measure = F.upper(F.trim(F.coalesce(as_column(measure_id_col).cast("string"), F.lit(""))))
    return F.when(measure.isin(*sorted(LOWER_IS_BETTER_MEASURES)), F.lit("lower_is_better")).otherwise(F.lit("higher_is_better"))


def continuous_prior_year_enrollment_expr(py: int, start_col: str = "enroll_start", end_col: str = "enroll_end") -> Column:
    """True when enrollment covers the full year before the measurement year."""
    return F.to_date(as_column(start_col)) <= date_literal(date(py - 1, 1, 1))


def continuous_measurement_year_enrollment_expr(py: int, end_col: str = "enroll_end", death_col: str = "dod") -> Column:
    """True when enrollment covers the measurement year or lasts through death."""
    end = F.to_date(as_column(end_col))
    death = F.to_date(as_column(death_col))
    return end.isNull() | (end >= date_literal(date(py, 12, 31))) | (death.isNotNull() & (end >= death))
