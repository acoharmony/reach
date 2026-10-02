"""Small PySpark expression helpers shared by REACH transforms."""

from __future__ import annotations

from collections.abc import Iterable
from functools import reduce

from pyspark.sql import Column, functions as F

ColumnOrName = Column | str
TRUE_TEXT_VALUES = frozenset({"1", "T", "TRUE", "Y", "YES"})


def as_column(column: ColumnOrName) -> Column:
    """Return a PySpark ``Column`` from a column name or existing column."""
    if isinstance(column, str):
        return F.col(column)
    return column


def blank_to_null(column: ColumnOrName) -> Column:
    """Trim text and convert empty strings to null."""
    text = F.trim(F.coalesce(as_column(column).cast("string"), F.lit("")))
    return F.when(F.length(text) > 0, text)


def normalize_mbi(column: ColumnOrName) -> Column:
    """Normalize an MBI-like value to uppercase alphanumeric text."""
    return F.upper(F.regexp_replace(F.trim(F.coalesce(as_column(column).cast("string"), F.lit(""))), r"[^A-Za-z0-9]", ""))


def normalize_tin(column: ColumnOrName) -> Column:
    """Normalize a tax identifier to digit-only text."""
    return F.regexp_replace(F.trim(F.coalesce(as_column(column).cast("string"), F.lit(""))), r"[^0-9]", "")


def normalize_npi(column: ColumnOrName) -> Column:
    """Normalize an NPI to digit-only text."""
    return F.regexp_replace(F.trim(F.coalesce(as_column(column).cast("string"), F.lit(""))), r"[^0-9]", "")


def normalize_code(column: ColumnOrName) -> Column:
    """Normalize code-system tokens to uppercase alphanumeric text."""
    return F.upper(F.regexp_replace(F.trim(F.coalesce(as_column(column).cast("string"), F.lit(""))), r"[^A-Za-z0-9]", ""))


def normalize_icd10(column: ColumnOrName) -> Column:
    """Normalize ICD-10 values so dotted references match dotless claim values."""
    return normalize_code(column)


def normalize_hcpcs(column: ColumnOrName) -> Column:
    """Normalize HCPCS/CPT procedure codes."""
    return normalize_code(column)


def trueish(column: ColumnOrName) -> Column:
    """Interpret common enterprise boolean encodings as true."""
    return F.upper(F.trim(F.coalesce(as_column(column).cast("string"), F.lit("")))).isin(*sorted(TRUE_TEXT_VALUES))


def any_true(columns: Iterable[Column]) -> Column:
    """OR a sequence of boolean expressions, returning false for an empty sequence."""
    cols = list(columns)
    if not cols:
        return F.lit(False)
    return reduce(lambda left, right: left | right, cols)


def zero_if_null(column: ColumnOrName) -> Column:
    """Numeric null-to-zero helper used in display and tie-out math."""
    return F.coalesce(as_column(column), F.lit(0))


def safe_divide(numerator: ColumnOrName, denominator: ColumnOrName) -> Column:
    """Divide with null output when denominator is zero or null."""
    denom = as_column(denominator).cast("double")
    return F.when(denom.isNull() | (denom == 0), F.lit(None).cast("double")).otherwise(as_column(numerator).cast("double") / denom)


def date_literal(value) -> Column:
    """Build a Spark date literal from a Python date or ISO date string."""
    iso = value.isoformat() if hasattr(value, "isoformat") else str(value)
    return F.to_date(F.lit(iso))
