"""Spark expressions for REACH participant/provider reference data."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, functions as F

from reach._expressions._common import as_column, blank_to_null, normalize_npi, normalize_tin
from reach._expressions._registry import register_expression


@register_expression("participant_provider", concern="participant")
def participant_provider_expr(
    *,
    performance_year: int | None = None,
    individual_npi_col: str = "individual_npi",
    provider_type_col: str = "provider_type",
    provider_class_col: str = "provider_class",
    performance_year_col: str = "performance_year",
) -> Column:
    """True for current individual participant providers in participant_list."""
    expr = normalize_npi(individual_npi_col) != F.lit("")
    expr = expr & (F.upper(F.trim(as_column(provider_type_col).cast("string"))) == F.lit("INDIVIDUAL PROVIDER"))
    expr = expr & (F.upper(F.trim(as_column(provider_class_col).cast("string"))) == F.lit("PARTICIPANT PROVIDER"))
    if performance_year is not None:
        year_text = F.regexp_replace(as_column(performance_year_col).cast("string"), r"[^0-9]", "").cast("int")
        expr = expr & (year_text == F.lit(int(performance_year)))
    return expr


@register_expression("performance_year_from_file_date", concern="participant")
def performance_year_from_file_date_expr(file_date_col: str = "file_date") -> Column:
    """Derive CMS-style performance-year text from a file date."""
    return F.concat(F.lit("PY"), F.year(F.to_date(as_column(file_date_col))).cast("string"))


@register_expression("entity_column_fills", concern="participant")
def entity_column_fill_exprs(identity: dict[str, Any]) -> list[Column]:
    """Coalesce missing participant-list entity columns from an ACO identity row."""
    return [
        F.coalesce(blank_to_null("entity_id"), F.lit(identity.get("apm_id"))).alias("entity_id"),
        F.coalesce(blank_to_null(normalize_tin("entity_tin")), F.lit(identity.get("tin"))).alias("entity_tin"),
        F.coalesce(blank_to_null("entity_legal_business_name"), F.lit(identity.get("legal_business_name"))).alias("entity_legal_business_name"),
    ]
