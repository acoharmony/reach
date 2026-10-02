"""Clinician and patient identity helpers for clinical oversight transforms."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import chain

from pyspark.sql import Column, DataFrame, functions as F

from reach._expressions._alignment import patient_key_expr
from reach._transforms._clinical_oversight._constants import (
    DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES,
    DISCUSSION_GROUP_CLINICIAN_NPIS,
)


def _npi_key(value: str) -> str:
    return "".join(char for char in str(value or "") if char.isdigit())


def _npi_key_col(column: Column) -> Column:
    return F.regexp_replace(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"[^0-9]", "")


def _clinician_npi_keys(clinician_npis: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(key for key in {_npi_key(npi) for npi in clinician_npis} if key))


def _display_name_expr(npi_col: Column, fallback: Column) -> Column:
    mapping = F.create_map(
        *list(chain.from_iterable((F.lit(npi), F.lit(name)) for npi, name in DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES.items()))
    )
    mapped = mapping[_npi_key_col(npi_col)]
    return F.coalesce(mapped, F.when(F.length(fallback) > 0, fallback), npi_col.cast("string"))


def with_review_identity(worklist: DataFrame) -> DataFrame:
    """Add stable patient and clinician keys for clinical oversight review."""
    if "sva_provider_name" in worklist.columns:
        provider_name = F.trim(F.coalesce(F.col("sva_provider_name").cast("string"), F.lit("")))
    elif "clinician_name" in worklist.columns:
        provider_name = F.trim(F.coalesce(F.col("clinician_name").cast("string"), F.lit("")))
    else:
        provider_name = F.lit("")
    if "latest_aligned_npi" in worklist.columns:
        clinician_npi = F.col("latest_aligned_npi")
    elif "clinician_npi" in worklist.columns:
        clinician_npi = F.col("clinician_npi")
    else:
        clinician_npi = F.lit(None).cast("string")
    return (
        worklist.withColumn("patient_key", patient_key_expr())
        .withColumn("clinician_npi", _npi_key_col(clinician_npi))
        .withColumn("clinician_name", _display_name_expr(clinician_npi, provider_name))
    )


def filter_discussion_group(worklist: DataFrame, clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS) -> DataFrame:
    """Restrict rows to the REACH clinical oversight discussion group NPIs."""
    keys = _clinician_npi_keys(clinician_npis or ())
    identified = with_review_identity(worklist)
    if not keys:
        return identified
    return identified.where(F.col("clinician_npi").isin(*keys))
