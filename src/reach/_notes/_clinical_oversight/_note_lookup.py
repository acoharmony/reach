"""Patient lookup helpers for clinical oversight notes."""

from __future__ import annotations

from functools import reduce
from operator import or_
import re

from pyspark.sql import functions as F


def blank_to_none(value):
    """Normalize blank scalar values before writing notes."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def clean_identifier(value: str | None) -> str:
    """Normalize HCMPI, MBI, or patient key text for lookup."""
    if value is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(value).strip()).upper()


def normalized_column(column: str):
    """Return a normalized Spark string column for identifier joins."""
    return F.upper(F.regexp_replace(F.trim(F.coalesce(F.col(column).cast("string"), F.lit(""))), r"[^A-Za-z0-9]", ""))


def optional_column(df, name: str, alias: str | None = None, data_type: str = "string"):
    """Select an optional Spark column or a typed null placeholder."""
    if name in df.columns:
        return F.col(name).cast(data_type).alias(alias or name)
    return F.lit(None).cast(data_type).alias(alias or name)


def lookup_patient_for_note(worklist, *, identifier: str | None = None, hcmpi: str | None = None, medicare_beneficiary_id: str | None = None):
    """Return the workbook row matching one HCMPI or MBI identifier."""
    target = clean_identifier(identifier or hcmpi or medicare_beneficiary_id)
    if not target:
        raise ValueError("Provide one HCMPI or MBI identifier.")
    searchable = [col for col in ("patient_key", "hcmpi", "MedicareBeneficiaryID") if col in worklist.columns]
    if not searchable:
        raise ValueError("The worklist has no patient identifier columns to search.")
    condition = reduce(or_, (normalized_column(col) == F.lit(target) for col in searchable))
    result = worklist.where(condition)
    order_cols = []
    if "provider_rank" in result.columns:
        order_cols.append(F.col("provider_rank").asc_nulls_last())
    if "power_score" in result.columns:
        order_cols.append(F.desc("power_score"))
    return result.orderBy(*order_cols) if order_cols else result


def row_dict_for_note(worklist, *, identifier: str | None, hcmpi: str | None, medicare_beneficiary_id: str | None) -> dict:
    """Return one matching worklist row as a plain dict."""
    rows = lookup_patient_for_note(worklist, identifier=identifier, hcmpi=hcmpi, medicare_beneficiary_id=medicare_beneficiary_id).limit(2).collect()
    if not rows:
        raise ValueError("No patient in the current review worklist matched that HCMPI or MBI.")
    return rows[0].asDict(recursive=True)
