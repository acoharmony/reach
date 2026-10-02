"""Spark transforms for consolidated REACH alignment views."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, functions as F

from reach._expressions._alignment import latest_alignment_year_month, living_beneficiary_expr
from reach._expressions._common import blank_to_null, normalize_mbi, normalize_npi, normalize_tin
from reach._transforms._registry import register_transform


def _optional_col(df: DataFrame, name: str, alias: str | None = None, data_type: str = "string") -> Column:
    output_name = alias or name
    if name in df.columns:
        return F.col(name).cast(data_type).alias(output_name)
    return F.lit(None).cast(data_type).alias(output_name)


def _first_populated(*column_names: str) -> Column:
    return F.coalesce(*(blank_to_null(name) for name in column_names))


def _current_program_expr(reach_col: str, mssp_col: str) -> Column:
    reach = F.coalesce(F.col(reach_col).cast("boolean"), F.lit(False))
    mssp = F.coalesce(F.col(mssp_col).cast("boolean"), F.lit(False))
    return (
        F.when(reach & mssp, F.lit("REACH+MSSP"))
        .when(reach, F.lit("REACH"))
        .when(mssp, F.lit("MSSP"))
        .otherwise(F.lit("FFS"))
    )


def _current_attribution_tin_expr() -> Column:
    return _first_populated(
        "aligned_provider_tin",
        "current_provider_tin",
        "reach_tin",
        "mssp_tin",
        "voluntary_provider_tin",
    )


def _current_attribution_npi_expr() -> Column:
    return _first_populated(
        "aligned_provider_npi",
        "reach_npi",
        "mssp_npi",
        "voluntary_provider_npi",
    )


def _current_attribution_provider_expr() -> Column:
    return _first_populated(
        "aligned_provider_org",
        "reach_provider_name",
        "mssp_provider_name",
        "voluntary_provider_name",
    )


def _source_columns(alignment: DataFrame, extra_columns: Sequence[str]) -> list[Column]:
    output_names = {
        "mbi",
        "hcmpi",
        "member_id",
        "person_id",
        "death_date",
        "death_date_alt",
        "aligned_provider_tin",
        "aligned_provider_npi",
        "aligned_provider_org",
        "current_provider_tin",
        "reach_tin",
        "reach_npi",
        "reach_provider_name",
        "mssp_tin",
        "mssp_npi",
        "mssp_provider_name",
        "voluntary_provider_tin",
        "voluntary_provider_npi",
        "voluntary_provider_name",
        "primary_alignment_source",
    }
    columns = [
        _optional_col(alignment, "current_mbi", "mbi"),
        _optional_col(alignment, "hcmpi"),
        _optional_col(alignment, "member_id"),
        _optional_col(alignment, "person_id"),
        _optional_col(alignment, "death_date", data_type="date"),
        _optional_col(alignment, "date_of_death", "death_date_alt", "date"),
        _optional_col(alignment, "aligned_provider_tin"),
        _optional_col(alignment, "aligned_provider_npi"),
        _optional_col(alignment, "aligned_provider_org"),
        _optional_col(alignment, "current_provider_tin"),
        _optional_col(alignment, "reach_tin"),
        _optional_col(alignment, "reach_npi"),
        _optional_col(alignment, "reach_provider_name"),
        _optional_col(alignment, "mssp_tin"),
        _optional_col(alignment, "mssp_npi"),
        _optional_col(alignment, "mssp_provider_name"),
        _optional_col(alignment, "voluntary_provider_tin"),
        _optional_col(alignment, "voluntary_provider_npi"),
        _optional_col(alignment, "voluntary_provider_name"),
        _optional_col(alignment, "primary_alignment_source"),
    ]
    for name in extra_columns:
        if name in alignment.columns and name not in output_names:
            columns.append(F.col(name))
    return columns


@register_transform(
    "current_alignment_attribution",
    concern="alignment",
    output_table="reach_current_alignment_attribution",
    description="Latest wide-month alignment status with normalized current TIN/NPI/provider attribution.",
)
def build_current_alignment_attribution(
    alignment: DataFrame,
    *,
    latest_year_month: str | None = None,
    include_dead: bool = False,
    extra_columns: Sequence[str] = (),
) -> DataFrame:
    """Build a current beneficiary attribution view from consolidated alignment."""
    latest_ym = latest_year_month or latest_alignment_year_month(alignment.columns)
    reach_col = f"ym_{latest_ym}_reach"
    mssp_col = f"ym_{latest_ym}_mssp"
    if reach_col not in alignment.columns and mssp_col not in alignment.columns:
        raise ValueError(f"Alignment frame does not contain {reach_col!r} or {mssp_col!r}.")
    existing_extra_columns = tuple(name for name in extra_columns if name in alignment.columns)

    prepared = alignment.select(
        *_source_columns(alignment, existing_extra_columns),
        F.coalesce(F.col(reach_col).cast("boolean"), F.lit(False)).alias("latest_reach") if reach_col in alignment.columns else F.lit(False).alias("latest_reach"),
        F.coalesce(F.col(mssp_col).cast("boolean"), F.lit(False)).alias("latest_mssp") if mssp_col in alignment.columns else F.lit(False).alias("latest_mssp"),
    )
    if "death_date_alt" in prepared.columns:
        prepared = prepared.withColumn("death_date", F.coalesce(F.col("death_date"), F.col("death_date_alt"))).drop("death_date_alt")
    if not include_dead:
        prepared = prepared.where(living_beneficiary_expr(prepared.columns))

    return (
        prepared.withColumn("latest_alignment_year_month", F.lit(latest_ym))
        .withColumn("current_program", _current_program_expr("latest_reach", "latest_mssp"))
        .withColumn("current_attribution_tin", normalize_tin(_current_attribution_tin_expr()))
        .withColumn("current_attribution_npi", normalize_npi(_current_attribution_npi_expr()))
        .withColumn("current_attribution_provider", _current_attribution_provider_expr())
        .withColumn("normalized_mbi", normalize_mbi("mbi"))
        .withColumn("patient_key", F.coalesce(blank_to_null("hcmpi"), normalize_mbi("mbi"), blank_to_null("member_id"), blank_to_null("person_id")))
        .select(
            "patient_key",
            "normalized_mbi",
            "hcmpi",
            "member_id",
            "person_id",
            "latest_alignment_year_month",
            "latest_reach",
            "latest_mssp",
            "current_program",
            "current_attribution_tin",
            "current_attribution_npi",
            "current_attribution_provider",
            "primary_alignment_source",
            "death_date",
            *existing_extra_columns,
        )
    )


@register_transform(
    "current_alignment_tin_summary",
    concern="alignment",
    output_table="reach_current_alignment_tin_summary",
    description="TIN-level summary of the latest consolidated alignment attribution view.",
)
def build_current_alignment_tin_summary(current_alignment_attribution: DataFrame) -> DataFrame:
    """Summarize latest alignment attribution by current TIN/provider/program."""
    return (
        current_alignment_attribution.groupBy(
            "current_attribution_tin",
            "current_attribution_provider",
            "current_program",
            "latest_alignment_year_month",
        )
        .agg(
            F.countDistinct("patient_key").alias("beneficiaries"),
            F.sum(F.col("latest_reach").cast("int")).alias("reach_beneficiaries"),
            F.sum(F.col("latest_mssp").cast("int")).alias("mssp_beneficiaries"),
        )
        .orderBy(F.desc("beneficiaries"), "current_attribution_tin")
    )
