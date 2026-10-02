"""Spark transform for REACH Health Equity Data Reporting eligibility."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, Column, functions as F

from reach._expressions._common import date_literal
from reach._expressions._hedr import hedr_status_expr, missing_data_fields_expr, reach_hedr_denominator_expr, reach_hedr_numerator_expr, reach_month_count_expr
from reach._transforms._registry import register_transform

DEFAULT_HEDR_REQUIRED_COLUMNS = (
    "bene_first_name",
    "bene_last_name",
    "bene_address_line_1",
    "bene_city",
    "bene_state",
    "bene_zip_5",
    "bene_race_ethnicity",
)


def _first_existing(df: DataFrame, names: Sequence[str], alias: str, data_type: str = "string") -> Column:
    for name in names:
        if name in df.columns:
            return F.col(name).cast(data_type).alias(alias)
    return F.lit(None).cast(data_type).alias(alias)


def _py_months_from_dates(start_col: str, end_col: str, performance_year: int) -> Column:
    py_start = date_literal(date(performance_year, 1, 1))
    october_first = date_literal(date(performance_year, 10, 1))
    start_date = F.to_date(F.col(start_col))
    end_date = F.coalesce(F.to_date(F.col(end_col)), october_first)
    effective_start = F.greatest(start_date, py_start)
    effective_end = F.least(end_date, october_first)
    return F.when(
        start_date.isNull() | (effective_start > effective_end),
        F.lit(0),
    ).otherwise((F.year(effective_end) * 12 + F.month(effective_end)) - (F.year(effective_start) * 12 + F.month(effective_start)) + 1)


def _prepare_bar(bar: DataFrame, required_columns: Sequence[str]) -> DataFrame:
    selected = [
        _first_existing(bar, ("bene_mbi", "MedicareBeneficiaryID", "mbi", "member_id"), "mbi"),
        _first_existing(bar, ("start_date", "reach_start_date", "alignment_start_date", "first_reach_date"), "reach_start_date", "date"),
        _first_existing(bar, ("end_date", "reach_end_date", "alignment_end_date", "last_reach_date"), "reach_end_date", "date"),
        _first_existing(bar, ("bene_date_of_death", "death_date", "dod"), "death_date", "date"),
        _first_existing(bar, ("bene_date_of_birth", "date_of_birth", "birth_date"), "date_of_birth", "date"),
        _first_existing(bar, ("source_filename", "FileNM"), "source_filename"),
        _first_existing(bar, ("file_date", "FileDate"), "bar_file_date", "date"),
    ]
    selected_names = {"mbi", "reach_start_date", "reach_end_date", "death_date", "date_of_birth", "source_filename", "bar_file_date"}
    for col in required_columns:
        if col in bar.columns and col not in selected_names:
            selected.append(F.col(col))
    return bar.select(*selected).withColumn("first_reach_date", F.col("reach_start_date")).withColumn("last_reach_date", F.col("reach_end_date"))


def _join_alignment(base: DataFrame, alignment: DataFrame | None, performance_year: int) -> DataFrame:
    if alignment is None:
        return base.withColumn(f"reach_months_{performance_year}", _py_months_from_dates("first_reach_date", "last_reach_date", performance_year))
    key = next((col for col in ("current_mbi", "mbi", "person_id", "member_id") if col in alignment.columns), None)
    if key is None:
        return base.withColumn(f"reach_months_{performance_year}", _py_months_from_dates("first_reach_date", "last_reach_date", performance_year))
    keep = [F.col(key).alias("_alignment_mbi")]
    date_columns = ("first_reach_date", "last_reach_date", "observable_start", "observable_end")
    for col in alignment.columns:
        if col == key:
            continue
        if col in {"months_in_reach", f"months_in_reach_{performance_year}"} or (col.startswith(f"ym_{performance_year}") and col.endswith("_reach")):
            keep.append(F.col(col))
        elif col in date_columns:
            keep.append(F.col(col).alias(f"_alignment_{col}"))
    joined = base.join(alignment.select(*keep), base.mbi == F.col("_alignment_mbi"), "left").drop("_alignment_mbi")
    for col in date_columns:
        alignment_col = f"_alignment_{col}"
        if alignment_col not in joined.columns:
            continue
        if col in joined.columns:
            joined = joined.withColumn(col, F.coalesce(F.col(alignment_col), F.col(col)))
        else:
            joined = joined.withColumn(col, F.col(alignment_col))
        joined = joined.drop(alignment_col)
    schema = joined.columns
    reach_month_col = f"reach_months_{performance_year}"
    if reach_month_col not in schema:
        joined = joined.withColumn(reach_month_col, reach_month_count_expr(schema, performance_year, through_month=10))
    return joined


@register_transform(
    "hedr_eligibility",
    concern="hedr",
    output_table="reach_hedr_eligibility",
    description="REACH HEDR denominator/numerator eligibility from warehouse BAR/alignment/SDOH frames.",
)
def build_reach_hedr_eligibility(
    bar: DataFrame,
    *,
    performance_year: int,
    alignment: DataFrame | None = None,
    sdoh: DataFrame | None = None,
    required_data_columns: Sequence[str] = DEFAULT_HEDR_REQUIRED_COLUMNS,
) -> DataFrame:
    """Build one HEDR eligibility output table from already-loaded Spark DataFrames."""
    data = _join_alignment(_prepare_bar(bar, required_data_columns), alignment, performance_year)
    if sdoh is not None and "mbi" in sdoh.columns:
        data = data.join(sdoh.select("mbi", "date_assessment_complete", "assessment_declined"), on="mbi", how="left")
        required_data_columns = tuple(required_data_columns) + ("date_assessment_complete",)
    denominator = reach_hedr_denominator_expr(performance_year, schema=data.columns)
    numerator = reach_hedr_numerator_expr(performance_year, schema=data.columns, required_data_columns=required_data_columns)
    if "assessment_declined" in data.columns:
        numerator = numerator & (F.coalesce(F.col("assessment_declined"), F.lit("")) != F.lit("Yes"))
    return (
        data.withColumn("hedr_denominator", denominator)
        .withColumn("hedr_numerator", numerator)
        .withColumn("hedr_status", hedr_status_expr())
        .withColumn("missing_data_fields", F.when(F.col("hedr_denominator") & ~F.col("hedr_numerator"), missing_data_fields_expr(required_data_columns, schema=data.columns)))
        .withColumn("performance_year", F.lit(int(performance_year)))
        .withColumn("alignment_date", date_literal(date(performance_year, 10, 1)))
        .select(
            "mbi",
            "performance_year",
            "alignment_date",
            "hedr_denominator",
            "hedr_numerator",
            "hedr_status",
            "missing_data_fields",
            "first_reach_date",
            "last_reach_date",
            f"reach_months_{performance_year}",
            "bar_file_date",
            "source_filename",
        )
    )
