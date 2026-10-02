"""Spark transforms for warehouse-ready REACH quality summaries."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, functions as F

from reach._expressions._quality import (
    continuous_measurement_year_enrollment_expr,
    continuous_prior_year_enrollment_expr,
    measure_direction_expr,
    measure_rate_expr,
)
from reach._transforms._registry import register_transform


@register_transform("quality_measure_rates", concern="quality", output_table="reach_quality_measure_rates")
def build_quality_measure_rates(measure_counts: DataFrame, *, rate_scale: float = 1.0) -> DataFrame:
    """Add rate and direction fields to numerator/denominator measure counts."""
    return (
        measure_counts.withColumn("rate", measure_rate_expr(scale=rate_scale))
        .withColumn("measure_direction", measure_direction_expr())
        .select("measure_id", "measure_name", "numerator", "denominator", "rate", "measure_direction")
    )


@register_transform("uamcc_denominator_keep_set", concern="quality", output_table="reach_uamcc_denominator_keep_set")
def build_uamcc_denominator_keep_set(eligibility: DataFrame, claims: DataFrame, *, performance_year: int) -> DataFrame:
    """Apply the REACH UAMCC enrollment/hospice denominator exclusion keep-set."""
    py_start = date(performance_year, 1, 1)
    enrollment = (
        eligibility.select("person_id", "enrollment_start_date", "enrollment_end_date", F.col("death_date").alias("dod"))
        .groupBy("person_id")
        .agg(F.min("enrollment_start_date").alias("enroll_start"), F.max("enrollment_end_date").alias("enroll_end"), F.min("dod").alias("dod"))
        .where(continuous_prior_year_enrollment_expr(performance_year) & continuous_measurement_year_enrollment_expr(performance_year))
        .select("person_id")
    )
    if "claim_start_date" in claims.columns and "bill_type_code" in claims.columns:
        hospice_pre_py = (
            claims.where(F.substring(F.regexp_replace(F.col("bill_type_code").cast("string"), r"^0+", ""), 1, 2) == "81")
            .where(F.to_date("claim_start_date") <= F.lit(py_start))
            .select("person_id")
            .dropDuplicates()
        )
        return enrollment.join(hospice_pre_py, on="person_id", how="left_anti")
    return enrollment
