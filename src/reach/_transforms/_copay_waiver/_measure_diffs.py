"""Quality-measure comparison for copay-waiver cohorts."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import safe_divide
from reach._expressions._quality import measure_direction_expr
from reach._transforms._copay_waiver._common import _comparison_group_expr, _hcmpi_key, _require_columns, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_MEASURE_DIFFS_VIEW
from reach._transforms._registry import register_transform


_QUALITY_COLUMNS = (
    "measure_id",
    "measure_name",
    "year",
    "month",
    "hcmpi",
    "denominator",
    "numerator",
)


@register_transform(
    "copay_waiver_measure_diffs",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_MEASURE_DIFFS_VIEW,
    description="Quality measure rates for copays-waived patients versus the not-waived aligned population.",
)
def build_copay_waiver_measure_diffs(patient_profile: DataFrame, quality_metrics: DataFrame, *, performance_year: int) -> DataFrame:
    """Compare quality numerator/denominator rates between waived and not-waived patients."""
    source = _select_quality_source(quality_metrics, performance_year=performance_year)
    base = _with_hcmpi_key(patient_profile).withColumn("copay_waiver_comparison_group", _comparison_group_expr()).select("hcmpi", "copay_waiver_comparison_group")
    joined = base.join(source, on="hcmpi", how="inner")
    return (
        joined.groupBy("measure_id", "measure_name")
        .agg(
            F.countDistinct(F.when(F.col("copay_waiver_comparison_group") == F.lit("copays_waived"), F.col("hcmpi"))).alias("waived_patients"),
            F.countDistinct(F.when(F.col("copay_waiver_comparison_group") == F.lit("not_copays_waived"), F.col("hcmpi"))).alias("not_waived_patients"),
            F.sum(F.when(F.col("copay_waiver_comparison_group") == F.lit("copays_waived"), F.col("denominator")).otherwise(F.lit(0.0))).alias("waived_denominator"),
            F.sum(F.when(F.col("copay_waiver_comparison_group") == F.lit("copays_waived"), F.col("numerator")).otherwise(F.lit(0.0))).alias("waived_numerator"),
            F.sum(F.when(F.col("copay_waiver_comparison_group") == F.lit("not_copays_waived"), F.col("denominator")).otherwise(F.lit(0.0))).alias(
                "not_waived_denominator"
            ),
            F.sum(F.when(F.col("copay_waiver_comparison_group") == F.lit("not_copays_waived"), F.col("numerator")).otherwise(F.lit(0.0))).alias("not_waived_numerator"),
        )
        .withColumn("measure_direction", measure_direction_expr("measure_id"))
        .withColumn("waived_rate", safe_divide("waived_numerator", "waived_denominator"))
        .withColumn("not_waived_rate", safe_divide("not_waived_numerator", "not_waived_denominator"))
        .withColumn("rate_diff_waived_minus_not", F.col("waived_rate") - F.col("not_waived_rate"))
        .orderBy("measure_id", "measure_name")
    )


def _select_quality_source(quality_metrics: DataFrame, *, performance_year: int) -> DataFrame:
    source = _require_columns(quality_metrics, _QUALITY_COLUMNS, context="quality patient metrics for copay measure diffs")
    quality = (
        quality_metrics.select(*source)
        .withColumn("hcmpi", _hcmpi_key("hcmpi"))
        .withColumn("_quality_year", F.col("year").cast("int"))
        .withColumn("_quality_month", F.col("month").cast("int"))
        .where(F.col("_quality_year") == int(performance_year))
    )
    latest_month = quality.agg(F.max("_quality_month").alias("_latest_quality_month"))
    return (
        quality.crossJoin(latest_month)
        .where(F.col("_quality_month") == F.col("_latest_quality_month"))
        .select(
            "hcmpi",
            F.col("measure_id").cast("string").alias("measure_id"),
            F.col("measure_name").cast("string").alias("measure_name"),
            F.col("denominator").cast("double").alias("denominator"),
            F.col("numerator").cast("double").alias("numerator"),
        )
        .where(F.col("hcmpi").isNotNull() & F.col("measure_id").isNotNull())
    )


__all__ = ["build_copay_waiver_measure_diffs"]
