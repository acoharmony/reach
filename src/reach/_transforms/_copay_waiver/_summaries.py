"""Summary tables for copay-waiver analysis."""

from __future__ import annotations

from collections.abc import Sequence
from functools import reduce

from pyspark.sql import DataFrame, Window, functions as F

from reach._expressions._common import blank_to_null, safe_divide
from reach._transforms._copay_waiver._common import _comparison_group_expr, _population_group_expr, _require_columns, _with_hcmpi_key
from reach._transforms._copay_waiver._views import (
    COPAY_WAIVER_COHORT_DIFFS_VIEW,
    COPAY_WAIVER_POPULATION_SUMMARY_VIEW,
    COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW,
    DEFAULT_NUMERIC_DIFF_COLUMNS,
    DEFAULT_SEGMENT_COLUMNS,
)
from reach._transforms._registry import register_transform


_POPULATION_SUMMARY_REQUIRED_COLUMNS = (
    "hcmpi",
    "no_secondary_or_tertiary_coverage_flag",
    "copays_waived_flag",
    "performance_year_charge_count",
    "performance_year_claim_count",
    "alignment_period_copay_due_amt",
    "alignment_period_copay_due_without_secondary_amt",
    "alignment_period_copay_waived_amt",
    "alignment_period_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "pre_alignment_patient_payment_amt",
    "alignment_period_patient_payment_on_responsibility_service_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_copay_unresolved_amt",
    "Total_Paid_Dollars",
    "Total_Paid_PMPM",
    "Prosp_RAF_v28",
    "Concur_RAF_v28",
    "Member_Months",
)


@register_transform(
    "copay_waiver_population_summary",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_POPULATION_SUMMARY_VIEW,
    description="Population-level counts and amounts for secondary coverage, copay due, and waived copays.",
)
def build_copay_waiver_population_summary(patient_profile: DataFrame) -> DataFrame:
    """Summarize aligned patients by secondary-coverage and copay-waiver state."""
    prof = _with_hcmpi_key(patient_profile)
    _require_columns(prof, _POPULATION_SUMMARY_REQUIRED_COLUMNS, context="patient profile for copay population summary")
    prof = prof.withColumn("copay_waiver_population_group", _population_group_expr())
    agg_exprs = [
        F.countDistinct("hcmpi").alias("patients"),
        F.sum(F.col("no_secondary_or_tertiary_coverage_flag").cast("int")).alias("patients_no_secondary_or_tertiary"),
        F.sum(F.when(F.col("alignment_period_copay_due_without_secondary_amt") > 0, F.lit(1)).otherwise(F.lit(0))).alias("patients_no_secondary_with_copay_due"),
        F.sum(F.col("copays_waived_flag").cast("int")).alias("patients_copays_waived"),
        F.sum("performance_year_charge_count").alias("performance_year_charge_count"),
        F.sum("performance_year_claim_count").alias("performance_year_claim_count"),
        F.sum("alignment_period_copay_due_amt").alias("alignment_period_copay_due_amt"),
        F.sum("alignment_period_copay_due_without_secondary_amt").alias("alignment_period_copay_due_without_secondary_amt"),
        F.sum("alignment_period_copay_waived_amt").alias("alignment_period_copay_waived_amt"),
        F.sum("alignment_period_patient_responsibility_transfer_amt").alias("alignment_period_patient_responsibility_transfer_amt"),
        F.sum("posted_rev_cycle_patient_payment_amt").alias("posted_rev_cycle_patient_payment_amt"),
        F.sum("pre_alignment_patient_payment_amt").alias("pre_alignment_patient_payment_amt"),
        F.sum("alignment_period_patient_payment_on_responsibility_service_amt").alias("alignment_period_patient_payment_on_responsibility_service_amt"),
        F.sum("alignment_period_reach_cost_sharing_support_amt").alias("alignment_period_reach_cost_sharing_support_amt"),
        F.sum("alignment_period_patient_copay_payment_amt").alias("alignment_period_patient_copay_payment_amt"),
        F.sum("alignment_period_copay_unresolved_amt").alias("alignment_period_copay_unresolved_amt"),
        F.round(F.avg(F.col("Total_Paid_Dollars").cast("double")), 2).alias("avg_Total_Paid_Dollars"),
        F.round(F.avg(F.col("Total_Paid_PMPM").cast("double")), 2).alias("avg_Total_Paid_PMPM"),
        F.round(F.avg(F.col("Prosp_RAF_v28").cast("double")), 2).alias("avg_Prosp_RAF_v28"),
        F.round(F.avg(F.col("Concur_RAF_v28").cast("double")), 2).alias("avg_Concur_RAF_v28"),
        F.round(F.avg(F.col("Member_Months").cast("double")), 2).alias("avg_Member_Months"),
    ]
    summary = prof.groupBy("copay_waiver_population_group").agg(*agg_exprs)
    total_patients = summary.agg(F.sum("patients").alias("total_aligned_patients"))
    return (
        summary.crossJoin(total_patients)
        .withColumn("patients_pct_of_aligned", safe_divide("patients", "total_aligned_patients"))
        .withColumn("copay_waiver_rate_among_due_without_secondary", safe_divide("patients_copays_waived", "patients_no_secondary_with_copay_due"))
        .withColumn("alignment_period_copay_dollars_waived_rate", safe_divide("alignment_period_copay_waived_amt", "alignment_period_copay_due_without_secondary_amt"))
        .withColumn("alignment_period_patient_copay_payment_rate", safe_divide("alignment_period_patient_copay_payment_amt", "alignment_period_copay_due_amt"))
        .withColumn("alignment_period_copay_unresolved_rate", safe_divide("alignment_period_copay_unresolved_amt", "alignment_period_copay_due_without_secondary_amt"))
        .orderBy("copay_waiver_population_group")
    )


def _segment_value_expr(segment: str):
    value = blank_to_null(segment)
    if segment in {"clinician_npi", "latest_aligned_npi", "primary_provider_npi", "primary_provider_npi_current"}:
        name = blank_to_null("clinician_name")
        return F.when(value.isNotNull() & name.isNotNull(), F.concat_ws(" - ", value, name)).otherwise(value)
    return value


@register_transform(
    "copay_waiver_segment_breakdown",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW,
    description="Human-readable segment distribution for waived versus not-waived aligned patients.",
)
def build_copay_waiver_segment_breakdown(patient_profile: DataFrame, segment_columns: Sequence[str] = DEFAULT_SEGMENT_COLUMNS) -> DataFrame:
    """Build categorical breakdowns for demographics, clinician, payer, status, and office."""
    base = _with_hcmpi_key(patient_profile)
    required = tuple(column for column in segment_columns if column != "age_band") + ("BirthDate", "copays_waived_flag", "clinician_name")
    _require_columns(base, required, context="patient profile for copay segment breakdown")
    base = base.withColumn("copay_waiver_comparison_group", _comparison_group_expr())
    age = F.floor(F.months_between(F.current_date(), F.to_date("BirthDate")) / F.lit(12.0))
    base = base.withColumn(
        "age_band",
        F.when(age < 65, F.lit("<65")).when(age < 75, F.lit("65-74")).when(age < 85, F.lit("75-84")).when(age >= 85, F.lit("85+")),
    )
    frames = []
    for segment in segment_columns:
        segment_frame = (
            base.withColumn("segment_value", F.coalesce(_segment_value_expr(segment), F.lit("Unknown")))
            .groupBy("copay_waiver_comparison_group", "segment_value")
            .agg(F.countDistinct("hcmpi").alias("patients"))
            .withColumn("segment", F.lit(segment))
        )
        frames.append(segment_frame.select("segment", "segment_value", "copay_waiver_comparison_group", "patients"))
    result = reduce(lambda left, right: left.unionByName(right), frames)
    group_total = F.sum("patients").over(Window.partitionBy("segment", "copay_waiver_comparison_group"))
    return result.withColumn("patients_pct_within_group", safe_divide("patients", group_total)).orderBy("segment", "segment_value", "copay_waiver_comparison_group")


@register_transform(
    "copay_waiver_cohort_diffs",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_COHORT_DIFFS_VIEW,
    description="Numeric metric deltas for copays-waived patients versus the not-waived aligned population.",
)
def build_copay_waiver_cohort_diffs(patient_profile: DataFrame, numeric_columns: Sequence[str] = DEFAULT_NUMERIC_DIFF_COLUMNS) -> DataFrame:
    """Return long-form average/sum diffs for waived patients versus all not-waived aligned patients."""
    base = _with_hcmpi_key(patient_profile)
    required_metrics = tuple(column for column in numeric_columns if column != "age_years")
    _require_columns(base, (*required_metrics, "BirthDate", "copays_waived_flag"), context="patient profile for copay cohort diffs")
    base = base.withColumn("copay_waiver_comparison_group", _comparison_group_expr()).withColumn(
        "age_years", F.floor(F.months_between(F.current_date(), F.to_date("BirthDate")) / F.lit(12.0))
    )
    frames = []
    for metric in numeric_columns:
        metric_value = F.col(metric).cast("double")
        waived_metric = F.when(F.col("copay_waiver_comparison_group") == F.lit("copays_waived"), metric_value)
        not_waived_metric = F.when(F.col("copay_waiver_comparison_group") == F.lit("not_copays_waived"), metric_value)
        frames.append(
            base.agg(
                F.count(waived_metric).alias("waived_patients_with_metric"),
                F.count(not_waived_metric).alias("not_waived_patients_with_metric"),
                F.avg(waived_metric).alias("waived_avg_value"),
                F.avg(not_waived_metric).alias("not_waived_avg_value"),
                F.sum(waived_metric).alias("waived_sum_value"),
                F.sum(not_waived_metric).alias("not_waived_sum_value"),
            ).select(
                F.lit(metric).alias("metric"),
                "waived_patients_with_metric",
                "not_waived_patients_with_metric",
                "waived_avg_value",
                "not_waived_avg_value",
                (F.col("waived_avg_value") - F.col("not_waived_avg_value")).alias("avg_diff_waived_minus_not"),
                "waived_sum_value",
                "not_waived_sum_value",
                (F.col("waived_sum_value") - F.col("not_waived_sum_value")).alias("sum_diff_waived_minus_not"),
            )
        )
    return reduce(lambda left, right: left.unionByName(right), frames).orderBy("metric")


__all__ = [
    "build_copay_waiver_cohort_diffs",
    "build_copay_waiver_population_summary",
    "build_copay_waiver_segment_breakdown",
]
