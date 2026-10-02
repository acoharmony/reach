"""Notebook assembly helpers for REACH copay-waiver analysis."""

from __future__ import annotations

from pyspark.sql import DataFrame

from reach._transforms._copay_waiver import (
    ATHENA_PATIENT_COPAY_ACTIVITY_VIEW,
    COPAY_WAIVER_COHORT_DIFFS_VIEW,
    COPAY_WAIVER_COVERAGE_STATUS_VIEW,
    COPAY_WAIVER_MEASURE_DIFFS_VIEW,
    COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW,
    COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW,
    COPAY_WAIVER_PATIENT_PROFILE_VIEW,
    COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW,
    COPAY_WAIVER_POPULATION_SUMMARY_VIEW,
    COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW,
    COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW,
    COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW,
    REACH_ALIGNED_POPULATION_VIEW,
    REACH_ALIGNMENT_COUNT_REFERENCE_VIEW,
    build_athena_patient_copay_activity,
    build_copay_waiver_cohort_diffs,
    build_copay_waiver_coverage_status,
    build_copay_waiver_measure_diffs,
    build_copay_waiver_no_secondary_due_not_waived_patients,
    build_copay_waiver_patient_cost_context,
    build_copay_waiver_patient_profile,
    build_copay_waiver_patient_responsibility,
    build_copay_waiver_population_summary,
    build_copay_waiver_revenue_cycle_transactions,
    build_copay_waiver_segment_breakdown,
    build_copay_waiver_support_service_detail,
    build_reach_alignment_count_reference,
    build_reach_census_aligned_population,
)

DEFAULT_CHARGE_TABLE = "prd_silver.financial.charge"
DEFAULT_PATIENT_INSURANCE_TABLE = "prd_silver.financial.patient_insurance"
DEFAULT_ATHENA_ACTIVITY_TABLE = "prd_athena_bronze.financials.activityfact"
DEFAULT_ATHENA_TRANSACTION_TABLE = "prd_athena_bronze.athenaone.transaction"
DEFAULT_HCMPI_MASTER_TABLE = "prd_silver.census.hcmpimaster"
DEFAULT_COST_TABLE = "prd_silver.patient_metrics.allpayer_cost_util_completed"
DEFAULT_UTILIZATION_TABLE = "prd_silver.patient_metrics.patient_registry_util_cost"
DEFAULT_QUALITY_TABLE = "prd_silver.quality.patient_quality_metrics"
DEFAULT_CENSUS_TABLE = "prd_silver.census.census"
DEFAULT_ALIGNMENT_OVERVIEW_TABLE = "prd_bronze.reach_ingested.beneficiary_alignment_report_overview"
DEFAULT_PROVIDER_ALIGNMENT_TABLE = "prd_bronze.reach_ingested.provider_alignment_report_r2history"
DEFAULT_PARTICIPANT_LIST_TABLE = "uat_sandbox.gov_programs.participant_list"
DEFAULT_PROVIDER_REFERENCE_TABLE = "prd_silver.organization.provider"


def load_copay_waiver_sources(
    spark,
    *,
    charge_table: str = DEFAULT_CHARGE_TABLE,
    patient_insurance_table: str = DEFAULT_PATIENT_INSURANCE_TABLE,
    athena_activity_table: str = DEFAULT_ATHENA_ACTIVITY_TABLE,
    athena_transaction_table: str = DEFAULT_ATHENA_TRANSACTION_TABLE,
    hcmpi_master_table: str = DEFAULT_HCMPI_MASTER_TABLE,
    cost_table: str = DEFAULT_COST_TABLE,
    utilization_table: str = DEFAULT_UTILIZATION_TABLE,
    quality_table: str = DEFAULT_QUALITY_TABLE,
    provider_reference_table: str = DEFAULT_PROVIDER_REFERENCE_TABLE,
) -> dict[str, DataFrame]:
    """Load canonical Databricks tables used by the copay-waiver workbook."""
    return {
        "charges": spark.table(charge_table),
        "patient_insurance": spark.table(patient_insurance_table),
        "athena_activity": spark.table(athena_activity_table),
        "athena_transactions": spark.table(athena_transaction_table),
        "hcmpi_master": spark.table(hcmpi_master_table),
        "cost_metrics": spark.table(cost_table),
        "utilization_metrics": spark.table(utilization_table),
        "quality_metrics": spark.table(quality_table),
        "provider_reference": spark.table(provider_reference_table),
    }


def build_copay_waiver_workbook(
    spark,
    *,
    aligned_population: DataFrame | None = None,
    performance_year: int,
    active_engaged_only: bool = False,
    charge_table: str = DEFAULT_CHARGE_TABLE,
    patient_insurance_table: str = DEFAULT_PATIENT_INSURANCE_TABLE,
    athena_activity_table: str = DEFAULT_ATHENA_ACTIVITY_TABLE,
    athena_transaction_table: str = DEFAULT_ATHENA_TRANSACTION_TABLE,
    hcmpi_master_table: str = DEFAULT_HCMPI_MASTER_TABLE,
    cost_table: str = DEFAULT_COST_TABLE,
    utilization_table: str = DEFAULT_UTILIZATION_TABLE,
    quality_table: str = DEFAULT_QUALITY_TABLE,
    provider_reference_table: str = DEFAULT_PROVIDER_REFERENCE_TABLE,
) -> dict[str, DataFrame]:
    """Build notebook outputs and temp views for secondary coverage/copay-waiver review."""
    sources = load_copay_waiver_sources(
        spark,
        charge_table=charge_table,
        patient_insurance_table=patient_insurance_table,
        athena_activity_table=athena_activity_table,
        athena_transaction_table=athena_transaction_table,
        hcmpi_master_table=hcmpi_master_table,
        cost_table=cost_table,
        utilization_table=utilization_table,
        quality_table=quality_table,
        provider_reference_table=provider_reference_table,
    )
    aligned = aligned_population if aligned_population is not None else build_reach_census_aligned_population(spark, active_engaged_only=active_engaged_only)
    aligned.createOrReplaceTempView(REACH_ALIGNED_POPULATION_VIEW)

    alignment_counts = build_reach_alignment_count_reference(spark)
    alignment_counts.createOrReplaceTempView(REACH_ALIGNMENT_COUNT_REFERENCE_VIEW)

    coverage_status = build_copay_waiver_coverage_status(
        aligned,
        sources["patient_insurance"],
        sources["charges"],
        performance_year=performance_year,
    )
    coverage_status.createOrReplaceTempView(COPAY_WAIVER_COVERAGE_STATUS_VIEW)

    athena_activity = build_athena_patient_copay_activity(
        sources["athena_activity"],
        sources["hcmpi_master"],
        performance_year=performance_year,
    )
    athena_activity.createOrReplaceTempView(ATHENA_PATIENT_COPAY_ACTIVITY_VIEW)

    patient_context_for_transactions = aligned.join(coverage_status, on="hcmpi", how="left")
    revenue_cycle_transactions = build_copay_waiver_revenue_cycle_transactions(
        patient_context_for_transactions,
        sources["athena_transactions"],
        sources["hcmpi_master"],
        performance_year=performance_year,
    )
    revenue_cycle_transactions.createOrReplaceTempView(COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW)

    patient_responsibility = build_copay_waiver_patient_responsibility(
        aligned,
        sources["charges"],
        performance_year=performance_year,
        athena_copay_activity=athena_activity,
        revenue_cycle_transactions=revenue_cycle_transactions,
    )
    patient_responsibility.createOrReplaceTempView(COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW)

    patient_profile = build_copay_waiver_patient_profile(
        aligned,
        coverage_status,
        patient_responsibility,
        performance_year=performance_year,
    )
    patient_profile.createOrReplaceTempView(COPAY_WAIVER_PATIENT_PROFILE_VIEW)

    cost_context = build_copay_waiver_patient_cost_context(
        patient_profile,
        sources["cost_metrics"],
        sources["utilization_metrics"],
        performance_year=performance_year,
    )
    support_service_detail = build_copay_waiver_support_service_detail(
        patient_profile,
        revenue_cycle_transactions,
        sources["provider_reference"],
    )
    not_waived_patients = build_copay_waiver_no_secondary_due_not_waived_patients(cost_context)
    summary = build_copay_waiver_population_summary(cost_context)
    segments = build_copay_waiver_segment_breakdown(cost_context)
    cohort_diffs = build_copay_waiver_cohort_diffs(cost_context)
    measure_diffs = build_copay_waiver_measure_diffs(
        cost_context,
        sources["quality_metrics"],
        performance_year=performance_year,
    )

    cost_context.createOrReplaceTempView(COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW)
    summary.createOrReplaceTempView(COPAY_WAIVER_POPULATION_SUMMARY_VIEW)
    segments.createOrReplaceTempView(COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW)
    support_service_detail.createOrReplaceTempView(COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW)
    not_waived_patients.createOrReplaceTempView(COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW)
    cohort_diffs.createOrReplaceTempView(COPAY_WAIVER_COHORT_DIFFS_VIEW)
    measure_diffs.createOrReplaceTempView(COPAY_WAIVER_MEASURE_DIFFS_VIEW)

    return {
        "aligned_population": aligned,
        "alignment_count_reference": alignment_counts,
        "coverage_status": coverage_status,
        "athena_patient_copay_activity": athena_activity,
        "revenue_cycle_transactions": revenue_cycle_transactions,
        "patient_responsibility": patient_responsibility,
        "patient_profile": patient_profile,
        "patient_cost_context": cost_context,
        "population_summary": summary,
        "segment_breakdown": segments,
        "support_service_detail": support_service_detail,
        "no_secondary_copay_due_not_waived_patients": not_waived_patients,
        "cohort_diffs": cohort_diffs,
        "measure_diffs": measure_diffs,
        "performance_year": performance_year,
        "active_engaged_only": active_engaged_only,
        "source_tables": {
            "charges": charge_table,
            "patient_insurance": patient_insurance_table,
            "athena_activity": athena_activity_table,
            "athena_transactions": athena_transaction_table,
            "hcmpi_master": hcmpi_master_table,
            "cost_metrics": cost_table,
            "utilization_metrics": utilization_table,
            "quality_metrics": quality_table,
            "provider_reference": provider_reference_table,
            "census": DEFAULT_CENSUS_TABLE,
            "beneficiary_alignment_report_overview": DEFAULT_ALIGNMENT_OVERVIEW_TABLE,
            "provider_alignment_report_r2history": DEFAULT_PROVIDER_ALIGNMENT_TABLE,
            "participant_list": DEFAULT_PARTICIPANT_LIST_TABLE,
        },
    }


__all__ = [
    "DEFAULT_ATHENA_ACTIVITY_TABLE",
    "DEFAULT_ATHENA_TRANSACTION_TABLE",
    "DEFAULT_ALIGNMENT_OVERVIEW_TABLE",
    "DEFAULT_CENSUS_TABLE",
    "DEFAULT_CHARGE_TABLE",
    "DEFAULT_COST_TABLE",
    "DEFAULT_HCMPI_MASTER_TABLE",
    "DEFAULT_UTILIZATION_TABLE",
    "DEFAULT_PATIENT_INSURANCE_TABLE",
    "DEFAULT_PARTICIPANT_LIST_TABLE",
    "DEFAULT_PROVIDER_REFERENCE_TABLE",
    "DEFAULT_PROVIDER_ALIGNMENT_TABLE",
    "DEFAULT_QUALITY_TABLE",
    "build_copay_waiver_workbook",
    "build_reach_census_aligned_population",
    "load_copay_waiver_sources",
]
