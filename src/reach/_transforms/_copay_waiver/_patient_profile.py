"""Patient-grain copay-waiver audit profile."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import blank_to_null, safe_divide, zero_if_null
from reach._transforms._copay_waiver._common import _select_required, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_PATIENT_PROFILE_VIEW, DEFAULT_ALIGNED_COLUMNS
from reach._transforms._registry import register_transform


_COVERAGE_COLUMNS = (
    "hcmpi",
    "has_patient_secondary_coverage",
    "has_patient_tertiary_coverage",
    "has_secondary_or_tertiary_coverage_flag",
    "no_secondary_or_tertiary_coverage_flag",
    "coverage_decision_source",
    "patient_secondary_insurance_package_count",
    "patient_secondary_insurance_names",
    "patient_secondary_insurance_groups",
    "patient_tertiary_insurance_package_count",
    "patient_tertiary_insurance_names",
    "patient_tertiary_insurance_groups",
    "priority_3_self_pay_or_no_coverage_flag",
    "priority_3_self_pay_or_no_coverage_names",
    "claim_time_secondary_coverage_charge_count",
    "claim_time_tertiary_coverage_charge_count",
    "has_claim_time_secondary_coverage",
    "has_claim_time_tertiary_coverage",
)

_RESPONSIBILITY_COLUMNS = (
    "hcmpi",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "performance_year_charge_count",
    "performance_year_claim_count",
    "alignment_period_charge_count",
    "alignment_period_claim_count",
    "alignment_period_charge_copay_due_charge_count",
    "alignment_period_charge_copay_due_claim_count",
    "alignment_period_charge_copay_due_amt",
    "alignment_period_charge_proxy_copay_zeroed_amt",
    "alignment_period_charge_proxy_copay_zeroed_charge_count",
    "alignment_period_charge_proxy_copay_residual_amt",
    "pre_alignment_charge_copay_due_amt",
    "pre_alignment_charge_proxy_copay_zeroed_amt",
    "athena_activity_charge_count",
    "athena_activity_claim_count",
    "athena_copay_due_amt",
    "athena_freecare_adjustment_amt",
    "athena_bad_debt_adjustment_amt",
    "athena_total_adjustment_amt",
    "athena_patient_payment_amt",
    "athena_writeoff_amt",
    "athena_copay_zeroed_amt",
    "athena_adjustment_reason_ids",
    "posted_rev_cycle_relevant_transaction_count",
    "posted_rev_cycle_patient_responsibility_transaction_count",
    "posted_rev_cycle_patient_responsibility_charge_count",
    "posted_rev_cycle_patient_responsibility_claim_count",
    "posted_rev_cycle_reach_cost_sharing_support_transaction_count",
    "posted_rev_cycle_reach_cost_sharing_support_claim_count",
    "posted_rev_cycle_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "posted_rev_cycle_reach_cost_sharing_support_amt",
    "posted_rev_cycle_reach_support_custom_transaction_codes",
    "alignment_period_rev_cycle_relevant_transaction_count",
    "alignment_period_patient_responsibility_transaction_count",
    "alignment_period_patient_responsibility_charge_count",
    "alignment_period_patient_responsibility_claim_count",
    "alignment_period_reach_cost_sharing_support_transaction_count",
    "alignment_period_reach_cost_sharing_support_claim_count",
    "alignment_period_patient_responsibility_transfer_amt",
    "alignment_period_patient_payment_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_patient_payment_responsibility_service_count",
    "alignment_period_patient_payment_on_responsibility_service_amt",
    "alignment_period_reach_support_custom_transaction_codes",
    "pre_alignment_patient_responsibility_transfer_amt",
    "pre_alignment_patient_payment_amt",
    "pre_alignment_reach_cost_sharing_support_amt",
    "pre_alignment_patient_payment_responsibility_service_count",
    "pre_alignment_patient_payment_on_responsibility_service_amt",
    "pre_alignment_reach_support_custom_transaction_codes",
    "alignment_period_copay_due_charge_count",
    "alignment_period_copay_due_claim_count",
    "alignment_period_copay_due_amt",
    "alignment_period_cost_sharing_support_amt",
    "alignment_period_copay_waived_amt",
    "alignment_period_cost_sharing_support_identified_flag",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_patient_copay_payment_rate",
    "alignment_period_copay_unresolved_amt",
)

_BOOL_COLUMNS = (
    "has_patient_secondary_coverage",
    "has_patient_tertiary_coverage",
    "has_secondary_or_tertiary_coverage_flag",
    "no_secondary_or_tertiary_coverage_flag",
    "priority_3_self_pay_or_no_coverage_flag",
    "has_claim_time_secondary_coverage",
    "has_claim_time_tertiary_coverage",
    "alignment_period_cost_sharing_support_identified_flag",
)

_NUMERIC_COLUMNS = (
    "patient_secondary_insurance_package_count",
    "patient_tertiary_insurance_package_count",
    "claim_time_secondary_coverage_charge_count",
    "claim_time_tertiary_coverage_charge_count",
    "performance_year_charge_count",
    "performance_year_claim_count",
    "alignment_period_charge_count",
    "alignment_period_claim_count",
    "alignment_period_charge_copay_due_charge_count",
    "alignment_period_charge_copay_due_claim_count",
    "alignment_period_charge_copay_due_amt",
    "alignment_period_charge_proxy_copay_zeroed_amt",
    "alignment_period_charge_proxy_copay_zeroed_charge_count",
    "alignment_period_charge_proxy_copay_residual_amt",
    "pre_alignment_charge_copay_due_amt",
    "pre_alignment_charge_proxy_copay_zeroed_amt",
    "athena_activity_charge_count",
    "athena_activity_claim_count",
    "athena_copay_due_amt",
    "athena_freecare_adjustment_amt",
    "athena_bad_debt_adjustment_amt",
    "athena_total_adjustment_amt",
    "athena_patient_payment_amt",
    "athena_writeoff_amt",
    "athena_copay_zeroed_amt",
    "posted_rev_cycle_relevant_transaction_count",
    "posted_rev_cycle_patient_responsibility_transaction_count",
    "posted_rev_cycle_patient_responsibility_charge_count",
    "posted_rev_cycle_patient_responsibility_claim_count",
    "posted_rev_cycle_reach_cost_sharing_support_transaction_count",
    "posted_rev_cycle_reach_cost_sharing_support_claim_count",
    "posted_rev_cycle_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "posted_rev_cycle_reach_cost_sharing_support_amt",
    "alignment_period_rev_cycle_relevant_transaction_count",
    "alignment_period_patient_responsibility_transaction_count",
    "alignment_period_patient_responsibility_charge_count",
    "alignment_period_patient_responsibility_claim_count",
    "alignment_period_reach_cost_sharing_support_transaction_count",
    "alignment_period_reach_cost_sharing_support_claim_count",
    "alignment_period_patient_responsibility_transfer_amt",
    "alignment_period_patient_payment_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_patient_payment_responsibility_service_count",
    "alignment_period_patient_payment_on_responsibility_service_amt",
    "pre_alignment_patient_responsibility_transfer_amt",
    "pre_alignment_patient_payment_amt",
    "pre_alignment_reach_cost_sharing_support_amt",
    "pre_alignment_patient_payment_responsibility_service_count",
    "pre_alignment_patient_payment_on_responsibility_service_amt",
    "alignment_period_copay_due_charge_count",
    "alignment_period_copay_due_claim_count",
    "alignment_period_copay_due_amt",
    "alignment_period_cost_sharing_support_amt",
    "alignment_period_copay_waived_amt",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_patient_copay_payment_rate",
    "alignment_period_copay_unresolved_amt",
)


def _dedup_hcmpi(frame: DataFrame, columns: tuple[str, ...], *, context: str) -> DataFrame:
    return _with_hcmpi_key(_select_required(frame, columns, context=context)).dropDuplicates(["hcmpi"])


@register_transform(
    "copay_waiver_patient_profile",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_PATIENT_PROFILE_VIEW,
    description="Aligned patient audit profile combining coverage status with alignment-period copay responsibility and support facts.",
)
def build_copay_waiver_patient_profile(
    aligned_population: DataFrame,
    coverage_status: DataFrame,
    patient_responsibility: DataFrame,
    *,
    performance_year: int,
) -> DataFrame:
    """Combine patient identity, coverage status, and financial responsibility at one row per patient."""
    aligned = _dedup_hcmpi(aligned_population, DEFAULT_ALIGNED_COLUMNS, context="aligned population for copay patient profile")
    coverage = _dedup_hcmpi(coverage_status, _COVERAGE_COLUMNS, context="coverage status for copay patient profile")
    responsibility = _dedup_hcmpi(
        patient_responsibility,
        _RESPONSIBILITY_COLUMNS,
        context="patient responsibility for copay patient profile",
    ).drop("reach_alignment_start_dt", "reach_alignment_end_dt")
    profile = aligned.join(coverage, on="hcmpi", how="left").join(responsibility, on="hcmpi", how="left")

    profile = profile.fillna(False, subset=list(_BOOL_COLUMNS)).fillna(0, subset=list(_NUMERIC_COLUMNS))

    return (
        profile.withColumn(
            "alignment_period_copay_due_without_secondary_amt",
            F.when(F.col("no_secondary_or_tertiary_coverage_flag"), zero_if_null("alignment_period_copay_due_amt")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "copays_waived_flag",
            F.col("no_secondary_or_tertiary_coverage_flag")
            & (F.col("alignment_period_copay_due_without_secondary_amt") > 0)
            & (F.col("alignment_period_cost_sharing_support_amt") > 0),
        )
        .withColumn(
            "alignment_period_copay_unresolved_amt",
            F.greatest(
                zero_if_null("alignment_period_copay_due_amt")
                - zero_if_null("alignment_period_cost_sharing_support_amt")
                - zero_if_null("alignment_period_patient_copay_payment_amt"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "alignment_period_patient_copay_payment_rate",
            safe_divide("alignment_period_patient_copay_payment_amt", "alignment_period_copay_due_amt"),
        )
        .withColumn(
            "beneficiary_identity",
            F.coalesce(blank_to_null("beneficiary_name"), F.concat_ws(" / ", F.lit("HCMPI"), F.col("hcmpi"), F.lit("MBI"), F.col("MedicareBeneficiaryID"))),
        )
        .withColumn("performance_year", F.lit(int(performance_year)))
    )


__all__ = ["build_copay_waiver_patient_profile"]
