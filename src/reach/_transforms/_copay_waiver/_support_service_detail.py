"""Service-grain Cost-Sharing Support detail for REACH copay waiver review."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import blank_to_null
from reach._transforms._copay_waiver._common import _select_required, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW
from reach._transforms._registry import register_transform


_PATIENT_COLUMNS = (
    "hcmpi",
    "MedicareBeneficiaryID",
    "beneficiary_name",
    "beneficiary_first_name",
    "beneficiary_last_name",
    "beneficiary_line_1_address",
    "beneficiary_city",
    "beneficiary_usps_state_code",
    "beneficiary_zip_5",
    "BirthDate",
    "beneficiary_gender",
    "race_ethnicity",
    "lc_status_current",
    "payer_current",
    "office_name",
    "clinician_name",
    "clinician_npi",
    "latest_aligned_npi",
    "latest_aligned_tin",
    "provider_alignment_type_claim",
    "provider_alignment_type_voluntary",
    "provider_alignment_file_dt",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "beneficiary_alignment_effective_start_date",
    "beneficiary_alignment_effective_termination_date",
    "no_secondary_or_tertiary_coverage_flag",
    "copays_waived_flag",
    "alignment_period_copay_due_amt",
    "alignment_period_copay_waived_amt",
    "alignment_period_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "pre_alignment_patient_payment_amt",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_reach_support_custom_transaction_codes",
    "athena_copay_due_amt",
    "athena_total_adjustment_amt",
    "athena_patient_payment_amt",
    "athena_adjustment_reason_ids",
)

_PROVIDER_COLUMNS = (
    "provider_id",
    "NPI",
    "reporting_name",
    "billed_name",
    "scheduling_name",
    "first_name",
    "last_name",
    "specialty",
)

_REVENUE_CYCLE_DETAIL_COLUMNS = (
    "beneficiary_name",
    "beneficiary_line_1_address",
    "beneficiary_city",
    "beneficiary_usps_state_code",
    "beneficiary_zip_5",
    "hcmpi",
    "MedicareBeneficiaryID",
    "BirthDate",
    "beneficiary_gender",
    "race_ethnicity",
    "clinician_name",
    "clinician_npi",
    "latest_aligned_npi",
    "latest_aligned_tin",
    "provider_alignment_type_claim",
    "provider_alignment_type_voluntary",
    "provider_alignment_file_dt",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "service_in_reach_alignment_window_flag",
    "no_secondary_or_tertiary_coverage_flag",
    "transaction_id",
    "claim_id",
    "charge_id",
    "transaction_charge_id",
    "transaction_post_dt",
    "service_from_dt",
    "service_to_dt",
    "part_b_service_nature",
    "procedure_cd",
    "procedure_modifiers",
    "place_of_service_cd",
    "transaction_amt",
    "patient_responsibility_transfer_amt",
    "patient_payment_amt",
    "cost_sharing_support_amt",
    "cost_sharing_support_identified_flag",
    "rendering_provider_id",
    "supervising_provider_id",
    "transaction_reason",
    "custom_transaction_code",
    "transaction_method",
    "transaction_created_by",
    "transaction_posted_by",
    "line_note",
)


def _provider_reference(provider: DataFrame) -> DataFrame:
    source = _select_required(provider, _PROVIDER_COLUMNS, context="provider reference for copay support service detail")
    name = F.coalesce(
        blank_to_null("reporting_name"),
        blank_to_null("billed_name"),
        blank_to_null("scheduling_name"),
        blank_to_null(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))),
    )
    return source.select(
        F.col("provider_id").cast("string").alias("_rendering_provider_join_id"),
        F.regexp_replace(F.col("NPI").cast("string"), r"[^0-9]", "").alias("service_rendering_provider_npi"),
        name.alias("service_rendering_provider_name"),
        F.col("specialty").alias("service_rendering_provider_specialty"),
    ).dropDuplicates(["_rendering_provider_join_id"])


def _patient_rollup(patients: DataFrame) -> DataFrame:
    return patients.select(
        "hcmpi",
        F.col("alignment_period_copay_due_amt").alias("patient_level_alignment_period_copay_due_amt"),
        F.col("alignment_period_copay_waived_amt").alias("patient_level_alignment_period_copay_waived_amt"),
        F.col("athena_copay_due_amt").alias("patient_level_athena_copay_due_amt"),
        F.col("athena_total_adjustment_amt").alias("patient_level_athena_total_adjustment_amt"),
        F.col("athena_patient_payment_amt").alias("patient_level_athena_patient_payment_amt"),
        F.col("athena_adjustment_reason_ids").alias("patient_level_athena_adjustment_reason_ids"),
        F.col("alignment_period_patient_responsibility_transfer_amt").alias("patient_level_alignment_period_patient_responsibility_transfer_amt"),
        F.col("posted_rev_cycle_patient_payment_amt").alias("patient_level_posted_rev_cycle_patient_payment_amt"),
        F.col("pre_alignment_patient_payment_amt").alias("patient_level_pre_alignment_patient_payment_amt"),
        F.col("alignment_period_patient_copay_payment_amt").alias("patient_level_alignment_period_patient_copay_payment_amt"),
        F.col("alignment_period_reach_cost_sharing_support_amt").alias("patient_level_alignment_period_reach_cost_sharing_support_amt"),
        F.col("alignment_period_reach_support_custom_transaction_codes").alias("patient_level_alignment_period_reach_support_custom_transaction_codes"),
    )


@register_transform(
    "copay_waiver_support_service_detail",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW,
    description="Transaction/service detail answering CMS Cost-Sharing Support audit questions for waived copays.",
)
def build_copay_waiver_support_service_detail(
    patient_profile: DataFrame,
    revenue_cycle_transactions: DataFrame,
    provider_reference: DataFrame,
) -> DataFrame:
    """Return service-grain Cost-Sharing Support detail for waived copay patients."""
    patients = _with_hcmpi_key(
        _select_required(patient_profile, _PATIENT_COLUMNS, context="patient profile for copay support service detail")
    ).dropDuplicates(["hcmpi"])
    transactions = (
        _with_hcmpi_key(
            _select_required(
                revenue_cycle_transactions,
                _REVENUE_CYCLE_DETAIL_COLUMNS,
                context="revenue-cycle transactions for copay support service detail",
            )
        )
        .withColumn("_service_key", F.coalesce(F.col("charge_id"), F.col("transaction_charge_id")))
        .where(F.col("_service_key").isNotNull())
    )
    service_context = (
        transactions.where(F.col("service_in_reach_alignment_window_flag"))
        .groupBy("hcmpi", "_service_key")
        .agg(
            F.min("service_from_dt").alias("_service_dt"),
            F.max("service_to_dt").alias("_service_to_dt"),
            F.array_join(F.array_sort(F.collect_set("part_b_service_nature")), "; ").alias("_part_b_service_nature"),
            F.array_join(F.array_sort(F.collect_set("procedure_cd")), "; ").alias("_procedure_cd"),
            F.array_join(F.array_sort(F.collect_set("procedure_modifiers")), "; ").alias("_procedure_modifiers"),
            F.first("place_of_service_cd", ignorenulls=True).alias("_place_of_service_cd"),
            F.sum("patient_responsibility_transfer_amt").alias("copay_due_amt_for_service"),
            F.sum("patient_payment_amt").alias("patient_payment_amt_for_service"),
        )
    )
    events = (
        transactions.where(F.col("no_secondary_or_tertiary_coverage_flag"))
        .where(F.col("service_in_reach_alignment_window_flag"))
        .where(F.col("cost_sharing_support_identified_flag"))
        .join(service_context, on=["hcmpi", "_service_key"], how="inner")
        .join(_patient_rollup(patients), on="hcmpi", how="inner")
        .withColumn("_rendering_provider_join_id", F.col("rendering_provider_id").cast("string"))
        .join(_provider_reference(provider_reference), on="_rendering_provider_join_id", how="left")
        .withColumn(
            "beneficiary_identity",
            F.coalesce(blank_to_null("beneficiary_name"), F.concat_ws(" / ", F.lit("HCMPI"), F.col("hcmpi"), F.lit("MBI"), F.col("MedicareBeneficiaryID"))),
        )
        .withColumn("aco_participant_provider_name", F.col("clinician_name"))
        .withColumn("aco_participant_provider_npi", F.col("clinician_npi"))
    )
    return events.select(
        "beneficiary_identity",
        "beneficiary_name",
        "hcmpi",
        "MedicareBeneficiaryID",
        "BirthDate",
        "beneficiary_gender",
        "race_ethnicity",
        "beneficiary_line_1_address",
        "beneficiary_city",
        "beneficiary_usps_state_code",
        "beneficiary_zip_5",
        "reach_alignment_start_dt",
        "reach_alignment_end_dt",
        F.col("_service_dt").alias("service_dt"),
        F.col("_service_to_dt").alias("service_to_dt"),
        "service_in_reach_alignment_window_flag",
        F.col("_part_b_service_nature").alias("part_b_service_nature"),
        F.col("_procedure_cd").alias("procedure_cd"),
        F.col("_procedure_cd").alias("proccode"),
        F.col("_procedure_modifiers").alias("procedure_modifiers"),
        F.col("_place_of_service_cd").alias("place_of_service_cd"),
        "charge_id",
        "claim_id",
        F.lit("mg-athena").alias("data_source"),
        F.lit(None).cast("double").alias("totalunits"),
        F.lit(None).cast("double").alias("charge_amt"),
        "copay_due_amt_for_service",
        "patient_payment_amt_for_service",
        "cost_sharing_support_amt",
        F.lit("athenaone_patient_adjustment_transaction").alias("cost_sharing_support_method"),
        F.lit("support_identified_on_alignment_period_service").alias("cost_sharing_support_status"),
        "cost_sharing_support_identified_flag",
        "patient_level_alignment_period_copay_due_amt",
        "patient_level_alignment_period_copay_waived_amt",
        "patient_level_athena_copay_due_amt",
        "patient_level_athena_total_adjustment_amt",
        "patient_level_athena_patient_payment_amt",
        "patient_level_athena_adjustment_reason_ids",
        "patient_level_alignment_period_patient_responsibility_transfer_amt",
        "patient_level_posted_rev_cycle_patient_payment_amt",
        "patient_level_pre_alignment_patient_payment_amt",
        "patient_level_alignment_period_patient_copay_payment_amt",
        "patient_level_alignment_period_reach_cost_sharing_support_amt",
        "patient_level_alignment_period_reach_support_custom_transaction_codes",
        F.col("cost_sharing_support_amt").alias("charge_level_cost_sharing_support_amt"),
        F.col("cost_sharing_support_amt").alias("service_adjusted_amt"),
        F.lit(None).cast("double").alias("service_owed_amt"),
        "aco_participant_provider_name",
        "aco_participant_provider_npi",
        "latest_aligned_npi",
        "latest_aligned_tin",
        "provider_alignment_type_claim",
        "provider_alignment_type_voluntary",
        "provider_alignment_file_dt",
        "service_rendering_provider_name",
        "service_rendering_provider_npi",
        "service_rendering_provider_specialty",
        "rendering_provider_id",
        "supervising_provider_id",
        F.lit(None).cast("string").alias("billing_provider_id"),
        F.col("transaction_id").alias("support_transaction_id"),
        F.col("transaction_post_dt").alias("support_post_dt"),
        F.col("transaction_amt").alias("support_transaction_amt"),
        F.col("custom_transaction_code").alias("support_custom_transaction_code"),
        F.col("transaction_reason").alias("support_transaction_reason"),
        F.col("transaction_method").alias("support_transaction_method"),
        F.col("transaction_created_by").alias("support_transaction_created_by"),
        F.col("transaction_posted_by").alias("support_transaction_posted_by"),
        "line_note",
    ).orderBy("beneficiary_identity", "support_post_dt", "support_transaction_id")


__all__ = ["build_copay_waiver_support_service_detail"]
