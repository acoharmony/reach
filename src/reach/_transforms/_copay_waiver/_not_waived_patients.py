"""Patient list for no-secondary copay due amounts that were not waived."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import blank_to_null
from reach._transforms._copay_waiver._common import _require_columns, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW
from reach._transforms._registry import register_transform


_COLUMNS = (
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
    "clinician_name",
    "clinician_npi",
    "latest_aligned_npi",
    "latest_aligned_tin",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "office_name",
    "lc_status_current",
    "payer_current",
    "coverage_decision_source",
    "patient_secondary_insurance_names",
    "patient_secondary_insurance_groups",
    "patient_tertiary_insurance_names",
    "patient_tertiary_insurance_groups",
    "priority_3_self_pay_or_no_coverage_flag",
    "priority_3_self_pay_or_no_coverage_names",
    "performance_year_charge_count",
    "performance_year_claim_count",
    "alignment_period_copay_due_charge_count",
    "alignment_period_copay_due_claim_count",
    "alignment_period_copay_due_amt",
    "alignment_period_copay_due_without_secondary_amt",
    "alignment_period_cost_sharing_support_amt",
    "alignment_period_copay_waived_amt",
    "posted_rev_cycle_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "pre_alignment_patient_responsibility_transfer_amt",
    "pre_alignment_patient_payment_amt",
    "pre_alignment_patient_payment_on_responsibility_service_amt",
    "alignment_period_patient_responsibility_transfer_amt",
    "alignment_period_patient_payment_on_responsibility_service_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_reach_support_custom_transaction_codes",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_patient_copay_payment_rate",
    "alignment_period_copay_unresolved_amt",
    "Total_Paid_PMPM",
    "Total_Paid_Dollars",
    "Prosp_RAF_v28",
    "Concur_RAF_v28",
)


@register_transform(
    "copay_waiver_no_secondary_due_not_waived_patients",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW,
    description="Patient list for REACH beneficiaries with no priority-2/real priority-3 coverage, copay due, and no support recorded.",
)
def build_copay_waiver_no_secondary_due_not_waived_patients(patient_profile: DataFrame) -> DataFrame:
    """Return the comparison list requested for explanation and audit follow-up."""
    base = (
        _with_hcmpi_key(patient_profile)
        .where(F.col("no_secondary_or_tertiary_coverage_flag"))
        .where(F.col("alignment_period_copay_due_without_secondary_amt") > 0)
        .where(~F.col("copays_waived_flag"))
        .withColumn("beneficiary_identity", F.coalesce(blank_to_null("beneficiary_name"), F.concat_ws(" / ", F.lit("HCMPI"), F.col("hcmpi"), F.lit("MBI"), F.col("MedicareBeneficiaryID"))))
    )
    _require_columns(base, _COLUMNS, context="patient profile for no-secondary due-not-waived detail")
    return base.select(*_COLUMNS).orderBy(F.desc("alignment_period_copay_due_without_secondary_amt"), "clinician_name", "beneficiary_identity")
