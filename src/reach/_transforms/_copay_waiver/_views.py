"""Shared view names and column groups for copay-waiver transforms."""

from reach._transforms._clinical_oversight._metrics import CLINICAL_OVERSIGHT_METRIC_COLUMNS

COPAY_WAIVER_COVERAGE_STATUS_VIEW = "reach_copay_waiver_coverage_status"
COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW = "reach_copay_waiver_patient_responsibility"
COPAY_WAIVER_PATIENT_PROFILE_VIEW = "reach_copay_waiver_patient_profile"
COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW = "reach_copay_waiver_patient_cost_context"
COPAY_WAIVER_POPULATION_SUMMARY_VIEW = "reach_copay_waiver_population_summary"
COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW = "reach_copay_waiver_segment_breakdown"
COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW = "reach_copay_waiver_support_service_detail"
COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW = "reach_copay_waiver_revenue_cycle_transactions"
COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW = "reach_copay_waiver_no_secondary_due_not_waived_patients"
COPAY_WAIVER_COHORT_DIFFS_VIEW = "reach_copay_waiver_cohort_diffs"
COPAY_WAIVER_MEASURE_DIFFS_VIEW = "reach_copay_waiver_measure_diffs"
COPAY_WAIVER_STAGE_MANIFEST_VIEW = "reach_copay_waiver_stage_manifest"
COPAY_WAIVER_STAGE_METRICS_VIEW = "reach_copay_waiver_stage_metrics"
COPAY_WAIVER_CHECK_RESULTS_VIEW = "reach_copay_waiver_check_results"
COPAY_WAIVER_SERVICE_TRACE_VIEW = "reach_copay_waiver_service_trace"
ATHENA_PATIENT_COPAY_ACTIVITY_VIEW = "reach_athena_patient_copay_activity"
REACH_ALIGNED_POPULATION_VIEW = "reach_census_aligned_population"
REACH_ALIGNMENT_COUNT_REFERENCE_VIEW = "reach_alignment_count_reference"

DEFAULT_ALIGNED_COLUMNS = (
    "hcmpi",
    "MedicareBeneficiaryID",
    "beneficiary_name",
    "beneficiary_first_name",
    "beneficiary_last_name",
    "beneficiary_line_1_address",
    "beneficiary_city",
    "beneficiary_usps_state_code",
    "beneficiary_zip_5",
    "census_monthyear",
    "alignment_source",
    "overview_file_dt",
    "overview_aligned_flag",
    "beneficiary_alignment_effective_start_date",
    "beneficiary_alignment_effective_termination_date",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "attr_start_dt",
    "attr_end_dt",
    "beneficiary_gender",
    "race_ethnicity",
    "beneficiary_age",
    "clinician_name",
    "clinician_npi",
    "primary_provider_npi",
    "primary_provider_npi_current",
    "latest_aligned_npi",
    "latest_aligned_tin",
    "provider_alignment_type_claim",
    "provider_alignment_type_voluntary",
    "provider_alignment_file_dt",
    "provider_alignment_file_name",
    "provider_alignment_source",
    "BirthDate",
    "lc_status_current",
    "lifecycle_status",
    "payer_current",
    "payer_type_current",
    "payer",
    "payer_type",
    "assigned_insurance_nm",
    "assigned_ins_reporting_group",
    "product",
    "program_nm",
    "cohort_name",
    "office_name",
    "census_PayerEligible",
    "active_panel",
)

DEFAULT_SEGMENT_COLUMNS = (
    "clinician_name",
    "clinician_npi",
    "primary_provider_npi",
    "primary_provider_npi_current",
    "latest_aligned_npi",
    "office_name",
    "payer_current",
    "lc_status_current",
    "age_band",
)

DEFAULT_NUMERIC_DIFF_COLUMNS = CLINICAL_OVERSIGHT_METRIC_COLUMNS

__all__ = [
    "ATHENA_PATIENT_COPAY_ACTIVITY_VIEW",
    "COPAY_WAIVER_COHORT_DIFFS_VIEW",
    "COPAY_WAIVER_COVERAGE_STATUS_VIEW",
    "COPAY_WAIVER_CHECK_RESULTS_VIEW",
    "COPAY_WAIVER_MEASURE_DIFFS_VIEW",
    "COPAY_WAIVER_NOT_WAIVED_PATIENTS_VIEW",
    "COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW",
    "COPAY_WAIVER_PATIENT_PROFILE_VIEW",
    "COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW",
    "COPAY_WAIVER_POPULATION_SUMMARY_VIEW",
    "COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW",
    "COPAY_WAIVER_SEGMENT_BREAKDOWN_VIEW",
    "COPAY_WAIVER_SERVICE_TRACE_VIEW",
    "COPAY_WAIVER_STAGE_MANIFEST_VIEW",
    "COPAY_WAIVER_STAGE_METRICS_VIEW",
    "COPAY_WAIVER_SUPPORT_SERVICE_DETAIL_VIEW",
    "DEFAULT_ALIGNED_COLUMNS",
    "DEFAULT_NUMERIC_DIFF_COLUMNS",
    "DEFAULT_SEGMENT_COLUMNS",
    "REACH_ALIGNED_POPULATION_VIEW",
    "REACH_ALIGNMENT_COUNT_REFERENCE_VIEW",
]
