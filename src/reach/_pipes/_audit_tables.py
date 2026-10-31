"""Output table names for REACH audit pipes."""

from __future__ import annotations

from collections.abc import Mapping

from reach._tables import reach_environment

COPAY_OUTPUT_TABLE_SUFFIXES: Mapping[str, str] = {
    "aligned_population": "reach_audit_copay_aligned_population",
    "alignment_count_reference": "reach_audit_copay_alignment_count_reference",
    "coverage_status": "reach_audit_copay_coverage_status",
    "athena_patient_copay_activity": "reach_audit_copay_athena_patient_copay_activity",
    "revenue_cycle_transactions": "reach_audit_copay_revenue_cycle_transactions",
    "patient_responsibility": "reach_audit_copay_patient_responsibility",
    "patient_profile": "reach_audit_copay_patient_profile",
    "patient_cost_context": "reach_audit_copay_patient_cost_context",
    "population_summary": "reach_audit_copay_population_summary",
    "segment_breakdown": "reach_audit_copay_segment_breakdown",
    "support_service_detail": "reach_audit_copay_support_service_detail",
    "no_secondary_copay_due_not_waived_patients": "reach_audit_copay_no_secondary_due_not_waived_patients",
    "cohort_diffs": "reach_audit_copay_cohort_diffs",
    "measure_diffs": "reach_audit_copay_measure_diffs",
    "stage_manifest": "reach_audit_copay_stage_manifest",
    "stage_metrics": "reach_audit_copay_stage_metrics",
    "check_results": "reach_audit_copay_check_results",
    "service_trace": "reach_audit_copay_service_trace",
}

COPAY_STAGE_OUTPUTS: Mapping[str, str] = {
    "reach_audit_copay_aligned_population": "aligned_population",
    "reach_audit_copay_alignment_count_reference": "alignment_count_reference",
    "reach_audit_copay_coverage_status": "coverage_status",
    "reach_audit_copay_athena_activity": "athena_patient_copay_activity",
    "reach_audit_copay_revenue_cycle_transactions": "revenue_cycle_transactions",
    "reach_audit_copay_patient_responsibility": "patient_responsibility",
    "reach_audit_copay_patient_profile": "patient_profile",
    "reach_audit_copay_patient_cost_context": "patient_cost_context",
    "reach_audit_copay_population_summary": "population_summary",
    "reach_audit_copay_segment_breakdown": "segment_breakdown",
    "reach_audit_copay_support_service_detail": "support_service_detail",
    "reach_audit_copay_no_secondary_due_not_waived": "no_secondary_copay_due_not_waived_patients",
    "reach_audit_copay_cohort_diffs": "cohort_diffs",
    "reach_audit_copay_measure_diffs": "measure_diffs",
    "reach_audit_copay_stage_manifest": "stage_manifest",
    "reach_audit_copay_stage_metrics": "stage_metrics",
    "reach_audit_copay_check_results": "check_results",
    "reach_audit_copay_service_trace": "service_trace",
}

GIFTCARD_OUTPUT_TABLE_SUFFIXES: Mapping[str, str] = {
    "giftcard_distribution": "reach_audit_giftcards_distribution",
}

GIFTCARD_STAGE_OUTPUTS: Mapping[str, str] = {
    "reach_audit_giftcards_distribution": "giftcard_distribution",
}

RUN_SUMMARY_SUFFIXES: Mapping[str, str] = {
    "reach_audit_copay": "reach_audit_copay_run_summary",
    "reach_audit_giftcards": "reach_audit_giftcards_run_summary",
}


def qualified_table(schema: str, suffix: str) -> str:
    """Return a fully qualified table name from a catalog.schema and unqualified suffix."""
    schema = str(schema or "").strip().rstrip(".")
    if not schema or schema.count(".") != 1:
        raise ValueError(f"Expected catalog.schema for REACH audit output, got {schema!r}.")
    return f"{schema}.{suffix}"


def audit_copay_table_names(uat_catalog_schema: str | None = None) -> dict[str, str]:
    """Return output-name to table-name mapping for the REACH copay audit pipe."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    return {name: qualified_table(schema, suffix) for name, suffix in COPAY_OUTPUT_TABLE_SUFFIXES.items()}


def audit_giftcard_table_names(uat_catalog_schema: str | None = None) -> dict[str, str]:
    """Return output-name to table-name mapping for the REACH gift-card audit pipe."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    return {name: qualified_table(schema, suffix) for name, suffix in GIFTCARD_OUTPUT_TABLE_SUFFIXES.items()}


def audit_run_summary_table(pipe_name: str, uat_catalog_schema: str | None = None) -> str:
    """Return the run-summary table for one REACH audit pipe."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    try:
        suffix = RUN_SUMMARY_SUFFIXES[pipe_name]
    except KeyError as exc:
        raise ValueError(f"Unknown REACH audit pipe {pipe_name!r}.") from exc
    return qualified_table(schema, suffix)


__all__ = [
    "COPAY_OUTPUT_TABLE_SUFFIXES",
    "COPAY_STAGE_OUTPUTS",
    "GIFTCARD_OUTPUT_TABLE_SUFFIXES",
    "GIFTCARD_STAGE_OUTPUTS",
    "RUN_SUMMARY_SUFFIXES",
    "audit_copay_table_names",
    "audit_giftcard_table_names",
    "audit_run_summary_table",
    "qualified_table",
]
