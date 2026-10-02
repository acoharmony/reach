"""Audit-focused REACH pipe definitions."""

from __future__ import annotations

from reach._pipes._audit_tables import audit_copay_table_names, audit_giftcard_table_names
from reach._pipes._builder import PipeStage, ordered_pipe_stages
from reach._pipes._registry import register_pipe


@register_pipe(
    "reach_audit_copay",
    description="Build REACH cost-sharing support audit tables for copay waiver review.",
)
def reach_audit_copay_pipe() -> tuple[PipeStage, ...]:
    """Return the REACH copay audit stages."""
    tables = audit_copay_table_names()
    return ordered_pipe_stages(
        (
            PipeStage("reach_audit_copay_aligned_population", "reach_census_aligned_population", tables["aligned_population"], "copay_waiver", 1),
            PipeStage("reach_audit_copay_alignment_count_reference", "reach_alignment_count_reference", tables["alignment_count_reference"], "copay_waiver", 2),
            PipeStage(
                "reach_audit_copay_coverage_status",
                "copay_waiver_coverage_status",
                tables["coverage_status"],
                "copay_waiver",
                3,
                ("reach_audit_copay_aligned_population",),
            ),
            PipeStage("reach_audit_copay_athena_activity", "athena_patient_copay_activity", tables["athena_patient_copay_activity"], "copay_waiver", 4),
            PipeStage(
                "reach_audit_copay_revenue_cycle_transactions",
                "copay_waiver_revenue_cycle_transactions",
                tables["revenue_cycle_transactions"],
                "copay_waiver",
                5,
                ("reach_audit_copay_aligned_population", "reach_audit_copay_coverage_status"),
            ),
            PipeStage(
                "reach_audit_copay_patient_responsibility",
                "copay_waiver_patient_responsibility",
                tables["patient_responsibility"],
                "copay_waiver",
                6,
                ("reach_audit_copay_aligned_population", "reach_audit_copay_athena_activity", "reach_audit_copay_revenue_cycle_transactions"),
            ),
            PipeStage(
                "reach_audit_copay_patient_profile",
                "copay_waiver_patient_profile",
                tables["patient_profile"],
                "copay_waiver",
                7,
                ("reach_audit_copay_aligned_population", "reach_audit_copay_coverage_status", "reach_audit_copay_patient_responsibility"),
            ),
            PipeStage(
                "reach_audit_copay_patient_cost_context",
                "copay_waiver_patient_cost_context",
                tables["patient_cost_context"],
                "copay_waiver",
                8,
                ("reach_audit_copay_patient_profile",),
            ),
            PipeStage(
                "reach_audit_copay_population_summary",
                "copay_waiver_population_summary",
                tables["population_summary"],
                "copay_waiver",
                9,
                ("reach_audit_copay_patient_cost_context",),
            ),
            PipeStage(
                "reach_audit_copay_segment_breakdown",
                "copay_waiver_segment_breakdown",
                tables["segment_breakdown"],
                "copay_waiver",
                10,
                ("reach_audit_copay_patient_cost_context",),
            ),
            PipeStage(
                "reach_audit_copay_support_service_detail",
                "copay_waiver_support_service_detail",
                tables["support_service_detail"],
                "copay_waiver",
                11,
                ("reach_audit_copay_patient_profile", "reach_audit_copay_revenue_cycle_transactions"),
            ),
            PipeStage(
                "reach_audit_copay_no_secondary_due_not_waived",
                "copay_waiver_no_secondary_due_not_waived_patients",
                tables["no_secondary_copay_due_not_waived_patients"],
                "copay_waiver",
                12,
                ("reach_audit_copay_patient_cost_context",),
            ),
            PipeStage(
                "reach_audit_copay_cohort_diffs",
                "copay_waiver_cohort_diffs",
                tables["cohort_diffs"],
                "copay_waiver",
                13,
                ("reach_audit_copay_patient_cost_context",),
            ),
            PipeStage(
                "reach_audit_copay_measure_diffs",
                "copay_waiver_measure_diffs",
                tables["measure_diffs"],
                "copay_waiver",
                14,
                ("reach_audit_copay_patient_cost_context",),
            ),
        )
    )


@register_pipe(
    "reach_audit_giftcards",
    description="Build REACH gift-card distribution audit table from the UAT government programs bronze volume.",
)
def reach_audit_giftcards_pipe() -> tuple[PipeStage, ...]:
    """Return the REACH gift-card audit stages."""
    tables = audit_giftcard_table_names()
    return ordered_pipe_stages(
        (
            PipeStage(
                "reach_audit_giftcards_distribution",
                "reach_audit_giftcards_distribution",
                tables["giftcard_distribution"],
                "giftcards",
                1,
            ),
        )
    )


__all__ = ["reach_audit_copay_pipe", "reach_audit_giftcards_pipe"]
