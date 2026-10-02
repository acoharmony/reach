"""Warehouse-facing REACH analytic pipe definitions.

These pipes intentionally do not include bronze/file ingestion stages. File
parsing and delivery tracking now belong to the importer layer; REACH consumes
warehouse-ready Spark tables.
"""

from __future__ import annotations

from reach._pipes._builder import PipeStage, ordered_pipe_stages
from reach._pipes._registry import register_pipe


@register_pipe("hedr", description="Build REACH HEDR eligibility from warehouse BAR/alignment/SDOH tables.")
def hedr_pipe() -> tuple[PipeStage, ...]:
    """Return the HEDR analytic stage."""
    return ordered_pipe_stages((PipeStage("hedr_eligibility", "hedr_eligibility", "reach_hedr_eligibility", "hedr", 1),))


@register_pipe("high-needs", description="Build REACH High-Needs eligibility from warehouse claims, appendix values, and scores.")
def high_needs_pipe() -> tuple[PipeStage, ...]:
    """Return the High-Needs analytic stage."""
    return ordered_pipe_stages(
        (
            PipeStage("high_needs_eligibility", "high_needs_eligibility", "reach_high_needs_eligibility", "high_needs", 1),
            PipeStage(
                "high_needs_reconciliation",
                "high_needs_reconciliation",
                "reach_high_needs_reconciliation",
                "high_needs",
                2,
                depends_on=("high_needs_eligibility",),
            ),
        )
    )


@register_pipe("bnmr", description="Build REACH BNMR reconciliation views from warehouse-ready BNMR/PLARU/scored tables.")
def bnmr_pipe() -> tuple[PipeStage, ...]:
    """Return the BNMR reconciliation stages."""
    return ordered_pipe_stages(
        (
            PipeStage("bnmr_claims_reconciliation", "bnmr_claims_reconciliation", "reach_bnmr_claims_reconciliation", "bnmr", 1),
            PipeStage("bnmr_cap_reconciliation", "bnmr_cap_reconciliation", "reach_bnmr_cap_reconciliation", "bnmr", 2),
            PipeStage("bnmr_risk_count_reconciliation", "bnmr_risk_count_reconciliation", "reach_bnmr_risk_count_reconciliation", "bnmr", 3),
            PipeStage("bnmr_risk_annual_count_reconciliation", "bnmr_risk_annual_count_reconciliation", "reach_bnmr_risk_annual_count_reconciliation", "bnmr", 4),
            PipeStage("bnmr_risk_score_reconciliation", "bnmr_risk_score_reconciliation", "reach_bnmr_risk_score_reconciliation", "bnmr", 5),
            PipeStage("bnmr_risk_normalization_reconciliation", "bnmr_risk_normalization_reconciliation", "reach_bnmr_risk_normalization_reconciliation", "bnmr", 6),
        )
    )


@register_pipe("quality", description="Build warehouse-ready REACH quality summary views.")
def quality_pipe() -> tuple[PipeStage, ...]:
    """Return REACH quality analytic stages."""
    return ordered_pipe_stages(
        (
            PipeStage("uamcc_denominator_keep_set", "uamcc_denominator_keep_set", "reach_uamcc_denominator_keep_set", "quality", 1),
            PipeStage("quality_measure_rates", "quality_measure_rates", "reach_quality_measure_rates", "quality", 2),
        )
    )


@register_pipe("warehouse-analytics", description="All REACH warehouse-facing analytic outputs, excluding importer/file stages.")
def warehouse_analytics_pipe() -> tuple[PipeStage, ...]:
    """Return the broad warehouse-facing analytic stage catalog."""
    stages = hedr_pipe() + high_needs_pipe() + bnmr_pipe() + quality_pipe()
    return ordered_pipe_stages(
        PipeStage(stage.name, stage.transform_name, stage.output_table, stage.concern, order, stage.depends_on)
        for order, stage in enumerate(stages, start=1)
    )
