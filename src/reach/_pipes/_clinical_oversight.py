"""Ordered REACH clinical oversight workbook pipe."""

from __future__ import annotations

from reach._pipes._builder import PipeStage, ordered_pipe_stages
from reach._pipes._oversight_tables import oversight_table_names
from reach._pipes._registry import register_pipe


@register_pipe(
    "clinical-oversight",
    description="Build dashboard-equivalent worklist, active review queue, and clinician review summary.",
)
def clinical_oversight_pipe() -> tuple[PipeStage, ...]:
    """Return the canonical clinical-oversight workbook stages."""
    tables = oversight_table_names()
    return ordered_pipe_stages(
        (
            PipeStage("clinical_oversight_dashboard", "clinical_oversight_dashboard", tables["dashboard"], "clinical_oversight", 1),
            PipeStage(
                "clinical_oversight_review_worklist",
                "clinical_oversight_review_worklist",
                tables["worklist"],
                "clinical_oversight",
                2,
                ("clinical_oversight_dashboard",),
            ),
            PipeStage(
                "clinical_oversight_clinician_summary",
                "clinical_oversight_clinician_summary",
                tables["clinician_summary"],
                "clinical_oversight",
                3,
                ("clinical_oversight_review_worklist",),
            ),
        )
    )


__all__ = ["clinical_oversight_pipe"]
