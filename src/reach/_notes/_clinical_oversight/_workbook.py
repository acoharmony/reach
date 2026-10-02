"""Notebook assembly for the REACH clinical oversight workbook."""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import functions as F

from reach._notes._clinical_oversight._note_tables import DEFAULT_NOTES_TABLE, ensure_notes_table
from reach._notes._clinical_oversight._views import (
    ACTIVE_ENGAGED_LIFECYCLE_STATUS,
    clinician_open_panel_views,
    clinician_reviewed_panel_views,
)
from reach._transforms._clinical_oversight import (
    CLINICIAN_SUMMARY_VIEW,
    DASHBOARD_VIEW,
    DISCUSSION_GROUP_CLINICIAN_NPIS,
    REVIEW_WORKLIST_VIEW,
    build_clinical_oversight_clinician_summary,
    build_clinical_oversight_dashboard,
    build_clinical_oversight_review_worklist,
)


def build_review_workbook(
    spark,
    notes_table: str = DEFAULT_NOTES_TABLE,
    active_only: bool = True,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
):
    """Build notebook outputs and register temp views for ad hoc SQL checks."""
    ensure_notes_table(spark, notes_table)
    dashboard = build_clinical_oversight_dashboard(spark, clinician_npis=clinician_npis)
    worklist = build_clinical_oversight_review_worklist(
        spark,
        dashboard_worklist=dashboard,
        notes=spark.table(notes_table),
        active_only=active_only,
        clinician_npis=clinician_npis,
    )
    summary = build_clinical_oversight_clinician_summary(worklist)
    open_worklist = worklist.where(~F.col("reviewed_flag")).orderBy("clinician_name", "provider_rank", F.desc("power_score"))
    reviewed_patients = worklist.where(F.col("reviewed_flag")).orderBy(F.desc("reviewed_at"), "clinician_name")
    clinician_open_panels = clinician_open_panel_views(worklist, clinician_npis=clinician_npis)
    clinician_reviewed_panels = clinician_reviewed_panel_views(worklist, clinician_npis=clinician_npis)

    dashboard.createOrReplaceTempView(DASHBOARD_VIEW)
    worklist.createOrReplaceTempView(REVIEW_WORKLIST_VIEW)
    summary.createOrReplaceTempView(CLINICIAN_SUMMARY_VIEW)
    open_worklist.createOrReplaceTempView("reach_clinical_oversight_open_worklist")
    reviewed_patients.createOrReplaceTempView("reach_clinical_oversight_reviewed_patients")

    return {
        "dashboard": dashboard,
        "worklist": worklist,
        "clinician_summary": summary,
        "open_worklist": open_worklist,
        "reviewed_patients": reviewed_patients,
        "clinician_open_panels": clinician_open_panels,
        "clinician_reviewed_panels": clinician_reviewed_panels,
        "notes_table": notes_table,
        "clinician_npis": tuple(clinician_npis or ()),
        "lifecycle_status": ACTIVE_ENGAGED_LIFECYCLE_STATUS,
    }
