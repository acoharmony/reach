"""Clinical oversight review-worklist transform."""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, functions as F

from reach._expressions._alignment import active_engaged_patient_status_expr
from reach._transforms._clinical_oversight._constants import DISCUSSION_GROUP_CLINICIAN_NPIS, REVIEW_WORKLIST_VIEW
from reach._transforms._clinical_oversight._dashboard import build_clinical_oversight_dashboard
from reach._transforms._clinical_oversight._identity import filter_discussion_group
from reach._transforms._clinical_oversight._metrics import CLINICAL_OVERSIGHT_METRIC_COLUMNS
from reach._transforms._registry import register_transform


@register_transform(
    "clinical_oversight_review_worklist",
    concern="clinical_oversight",
    output_table=REVIEW_WORKLIST_VIEW,
    description="Active-engaged dashboard patients joined to persisted review notes.",
)
def build_clinical_oversight_review_worklist(
    spark,
    dashboard_worklist: DataFrame | None = None,
    *,
    notes: DataFrame | None = None,
    active_only: bool = True,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
) -> DataFrame:
    """Build active-engaged dashboard rows, optionally enriched with note columns."""
    source_worklist = dashboard_worklist if dashboard_worklist is not None else build_clinical_oversight_dashboard(spark, clinician_npis=clinician_npis)
    dashboard = filter_discussion_group(source_worklist, clinician_npis=clinician_npis)
    if active_only:
        dashboard = dashboard.where(active_engaged_patient_status_expr())

    if notes is None:
        enriched = (
            dashboard.withColumn("priority_override", F.lit(None).cast("string"))
            .withColumn("review_status", F.lit("available"))
            .withColumn("reviewed_flag", F.lit(False))
            .withColumn("reviewed_at", F.lit(None).cast("timestamp"))
            .withColumn("reviewed_by", F.lit(None).cast("string"))
            .withColumn("note_text", F.lit(None).cast("string"))
            .withColumn("next_action", F.lit(None).cast("string"))
        )
    else:
        note_rows = notes.select(
            "patient_key",
            F.col("review_status").alias("saved_review_status"),
            "priority_override",
            "note_text",
            "next_action",
            "reviewed_by",
            "reviewed_at",
            "updated_at",
        )
        enriched = (
            dashboard.join(note_rows, on="patient_key", how="left")
            .withColumn("review_status", F.coalesce(F.col("saved_review_status"), F.lit("available")))
            .withColumn("reviewed_flag", F.col("reviewed_at").isNotNull())
            .drop("saved_review_status")
        )
    return enriched.select(
        "clinician_name",
        "clinician_npi",
        "latest_aligned_tin",
        "provider_rank",
        "provider_panel_size",
        "patient_key",
        "MedicareBeneficiaryID",
        "hcmpi",
        *CLINICAL_OVERSIGHT_METRIC_COLUMNS,
        "priority_override",
        "review_status",
        "reviewed_flag",
        "reviewed_at",
        "reviewed_by",
        "note_text",
        "next_action",
        "BirthDate",
        "lc_status_current",
        "payer_current",
        "office_name",
        "alignment_journey_status",
        "signature_status",
        "first_alignment_date",
        "latest_alignment_date",
        "latest_sva_date",
        "first_engagement_dt",
        "latest_engagement_dt",
        "status_note",
    )
