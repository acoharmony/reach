"""Clinical oversight workbook DataFrame views."""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, functions as F

from reach._notes._clinical_oversight._note_lookup import lookup_patient_for_note
from reach._notes._clinical_oversight._note_tables import DEFAULT_NOTES_TABLE, ensure_notes_table
from reach._transforms._clinical_oversight._metrics import (
    CLINICAL_OVERSIGHT_METRIC_COLUMNS,
    CLINICAL_OVERSIGHT_PANEL_METRIC_COLUMNS,
)
from reach._transforms._clinical_oversight import (
    DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES,
    DISCUSSION_GROUP_CLINICIAN_NPIS,
    build_clinical_oversight_clinician_summary,
    build_clinical_oversight_dashboard,
    build_clinical_oversight_review_worklist,
)

ACTIVE_ENGAGED_LIFECYCLE_STATUS = "active - engaged"

CLINICIAN_PANEL_COLUMNS = (
    "clinician_name",
    "provider_rank",
    "hcmpi",
    "MedicareBeneficiaryID",
    *CLINICAL_OVERSIGHT_PANEL_METRIC_COLUMNS,
    "payer_current",
    "office_name",
    "lc_status_current",
    "alignment_journey_status",
    "signature_status",
    "latest_sva_date",
    "first_engagement_dt",
    "latest_engagement_dt",
)
REVIEWED_PANEL_COLUMNS = CLINICIAN_PANEL_COLUMNS + ("review_status", "reviewed_at", "reviewed_by", "note_text")


def select_existing(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Select only columns that are present on the given DataFrame."""
    selected = [col for col in columns if col in df.columns]
    return df.select(*selected) if selected else df


def npi_key(value: str | None) -> str:
    """Normalize an NPI for display-filter joins."""
    return "".join(char for char in str(value or "") if char.isdigit())


def npi_key_col(column: str):
    """Return a normalized Spark NPI column."""
    return F.regexp_replace(F.trim(F.coalesce(F.col(column).cast("string"), F.lit(""))), r"[^0-9]", "")


def order_patient_rows(df: DataFrame) -> DataFrame:
    """Order patient rows by review priority."""
    order_cols = []
    if "provider_rank" in df.columns:
        order_cols.append(F.col("provider_rank").asc_nulls_last())
    if "power_score" in df.columns:
        order_cols.append(F.desc("power_score"))
    if "hcmpi" in df.columns:
        order_cols.append(F.col("hcmpi"))
    return df.orderBy(*order_cols) if order_cols else df


def clinician_display_name(npi: str) -> str:
    """Return the known clinician display name for an NPI."""
    return DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES.get(npi_key(npi), npi_key(npi))


def clinician_panel(worklist: DataFrame, npi: str, columns: Iterable[str]) -> DataFrame:
    """Return one clinician's panel with standard ordering and columns."""
    panel = worklist.where(npi_key_col("clinician_npi") == F.lit(npi_key(npi)))
    return order_patient_rows(select_existing(panel, columns))


def clinician_open_panel_views(
    worklist: DataFrame,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
) -> dict[str, DataFrame]:
    """Return active-engaged unreviewed patient panels keyed by clinician display name."""
    open_rows = worklist.where(~F.col("reviewed_flag")) if "reviewed_flag" in worklist.columns else worklist
    return {clinician_display_name(npi): clinician_panel(open_rows, npi, CLINICIAN_PANEL_COLUMNS) for npi in clinician_npis or ()}


def clinician_reviewed_panel_views(
    worklist: DataFrame,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
) -> dict[str, DataFrame]:
    """Return reviewed patient panels keyed by clinician display name."""
    reviewed_rows = worklist.where(F.col("reviewed_flag")) if "reviewed_flag" in worklist.columns else worklist.limit(0)
    return {clinician_display_name(npi): clinician_panel(reviewed_rows, npi, REVIEWED_PANEL_COLUMNS) for npi in clinician_npis or ()}


def load_dashboard_worklist(spark, clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS):
    """Notebook-facing alias for the dashboard transform."""
    return build_clinical_oversight_dashboard(spark, clinician_npis=clinician_npis)


def review_worklist(
    spark,
    notes_table: str = DEFAULT_NOTES_TABLE,
    active_only: bool = True,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
):
    """Notebook-facing alias for the review-worklist transform."""
    ensure_notes_table(spark, notes_table)
    return build_clinical_oversight_review_worklist(
        spark,
        notes=spark.table(notes_table),
        active_only=active_only,
        clinician_npis=clinician_npis,
    )


def clinician_queue_summary(worklist):
    """Notebook-facing alias for the clinician summary transform."""
    return build_clinical_oversight_clinician_summary(worklist)


def patient_review_views(worklist: DataFrame, hcmpi: str) -> dict[str, DataFrame]:
    """Build patient-level workbook views from one HCMPI."""
    patient = lookup_patient_for_note(worklist, hcmpi=hcmpi)
    return {
        "overview": select_existing(
            patient,
            (
                "clinician_name",
                "provider_rank",
                "hcmpi",
                "MedicareBeneficiaryID",
                "BirthDate",
                "lc_status_current",
                "payer_current",
                "office_name",
                "review_status",
                "reviewed_at",
                "reviewed_by",
                "note_text",
            ),
        ),
        "utilization": select_existing(patient, CLINICAL_OVERSIGHT_METRIC_COLUMNS),
        "alignment": select_existing(
            patient,
            (
                "alignment_journey_status",
                "signature_status",
                "first_alignment_date",
                "latest_alignment_date",
                "latest_sva_date",
                "first_engagement_dt",
                "latest_engagement_dt",
                "status_note",
                "latest_aligned_tin",
                "clinician_npi",
            ),
        ),
        "all_fields": patient,
    }
