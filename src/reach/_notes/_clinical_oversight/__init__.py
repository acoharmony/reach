"""Notebook helpers for the REACH clinical oversight workbook."""

from reach._notes._clinical_oversight._display import patient_review_html
from reach._notes._clinical_oversight._note_lookup import lookup_patient_for_note
from reach._notes._clinical_oversight._note_seed import parse_discussed_identifiers, seed_discussed_patient_notes
from reach._notes._clinical_oversight._note_tables import DEFAULT_NOTES_TABLE, ensure_notes_table, quote_table_name
from reach._notes._clinical_oversight._note_upserts import (
    save_patient_review_note_by_hcmpi,
    upsert_patient_note,
    upsert_patient_note_from_worklist,
)
from reach._notes._clinical_oversight._views import (
    ACTIVE_ENGAGED_LIFECYCLE_STATUS,
    clinician_open_panel_views,
    clinician_queue_summary,
    clinician_reviewed_panel_views,
    load_dashboard_worklist,
    patient_review_views,
    review_worklist,
)
from reach._notes._clinical_oversight._workbook import build_review_workbook
from reach._transforms._clinical_oversight import DISCUSSION_GROUP_CLINICIAN_NPIS, DISCUSSION_GROUP_CLINICIANS

__all__ = [
    "ACTIVE_ENGAGED_LIFECYCLE_STATUS",
    "DEFAULT_NOTES_TABLE",
    "DISCUSSION_GROUP_CLINICIAN_NPIS",
    "DISCUSSION_GROUP_CLINICIANS",
    "build_review_workbook",
    "clinician_open_panel_views",
    "clinician_queue_summary",
    "clinician_reviewed_panel_views",
    "ensure_notes_table",
    "load_dashboard_worklist",
    "lookup_patient_for_note",
    "parse_discussed_identifiers",
    "patient_review_html",
    "patient_review_views",
    "quote_table_name",
    "review_worklist",
    "save_patient_review_note_by_hcmpi",
    "seed_discussed_patient_notes",
    "upsert_patient_note",
    "upsert_patient_note_from_worklist",
]
