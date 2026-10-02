"""Compatibility exports for clinical oversight patient-review notes."""

from reach._notes._clinical_oversight._note_lookup import lookup_patient_for_note
from reach._notes._clinical_oversight._note_seed import parse_discussed_identifiers, seed_discussed_patient_notes
from reach._notes._clinical_oversight._note_tables import DEFAULT_NOTES_TABLE, ensure_notes_table, quote_table_name
from reach._notes._clinical_oversight._note_upserts import (
    save_patient_review_note_by_hcmpi,
    upsert_patient_note,
    upsert_patient_note_from_worklist,
)

__all__ = [
    "DEFAULT_NOTES_TABLE",
    "ensure_notes_table",
    "lookup_patient_for_note",
    "parse_discussed_identifiers",
    "quote_table_name",
    "save_patient_review_note_by_hcmpi",
    "seed_discussed_patient_notes",
    "upsert_patient_note",
    "upsert_patient_note_from_worklist",
]
