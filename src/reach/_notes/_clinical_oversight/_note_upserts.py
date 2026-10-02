"""Upsert helpers for clinical oversight review notes."""

from __future__ import annotations

from pyspark.sql import functions as F

from reach._notes._clinical_oversight._note_lookup import blank_to_none, row_dict_for_note
from reach._notes._clinical_oversight._note_tables import (
    DEFAULT_NOTES_TABLE,
    current_timestamp_value,
    current_user,
    ensure_notes_table,
    notes_schema,
    quote_table_name,
)


def upsert_patient_note(
    spark,
    *,
    medicare_beneficiary_id: str | None = None,
    hcmpi: str | None = None,
    latest_aligned_npi: str | None = None,
    latest_aligned_tin: str | None = None,
    clinician_name: str | None = None,
    provider_rank: int | None = None,
    power_score: float | None = None,
    review_status: str = "reviewed",
    priority_override: str | None = None,
    note_text: str = "",
    next_action: str | None = None,
    reviewed_by: str | None = None,
    notes_table: str = DEFAULT_NOTES_TABLE,
):
    """Insert or update one patient-review note and return the saved row."""
    ensure_notes_table(spark, notes_table)
    medicare_beneficiary_id = blank_to_none(medicare_beneficiary_id)
    hcmpi = blank_to_none(hcmpi)
    patient_key = str(hcmpi or medicare_beneficiary_id or "").strip()
    if not patient_key:
        raise ValueError("Provide hcmpi or medicare_beneficiary_id before saving a note.")

    reviewed_by = reviewed_by or current_user(spark)
    now = current_timestamp_value()
    payload = [
        {
            "patient_key": patient_key,
            "MedicareBeneficiaryID": medicare_beneficiary_id,
            "hcmpi": hcmpi,
            "latest_aligned_npi": blank_to_none(latest_aligned_npi),
            "latest_aligned_tin": blank_to_none(latest_aligned_tin),
            "clinician_name": blank_to_none(clinician_name),
            "provider_rank": int(provider_rank) if provider_rank is not None else None,
            "power_score": float(power_score) if power_score is not None else None,
            "review_status": review_status or "reviewed",
            "priority_override": blank_to_none(priority_override),
            "note_text": note_text,
            "next_action": blank_to_none(next_action),
            "reviewed_by": reviewed_by,
            "reviewed_at": now,
            "updated_at": now,
        }
    ]
    spark.createDataFrame(payload, schema=notes_schema()).createOrReplaceTempView("_reach_patient_note_upsert")
    spark.sql(
        f"""
        MERGE INTO {quote_table_name(notes_table)} target
        USING _reach_patient_note_upsert source
          ON target.patient_key = source.patient_key
        WHEN MATCHED THEN UPDATE SET
          target.MedicareBeneficiaryID = coalesce(source.MedicareBeneficiaryID, target.MedicareBeneficiaryID),
          target.hcmpi = coalesce(source.hcmpi, target.hcmpi),
          target.latest_aligned_npi = coalesce(source.latest_aligned_npi, target.latest_aligned_npi),
          target.latest_aligned_tin = coalesce(source.latest_aligned_tin, target.latest_aligned_tin),
          target.clinician_name = coalesce(source.clinician_name, target.clinician_name),
          target.provider_rank = coalesce(source.provider_rank, target.provider_rank),
          target.power_score = coalesce(source.power_score, target.power_score),
          target.review_status = source.review_status,
          target.priority_override = source.priority_override,
          target.note_text = source.note_text,
          target.next_action = source.next_action,
          target.reviewed_by = source.reviewed_by,
          target.reviewed_at = source.reviewed_at,
          target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
          patient_key,
          MedicareBeneficiaryID,
          hcmpi,
          latest_aligned_npi,
          latest_aligned_tin,
          clinician_name,
          provider_rank,
          power_score,
          review_status,
          priority_override,
          note_text,
          next_action,
          reviewed_by,
          reviewed_at,
          updated_at
        ) VALUES (
          source.patient_key,
          source.MedicareBeneficiaryID,
          source.hcmpi,
          source.latest_aligned_npi,
          source.latest_aligned_tin,
          source.clinician_name,
          source.provider_rank,
          source.power_score,
          source.review_status,
          source.priority_override,
          source.note_text,
          source.next_action,
          source.reviewed_by,
          source.reviewed_at,
          source.updated_at
        )
        """
    )
    return spark.table(notes_table).where(F.col("patient_key") == patient_key)


def upsert_patient_note_from_worklist(
    spark,
    worklist,
    *,
    identifier: str | None = None,
    hcmpi: str | None = None,
    medicare_beneficiary_id: str | None = None,
    review_status: str = "reviewed",
    priority_override: str | None = None,
    note_text: str = "",
    next_action: str | None = None,
    reviewed_by: str | None = None,
    notes_table: str = DEFAULT_NOTES_TABLE,
):
    """Lookup a patient in the worklist, auto-fill context, and upsert the note."""
    row = row_dict_for_note(worklist, identifier=identifier, hcmpi=hcmpi, medicare_beneficiary_id=medicare_beneficiary_id)
    return upsert_patient_note(
        spark,
        medicare_beneficiary_id=row.get("MedicareBeneficiaryID") or medicare_beneficiary_id,
        hcmpi=row.get("hcmpi") or hcmpi,
        latest_aligned_npi=row.get("clinician_npi") or row.get("latest_aligned_npi"),
        latest_aligned_tin=row.get("latest_aligned_tin"),
        clinician_name=row.get("clinician_name"),
        provider_rank=row.get("provider_rank"),
        power_score=row.get("power_score"),
        review_status=review_status,
        priority_override=priority_override,
        note_text=note_text,
        next_action=next_action,
        reviewed_by=reviewed_by,
        notes_table=notes_table,
    )


def save_patient_review_note_by_hcmpi(
    spark,
    worklist,
    *,
    hcmpi: str,
    note_text: str,
    reviewed_by: str | None = None,
    notes_table: str = DEFAULT_NOTES_TABLE,
):
    """Save a reviewed note using only HCMPI plus note text as required input."""
    clean_hcmpi = blank_to_none(hcmpi)
    clean_note = blank_to_none(note_text)
    if clean_hcmpi is None:
        raise ValueError("Provide HCMPI before saving a review note.")
    if clean_note is None:
        raise ValueError("Provide note text before saving a review note.")
    return upsert_patient_note_from_worklist(
        spark,
        worklist,
        hcmpi=clean_hcmpi,
        review_status="reviewed",
        note_text=clean_note,
        reviewed_by=reviewed_by,
        notes_table=notes_table,
    )
