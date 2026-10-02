"""Delta table helpers for clinical oversight review notes."""

from __future__ import annotations

from datetime import datetime, timezone
import re

DEFAULT_NOTES_TABLE = "uat_sandbox.gov_programs.reach_clinical_oversight_patient_notes"
_VALID_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def quote_table_name(table_name: str) -> str:
    """Return a safely quoted one-, two-, or three-part Unity Catalog table name."""
    if not _VALID_TABLE_NAME.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return ".".join(f"`{part}`" for part in table_name.split("."))


def ensure_notes_table(spark, notes_table: str = DEFAULT_NOTES_TABLE) -> str:
    """Create the persistent patient-review notes table when it does not exist."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_table_name(notes_table)} (
          patient_key STRING NOT NULL,
          MedicareBeneficiaryID STRING,
          hcmpi STRING,
          latest_aligned_npi STRING,
          latest_aligned_tin STRING,
          clinician_name STRING,
          provider_rank INT,
          power_score DOUBLE,
          review_status STRING NOT NULL,
          priority_override STRING,
          note_text STRING,
          next_action STRING,
          reviewed_by STRING,
          reviewed_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    return notes_table


def current_timestamp_value() -> datetime:
    """Return a UTC timestamp value compatible with Spark local timestamp fields."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_user(spark) -> str:
    """Return the Databricks current user when available."""
    rows = spark.sql("select current_user() as current_user").collect()
    if not rows:
        return "unknown"
    return rows[0]["current_user"] or "unknown"


def notes_schema():
    """Return the schema shared by note inserts and empty result frames."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("patient_key", T.StringType(), nullable=False),
            T.StructField("MedicareBeneficiaryID", T.StringType(), nullable=True),
            T.StructField("hcmpi", T.StringType(), nullable=True),
            T.StructField("latest_aligned_npi", T.StringType(), nullable=True),
            T.StructField("latest_aligned_tin", T.StringType(), nullable=True),
            T.StructField("clinician_name", T.StringType(), nullable=True),
            T.StructField("provider_rank", T.IntegerType(), nullable=True),
            T.StructField("power_score", T.DoubleType(), nullable=True),
            T.StructField("review_status", T.StringType(), nullable=False),
            T.StructField("priority_override", T.StringType(), nullable=True),
            T.StructField("note_text", T.StringType(), nullable=True),
            T.StructField("next_action", T.StringType(), nullable=True),
            T.StructField("reviewed_by", T.StringType(), nullable=True),
            T.StructField("reviewed_at", T.TimestampType(), nullable=True),
            T.StructField("updated_at", T.TimestampType(), nullable=True),
        ]
    )
