"""Already-discussed patient seeding for clinical oversight notes."""

from __future__ import annotations

from collections.abc import Iterable
import ast
import re

from pyspark.sql import Window, functions as F

from reach._notes._clinical_oversight._note_lookup import clean_identifier, normalized_column, optional_column
from reach._notes._clinical_oversight._note_tables import (
    DEFAULT_NOTES_TABLE,
    current_timestamp_value,
    current_user,
    ensure_notes_table,
    notes_schema,
    quote_table_name,
)


def parse_discussed_identifiers(text: str) -> tuple[str, ...]:
    """Parse the ignored discussed.txt Python-list format into unique identifiers."""
    values: list[str] = []
    try:
        module = ast.parse(text)
        for node in module.body:
            if isinstance(node, ast.Assign) and any(getattr(target, "id", "") == "discussed" for target in node.targets):
                parsed = ast.literal_eval(node.value)
                values.extend(str(value) for value in parsed)
                break
    except (SyntaxError, ValueError):
        values = []
    if not values:
        values = re.findall(r"[\"']([^\"']+)[\"']", text)
    return unique_identifiers(values)


def unique_identifiers(values: Iterable[str | None]) -> tuple[str, ...]:
    """Return de-duplicated nonblank identifiers in source order."""
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = clean_identifier(value)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return tuple(out)


def seed_discussed_patient_notes(
    spark,
    identifiers: Iterable[str],
    *,
    worklist=None,
    notes_table: str = DEFAULT_NOTES_TABLE,
    review_status: str = "discussed",
    note_text: str = "Already discussed in REACH clinical oversight workgroup.",
    next_action: str | None = None,
    reviewed_by: str | None = None,
):
    """Insert missing note rows for already-discussed patients from an identifier list."""
    ensure_notes_table(spark, notes_table)
    unique_ids = unique_identifiers(identifiers)
    if not unique_ids:
        return spark.createDataFrame([], schema=notes_schema())

    reviewed_by = reviewed_by or current_user(spark)
    now = current_timestamp_value()
    seeds = spark.createDataFrame([(value,) for value in unique_ids], ["_seed_identifier"]).withColumn("_seed_key", normalized_column("_seed_identifier"))
    if worklist is not None:
        context = worklist.select(
            optional_column(worklist, "patient_key", "_context_patient_key"),
            optional_column(worklist, "MedicareBeneficiaryID"),
            optional_column(worklist, "hcmpi"),
            optional_column(worklist, "clinician_npi", "latest_aligned_npi"),
            optional_column(worklist, "latest_aligned_tin"),
            optional_column(worklist, "clinician_name"),
            optional_column(worklist, "provider_rank", data_type="int"),
            optional_column(worklist, "power_score", data_type="double"),
        ).withColumn("_mbi_key", normalized_column("MedicareBeneficiaryID")).withColumn("_hcmpi_key", normalized_column("hcmpi")).withColumn(
            "_patient_key", normalized_column("_context_patient_key")
        )
        joined = seeds.join(
            context,
            (F.col("_seed_key") == F.col("_mbi_key")) | (F.col("_seed_key") == F.col("_hcmpi_key")) | (F.col("_seed_key") == F.col("_patient_key")),
            "left",
        )
        win = Window.partitionBy("_seed_identifier").orderBy(
            F.col("_context_patient_key").isNull().cast("int"),
            F.col("provider_rank").asc_nulls_last(),
            F.desc("power_score"),
        )
        enriched = joined.withColumn("_rn", F.row_number().over(win)).where(F.col("_rn") == 1)
    else:
        enriched = seeds
        for column in ("_context_patient_key", "MedicareBeneficiaryID", "hcmpi", "latest_aligned_npi", "latest_aligned_tin", "clinician_name"):
            enriched = enriched.withColumn(column, F.lit(None).cast("string"))
        enriched = enriched.withColumn("provider_rank", F.lit(None).cast("int")).withColumn("power_score", F.lit(None).cast("double"))

    payload = enriched.select(
        F.coalesce(F.col("_context_patient_key"), F.col("hcmpi"), F.col("MedicareBeneficiaryID"), F.col("_seed_identifier")).alias("patient_key"),
        F.coalesce(F.col("MedicareBeneficiaryID"), F.col("_seed_identifier")).alias("MedicareBeneficiaryID"),
        "hcmpi",
        "latest_aligned_npi",
        "latest_aligned_tin",
        "clinician_name",
        "provider_rank",
        "power_score",
        F.lit(review_status).alias("review_status"),
        F.lit(None).cast("string").alias("priority_override"),
        F.lit(note_text).alias("note_text"),
        F.lit(next_action).cast("string").alias("next_action"),
        F.lit(reviewed_by).alias("reviewed_by"),
        F.lit(now).cast("timestamp").alias("reviewed_at"),
        F.lit(now).cast("timestamp").alias("updated_at"),
    ).dropDuplicates(["patient_key"])
    seed_keys = payload.select("patient_key").dropDuplicates()
    existing = spark.table(notes_table).select("patient_key").dropDuplicates()
    to_insert = payload.join(existing, on="patient_key", how="left_anti")
    to_insert.createOrReplaceTempView("_reach_discussed_note_seed")
    spark.sql(f"INSERT INTO {quote_table_name(notes_table)} SELECT * FROM _reach_discussed_note_seed")
    return spark.table(notes_table).join(seed_keys, on="patient_key", how="inner").orderBy("clinician_name", "provider_rank", "MedicareBeneficiaryID")
