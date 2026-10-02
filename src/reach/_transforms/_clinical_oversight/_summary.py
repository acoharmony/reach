"""Clinician summary transform for clinical oversight."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._transforms._clinical_oversight._constants import CLINICIAN_SUMMARY_VIEW
from reach._transforms._registry import register_transform


@register_transform(
    "clinical_oversight_clinician_summary",
    concern="clinical_oversight",
    output_table=CLINICIAN_SUMMARY_VIEW,
    description="Clinician-level active/reviewed/remaining queue summary.",
)
def build_clinical_oversight_clinician_summary(review_worklist: DataFrame) -> DataFrame:
    """Summarize available and reviewed patient counts by clinician."""
    reviewed = F.sum(F.when(F.col("reviewed_flag"), F.lit(1)).otherwise(F.lit(0)))
    total = F.count(F.lit(1))
    return (
        review_worklist.groupBy("clinician_name", "clinician_npi", "latest_aligned_tin", "office_name")
        .agg(
            total.alias("active_patients_available"),
            reviewed.alias("reviewed_patients"),
            (total - reviewed).alias("patients_remaining"),
            F.round(reviewed / total * F.lit(100.0), 1).alias("reviewed_pct"),
            F.round(F.avg("power_score"), 2).alias("avg_power_score"),
            F.round(F.max("power_score"), 2).alias("highest_power_score"),
            F.sum(F.coalesce(F.col("total_events_90days"), F.lit(0))).alias("events_90days"),
        )
        .orderBy(F.desc("patients_remaining"), F.desc("highest_power_score"), "clinician_name")
    )
