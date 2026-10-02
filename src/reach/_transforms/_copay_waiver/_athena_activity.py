"""Athena activity rollups for copay-waiver analysis."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import zero_if_null
from reach._transforms._copay_waiver._common import _py_end, _py_start
from reach._transforms._copay_waiver._views import ATHENA_PATIENT_COPAY_ACTIVITY_VIEW
from reach._transforms._registry import register_transform


@register_transform(
    "athena_patient_copay_activity",
    concern="copay_waiver",
    output_table=ATHENA_PATIENT_COPAY_ACTIVITY_VIEW,
    description="Patient-level Athena activityfact copay transfer and freecare/bad-debt write-off rollup.",
)
def build_athena_patient_copay_activity(activity_fact: DataFrame, hcmpi_master: DataFrame, *, performance_year: int) -> DataFrame:
    """Aggregate raw Athena activity rows to patient-level copay due/write-off evidence."""
    py_start = _py_start(performance_year)
    py_end = _py_end(performance_year)
    hcmpi = (
        hcmpi_master.where(F.lower(F.trim(F.col("identifiersrcfield"))) == F.lit("athena_id"))
        .select(
            F.regexp_replace(F.col("identifier").cast("string"), r"\.0+$", "").alias("athena_patient_id"),
            F.col("hcmpi").cast("string").alias("hcmpi"),
        )
        .where(F.col("athena_patient_id").isNotNull() & F.col("hcmpi").isNotNull())
        .dropDuplicates(["athena_patient_id", "hcmpi"])
    )
    activity = (
        activity_fact.where(F.col("VOIDEDBY").isNull() & F.col("VOIDED").isNull())
        .withColumn("activity_dt", F.coalesce(F.to_date("FROMDATE"), F.to_date("POSTDATE"), F.to_date("CREATED")))
        .where((F.col("activity_dt") >= py_start) & (F.col("activity_dt") <= py_end))
        .withColumn("athena_patient_id", F.regexp_replace(F.col("PATIENTID").cast("string"), r"\.0+$", ""))
        .join(hcmpi, on="athena_patient_id", how="inner")
    )
    due_amount = F.when(F.col("TYPE") == F.lit("TRANSFEROUT"), -1.0 * F.col("AMOUNT").cast("double")).otherwise(F.lit(0.0))
    patient_payment = F.when(F.col("PATIENTPAYMENTID").isNotNull(), F.abs(F.col("AMOUNT").cast("double"))).otherwise(F.lit(0.0))
    rolled = (
        activity.groupBy("hcmpi")
        .agg(
            F.countDistinct("CHARGEID").alias("athena_activity_charge_count"),
            F.countDistinct("CLAIMID").alias("athena_activity_claim_count"),
            F.sum(due_amount).alias("athena_copay_due_amt"),
            F.sum(F.abs(zero_if_null("FREECARE").cast("double"))).alias("athena_freecare_adjustment_amt"),
            F.sum(F.abs(zero_if_null("BADDEBT").cast("double"))).alias("athena_bad_debt_adjustment_amt"),
            F.sum(F.abs(zero_if_null("ADJUSTMENT").cast("double"))).alias("athena_total_adjustment_amt"),
            F.sum(patient_payment).alias("athena_patient_payment_amt"),
            F.array_join(F.array_sort(F.collect_set(F.when(F.col("TYPE") == F.lit("ADJUSTMENT"), F.col("TRANSACTIONREASONID")))), "; ").alias(
                "athena_adjustment_reason_ids"
            ),
        )
        .withColumn("athena_writeoff_amt", zero_if_null("athena_freecare_adjustment_amt") + zero_if_null("athena_bad_debt_adjustment_amt"))
        .withColumn("athena_copay_zeroed_amt", F.least(zero_if_null("athena_copay_due_amt"), zero_if_null("athena_writeoff_amt")))
        .withColumn("athena_copays_zeroed_flag", (F.col("athena_copay_due_amt") > 0) & (F.col("athena_copay_zeroed_amt") > 0))
    )
    return rolled
