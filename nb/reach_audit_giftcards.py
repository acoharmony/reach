# Databricks notebook source
# MAGIC %md
# MAGIC # REACH Audit Gift Cards
# MAGIC
# MAGIC Read-only 2026 gift-card distribution review. The complete historical CSV is kept separately for reference, but this notebook reads the 2026 file.

# COMMAND ----------

# MAGIC %pip install --force-reinstall --no-deps /Workspace/Users/kcorwin@harmonycares.com/reach/lib/reach-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F

from reach._transforms._clinical_oversight._metrics import clinical_oversight_materialized_metric_labels

SOURCE_CSV_PATH = "/Volumes/uat_sandbox/gov_programs/reach_audit/reach_giftcards_2026.csv"
COMPLETE_HISTORICAL_CSV_PATH = "/Volumes/uat_sandbox/gov_programs/reach_audit/reach_gcm_all.csv"
TARGET_TABLE = "uat_sandbox.gov_programs.reach_audit_giftcards_distribution"
PATIENT_METRICS_TABLE = "uat_sandbox.gov_programs.reach_audit_copay_patient_cost_context"

SOURCE_TABLES = [
    ("Gift-card 2026 CSV", SOURCE_CSV_PATH, "Distribution source used by this notebook and the gift-card audit pipe."),
    ("Gift-card historical CSV", COMPLETE_HISTORICAL_CSV_PATH, "Historical retained file, not used for the 2026 workbook totals."),
    ("Aligned patient/cost context", PATIENT_METRICS_TABLE, "Aligned patient context used to compare gift-card recipients with aligned non-recipients."),
    ("Materialized gift-card table", TARGET_TABLE, "Typed Delta output from the gift-card ingestion pipe."),
]

CLINICAL_OVERSIGHT_PATIENT_METRIC_LABELS = clinical_oversight_materialized_metric_labels()

SOURCE_COLUMNS = {
    "card_type": "Card Type",
    "recipient_name": "Recipient Name",
    "recipient_contact": "Email/SMS/Address",
    "recipient_id": "Recipient ID",
    "quantity_received": "Quantity of Gift Cards Received",
    "face_value_received": "Total Face Value of Gift Cards Received",
    "quantity_claimed": "Quantity of Gift Cards Claimed",
    "face_value_claimed": "Total Face Value of Gift Cards Claimed",
}

PATIENT_METRIC_COLUMNS = (
    "medicarebeneficiaryid",
    "hcmpi",
    "lc_status_current",
    "clinician_name",
    *(column for column, _label in CLINICAL_OVERSIGHT_PATIENT_METRIC_LABELS),
)


def require_columns(df, columns, context):
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{context} is missing required column(s): {', '.join(missing)}")
    return df


def source_column(df, label):
    normalized = {column.lstrip("﻿").strip(): column for column in df.columns}
    if label not in normalized:
        raise ValueError(f"Gift-card CSV is missing required column: {label}")
    return normalized[label]


def raw_column(df, label):
    return F.col(f"`{source_column(df, label)}`").cast("string")


def integer_amount(column):
    normalized = F.regexp_replace(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"[^0-9-]", "")
    return F.when(F.length(normalized) > 0, normalized.cast("int"))


def money_amount(column):
    normalized = F.regexp_replace(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"[$,]", "")
    return F.when(F.length(normalized) > 0, normalized.cast("double"))


def normalized_patient_id(column):
    return F.upper(F.regexp_replace(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"[^A-Za-z0-9]", ""))


def metric_decimal_places(label):
    if "RAF" in label:
        return 3
    if label == "Member Months":
        return 1
    return 2


def cohort_agg_exprs():
    exprs = [
        F.countDistinct("hcmpi").alias("Beneficiaries"),
        F.countDistinct(F.when(F.lower(F.col("lc_status_current")) == "active - engaged", F.col("hcmpi"))).alias("Active-Engaged Beneficiaries"),
        F.sum("Cards Received").alias("Cards Received"),
        F.round(F.sum("Face Value Received"), 2).alias("Face Value Received"),
        F.coalesce(F.sum("Cards Claimed"), F.lit(0)).alias("Cards Claimed"),
        F.coalesce(F.round(F.sum("Face Value Claimed"), 2), F.lit(0.0)).alias("Face Value Claimed"),
    ]
    exprs.extend(
        F.round(F.avg(column), metric_decimal_places(label)).alias(f"Avg {label}")
        for column, label in CLINICAL_OVERSIGHT_PATIENT_METRIC_LABELS
    )
    return exprs


raw = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .option("mode", "FAILFAST")
    .csv(SOURCE_CSV_PATH)
)

giftcards = raw.select(
    F.trim(raw_column(raw, SOURCE_COLUMNS["card_type"])).alias("Card Type"),
    F.trim(raw_column(raw, SOURCE_COLUMNS["recipient_name"])).alias("Recipient"),
    F.trim(raw_column(raw, SOURCE_COLUMNS["recipient_contact"])).alias("Contact / Address"),
    F.trim(raw_column(raw, SOURCE_COLUMNS["recipient_id"])).alias("Recipient ID"),
    integer_amount(raw_column(raw, SOURCE_COLUMNS["quantity_received"])).alias("Cards Received"),
    money_amount(raw_column(raw, SOURCE_COLUMNS["face_value_received"])).alias("Face Value Received"),
    integer_amount(raw_column(raw, SOURCE_COLUMNS["quantity_claimed"])).alias("Cards Claimed"),
    money_amount(raw_column(raw, SOURCE_COLUMNS["face_value_claimed"])).alias("Face Value Claimed"),
)

patient_metrics = require_columns(spark.table(PATIENT_METRICS_TABLE), PATIENT_METRIC_COLUMNS, PATIENT_METRICS_TABLE)
giftcard_patients = (
    giftcards.withColumn("_giftcard_patient_id", normalized_patient_id(F.col("Recipient ID")))
    .where(F.length(F.col("_giftcard_patient_id")) > 0)
    .groupBy("_giftcard_patient_id")
    .agg(
        F.count("Recipient ID").alias("Gift Card Distribution Rows"),
        F.sum("Cards Received").alias("Cards Received"),
        F.round(F.sum("Face Value Received"), 2).alias("Face Value Received"),
        F.sum("Cards Claimed").alias("Cards Claimed"),
        F.round(F.sum("Face Value Claimed"), 2).alias("Face Value Claimed"),
    )
)
giftcard_patient_metrics = (
    patient_metrics.withColumn("_giftcard_patient_id", normalized_patient_id(F.col("medicarebeneficiaryid")))
    .join(giftcard_patients, on="_giftcard_patient_id", how="left")
    .withColumn("Cards Received", F.coalesce(F.col("Cards Received"), F.lit(0)))
    .withColumn("Face Value Received", F.coalesce(F.col("Face Value Received"), F.lit(0.0)))
    .withColumn("Cards Claimed", F.coalesce(F.col("Cards Claimed"), F.lit(0)))
    .withColumn("Face Value Claimed", F.coalesce(F.col("Face Value Claimed"), F.lit(0.0)))
    .withColumn("Gift Card Cohort", F.when(F.col("Gift Card Distribution Rows").isNotNull(), F.lit("Gift card")).otherwise(F.lit("No gift card")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source Tables
# MAGIC
# MAGIC Primary input tables and files used by this workbook.

# COMMAND ----------

display(spark.createDataFrame(SOURCE_TABLES, ["Role", "Source", "Use"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Summary
# MAGIC
# MAGIC 2026 gift-card distribution totals. The 2026 source populates received fields; claimed fields are displayed when present in the source.

# COMMAND ----------

display(
    giftcards.agg(
        F.count("Recipient ID").alias("Distribution Rows"),
        F.countDistinct("Recipient ID").alias("Recipients"),
        F.sum("Cards Received").alias("Cards Received"),
        F.round(F.sum("Face Value Received"), 2).alias("Face Value Received"),
        F.coalesce(F.sum("Cards Claimed"), F.lit(0)).alias("Cards Claimed"),
        F.coalesce(F.round(F.sum("Face Value Claimed"), 2), F.lit(0.0)).alias("Face Value Claimed"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. By Card Type

# COMMAND ----------

display(
    giftcards.groupBy("Card Type")
    .agg(
        F.count("Recipient ID").alias("Distribution Rows"),
        F.countDistinct("Recipient ID").alias("Recipients"),
        F.sum("Cards Received").alias("Cards Received"),
        F.round(F.sum("Face Value Received"), 2).alias("Face Value Received"),
        F.coalesce(F.sum("Cards Claimed"), F.lit(0)).alias("Cards Claimed"),
        F.coalesce(F.round(F.sum("Face Value Claimed"), 2), F.lit(0.0)).alias("Face Value Claimed"),
    )
    .orderBy("Card Type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gift Card Versus No Gift Card Patient Metrics
# MAGIC
# MAGIC Aligned-patient comparison using the same utilization, spend, and risk context used by the clinical oversight report.

# COMMAND ----------

display(
    giftcard_patient_metrics.groupBy("Gift Card Cohort")
    .agg(*cohort_agg_exprs())
    .orderBy("Gift Card Cohort")
)

display(
    giftcard_patient_metrics.where(F.col("Gift Card Cohort") == "Gift card")
    .groupBy("clinician_name")
    .agg(
        F.countDistinct("hcmpi").alias("Gift Card Beneficiaries"),
        F.sum("Cards Received").alias("Cards Received"),
        F.round(F.sum("Face Value Received"), 2).alias("Face Value Received"),
        F.round(F.avg("power_score"), 2).alias("Avg Power Score"),
        F.round(F.avg("total_events_90days"), 2).alias("Avg 90-Day Events"),
        F.round(F.avg("admits_90days"), 2).alias("Avg 90-Day Admits"),
        F.round(F.avg("ed_visit_90days"), 2).alias("Avg 90-Day ED"),
        F.round(F.avg("ip_paid_pmpm"), 2).alias("Avg Inpatient PMPM"),
        F.round(F.avg("hh_paid_pmpm"), 2).alias("Avg Home Health PMPM"),
        F.round(F.avg("total_paid_pmpm"), 2).alias("Avg TCOC PMPM"),
    )
    .orderBy(F.desc("Gift Card Beneficiaries"), "clinician_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribution Detail

# COMMAND ----------

display(giftcards.orderBy("Recipient", "Recipient ID"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix
# MAGIC
# MAGIC Source paths and the materialized audit table used by the pipe.

# COMMAND ----------

display(
    spark.createDataFrame(
        [
            ("2026 CSV used by this notebook", SOURCE_CSV_PATH),
            ("Complete historical CSV retained", COMPLETE_HISTORICAL_CSV_PATH),
            ("Materialized audit table", TARGET_TABLE),
        ],
        ["Name", "Path"],
    )
)
