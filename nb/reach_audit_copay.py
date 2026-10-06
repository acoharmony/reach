# Databricks notebook source
# MAGIC %md
# MAGIC # REACH Audit Copay
# MAGIC
# MAGIC Read-only answer sheet for the materialized REACH Cost-Sharing Support outputs. This notebook reads tables only; it does not rebuild the pipe.

# COMMAND ----------

# MAGIC %pip install --force-reinstall --no-deps /Workspace/Users/kcorwin@harmonycares.com/reach/lib/reach-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F

from reach._transforms._clinical_oversight._metrics import (
    CLINICAL_OVERSIGHT_METRIC_LABELS,
    clinical_oversight_materialized_metric_labels,
)

TARGET_SCHEMA = "uat_sandbox.gov_programs"
PIPE_NAME = "reach_audit_copay"

TABLES = {
    "coverage_status": "reach_audit_copay_coverage_status",
    "responsibility": "reach_audit_copay_patient_responsibility",
    "patients": "reach_audit_copay_patient_cost_context",
    "support_detail": "reach_audit_copay_support_service_detail",
    "exceptions": "reach_audit_copay_no_secondary_due_not_waived_patients",
    "cohort_diffs": "reach_audit_copay_cohort_diffs",
    "measure_diffs": "reach_audit_copay_measure_diffs",
    "run_summary": "reach_audit_copay_run_summary",
}

CLINICAL_OVERSIGHT_PATIENT_METRIC_LABELS = clinical_oversight_materialized_metric_labels()

SOURCE_TABLES = [
    ("Aligned population", "prd_silver.census.census", "Current REACH census denominator, lifecycle status, office, and current clinician context."),
    ("Medicare beneficiary ID map", "prd_silver.census.hcmpimaster", "Maps HCMPI to MBI and Athena patient ID."),
    ("CMS beneficiary alignment", "prd_bronze.reach_ingested.beneficiary_alignment_report_overview", "Latest CMS BAR overview used for beneficiary identity, demographics, and beneficiary alignment start/end dates."),
    ("CMS provider alignment", "prd_bronze.reach_ingested.provider_alignment_report_r2history", "Latest provider alignment NPI/TIN and claim/voluntary alignment context."),
    ("Participant list", "uat_sandbox.gov_programs.participant_list", "Provider display names for aligned NPIs."),
    ("Patient insurance", "prd_silver.financial.patient_insurance", "Active performance-year priority 2 and real priority 3 coverage status."),
    ("Charge", "prd_silver.financial.charge", "Accepted Part B service dates, copay due, charge-level adjustment, and residual responsibility context. Audit amounts use services inside the alignment window."),
    ("Athena activity", "prd_athena_bronze.financials.activityfact", "Athena transfer/write-off/payment rollup context."),
    ("Athena transaction", "prd_athena_bronze.athenaone.transaction", "2026-posted transaction-grain patient responsibility transfers, patient payments, and REACH support adjustments. Audit payment/support amounts require service dates inside the alignment window."),
    ("Cost and risk", "prd_silver.patient_metrics.allpayer_cost_util_completed", "Latest-12-month clinical oversight spend, member months, and RAF context."),
    ("Utilization", "prd_silver.patient_metrics.patient_registry_util_cost", "The same 90-day/365-day utilization metrics used by the clinical oversight report."),
    ("Quality", "prd_silver.quality.patient_quality_metrics", "Quality-measure comparison context."),
    ("Provider reference", "prd_silver.organization.provider", "Rendering and ACO participant provider names/NPIs for service detail."),
]


def table_name(name):
    return f"{TARGET_SCHEMA}.{TABLES[name]}"


def require_columns(df, columns, context):
    missing = [source for source, _label in columns if source not in df.columns]
    if missing:
        raise ValueError(f"{context} is missing required column(s): {', '.join(missing)}")
    return df


def select_labeled(df, columns, context):
    require_columns(df, columns, context)
    return df.select(*[F.col(source).alias(label) for source, label in columns])


def pct(part, whole):
    return F.when(whole == 0, F.lit(None).cast("double")).otherwise(part / whole)


coverage_status = spark.table(table_name("coverage_status"))
responsibility = spark.table(table_name("responsibility"))
patients = spark.table(table_name("patients"))
support_detail = spark.table(table_name("support_detail"))
exceptions = spark.table(table_name("exceptions"))
cohort_diffs = spark.table(table_name("cohort_diffs"))
measure_diffs = spark.table(table_name("measure_diffs"))
run_summary = spark.table(table_name("run_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run Status
# MAGIC
# MAGIC Latest materialized outputs this notebook is reading.

# COMMAND ----------

latest_run = run_summary.where(F.col("pipe_name") == PIPE_NAME).agg(F.max("run_started_at").alias("run_started_at"))
display(
    run_summary.where(F.col("pipe_name") == PIPE_NAME)
    .join(latest_run, on="run_started_at", how="inner")
    .select(
        F.col("output_name").alias("Output"),
        F.col("row_count").alias("Rows"),
        F.col("output_table").alias("Table"),
        F.col("run_started_at").alias("Run Started At"),
    )
    .orderBy("Output")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source Tables
# MAGIC
# MAGIC Primary input tables and files used by the materialized audit pipe.

# COMMAND ----------

display(spark.createDataFrame(SOURCE_TABLES, ["Role", "Source", "Use"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Coverage Decision
# MAGIC
# MAGIC Secondary coverage is decided from active performance-year `financial.patient_insurance` priority 2 rows. Real priority 3 rows count as tertiary coverage; self-pay/no-coverage priority 3 rows are only shown as context.

# COMMAND ----------

display(
    coverage_status.agg(
        F.countDistinct("hcmpi").alias("Aligned Beneficiaries"),
        F.countDistinct(F.when(F.col("has_patient_secondary_coverage"), F.col("hcmpi"))).alias("Active Priority 2 Secondary"),
        F.countDistinct(F.when(F.col("has_patient_tertiary_coverage"), F.col("hcmpi"))).alias("Active Real Priority 3 Tertiary"),
        F.countDistinct(F.when(F.col("no_secondary_or_tertiary_coverage_flag"), F.col("hcmpi"))).alias("No Priority 2 Or Real Priority 3"),
        F.countDistinct(F.when(F.col("priority_3_self_pay_or_no_coverage_flag"), F.col("hcmpi"))).alias("Priority 3 Self-Pay/No Coverage Seen"),
    )
)
display(
    coverage_status.groupBy("coverage_decision_source")
    .agg(F.countDistinct("hcmpi").alias("Beneficiaries"))
    .orderBy(F.desc("Beneficiaries"))
)
display(
    coverage_status.where(F.col("has_patient_secondary_coverage"))
    .groupBy("patient_secondary_insurance_groups")
    .agg(F.countDistinct("hcmpi").alias("Beneficiaries"))
    .orderBy(F.desc("Beneficiaries"), "patient_secondary_insurance_groups")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Copay Responsibility And Support
# MAGIC
# MAGIC The first displays are the no-secondary/no-tertiary audit funnel and its dollar reconciliation. Source-specific charge, Athena activity, and transaction totals are shown separately as diagnostics, not as a single balancing equation.

# COMMAND ----------

no_coverage = F.col("no_secondary_or_tertiary_coverage_flag")
no_coverage_due = no_coverage & (F.col("alignment_period_copay_due_without_secondary_amt") > 0)

display(
    patients.agg(
        F.countDistinct("hcmpi").alias("Aligned Beneficiaries"),
        F.countDistinct(F.when(F.lower(F.col("lc_status_current")) == "active - engaged", F.col("hcmpi"))).alias("Active-Engaged Beneficiaries"),
        F.countDistinct(F.when(no_coverage, F.col("hcmpi"))).alias("No Secondary/Tertiary Coverage"),
        F.countDistinct(F.when(no_coverage_due, F.col("hcmpi"))).alias("No Coverage With Copay Due"),
        F.countDistinct(F.when(F.col("copays_waived_flag"), F.col("hcmpi"))).alias("No Coverage With REACH Support"),
        F.countDistinct(F.when(no_coverage_due & ~F.col("copays_waived_flag"), F.col("hcmpi"))).alias("No Coverage Due Without Support"),
    )
)

display(
    patients.where(no_coverage).agg(
        F.round(F.sum("alignment_period_copay_due_without_secondary_amt"), 2).alias("No-Coverage Copay Due"),
        F.round(F.sum("alignment_period_cost_sharing_support_amt"), 2).alias("No-Coverage REACH Support"),
        F.round(F.sum("alignment_period_patient_copay_payment_amt"), 2).alias("No-Coverage Patient Copay Payments"),
        F.round(F.sum("alignment_period_copay_unresolved_amt"), 2).alias("No-Coverage Unresolved Responsibility"),
    )
)

display(
    responsibility.agg(
        F.round(F.sum("alignment_period_copay_due_amt"), 2).alias("Audit Copay Due (Max Source)"),
        F.round(F.sum("alignment_period_charge_copay_due_amt"), 2).alias("Charge-Source Copay Due"),
        F.round(F.sum("alignment_period_patient_responsibility_transfer_amt"), 2).alias("Transaction-Source Patient Responsibility"),
        F.round(F.sum("alignment_period_reach_cost_sharing_support_amt"), 2).alias("REACH Support Adjustments"),
        F.round(F.sum("alignment_period_patient_payment_on_responsibility_service_amt"), 2).alias("Matched Patient Payments"),
        F.round(F.sum("alignment_period_copay_unresolved_amt"), 2).alias("Residual After Support/Matched Payments"),
    )
)

display(
    responsibility.agg(
        F.round(F.sum("athena_copay_due_amt"), 2).alias("Athena Activity Copay Due (Context Only)"),
        F.round(F.sum("posted_rev_cycle_patient_payment_amt"), 2).alias("All Posted Patient Payments"),
        F.round(F.sum("pre_alignment_patient_payment_amt"), 2).alias("Pre-Alignment Patient Payments"),
        F.round(F.sum("alignment_period_patient_payment_amt"), 2).alias("Alignment-Period Patient Payments"),
        F.round(F.sum("alignment_period_charge_proxy_copay_residual_amt"), 2).alias("Charge Residual Proxy (Context Only)"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost-Sharing Support Detail
# MAGIC
# MAGIC Beneficiary, Part B service, service date, support amount, and furnishing/ACO participant provider.

# COMMAND ----------

support_columns = (
    ("beneficiary_identity", "Beneficiary"),
    ("hcmpi", "HCMPI"),
    ("medicarebeneficiaryid", "MBI"),
    ("reach_alignment_start_dt", "Alignment Start"),
    ("reach_alignment_end_dt", "Alignment End"),
    ("service_dt", "Service Date"),
    ("service_to_dt", "Service Through"),
    ("service_in_reach_alignment_window_flag", "Service In Alignment Window"),
    ("part_b_service_nature", "Part B Service"),
    ("procedure_cd", "Procedure"),
    ("copay_due_amt_for_service", "Service Patient Responsibility"),
    ("patient_payment_amt_for_service", "Service Patient Payment"),
    ("cost_sharing_support_amt", "Support Amount"),
    ("support_post_dt", "Support Posted"),
    ("support_custom_transaction_code", "Support Code"),
    ("support_transaction_reason", "Support Reason"),
    ("service_rendering_provider_name", "Furnishing Provider"),
    ("service_rendering_provider_npi", "Furnishing Provider NPI"),
    ("aco_participant_provider_name", "ACO Participant Provider"),
    ("aco_participant_provider_npi", "ACO Participant NPI"),
    ("claim_id", "Claim"),
    ("charge_id", "Charge"),
    ("support_transaction_id", "Transaction"),
)

display(select_labeled(support_detail, support_columns, "support service detail").orderBy("Beneficiary", "Service Date", "Transaction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Copay Due But No Support
# MAGIC
# MAGIC Beneficiaries with no active priority-2 secondary or real priority-3 tertiary coverage, an alignment-period copay due amount, and no identified alignment-period Cost-Sharing Support.

# COMMAND ----------

exception_columns = (
    ("beneficiary_identity", "Beneficiary"),
    ("hcmpi", "HCMPI"),
    ("medicarebeneficiaryid", "MBI"),
    ("clinician_name", "Clinician"),
    ("clinician_npi", "Clinician NPI"),
    ("office_name", "Office"),
    ("reach_alignment_start_dt", "Alignment Start"),
    ("reach_alignment_end_dt", "Alignment End"),
    ("lc_status_current", "Lifecycle Status"),
    ("coverage_decision_source", "Coverage Decision"),
    ("priority_3_self_pay_or_no_coverage_names", "Priority 3 Self-Pay/No Coverage"),
    ("alignment_period_copay_due_without_secondary_amt", "Alignment-Period Copay Due Without Coverage"),
    ("posted_rev_cycle_patient_responsibility_transfer_amt", "All 2026-Posted Patient Responsibility"),
    ("posted_rev_cycle_patient_payment_amt", "All 2026-Posted Patient Payments"),
    ("pre_alignment_patient_responsibility_transfer_amt", "Pre-Alignment Patient Responsibility"),
    ("pre_alignment_patient_payment_amt", "Pre-Alignment Patient Payments"),
    ("pre_alignment_patient_payment_on_responsibility_service_amt", "Pre-Alignment Payments On Responsibility Services"),
    ("alignment_period_patient_responsibility_transfer_amt", "Alignment-Period Patient Responsibility"),
    ("alignment_period_patient_payment_on_responsibility_service_amt", "Alignment-Period Payments On Responsibility Services"),
    ("alignment_period_patient_copay_payment_amt", "Alignment-Period Patient Copay Payments"),
    ("alignment_period_patient_copay_payment_rate", "Alignment-Period Copay Payment Rate"),
    ("alignment_period_copay_unresolved_amt", "Alignment-Period Unresolved Responsibility"),
)

display(select_labeled(exceptions, exception_columns, "no-secondary due-not-waived patients").orderBy("Clinician", "Beneficiary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Supported Beneficiaries
# MAGIC
# MAGIC One row per beneficiary who received support, with demographics, clinician, cost, risk, and utilization context.

# COMMAND ----------

supported_columns = (
    ("beneficiary_identity", "Beneficiary"),
    ("hcmpi", "HCMPI"),
    ("medicarebeneficiaryid", "MBI"),
    ("birthdate", "Date of Birth"),
    ("beneficiary_gender", "Gender"),
    ("race_ethnicity", "Race/Ethnicity"),
    ("clinician_name", "Clinician"),
    ("clinician_npi", "Clinician NPI"),
    ("office_name", "Office"),
    ("reach_alignment_start_dt", "Alignment Start"),
    ("reach_alignment_end_dt", "Alignment End"),
    ("lc_status_current", "Lifecycle Status"),
    ("coverage_decision_source", "Coverage Decision"),
    ("alignment_period_copay_due_without_secondary_amt", "Alignment-Period Copay Due Without Coverage"),
    ("alignment_period_copay_waived_amt", "Alignment-Period Support Amount"),
    ("alignment_period_patient_copay_payment_amt", "Alignment-Period Patient Copay Payments"),
    ("alignment_period_copay_unresolved_amt", "Alignment-Period Unresolved Responsibility"),
    *CLINICAL_OVERSIGHT_PATIENT_METRIC_LABELS,
)

display(
    select_labeled(patients.where(F.col("copays_waived_flag")), supported_columns, "supported beneficiary patient context")
    .orderBy(F.desc("Alignment-Period Support Amount"), "Beneficiary")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Supported Versus Other Aligned Beneficiaries
# MAGIC
# MAGIC Clinical-overview-style utilization, spend, and risk comparisons for supported versus other aligned beneficiaries.

# COMMAND ----------

metric_labels = spark.createDataFrame(list(CLINICAL_OVERSIGHT_METRIC_LABELS), ["metric_key", "metric_label"])

display(
    cohort_diffs.alias("d")
    .join(metric_labels.alias("l"), F.col("d.metric") == F.col("l.metric_key"), how="inner")
    .select(
        F.col("l.metric_label").alias("Metric"),
        F.col("d.waived_patients_with_metric").alias("Supported Beneficiaries"),
        F.col("d.not_waived_patients_with_metric").alias("Other Aligned Beneficiaries"),
        F.round(F.col("d.waived_avg_value"), 2).alias("Supported Avg"),
        F.round(F.col("d.not_waived_avg_value"), 2).alias("Other Avg"),
        F.round(F.col("d.avg_diff_waived_minus_not"), 2).alias("Avg Difference"),
        F.round(F.col("d.waived_sum_value"), 2).alias("Supported Sum"),
        F.round(F.col("d.not_waived_sum_value"), 2).alias("Other Sum"),
    )
    .orderBy(F.col("Metric"))
)

display(
    measure_diffs.select(
        F.col("measure_name").alias("Measure"),
        F.col("measure_direction").alias("Direction"),
        F.col("waived_patients").alias("Supported Beneficiaries"),
        F.col("not_waived_patients").alias("Other Aligned Beneficiaries"),
        F.round("waived_rate", 4).alias("Supported Rate"),
        F.round("not_waived_rate", 4).alias("Other Rate"),
        F.round("rate_diff_waived_minus_not", 4).alias("Rate Difference"),
    )
    .orderBy("Measure")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix
# MAGIC
# MAGIC Tables expected by this notebook.

# COMMAND ----------

display(
    spark.createDataFrame(
        sorted((name, table_name(name)) for name in TABLES),
        ["Name", "Table"],
    )
)
