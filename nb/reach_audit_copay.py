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
    "stage_manifest": "reach_audit_copay_stage_manifest",
    "stage_metrics": "reach_audit_copay_stage_metrics",
    "check_results": "reach_audit_copay_check_results",
    "service_trace": "reach_audit_copay_service_trace",
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

REVIEW_PULL_ORDER = [
    (1, "Can I use this run?", "check_results", "Show fail, then warn, then pass. Resolve fail rows before using any summary dollars."),
    (2, "What changed at each transformation?", "stage_metrics", "Read in stage order. Check row-count, date-range, and dollar jumps."),
    (3, "How was each table generated?", "stage_manifest", "Read in stage order when a reviewer needs source, dependency, grain, or join-key context."),
    (4, "Which patients have copay due and no support?", "exceptions", "Sort by unresolved responsibility, then copay due, then clinician."),
    (5, "Which REACH support codes are in the data?", "service_trace", "Filter to support transactions and sort by support amount; use trace_decision to separate included rows from review rows."),
    (6, "Which service rows support the CMS audit file?", "support_detail", "Sort by support amount, beneficiary, service date, transaction."),
    (7, "Which beneficiaries received support?", "patients", "Filter to copays_waived_flag and sort by support amount."),
    (8, "How was coverage decided?", "coverage_status", "Group by coverage_decision_source, then inspect high-volume payer groups."),
    (9, "How do supported patients compare to the rest?", "cohort_diffs / measure_diffs", "Sort by absolute differences so large signal rises first."),
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


def status_rank(column):
    value = F.lower(F.col(column))
    return F.when(value == "fail", F.lit(1)).when(value == "warn", F.lit(2)).when(value == "pass", F.lit(3)).otherwise(F.lit(9))


coverage_status = spark.table(table_name("coverage_status"))
responsibility = spark.table(table_name("responsibility"))
patients = spark.table(table_name("patients"))
support_detail = spark.table(table_name("support_detail"))
exceptions = spark.table(table_name("exceptions"))
cohort_diffs = spark.table(table_name("cohort_diffs"))
measure_diffs = spark.table(table_name("measure_diffs"))
stage_manifest = spark.table(table_name("stage_manifest"))
stage_metrics = spark.table(table_name("stage_metrics"))
check_results = spark.table(table_name("check_results"))
service_trace = spark.table(table_name("service_trace"))
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
    .join(stage_manifest.select("output_name", "stage_order"), on="output_name", how="left")
    .select(
        F.col("stage_order").alias("Stage"),
        F.col("output_name").alias("Output"),
        F.col("row_count").alias("Rows"),
        F.col("output_table").alias("Table"),
        F.col("run_started_at").alias("Run Started At"),
    )
    .orderBy("Stage", "Output")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformation Observability
# MAGIC
# MAGIC Review checks and stage-level metrics written by the materialized audit pipe.

# COMMAND ----------

display(spark.createDataFrame(REVIEW_PULL_ORDER, ["Step", "Reviewer Question", "Table", "Filter And Sort"]))

display(
    check_results.select(
        F.col("check_status").alias("Status"),
        F.col("check_name").alias("Check"),
        F.col("observed_value").alias("Observed"),
        F.col("expected_value").alias("Expected"),
        F.col("reviewer_note").alias("Reviewer Note"),
        status_rank("check_status").alias("_status_rank"),
    )
    .orderBy("_status_rank", "Check")
    .drop("_status_rank")
)

display(
    stage_metrics.select(
        F.col("stage_order").alias("Stage"),
        F.col("output_name").alias("Output"),
        F.col("row_count").alias("Rows"),
        F.col("beneficiary_count").alias("Beneficiaries"),
        F.col("transaction_count").alias("Transactions"),
        F.col("claim_count").alias("Claims"),
        F.col("charge_count").alias("Charges"),
        F.col("support_transaction_count").alias("Support Transactions"),
        F.round("alignment_period_copay_due_amt", 2).alias("Copay Due"),
        F.round("alignment_period_reach_cost_sharing_support_amt", 2).alias("REACH Support"),
        F.round("alignment_period_patient_copay_payment_amt", 2).alias("Patient Payments"),
        F.round("alignment_period_copay_unresolved_amt", 2).alias("Residual"),
        F.col("min_observed_date").alias("Min Date"),
        F.col("max_observed_date").alias("Max Date"),
    ).orderBy("Stage")
)

display(
    stage_manifest.select(
        F.col("stage_order").alias("Stage"),
        F.col("output_name").alias("Output"),
        F.col("grain").alias("Grain"),
        F.col("source_tables_or_inputs").alias("Sources"),
        F.col("depends_on_outputs").alias("Depends On"),
        F.col("primary_join_keys").alias("Join Keys"),
        F.col("transformation_purpose").alias("Purpose"),
    ).orderBy("Stage")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Source Tables
# MAGIC
# MAGIC Primary input tables and files used by the materialized audit pipe.

# COMMAND ----------

display(spark.createDataFrame(SOURCE_TABLES, ["Role", "Source", "Use"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Coverage Decision
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
    .agg(
        F.countDistinct("hcmpi").alias("Beneficiaries"),
        F.sum("patient_secondary_insurance_package_count").alias("Secondary Package Rows"),
        F.sum("patient_tertiary_insurance_package_count").alias("Tertiary Package Rows"),
        F.sum("claim_time_secondary_coverage_charge_count").alias("Claim-Time Secondary Charges"),
        F.sum("claim_time_tertiary_coverage_charge_count").alias("Claim-Time Tertiary Charges"),
    )
    .orderBy(F.desc("Beneficiaries"), "coverage_decision_source")
)
display(
    coverage_status.where(F.col("has_patient_secondary_coverage"))
    .groupBy("patient_secondary_insurance_groups")
    .agg(F.countDistinct("hcmpi").alias("Beneficiaries"))
    .orderBy(F.desc("Beneficiaries"), "patient_secondary_insurance_groups")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Copay Responsibility And Support
# MAGIC
# MAGIC The first displays are the no-secondary/no-tertiary audit funnel and its dollar reconciliation. The audit due amount is computed at beneficiary grain before summing: for each beneficiary, the pipe takes the larger of charge-source copay due and transaction-source patient-responsibility transfers. Residual responsibility is also computed at beneficiary grain and floored at zero, so it is not expected to equal the displayed aggregate due minus aggregate support minus aggregate payments.

# COMMAND ----------

no_coverage = F.col("no_secondary_or_tertiary_coverage_flag")
no_coverage_due = no_coverage & (F.col("alignment_period_copay_due_without_secondary_amt") > 0)

display(
    spark.createDataFrame(
        [
            (
                "Audit Copay Due",
                "Sum of each beneficiary's larger alignment-window amount from charge copay due or transaction patient-responsibility transfers.",
            ),
            ("Charge Due Source Total", "Accepted financial.charge copay amounts for services inside each beneficiary's REACH alignment window."),
            (
                "Transfer Due Source Total",
                "Athena transaction patient-responsibility transfer amounts for services inside each beneficiary's REACH alignment window.",
            ),
            ("REACH Support Applied", "REACH-coded Cost-Sharing Support adjustment transactions on alignment-window services."),
            (
                "Patient Copay Payments Applied",
                "Patient payments matched to responsibility services, capped at the beneficiary audit copay due before residual responsibility is calculated.",
            ),
            (
                "Residual Responsibility",
                "Sum of beneficiary-level max(audit copay due - REACH support - applied patient payments, 0).",
            ),
            (
                "Athena Activity Context",
                "Performance-year activityfact rollup by activity date. It is not service-date aligned and should not be reconciled to the audit due row.",
            ),
            (
                "Charge Residual Proxy",
                "Charge-system copay less charge-system zeroed/write-off proxy. It is diagnostic context, not a REACH support or payment measure.",
            ),
        ],
        ["Metric", "How To Read It"],
    )
)

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
        F.round(F.sum("alignment_period_copay_due_amt"), 2).alias("Audit Copay Due"),
        F.round(F.sum("alignment_period_charge_copay_due_amt"), 2).alias("Charge Due Source Total"),
        F.round(F.sum("alignment_period_patient_responsibility_transfer_amt"), 2).alias("Transfer Due Source Total"),
        F.round(F.sum("alignment_period_reach_cost_sharing_support_amt"), 2).alias("REACH Support Applied"),
        F.round(F.sum("alignment_period_patient_copay_payment_amt"), 2).alias("Patient Copay Payments Applied"),
        F.round(F.sum("alignment_period_copay_unresolved_amt"), 2).alias("Residual Responsibility"),
    )
)

display(
    responsibility.agg(
        F.round(
            F.sum("alignment_period_copay_due_amt")
            - F.sum("alignment_period_reach_cost_sharing_support_amt")
            - F.sum("alignment_period_patient_copay_payment_amt"),
            2,
        ).alias("Aggregate Due Less Support/Payments"),
        F.round(F.sum("alignment_period_copay_unresolved_amt"), 2).alias("Sum Of Beneficiary-Floored Residuals"),
        F.round(
            F.sum("alignment_period_copay_unresolved_amt")
            - (
                F.sum("alignment_period_copay_due_amt")
                - F.sum("alignment_period_reach_cost_sharing_support_amt")
                - F.sum("alignment_period_patient_copay_payment_amt")
            ),
            2,
        ).alias("Beneficiary-Level Floor Difference"),
    )
)

display(
    responsibility.agg(
        F.round(F.sum("athena_copay_due_amt"), 2).alias("Athena Activity Copay Due Context"),
        F.round(F.sum("posted_rev_cycle_patient_payment_amt"), 2).alias("All Posted Patient Payments Context"),
        F.round(F.sum("pre_alignment_patient_payment_amt"), 2).alias("Pre-Alignment Patient Payments"),
        F.round(F.sum("alignment_period_patient_payment_amt"), 2).alias("Alignment-Period Patient Payments"),
        F.round(F.sum("alignment_period_charge_proxy_copay_residual_amt"), 2).alias("Charge Residual Proxy Context"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cost-Sharing Support Detail
# MAGIC
# MAGIC Beneficiary, Part B service, service date, support amount, and furnishing/ACO participant provider.

# COMMAND ----------

display(
    service_trace.where(F.col("cost_sharing_support_identified_flag"))
    .withColumn(
        "_trace_rank",
        F.when(F.col("trace_decision") == "included_support_on_no_coverage_alignment_service", F.lit(1))
        .when(F.col("trace_decision") == "review_support_beneficiary_has_secondary_or_tertiary_coverage", F.lit(2))
        .when(F.col("trace_decision") == "excluded_support_outside_alignment_service_window", F.lit(3))
        .otherwise(F.lit(9)),
    )
    .groupBy("custom_transaction_code", "trace_decision")
    .agg(
        F.min("_trace_rank").alias("_trace_rank"),
        F.countDistinct("transaction_id").alias("Support Transactions"),
        F.countDistinct("hcmpi").alias("Beneficiaries"),
        F.round(F.sum("cost_sharing_support_amt"), 2).alias("Support Amount"),
    )
    .orderBy("_trace_rank", F.desc("Support Amount"), "custom_transaction_code", "trace_decision")
    .drop("_trace_rank")
)

# COMMAND ----------

display(
    service_trace.where(F.col("cost_sharing_support_identified_flag"))
    .withColumn(
        "_trace_rank",
        F.when(F.col("trace_decision") == "included_support_on_no_coverage_alignment_service", F.lit(1))
        .when(F.col("trace_decision") == "review_support_beneficiary_has_secondary_or_tertiary_coverage", F.lit(2))
        .when(F.col("trace_decision") == "excluded_support_outside_alignment_service_window", F.lit(3))
        .otherwise(F.lit(9)),
    )
    .select(
        F.col("trace_decision").alias("Trace Decision"),
        F.col("beneficiary_identity").alias("Beneficiary"),
        F.col("hcmpi").alias("HCMPI"),
        F.col("medicarebeneficiaryid").alias("MBI"),
        F.col("service_from_dt").alias("Service Date"),
        F.col("transaction_post_dt").alias("Post Date"),
        F.col("custom_transaction_code").alias("Support Code"),
        F.round("cost_sharing_support_amt", 2).alias("Support Amount"),
        F.col("coverage_decision_source").alias("Coverage Decision"),
        F.col("service_in_reach_alignment_window_flag").alias("Service In Alignment Window"),
        F.col("no_secondary_or_tertiary_coverage_flag").alias("No Secondary/Tertiary Coverage"),
        F.col("transaction_id").alias("Transaction"),
        F.col("claim_id").alias("Claim"),
        F.col("charge_id").alias("Charge"),
        F.col("part_b_service_nature").alias("Part B Service"),
        F.col("_trace_rank"),
    )
    .orderBy("_trace_rank", F.desc("Support Amount"), "Beneficiary", "Service Date", "Transaction")
    .drop("_trace_rank")
)

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

display(
    select_labeled(support_detail, support_columns, "support service detail")
    .orderBy(F.desc("Support Amount"), "Beneficiary", "Service Date", "Transaction")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Copay Due But No Support Review Queue
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

display(
    select_labeled(exceptions, exception_columns, "no-secondary due-not-waived patients")
    .orderBy(
        F.desc("Alignment-Period Unresolved Responsibility"),
        F.desc("Alignment-Period Copay Due Without Coverage"),
        "Clinician",
        "Beneficiary",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Supported Beneficiaries
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
# MAGIC ## 9. Supported Versus Other Aligned Beneficiaries
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
        F.abs(F.round(F.col("d.avg_diff_waived_minus_not"), 2)).alias("_abs_avg_difference"),
        F.round(F.col("d.waived_sum_value"), 2).alias("Supported Sum"),
        F.round(F.col("d.not_waived_sum_value"), 2).alias("Other Sum"),
    )
    .orderBy(F.desc("_abs_avg_difference"), "Metric")
    .drop("_abs_avg_difference")
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
        F.abs(F.round("rate_diff_waived_minus_not", 4)).alias("_abs_rate_difference"),
    )
    .orderBy(F.desc("_abs_rate_difference"), "Measure")
    .drop("_abs_rate_difference")
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
