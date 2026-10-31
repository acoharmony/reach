"""Reviewability outputs for the REACH copay audit."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import zero_if_null
from reach._transforms._copay_waiver._common import _select_required, _with_hcmpi_key
from reach._transforms._copay_waiver._views import (
    COPAY_WAIVER_CHECK_RESULTS_VIEW,
    COPAY_WAIVER_SERVICE_TRACE_VIEW,
    COPAY_WAIVER_STAGE_MANIFEST_VIEW,
    COPAY_WAIVER_STAGE_METRICS_VIEW,
)
from reach._transforms._registry import register_transform

_MANIFEST_ROWS = (
    (
        1,
        "aligned_population",
        "hcmpi",
        "prd_silver.census.census; prd_silver.census.hcmpimaster; prd_bronze.reach_ingested.beneficiary_alignment_report_overview; prd_bronze.reach_ingested.provider_alignment_report_r2history",
        "Select the current REACH census denominator and enrich it with latest BAR and provider-alignment context.",
        "",
        "hcmpi, MedicareBeneficiaryID",
    ),
    (
        2,
        "alignment_count_reference",
        "count_source",
        "prd_bronze.reach_ingested.beneficiary_alignment_report_overview; prd_bronze.reach_ingested.provider_alignment_report_r2history",
        "Expose bronze alignment-file counts as reference context for denominator review.",
        "",
        "source table/file date",
    ),
    (
        3,
        "coverage_status",
        "hcmpi",
        "aligned_population; prd_silver.financial.patient_insurance; prd_silver.financial.charge",
        "Classify active priority-2 secondary and real priority-3 tertiary coverage, with claim-time coverage context.",
        "aligned_population",
        "hcmpi",
    ),
    (
        4,
        "athena_patient_copay_activity",
        "hcmpi",
        "prd_athena_bronze.financials.activityfact; prd_silver.census.hcmpimaster",
        "Roll up Athena activity context for copay due, adjustment, payment, and write-off amounts.",
        "",
        "athena patient id, hcmpi",
    ),
    (
        5,
        "revenue_cycle_transactions",
        "transaction_id",
        "aligned_population; coverage_status; prd_athena_bronze.athenaone.transaction; prd_silver.census.hcmpimaster",
        "Preserve transaction-grain patient-responsibility, payment, and REACH-coded support events.",
        "aligned_population; coverage_status",
        "athena patient id, hcmpi, charge id, claim id",
    ),
    (
        6,
        "patient_responsibility",
        "hcmpi",
        "aligned_population; athena_patient_copay_activity; revenue_cycle_transactions; prd_silver.financial.charge",
        "Reconcile charge-source copay due, transaction-source patient responsibility, payments, and support at beneficiary grain.",
        "aligned_population; athena_patient_copay_activity; revenue_cycle_transactions",
        "hcmpi, charge id, claim id",
    ),
    (
        7,
        "patient_profile",
        "hcmpi",
        "aligned_population; coverage_status; patient_responsibility",
        "Combine beneficiary identity, coverage decision, and beneficiary-level copay audit facts.",
        "aligned_population; coverage_status; patient_responsibility",
        "hcmpi",
    ),
    (
        8,
        "patient_cost_context",
        "hcmpi",
        "patient_profile; prd_silver.patient_metrics.allpayer_cost_util_completed; prd_silver.patient_metrics.patient_registry_util_cost",
        "Add clinical oversight spend, RAF, and utilization context to the beneficiary audit profile.",
        "patient_profile",
        "hcmpi",
    ),
    (
        9,
        "population_summary",
        "population_group",
        "patient_cost_context",
        "Summarize the beneficiary funnel and dollars by coverage/support population.",
        "patient_cost_context",
        "population group",
    ),
    (
        10,
        "segment_breakdown",
        "segment",
        "patient_cost_context",
        "Break out the audit population by provider, office, payer, status, and age-band segments.",
        "patient_cost_context",
        "segment columns",
    ),
    (
        11,
        "support_service_detail",
        "support_transaction_id",
        "patient_profile; revenue_cycle_transactions; prd_silver.organization.provider",
        "List the alignment-window service and transaction detail behind identified Cost-Sharing Support.",
        "patient_profile; revenue_cycle_transactions",
        "hcmpi, charge id, transaction id",
    ),
    (
        12,
        "no_secondary_copay_due_not_waived_patients",
        "hcmpi",
        "patient_cost_context",
        "List beneficiaries with no secondary or tertiary coverage, copay due, and no identified support.",
        "patient_cost_context",
        "hcmpi",
    ),
    (
        13,
        "cohort_diffs",
        "metric",
        "patient_cost_context",
        "Compare cost and utilization metrics for waived and not-waived cohorts.",
        "patient_cost_context",
        "metric",
    ),
    (
        14,
        "measure_diffs",
        "measure",
        "patient_cost_context; prd_silver.quality.patient_quality_metrics",
        "Compare patient-level quality measure context for waived and not-waived cohorts.",
        "patient_cost_context",
        "measure",
    ),
)

_METRIC_STAGE_ORDER = {row[1]: row[0] for row in _MANIFEST_ROWS}

_PATIENT_PROFILE_TRACE_COLUMNS = (
    "hcmpi",
    "beneficiary_identity",
    "no_secondary_or_tertiary_coverage_flag",
    "coverage_decision_source",
    "copays_waived_flag",
    "alignment_period_copay_due_amt",
    "alignment_period_copay_due_without_secondary_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_patient_copay_payment_amt",
    "alignment_period_copay_unresolved_amt",
    "alignment_period_reach_support_custom_transaction_codes",
)

_REVENUE_CYCLE_TRACE_COLUMNS = (
    "performance_year",
    "revenue_cycle_event_class",
    "hcmpi",
    "MedicareBeneficiaryID",
    "transaction_id",
    "claim_id",
    "charge_id",
    "transaction_charge_id",
    "transaction_post_dt",
    "service_from_dt",
    "service_to_dt",
    "service_in_reach_alignment_window_flag",
    "transaction_post_in_reach_alignment_window_flag",
    "part_b_service_nature",
    "procedure_cd",
    "procedure_modifiers",
    "transaction_type",
    "transaction_transfer_type",
    "transaction_transfer_intent",
    "transaction_reason",
    "custom_transaction_code",
    "transaction_method",
    "transaction_amt",
    "patient_responsibility_transfer_amt",
    "patient_payment_amt",
    "cost_sharing_support_amt",
    "cost_sharing_support_identified_flag",
    "line_note",
)


def _sum_if_present(frame: DataFrame, column: str):
    return F.sum(zero_if_null(column).cast("double")) if column in frame.columns else F.lit(None).cast("double")


def _count_distinct_if_present(frame: DataFrame, column: str):
    return F.countDistinct(column) if column in frame.columns else F.lit(None).cast("long")


def _min_if_present(frame: DataFrame, columns: Sequence[str]):
    for column in columns:
        if column in frame.columns:
            return F.min(F.to_date(column))
    return F.lit(None).cast("date")


def _max_if_present(frame: DataFrame, columns: Sequence[str]):
    for column in columns:
        if column in frame.columns:
            return F.max(F.to_date(column))
    return F.lit(None).cast("date")


def _metric_row(output_name: str, frame: DataFrame) -> DataFrame:
    support_count = (
        F.countDistinct(F.when(F.col("cost_sharing_support_identified_flag"), F.col("transaction_id")))
        if {"cost_sharing_support_identified_flag", "transaction_id"}.issubset(frame.columns)
        else F.lit(None).cast("long")
    )
    null_hcmpi_count = F.sum(F.when(F.col("hcmpi").isNull(), F.lit(1)).otherwise(F.lit(0))) if "hcmpi" in frame.columns else F.lit(None).cast("long")
    return frame.agg(
        F.count("*").cast("long").alias("row_count"),
        _count_distinct_if_present(frame, "hcmpi").cast("long").alias("beneficiary_count"),
        _count_distinct_if_present(frame, "transaction_id").cast("long").alias("transaction_count"),
        _count_distinct_if_present(frame, "claim_id").cast("long").alias("claim_count"),
        _count_distinct_if_present(frame, "charge_id").cast("long").alias("charge_count"),
        null_hcmpi_count.cast("long").alias("null_hcmpi_count"),
        support_count.cast("long").alias("support_transaction_count"),
        _sum_if_present(frame, "alignment_period_copay_due_amt").alias("alignment_period_copay_due_amt"),
        _sum_if_present(frame, "alignment_period_copay_due_without_secondary_amt").alias("alignment_period_copay_due_without_secondary_amt"),
        _sum_if_present(frame, "alignment_period_reach_cost_sharing_support_amt").alias("alignment_period_reach_cost_sharing_support_amt"),
        _sum_if_present(frame, "alignment_period_patient_copay_payment_amt").alias("alignment_period_patient_copay_payment_amt"),
        _sum_if_present(frame, "alignment_period_copay_unresolved_amt").alias("alignment_period_copay_unresolved_amt"),
        _sum_if_present(frame, "cost_sharing_support_amt").alias("transaction_or_service_support_amt"),
        _min_if_present(frame, ("service_from_dt", "service_dt", "transaction_post_dt")).alias("min_observed_date"),
        _max_if_present(frame, ("service_from_dt", "service_dt", "transaction_post_dt")).alias("max_observed_date"),
    ).select(
        F.lit(_METRIC_STAGE_ORDER.get(output_name)).cast("int").alias("stage_order"),
        F.lit(output_name).alias("output_name"),
        "*",
    )


def _union_by_name(frames: Sequence[DataFrame]) -> DataFrame:
    head, *tail = frames
    result = head
    for frame in tail:
        result = result.unionByName(frame)
    return result


def _check_row(name: str, status, observed, expected: str, reviewer_note: str) -> DataFrame:
    return status.select(
        F.lit(name).alias("check_name"),
        F.col("check_status"),
        observed.alias("observed_value"),
        F.lit(expected).alias("expected_value"),
        F.lit(reviewer_note).alias("reviewer_note"),
    )


@register_transform(
    "copay_waiver_stage_manifest",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_STAGE_MANIFEST_VIEW,
    description="Human-reviewable map of copay audit stages, source tables, dependencies, grain, and join keys.",
)
def build_copay_waiver_stage_manifest(spark) -> DataFrame:
    """Return one row per copay audit output describing how reviewers should read it."""
    rows = [
        {
            "stage_order": stage_order,
            "output_name": output_name,
            "grain": grain,
            "source_tables_or_inputs": source_tables,
            "transformation_purpose": purpose,
            "depends_on_outputs": depends_on,
            "primary_join_keys": join_keys,
        }
        for stage_order, output_name, grain, source_tables, purpose, depends_on, join_keys in _MANIFEST_ROWS
    ]
    return spark.createDataFrame(rows)


@register_transform(
    "copay_waiver_stage_metrics",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_STAGE_METRICS_VIEW,
    description="Stage-by-stage counts, identifiers, date ranges, and audit-dollar totals for copay review.",
)
def build_copay_waiver_stage_metrics(outputs: Mapping[str, DataFrame]) -> DataFrame:
    """Return one metric row per base copay audit output."""
    rows = [_metric_row(output_name, outputs[output_name]) for output_name in _METRIC_STAGE_ORDER if output_name in outputs]
    return _union_by_name(rows).orderBy("stage_order")


@register_transform(
    "copay_waiver_check_results",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_CHECK_RESULTS_VIEW,
    description="Pass/warn/fail checks that make the copay audit safe to review before pulling detail tables.",
)
def build_copay_waiver_check_results(
    patient_profile: DataFrame,
    revenue_cycle_transactions: DataFrame,
    support_service_detail: DataFrame,
) -> DataFrame:
    """Return named audit checks over the materialized copay transformations."""
    support_codes = revenue_cycle_transactions.agg(
        F.countDistinct(F.when(F.col("cost_sharing_support_identified_flag"), F.col("custom_transaction_code"))).alias("support_code_count"),
        F.countDistinct(F.when(F.col("cost_sharing_support_identified_flag"), F.col("transaction_id"))).alias("support_transaction_count"),
        F.round(F.sum(F.when(F.col("cost_sharing_support_identified_flag"), zero_if_null("cost_sharing_support_amt")).otherwise(F.lit(0.0))), 2).alias(
            "support_amt"
        ),
    ).withColumn(
        "check_status",
        F.when(F.col("support_transaction_count") <= 0, F.lit("fail")).when(F.col("support_code_count") > 1, F.lit("warn")).otherwise(F.lit("pass")),
    )
    support_code_check = _check_row(
        "reach_support_code_distribution",
        support_codes,
        F.concat_ws(
            "; ",
            F.concat(F.lit("distinct REACH support codes="), F.col("support_code_count").cast("string")),
            F.concat(F.lit("support transactions="), F.col("support_transaction_count").cast("string")),
            F.concat(F.lit("support amount="), F.col("support_amt").cast("string")),
        ),
        "Cost-Sharing Support is identified from posted adjustment transactions with custom_transaction_code starting with REACH.",
        "Multiple REACH custom transaction codes are reviewable labels, not automatically duplicate support.",
    )

    profile_support = patient_profile.agg(
        F.round(
            F.sum(
                F.when(
                    F.col("no_secondary_or_tertiary_coverage_flag"),
                    zero_if_null("alignment_period_reach_cost_sharing_support_amt"),
                ).otherwise(F.lit(0.0))
            ),
            2,
        ).alias("patient_support_amt")
    )
    detail_support = support_service_detail.agg(F.round(F.sum(zero_if_null("cost_sharing_support_amt")), 2).alias("detail_support_amt"))
    support_tie = profile_support.crossJoin(detail_support).withColumn(
        "support_diff_amt",
        F.round(F.abs(zero_if_null("patient_support_amt") - zero_if_null("detail_support_amt")), 2),
    ).withColumn("check_status", F.when(F.col("support_diff_amt") <= F.lit(0.01), F.lit("pass")).otherwise(F.lit("fail")))
    support_tie_check = _check_row(
        "support_detail_ties_to_patient_rollup",
        support_tie,
        F.concat_ws(
            "; ",
            F.concat(F.lit("patient no-secondary support="), F.col("patient_support_amt").cast("string")),
            F.concat(F.lit("support detail amount="), F.col("detail_support_amt").cast("string")),
            F.concat(F.lit("difference="), F.col("support_diff_amt").cast("string")),
        ),
        "No-secondary support dollars in patient profile should tie to support service detail within one cent.",
        "If this fails, reviewers should pull service_trace and support_service_detail before using summary dollars.",
    )

    payment_cap = patient_profile.agg(
        F.round(F.max(zero_if_null("alignment_period_patient_copay_payment_amt") - zero_if_null("alignment_period_copay_due_amt")), 2).alias("max_payment_over_due_amt")
    ).withColumn("check_status", F.when(F.col("max_payment_over_due_amt") <= F.lit(0.01), F.lit("pass")).otherwise(F.lit("fail")))
    payment_cap_check = _check_row(
        "patient_payment_capped_at_copay_due",
        payment_cap,
        F.concat(F.lit("max payment over due="), F.col("max_payment_over_due_amt").cast("string")),
        "Beneficiary-level applied patient payment cannot exceed beneficiary-level audit copay due.",
        "The patient_responsibility transform caps applied patient copay payments before residual responsibility is calculated.",
    )

    residual_floor = patient_profile.agg(F.round(F.min("alignment_period_copay_unresolved_amt"), 2).alias("min_residual_amt")).withColumn(
        "check_status",
        F.when(F.col("min_residual_amt") >= F.lit(0.0), F.lit("pass")).otherwise(F.lit("fail")),
    )
    residual_floor_check = _check_row(
        "residual_responsibility_is_nonnegative",
        residual_floor,
        F.concat(F.lit("minimum residual="), F.col("min_residual_amt").cast("string")),
        "Residual responsibility is floored at zero at beneficiary grain.",
        "Negative residuals indicate the beneficiary-grain floor was bypassed or a source column changed semantics.",
    )

    unsupported = patient_profile.agg(
        F.countDistinct(
            F.when(
                F.col("no_secondary_or_tertiary_coverage_flag")
                & (F.col("alignment_period_copay_due_without_secondary_amt") > 0)
                & ~F.col("copays_waived_flag"),
                F.col("hcmpi"),
            )
        ).alias("beneficiary_count"),
        F.round(
            F.sum(
                F.when(
                    F.col("no_secondary_or_tertiary_coverage_flag")
                    & (F.col("alignment_period_copay_due_without_secondary_amt") > 0)
                    & ~F.col("copays_waived_flag"),
                    zero_if_null("alignment_period_copay_due_without_secondary_amt"),
                ).otherwise(F.lit(0.0))
            ),
            2,
        ).alias("copay_due_amt"),
    ).withColumn("check_status", F.when(F.col("beneficiary_count") > 0, F.lit("warn")).otherwise(F.lit("pass")))
    unsupported_check = _check_row(
        "no_secondary_copay_due_without_support",
        unsupported,
        F.concat_ws(
            "; ",
            F.concat(F.lit("beneficiaries="), F.col("beneficiary_count").cast("string")),
            F.concat(F.lit("copay due="), F.col("copay_due_amt").cast("string")),
        ),
        "Beneficiaries with no secondary or tertiary coverage, copay due, and no identified support are audit exceptions for review.",
        "A warning here is an actionable audit finding, not necessarily a pipeline defect.",
    )

    return _union_by_name(
        (
            support_code_check,
            support_tie_check,
            payment_cap_check,
            residual_floor_check,
            unsupported_check,
        )
    )


@register_transform(
    "copay_waiver_service_trace",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_SERVICE_TRACE_VIEW,
    description="Transaction/service-level trace showing why copay support events were included, excluded, or left for review.",
)
def build_copay_waiver_service_trace(patient_profile: DataFrame, revenue_cycle_transactions: DataFrame) -> DataFrame:
    """Return transaction-level trace rows with patient-level audit context."""
    profile = _with_hcmpi_key(
        _select_required(patient_profile, _PATIENT_PROFILE_TRACE_COLUMNS, context="patient profile for copay service trace")
    ).dropDuplicates(["hcmpi"])
    transactions = _with_hcmpi_key(
        _select_required(
            revenue_cycle_transactions,
            _REVENUE_CYCLE_TRACE_COLUMNS,
            context="revenue-cycle transactions for copay service trace",
        )
    ).withColumn("service_key", F.coalesce(F.col("charge_id"), F.col("transaction_charge_id")))
    traced = transactions.join(profile, on="hcmpi", how="left")
    return (
        traced.withColumn(
            "trace_decision",
            F.when(
                F.col("cost_sharing_support_identified_flag") & F.col("service_in_reach_alignment_window_flag") & F.col("no_secondary_or_tertiary_coverage_flag"),
                F.lit("included_support_on_no_coverage_alignment_service"),
            )
            .when(
                F.col("cost_sharing_support_identified_flag") & ~F.col("service_in_reach_alignment_window_flag"),
                F.lit("excluded_support_outside_alignment_service_window"),
            )
            .when(
                F.col("cost_sharing_support_identified_flag") & ~F.col("no_secondary_or_tertiary_coverage_flag"),
                F.lit("review_support_beneficiary_has_secondary_or_tertiary_coverage"),
            )
            .when(F.col("revenue_cycle_event_class") == F.lit("patient_responsibility_transfer"), F.lit("patient_responsibility_context"))
            .when(F.col("revenue_cycle_event_class") == F.lit("patient_payment"), F.lit("patient_payment_context"))
            .otherwise(F.lit("other_context")),
        )
        .withColumn(
            "reach_support_code_flag",
            F.col("cost_sharing_support_identified_flag") & F.upper(F.trim(F.coalesce(F.col("custom_transaction_code"), F.lit("")))).rlike(r"^REACH"),
        )
        .select(
            "performance_year",
            "hcmpi",
            "beneficiary_identity",
            "MedicareBeneficiaryID",
            "service_key",
            "transaction_id",
            "claim_id",
            "charge_id",
            "transaction_charge_id",
            "revenue_cycle_event_class",
            "trace_decision",
            "service_in_reach_alignment_window_flag",
            "transaction_post_in_reach_alignment_window_flag",
            "no_secondary_or_tertiary_coverage_flag",
            "coverage_decision_source",
            "copays_waived_flag",
            "reach_support_code_flag",
            "custom_transaction_code",
            "transaction_post_dt",
            "service_from_dt",
            "service_to_dt",
            "part_b_service_nature",
            "procedure_cd",
            "procedure_modifiers",
            "transaction_type",
            "transaction_transfer_type",
            "transaction_transfer_intent",
            "transaction_reason",
            "transaction_method",
            "transaction_amt",
            "patient_responsibility_transfer_amt",
            "patient_payment_amt",
            "cost_sharing_support_amt",
            "cost_sharing_support_identified_flag",
            "alignment_period_copay_due_amt",
            "alignment_period_copay_due_without_secondary_amt",
            "alignment_period_reach_cost_sharing_support_amt",
            "alignment_period_patient_copay_payment_amt",
            "alignment_period_copay_unresolved_amt",
            "alignment_period_reach_support_custom_transaction_codes",
            "line_note",
        )
        .orderBy("hcmpi", "service_from_dt", "transaction_post_dt", "transaction_id")
    )


__all__ = [
    "build_copay_waiver_check_results",
    "build_copay_waiver_service_trace",
    "build_copay_waiver_stage_manifest",
    "build_copay_waiver_stage_metrics",
]
