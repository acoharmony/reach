"""Patient-level copay responsibility, payment, and support amounts for REACH audit."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import safe_divide, zero_if_null
from reach._expressions._copay_waiver import (
    accepted_charge_expr,
    copay_due_expr,
    copay_zeroed_or_not_collected_amount_expr,
    copay_zeroed_or_not_collected_expr,
)
from reach._transforms._copay_waiver._common import _py_end, _py_start, _require_columns, _select_required, _service_in_alignment_window_expr, _with_alignment_window, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW
from reach._transforms._registry import register_transform


_CHARGE_COLUMNS = (
    "hcmpi",
    "service_dt",
    "data_source",
    "claim_status",
    "charge_status",
    "copay_amt",
    "copay_balance_amt",
    "adjusted_amt",
    "owed_amt",
    "charge_id",
    "claim_id",
)

_ATHENA_ACTIVITY_COLUMNS = (
    "hcmpi",
    "athena_activity_charge_count",
    "athena_activity_claim_count",
    "athena_copay_due_amt",
    "athena_freecare_adjustment_amt",
    "athena_bad_debt_adjustment_amt",
    "athena_total_adjustment_amt",
    "athena_patient_payment_amt",
    "athena_writeoff_amt",
    "athena_copay_zeroed_amt",
    "athena_adjustment_reason_ids",
)

_REVENUE_CYCLE_COLUMNS = (
    "hcmpi",
    "transaction_id",
    "charge_id",
    "transaction_charge_id",
    "claim_id",
    "service_in_reach_alignment_window_flag",
    "revenue_cycle_event_class",
    "cost_sharing_support_identified_flag",
    "custom_transaction_code",
    "patient_responsibility_transfer_amt",
    "patient_payment_amt",
    "cost_sharing_support_amt",
)

_NUMERIC_OUTPUT_COLUMNS = (
    "performance_year_charge_count",
    "performance_year_claim_count",
    "alignment_period_charge_count",
    "alignment_period_claim_count",
    "alignment_period_charge_copay_due_charge_count",
    "alignment_period_charge_copay_due_claim_count",
    "alignment_period_charge_copay_due_amt",
    "alignment_period_charge_proxy_copay_zeroed_amt",
    "alignment_period_charge_proxy_copay_zeroed_charge_count",
    "alignment_period_charge_proxy_copay_residual_amt",
    "pre_alignment_charge_copay_due_amt",
    "pre_alignment_charge_proxy_copay_zeroed_amt",
    "athena_activity_charge_count",
    "athena_activity_claim_count",
    "athena_copay_due_amt",
    "athena_freecare_adjustment_amt",
    "athena_bad_debt_adjustment_amt",
    "athena_total_adjustment_amt",
    "athena_patient_payment_amt",
    "athena_writeoff_amt",
    "athena_copay_zeroed_amt",
    "posted_rev_cycle_relevant_transaction_count",
    "posted_rev_cycle_patient_responsibility_transaction_count",
    "posted_rev_cycle_patient_responsibility_charge_count",
    "posted_rev_cycle_patient_responsibility_claim_count",
    "posted_rev_cycle_reach_cost_sharing_support_transaction_count",
    "posted_rev_cycle_reach_cost_sharing_support_claim_count",
    "posted_rev_cycle_patient_responsibility_transfer_amt",
    "posted_rev_cycle_patient_payment_amt",
    "posted_rev_cycle_reach_cost_sharing_support_amt",
    "alignment_period_rev_cycle_relevant_transaction_count",
    "alignment_period_patient_responsibility_transaction_count",
    "alignment_period_patient_responsibility_charge_count",
    "alignment_period_patient_responsibility_claim_count",
    "alignment_period_reach_cost_sharing_support_transaction_count",
    "alignment_period_reach_cost_sharing_support_claim_count",
    "alignment_period_patient_responsibility_transfer_amt",
    "alignment_period_patient_payment_amt",
    "alignment_period_reach_cost_sharing_support_amt",
    "alignment_period_patient_payment_responsibility_service_count",
    "alignment_period_patient_payment_on_responsibility_service_amt",
    "pre_alignment_patient_responsibility_transfer_amt",
    "pre_alignment_patient_payment_amt",
    "pre_alignment_reach_cost_sharing_support_amt",
    "pre_alignment_patient_payment_responsibility_service_count",
    "pre_alignment_patient_payment_on_responsibility_service_amt",
)

_STRING_OUTPUT_COLUMNS = (
    "athena_adjustment_reason_ids",
    "posted_rev_cycle_reach_support_custom_transaction_codes",
    "alignment_period_reach_support_custom_transaction_codes",
    "pre_alignment_reach_support_custom_transaction_codes",
)


def _sum_when(condition, value_col: str):
    return F.sum(F.when(condition, zero_if_null(value_col).cast("double")).otherwise(F.lit(0.0)))


def _count_distinct_when(condition, value_col: str):
    return F.countDistinct(F.when(condition, F.col(value_col)))


@register_transform(
    "copay_waiver_patient_responsibility",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_PATIENT_RESPONSIBILITY_VIEW,
    description="Patient-level alignment-period copay responsibility, payment, and Cost-Sharing Support amounts.",
)
def build_copay_waiver_patient_responsibility(
    aligned_population: DataFrame,
    charges: DataFrame,
    *,
    performance_year: int,
    athena_copay_activity: DataFrame,
    revenue_cycle_transactions: DataFrame,
) -> DataFrame:
    """Return one row per aligned patient with service-date-aware responsibility and support facts."""
    aligned = (
        _with_alignment_window(_with_hcmpi_key(aligned_population), performance_year=performance_year)
        .select("hcmpi", "reach_alignment_start_dt", "reach_alignment_end_dt")
        .dropDuplicates(["hcmpi"])
    )
    charge_source = _with_hcmpi_key(_select_required(charges, _CHARGE_COLUMNS, context="financial.charge for copay patient responsibility"))
    use_charge_proxy = F.lower(F.trim(F.coalesce(F.col("data_source").cast("string"), F.lit("")))) != F.lit("mg-athena")
    charge_rows = (
        aligned.join(charge_source, on="hcmpi", how="inner")
        .withColumn("_service_dt", F.to_date("service_dt"))
        .where((F.col("_service_dt") >= _py_start(performance_year)) & (F.col("_service_dt") <= _py_end(performance_year)))
        .where(accepted_charge_expr())
        .withColumn("service_in_reach_alignment_window_flag", _service_in_alignment_window_expr("_service_dt"))
        .withColumn("copay_due_flag", copay_due_expr())
        .withColumn("copay_zeroed_proxy_flag", use_charge_proxy & copay_zeroed_or_not_collected_expr())
        .withColumn("copay_zeroed_proxy_amt", F.when(F.col("copay_zeroed_proxy_flag"), copay_zeroed_or_not_collected_amount_expr()).otherwise(F.lit(0.0)))
        .withColumn("_charge_proxy_copay_residual_amt", F.greatest(zero_if_null("copay_amt").cast("double") - F.col("copay_zeroed_proxy_amt"), F.lit(0.0)))
    )
    in_window = F.col("service_in_reach_alignment_window_flag")
    charge_responsibility = charge_rows.groupBy("hcmpi").agg(
        F.countDistinct("charge_id").alias("performance_year_charge_count"),
        F.countDistinct("claim_id").alias("performance_year_claim_count"),
        _count_distinct_when(in_window, "charge_id").alias("alignment_period_charge_count"),
        _count_distinct_when(in_window, "claim_id").alias("alignment_period_claim_count"),
        _count_distinct_when(in_window & F.col("copay_due_flag"), "charge_id").alias("alignment_period_charge_copay_due_charge_count"),
        _count_distinct_when(in_window & F.col("copay_due_flag"), "claim_id").alias("alignment_period_charge_copay_due_claim_count"),
        _sum_when(in_window & F.col("copay_due_flag"), "copay_amt").alias("alignment_period_charge_copay_due_amt"),
        _sum_when(in_window & F.col("copay_zeroed_proxy_flag"), "copay_zeroed_proxy_amt").alias("alignment_period_charge_proxy_copay_zeroed_amt"),
        _count_distinct_when(in_window & F.col("copay_zeroed_proxy_flag"), "charge_id").alias("alignment_period_charge_proxy_copay_zeroed_charge_count"),
        _sum_when(in_window, "_charge_proxy_copay_residual_amt").alias("alignment_period_charge_proxy_copay_residual_amt"),
        _sum_when(~in_window & F.col("copay_due_flag"), "copay_amt").alias("pre_alignment_charge_copay_due_amt"),
        _sum_when(~in_window & F.col("copay_zeroed_proxy_flag"), "copay_zeroed_proxy_amt").alias("pre_alignment_charge_proxy_copay_zeroed_amt"),
    )

    athena_activity = _with_hcmpi_key(
        _select_required(athena_copay_activity, _ATHENA_ACTIVITY_COLUMNS, context="Athena activity rollup for copay patient responsibility")
    ).drop("performance_year")
    revenue_cycle_rows = (
        _with_hcmpi_key(_select_required(revenue_cycle_transactions, _REVENUE_CYCLE_COLUMNS, context="revenue-cycle transactions for copay patient responsibility"))
        .withColumn("_service_key", F.coalesce(F.col("charge_id"), F.col("transaction_charge_id")))
    )

    result = aligned.join(charge_responsibility, on="hcmpi", how="left").join(athena_activity, on="hcmpi", how="left")
    rc_in_window = F.col("service_in_reach_alignment_window_flag")
    responsibility_event = F.col("revenue_cycle_event_class") == F.lit("patient_responsibility_transfer")
    revenue_cycle = revenue_cycle_rows.groupBy("hcmpi").agg(
        F.countDistinct("transaction_id").alias("posted_rev_cycle_relevant_transaction_count"),
        _count_distinct_when(responsibility_event, "transaction_id").alias("posted_rev_cycle_patient_responsibility_transaction_count"),
        _count_distinct_when(responsibility_event, "charge_id").alias("posted_rev_cycle_patient_responsibility_charge_count"),
        _count_distinct_when(responsibility_event, "claim_id").alias("posted_rev_cycle_patient_responsibility_claim_count"),
        _count_distinct_when(F.col("cost_sharing_support_identified_flag"), "transaction_id").alias("posted_rev_cycle_reach_cost_sharing_support_transaction_count"),
        _count_distinct_when(F.col("cost_sharing_support_identified_flag"), "claim_id").alias("posted_rev_cycle_reach_cost_sharing_support_claim_count"),
        F.sum("patient_responsibility_transfer_amt").alias("posted_rev_cycle_patient_responsibility_transfer_amt"),
        F.sum("patient_payment_amt").alias("posted_rev_cycle_patient_payment_amt"),
        F.sum("cost_sharing_support_amt").alias("posted_rev_cycle_reach_cost_sharing_support_amt"),
        F.countDistinct(F.when(rc_in_window, F.col("transaction_id"))).alias("alignment_period_rev_cycle_relevant_transaction_count"),
        _count_distinct_when(rc_in_window & responsibility_event, "transaction_id").alias("alignment_period_patient_responsibility_transaction_count"),
        _count_distinct_when(rc_in_window & responsibility_event, "charge_id").alias("alignment_period_patient_responsibility_charge_count"),
        _count_distinct_when(rc_in_window & responsibility_event, "claim_id").alias("alignment_period_patient_responsibility_claim_count"),
        _count_distinct_when(rc_in_window & F.col("cost_sharing_support_identified_flag"), "transaction_id").alias("alignment_period_reach_cost_sharing_support_transaction_count"),
        _count_distinct_when(rc_in_window & F.col("cost_sharing_support_identified_flag"), "claim_id").alias("alignment_period_reach_cost_sharing_support_claim_count"),
        _sum_when(rc_in_window, "patient_responsibility_transfer_amt").alias("alignment_period_patient_responsibility_transfer_amt"),
        _sum_when(rc_in_window, "patient_payment_amt").alias("alignment_period_patient_payment_amt"),
        _sum_when(rc_in_window, "cost_sharing_support_amt").alias("alignment_period_reach_cost_sharing_support_amt"),
        _sum_when(~rc_in_window, "patient_responsibility_transfer_amt").alias("pre_alignment_patient_responsibility_transfer_amt"),
        _sum_when(~rc_in_window, "patient_payment_amt").alias("pre_alignment_patient_payment_amt"),
        _sum_when(~rc_in_window, "cost_sharing_support_amt").alias("pre_alignment_reach_cost_sharing_support_amt"),
        F.array_join(F.array_sort(F.collect_set(F.when(F.col("cost_sharing_support_identified_flag"), F.col("custom_transaction_code")))), "; ").alias(
            "posted_rev_cycle_reach_support_custom_transaction_codes"
        ),
        F.array_join(F.array_sort(F.collect_set(F.when(rc_in_window & F.col("cost_sharing_support_identified_flag"), F.col("custom_transaction_code")))), "; ").alias(
            "alignment_period_reach_support_custom_transaction_codes"
        ),
        F.array_join(F.array_sort(F.collect_set(F.when(~rc_in_window & F.col("cost_sharing_support_identified_flag"), F.col("custom_transaction_code")))), "; ").alias(
            "pre_alignment_reach_support_custom_transaction_codes"
        ),
    )
    result = result.join(revenue_cycle, on="hcmpi", how="left")
    service_payments = (
        revenue_cycle_rows.where(F.col("_service_key").isNotNull())
        .groupBy("hcmpi", "_service_key")
        .agg(
            F.max(F.col("service_in_reach_alignment_window_flag").cast("int")).alias("_service_in_window"),
            F.max(responsibility_event.cast("int")).alias("_has_patient_responsibility_transfer"),
            F.sum("patient_payment_amt").alias("_patient_payment_amt"),
        )
        .where(F.col("_has_patient_responsibility_transfer") > 0)
        .groupBy("hcmpi")
        .agg(
            F.countDistinct(F.when(F.col("_service_in_window") > 0, F.col("_service_key"))).alias("alignment_period_patient_payment_responsibility_service_count"),
            F.sum(F.when(F.col("_service_in_window") > 0, F.col("_patient_payment_amt")).otherwise(F.lit(0.0))).alias(
                "alignment_period_patient_payment_on_responsibility_service_amt"
            ),
            F.countDistinct(F.when(F.col("_service_in_window") <= 0, F.col("_service_key"))).alias("pre_alignment_patient_payment_responsibility_service_count"),
            F.sum(F.when(F.col("_service_in_window") <= 0, F.col("_patient_payment_amt")).otherwise(F.lit(0.0))).alias(
                "pre_alignment_patient_payment_on_responsibility_service_amt"
            ),
        )
    )
    result = result.join(service_payments, on="hcmpi", how="left")

    _require_columns(result, _NUMERIC_OUTPUT_COLUMNS, context="copay patient responsibility rollup")
    _require_columns(result, _STRING_OUTPUT_COLUMNS, context="copay patient responsibility rollup")
    result = result.fillna(0, subset=list(_NUMERIC_OUTPUT_COLUMNS))

    return (
        result.withColumn(
            "alignment_period_copay_due_amt",
            F.greatest(
                zero_if_null("alignment_period_charge_copay_due_amt").cast("double"),
                zero_if_null("alignment_period_patient_responsibility_transfer_amt").cast("double"),
            ),
        )
        .withColumn(
            "alignment_period_copay_due_charge_count",
            F.greatest(
                zero_if_null("alignment_period_charge_copay_due_charge_count").cast("long"),
                zero_if_null("alignment_period_patient_responsibility_charge_count").cast("long"),
            ),
        )
        .withColumn(
            "alignment_period_copay_due_claim_count",
            F.greatest(
                zero_if_null("alignment_period_charge_copay_due_claim_count").cast("long"),
                zero_if_null("alignment_period_patient_responsibility_claim_count").cast("long"),
            ),
        )
        .withColumn("alignment_period_cost_sharing_support_amt", zero_if_null("alignment_period_reach_cost_sharing_support_amt").cast("double"))
        .withColumn("alignment_period_copay_waived_amt", F.col("alignment_period_cost_sharing_support_amt"))
        .withColumn(
            "alignment_period_patient_copay_payment_amt",
            F.least(
                zero_if_null("alignment_period_copay_due_amt").cast("double"),
                zero_if_null("alignment_period_patient_payment_on_responsibility_service_amt").cast("double"),
            ),
        )
        .withColumn(
            "alignment_period_copay_unresolved_amt",
            F.greatest(
                zero_if_null("alignment_period_copay_due_amt").cast("double")
                - zero_if_null("alignment_period_cost_sharing_support_amt").cast("double")
                - zero_if_null("alignment_period_patient_copay_payment_amt").cast("double"),
                F.lit(0.0),
            ),
        )
        .withColumn("alignment_period_cost_sharing_support_identified_flag", F.col("alignment_period_cost_sharing_support_amt") > 0)
        .withColumn("alignment_period_patient_copay_payment_rate", safe_divide("alignment_period_patient_copay_payment_amt", "alignment_period_copay_due_amt"))
        .withColumn("performance_year", F.lit(int(performance_year)))
    )


__all__ = ["build_copay_waiver_patient_responsibility"]
