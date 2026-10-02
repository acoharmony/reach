"""Spark transforms for warehouse-ready REACH BNMR reconciliation views."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._bnmr import (
    DELIVERY_GRAIN,
    RISK_ANNUAL_GRAIN,
    RISK_MONTHLY_GRAIN,
    apo_reconcilable_expr,
    benchmark_risk_score_expr,
    bnmr_net_expenditure_expr,
    bpcc_reconcilable_expr,
    cap_ceiling_expr,
    cap_floor_expr,
    capped_risk_score_expr,
    claim_type_label_expr,
    claim_type_part_expr,
    epcc_reconcilable_expr,
    normalized_risk_score_expr,
    payment_month_from_payment_date_expr,
    program_expr,
    service_month_date_expr,
    tcc_reconcilable_expr,
)
from reach._transforms._registry import register_transform


@register_transform("bnmr_claims_reconciliation", concern="bnmr", output_table="reach_bnmr_claims_reconciliation")
def build_bnmr_claims_reconciliation(bnmr_claims: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Add BNMR claim reconciliation fields to ``reach_bnmr_claims`` rows."""
    df = bnmr_claims
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    return (
        df.withColumn("net_expenditure", bnmr_net_expenditure_expr())
        .withColumn("claim_type_label", claim_type_label_expr())
        .withColumn("claim_type_part", claim_type_part_expr())
        .withColumn("srvc_month_date", service_month_date_expr())
        .withColumn("program", program_expr())
    )


def aggregate_plaru_payment_history(plaru_payment_history: DataFrame, *, aco_id: str) -> DataFrame:
    """Collapse PLARU payment history to ``(aco_id, pmt_mnth)`` grain."""
    return (
        plaru_payment_history.withColumn("pmt_mnth", payment_month_from_payment_date_expr())
        .withColumn("_bpcc", F.col("base_pcc_total").cast("double"))
        .withColumn("_epcc", F.col("enhanced_pcc_total").cast("double"))
        .withColumn("_apo", F.col("apo_total").cast("double"))
        .groupBy("pmt_mnth")
        .agg(F.sum("_bpcc").alias("plaru_bpcc"), F.sum("_epcc").alias("plaru_epcc"), F.sum("_apo").alias("plaru_apo"))
        .withColumn("aco_id", F.lit(aco_id))
        .withColumn("plaru_tcc", F.col("plaru_bpcc") + F.col("plaru_epcc") + F.col("plaru_apo"))
        .select("aco_id", "pmt_mnth", "plaru_bpcc", "plaru_epcc", "plaru_apo", "plaru_tcc")
    )


def aggregate_bnmr_cap(bnmr_cap: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Collapse BNMR cap rows to reconcilable monthly component totals."""
    df = bnmr_cap
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    return (
        df.withColumn("bpcc_reconcilable", bpcc_reconcilable_expr().cast("double"))
        .withColumn("epcc_reconcilable", epcc_reconcilable_expr().cast("double"))
        .withColumn("apo_reconcilable", apo_reconcilable_expr().cast("double"))
        .withColumn("tcc_reconcilable", tcc_reconcilable_expr().cast("double"))
        .groupBy("aco_id", "pmt_mnth")
        .agg(
            F.sum("bpcc_reconcilable").alias("bnmr_bpcc"),
            F.sum("epcc_reconcilable").alias("bnmr_epcc"),
            F.sum("apo_reconcilable").alias("bnmr_apo"),
            F.sum("tcc_reconcilable").alias("bnmr_tcc"),
        )
    )


@register_transform("bnmr_cap_reconciliation", concern="bnmr", output_table="reach_bnmr_cap_reconciliation")
def build_bnmr_cap_reconciliation(
    bnmr_cap: DataFrame,
    plaru_payment_history: DataFrame,
    *,
    aco_id: str,
    as_of_delivery_date: str | None = None,
) -> DataFrame:
    """Tie out BNMR capitation totals against PLARU payment history."""
    joined = aggregate_bnmr_cap(bnmr_cap, as_of_delivery_date=as_of_delivery_date).join(
        aggregate_plaru_payment_history(plaru_payment_history, aco_id=aco_id),
        on=["aco_id", "pmt_mnth"],
        how="full_outer",
    )
    for col in ("bnmr_bpcc", "bnmr_epcc", "bnmr_apo", "bnmr_tcc", "plaru_bpcc", "plaru_epcc", "plaru_apo", "plaru_tcc"):
        joined = joined.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
    return (
        joined.withColumn("bpcc_diff", F.abs(F.col("bnmr_bpcc") - F.col("plaru_bpcc")))
        .withColumn("epcc_diff", F.abs(F.col("bnmr_epcc") - F.col("plaru_epcc")))
        .withColumn("apo_diff", F.abs(F.col("bnmr_apo") - F.col("plaru_apo")))
        .withColumn("tcc_diff", F.abs(F.col("bnmr_tcc") - F.col("plaru_tcc")))
        .withColumn("tcc_internal_diff", F.abs(F.col("bnmr_tcc") - (F.col("bnmr_bpcc") + F.col("bnmr_epcc") + F.col("bnmr_apo"))))
    )


@register_transform("bnmr_risk_count_reconciliation", concern="bnmr", output_table="reach_bnmr_risk_count_reconciliation")
def build_bnmr_risk_count_reconciliation(bnmr_risk: DataFrame, eligibility: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Tie out BNMR monthly beneficiary counts against stamped eligibility."""
    df = bnmr_risk
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    bnmr_side = df.select(*RISK_MONTHLY_GRAIN, "bene_dcnt")
    recon = eligibility.groupBy(*RISK_MONTHLY_GRAIN).agg(F.countDistinct("member_id").alias("expected_bene_dcnt"))
    joined = bnmr_side.join(recon, on=list(RISK_MONTHLY_GRAIN), how="full_outer")
    return joined.fillna({"bene_dcnt": 0, "expected_bene_dcnt": 0}).withColumn("bene_dcnt_diff", F.abs(F.col("bene_dcnt") - F.col("expected_bene_dcnt")))


@register_transform("bnmr_risk_annual_count_reconciliation", concern="bnmr", output_table="reach_bnmr_risk_annual_count_reconciliation")
def build_bnmr_risk_annual_count_reconciliation(bnmr_risk: DataFrame, eligibility: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Tie out BNMR annual beneficiary counts against stamped eligibility."""
    df = bnmr_risk
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    bnmr_side = df.select(*RISK_ANNUAL_GRAIN, "bene_dcnt_annual").dropDuplicates(list(RISK_ANNUAL_GRAIN))
    recon = eligibility.groupBy(*RISK_ANNUAL_GRAIN).agg(F.countDistinct("member_id").alias("expected_bene_dcnt_annual"))
    joined = bnmr_side.join(recon, on=list(RISK_ANNUAL_GRAIN), how="full_outer")
    return joined.fillna({"bene_dcnt_annual": 0, "expected_bene_dcnt_annual": 0}).withColumn("bene_dcnt_annual_diff", F.abs(F.col("bene_dcnt_annual") - F.col("expected_bene_dcnt_annual")))


@register_transform("bnmr_risk_score_reconciliation", concern="bnmr", output_table="reach_bnmr_risk_score_reconciliation")
def build_bnmr_risk_score_reconciliation(bnmr_risk: DataFrame, scored_benes: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Tie out BNMR raw risk scores against scored beneficiary averages."""
    df = bnmr_risk
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    bnmr_side = df.select(*RISK_MONTHLY_GRAIN, "raw_risk_score")
    recon = scored_benes.groupBy(*RISK_MONTHLY_GRAIN).agg(F.avg("risk_score").alias("expected_raw_risk_score"), F.countDistinct("member_id").alias("expected_bene_dcnt"))
    return bnmr_side.join(recon, on=list(RISK_MONTHLY_GRAIN), how="full_outer").withColumn("raw_risk_score_diff", F.abs(F.col("raw_risk_score") - F.col("expected_raw_risk_score")))


@register_transform("bnmr_risk_normalization_reconciliation", concern="bnmr", output_table="reach_bnmr_risk_normalization_reconciliation")
def build_bnmr_risk_normalization_reconciliation(riskscore_sheet: DataFrame, *, as_of_delivery_date: str | None = None) -> DataFrame:
    """Verify BNMR normalized/capped/benchmark risk-score arithmetic."""
    df = riskscore_sheet
    if as_of_delivery_date is not None and "file_date" in df.columns:
        df = df.where(F.to_date("file_date") <= F.to_date(F.lit(as_of_delivery_date)))
    numeric = ["raw_risk_score", "normalization_factor", "ry_normalized_risk_score", "cif", "normalized_risk_score_claims_py", "capped_risk_score_claims_py", "benchmark_risk_score_claims_py"]
    prepped = df.dropDuplicates(list(DELIVERY_GRAIN))
    for col in numeric:
        prepped = prepped.withColumn(col, F.col(col).cast("double"))
    staged = (
        prepped.withColumn("expected_normalized_risk_score", normalized_risk_score_expr())
        .withColumn("expected_cap_floor", cap_floor_expr())
        .withColumn("expected_cap_ceiling", cap_ceiling_expr())
        .withColumn("expected_capped_risk_score", capped_risk_score_expr())
        .withColumn("expected_benchmark_risk_score", benchmark_risk_score_expr())
    )
    return (
        staged.withColumn("normalized_risk_score_diff", F.abs(F.col("expected_normalized_risk_score") - F.col("normalized_risk_score_claims_py")))
        .withColumn("capped_risk_score_diff", F.abs(F.col("expected_capped_risk_score") - F.col("capped_risk_score_claims_py")))
        .withColumn("benchmark_risk_score_diff", F.abs(F.col("expected_benchmark_risk_score") - F.col("benchmark_risk_score_claims_py")))
    )
