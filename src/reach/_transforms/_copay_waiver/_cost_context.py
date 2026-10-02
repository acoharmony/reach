"""Cost and utilization context enrichment for copay-waiver patients."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._transforms._clinical_oversight._metrics import CLINICAL_OVERSIGHT_UTILIZATION_SOURCE_COLUMNS
from reach._transforms._copay_waiver._common import _metric_amount, _select_required, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW
from reach._transforms._registry import register_transform


_COST_METRIC_COLUMNS = (
    "hcmpi",
    "Yearmo",
    "Metric",
    "Completed_Value",
    "Raw_Value",
    "Program",
    "Payer",
)

_UTILIZATION_COLUMNS = ("hcmpi", *CLINICAL_OVERSIGHT_UTILIZATION_SOURCE_COLUMNS)


@register_transform(
    "copay_waiver_patient_cost_context",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_PATIENT_COST_CONTEXT_VIEW,
    description="Copay-waiver patient profile enriched with clinical oversight TCOC, RAF, and utilization metrics.",
)
def build_copay_waiver_patient_cost_context(
    patient_profile: DataFrame,
    cost_metrics: DataFrame,
    utilization_metrics: DataFrame,
    *,
    performance_year: int,
) -> DataFrame:
    """Join patient-level profile to the clinical oversight cost, risk, and utilization context."""
    source = _with_hcmpi_key(
        _select_required(cost_metrics, _COST_METRIC_COLUMNS, context="allpayer cost metrics for copay patient context")
    ).withColumn("_yearmo_date", F.to_date(F.col("Yearmo").cast("string"), "yyyyMM"))
    latest_yearmo = source.where(F.col("Metric") == F.lit("IP_Paid")).agg(F.max("_yearmo_date").alias("_latest_yearmo"))
    source = (
        source.crossJoin(latest_yearmo)
        .where(F.col("_latest_yearmo").isNotNull())
        .where((F.col("_yearmo_date") >= F.add_months(F.col("_latest_yearmo"), -12)) & (F.col("_yearmo_date") <= F.col("_latest_yearmo")))
        .drop("_latest_yearmo")
    )
    costs = source.groupBy("hcmpi").agg(
        _metric_amount("IP_Paid", "Completed_Value").alias("IP_Paid_PMPM"),
        _metric_amount("OP_Paid", "Completed_Value").alias("OP_Paid_PMPM"),
        _metric_amount("PB_DME_Paid", "Completed_Value").alias("PB_DME_Paid_PMPM"),
        _metric_amount("SNF_Paid", "Completed_Value").alias("SNF_Paid_PMPM"),
        _metric_amount("HH_Paid", "Completed_Value").alias("HH_Paid_PMPM"),
        _metric_amount("HSP_Paid", "Completed_Value").alias("HSP_Paid_PMPM"),
        F.sum(F.when(F.col("Metric").rlike("_Paid$"), F.col("Completed_Value").cast("double")).otherwise(F.lit(0.0))).alias("Total_Paid_PMPM"),
        _metric_amount("IP_Paid", "Raw_Value").alias("IP_Paid_Total"),
        _metric_amount("OP_Paid", "Raw_Value").alias("OP_Paid_Total"),
        _metric_amount("PB_DME_Paid", "Raw_Value").alias("PB_DME_Paid_Total"),
        _metric_amount("SNF_Paid", "Raw_Value").alias("SNF_Paid_Total"),
        _metric_amount("HH_Paid", "Raw_Value").alias("HH_Paid_Total"),
        _metric_amount("HSP_Paid", "Raw_Value").alias("HSP_Paid_Total"),
        F.sum(F.when(F.col("Metric").rlike("_Paid$"), F.col("Raw_Value").cast("double")).otherwise(F.lit(0.0))).alias("Total_Paid_Dollars"),
        F.avg(F.when(F.col("Metric") == F.lit("Prosp_RS_v28_AllSources"), F.col("Completed_Value").cast("double"))).alias("Prosp_RAF_v28"),
        F.avg(F.when(F.col("Metric") == F.lit("Concur_RS_v28_AllSources"), F.col("Completed_Value").cast("double"))).alias("Concur_RAF_v28"),
        _metric_amount("MM", "Completed_Value").alias("Member_Months"),
        F.max("Program").alias("Program"),
        F.max("Payer").alias("Payer"),
    )
    utilization = (
        _with_hcmpi_key(_select_required(utilization_metrics, _UTILIZATION_COLUMNS, context="patient registry utilization metrics for copay patient context"))
        .dropDuplicates(["hcmpi"])
        .withColumn(
            "total_events_90days",
            F.coalesce(F.col("admits_90DAYS"), F.lit(0))
            + F.coalesce(F.col("SNF_episodes_90DAYS"), F.lit(0))
            + F.coalesce(F.col("ED_visit_90DAYS"), F.lit(0))
            + F.coalesce(F.col("obs_90DAYS"), F.lit(0)),
        )
        .withColumn(
            "total_events_365days",
            F.coalesce(F.col("admits_365DAYS"), F.lit(0))
            + F.coalesce(F.col("SNF_episodes_365DAYS"), F.lit(0))
            + F.coalesce(F.col("ED_visit_365DAYS"), F.lit(0))
            + F.coalesce(F.col("obs_365DAYS"), F.lit(0)),
        )
        .withColumn(
            "power_score",
            F.coalesce(F.col("admits_90DAYS"), F.lit(0)) * F.lit(12.0)
            + F.coalesce(F.col("admits_180DAYS"), F.lit(0)) * F.lit(8.0)
            + F.coalesce(F.col("admits_365DAYS"), F.lit(0)) * F.lit(4.0)
            + F.coalesce(F.col("admits_YTD"), F.lit(0)) * F.lit(2.0)
            + F.coalesce(F.col("SNF_episodes_90DAYS"), F.lit(0)) * F.lit(9.0)
            + F.coalesce(F.col("SNF_episodes_180DAYS"), F.lit(0)) * F.lit(6.0)
            + F.coalesce(F.col("SNF_episodes_365DAYS"), F.lit(0)) * F.lit(3.0)
            + F.coalesce(F.col("SNF_episodes_YTD"), F.lit(0)) * F.lit(1.5)
            + F.coalesce(F.col("ED_visit_90DAYS"), F.lit(0)) * F.lit(6.0)
            + F.coalesce(F.col("ED_visit_180DAYS"), F.lit(0)) * F.lit(4.0)
            + F.coalesce(F.col("ED_visit_365DAYS"), F.lit(0)) * F.lit(2.0)
            + F.coalesce(F.col("ED_visit_YTD"), F.lit(0)) * F.lit(1.0)
            + F.coalesce(F.col("obs_90DAYS"), F.lit(0)) * F.lit(3.0)
            + F.coalesce(F.col("obs_180DAYS"), F.lit(0)) * F.lit(2.0)
            + F.coalesce(F.col("obs_365DAYS"), F.lit(0)) * F.lit(1.0)
            + F.coalesce(F.col("obs_YTD"), F.lit(0)) * F.lit(0.5),
        )
    )
    return _with_hcmpi_key(patient_profile).join(costs, on="hcmpi", how="left").join(utilization, on="hcmpi", how="left")


__all__ = ["build_copay_waiver_patient_cost_context"]
