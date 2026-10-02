"""Spark transform for REACH High-Needs reconciliation."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window, functions as F

from reach._transforms._registry import register_transform


def _first_existing_column(df: DataFrame, names: tuple[str, ...], alias: str, data_type: str = "string") -> Column:
    for name in names:
        if name in df.columns:
            return F.col(name).cast(data_type).alias(alias)
    return F.lit(None).cast(data_type).alias(alias)


def _nullable_bool(column: str) -> Column:
    text = F.upper(F.trim(F.coalesce(F.col(column).cast("string"), F.lit(""))))
    return (
        F.when(text.isin("1", "T", "TRUE", "Y", "YES"), F.lit(True))
        .when(text.isin("0", "F", "FALSE", "N", "NO"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _bool_any(column: str, alias: str) -> Column:
    return F.max(F.coalesce(F.col(column), F.lit(False)).cast("int")).cast("boolean").alias(alias)


def _latest_high_needs_snapshot(eligibility: DataFrame, performance_year: int) -> DataFrame:
    source = eligibility.where(F.col("performance_year") == F.lit(int(performance_year)))
    latest_window = Window.partitionBy("mbi").orderBy(F.desc(F.to_date("check_date")))
    latest = (
        source.withColumn("_rn", F.row_number().over(latest_window))
        .where(F.col("_rn") == 1)
        .select(
            "mbi",
            F.to_date("check_date").alias("check_date"),
            "criterion_a_met",
            "criterion_b_met",
            "criterion_c_met",
            "criterion_d_met",
            "criterion_e_met",
            F.col("eligible_sticky_across_pys").alias("high_needs_eligible_sticky"),
            F.col("eligible_as_of_check_date").alias("high_needs_eligible_this_py"),
            F.col("first_ever_eligible_py").alias("first_eligible_py"),
            F.col("first_ever_eligible_check_date").alias("first_eligible_check_date"),
        )
    )
    ever = source.groupBy("mbi").agg(
        _bool_any("criterion_a_met", "criterion_a_met_ever"),
        _bool_any("criterion_b_met", "criterion_b_met_ever"),
        _bool_any("criterion_c_met", "criterion_c_met_ever"),
        _bool_any("criterion_d_met", "criterion_d_met_ever"),
        _bool_any("criterion_e_met", "criterion_e_met_ever"),
    )
    return latest.join(ever, on="mbi", how="left")


def _latest_bar_high_needs_flags(bar: DataFrame, performance_year: int) -> DataFrame:
    py_suffix = str(int(performance_year))[-2:]
    prepared = bar.select(
        _first_existing_column(bar, ("bene_mbi", "MedicareBeneficiaryID", "mbi", "member_id", "person_id"), "mbi"),
        _first_existing_column(bar, ("source_filename", "FileNM"), "_source_filename"),
        _first_existing_column(bar, ("file_date", "FileDate", "meta_file_dt"), "bar_file_date", "date"),
        _first_existing_column(bar, ("mobility_impairment_flag", "MobilityImpairmentFlag"), "_bar_mobility_impairment_flag"),
        _first_existing_column(bar, ("high_risk_flag", "HighRiskFlag"), "_bar_high_risk_flag"),
        _first_existing_column(bar, ("medium_risk_unplanned_flag", "MediumRiskUnplannedFlag"), "_bar_medium_risk_unplanned_flag"),
        _first_existing_column(bar, ("frailty_flag", "FrailtyFlag"), "_bar_frailty_flag"),
        _first_existing_column(bar, ("claims_based_flag", "ClaimsBasedFlag"), "_bar_claims_based_flag"),
    )
    algc = F.col("_source_filename").rlike(rf"\.ALGC{py_suffix}\.RP\.")
    algr = F.col("_source_filename").rlike(rf"\.ALGR{py_suffix}\.RP\.")
    ranked = (
        prepared.where(F.col("mbi").isNotNull())
        .withColumn("_file_priority", F.when(algr, F.lit(2)).when(algc, F.lit(1)).otherwise(F.lit(0)))
        .where(F.col("_file_priority") > 0)
        .withColumn("_latest_rank", F.dense_rank().over(Window.orderBy(F.desc("_file_priority"), F.desc("_source_filename"))))
        .where(F.col("_latest_rank") == 1)
    )
    return ranked.groupBy("mbi").agg(
        F.max("bar_file_date").alias("bar_file_date"),
        F.max(_nullable_bool("_bar_mobility_impairment_flag").cast("int")).cast("boolean").alias("bar_mobility_impairment_flag"),
        F.max(_nullable_bool("_bar_high_risk_flag").cast("int")).cast("boolean").alias("bar_high_risk_flag"),
        F.max(_nullable_bool("_bar_medium_risk_unplanned_flag").cast("int")).cast("boolean").alias("bar_medium_risk_unplanned_flag"),
        F.max(_nullable_bool("_bar_frailty_flag").cast("int")).cast("boolean").alias("bar_frailty_flag"),
        F.max(_nullable_bool("_bar_claims_based_flag").cast("int")).cast("boolean").alias("bar_claims_based_flag"),
    )


def _pbvar_a2_flags(pbvar: DataFrame) -> DataFrame:
    prepared = pbvar.select(
        _first_existing_column(pbvar, ("bene_mbi", "BeneficiaryMBIID", "MBI_ID", "mbi", "member_id", "person_id"), "mbi"),
        _first_existing_column(pbvar, ("file_date", "FileDate", "meta_file_dt"), "pbvar_a2_file_date", "date"),
        _first_existing_column(pbvar, ("sva_response_code_list", "ResponseCodeList", "response_codes"), "pbvar_response_codes"),
    )
    response = F.upper(F.trim(F.coalesce(F.col("pbvar_response_codes"), F.lit(""))))
    return (
        prepared.where(F.col("mbi").isNotNull())
        .where(response.rlike(r"(^|[^A-Z0-9])A2([^A-Z0-9]|$)"))
        .groupBy("mbi")
        .agg(
            F.max("pbvar_a2_file_date").alias("pbvar_a2_file_date"),
            F.concat_ws("; ", F.array_sort(F.collect_set("pbvar_response_codes"))).alias("pbvar_response_codes"),
            F.max(F.lit(1)).cast("boolean").alias("pbvar_a2_present"),
        )
    )


def _add_missing_reconciliation_columns(df: DataFrame, *, include_bar: bool, include_pbvar: bool) -> DataFrame:
    if not include_bar:
        df = (
            df.withColumn("bar_file_date", F.lit(None).cast("date"))
            .withColumn("bar_mobility_impairment_flag", F.lit(None).cast("boolean"))
            .withColumn("bar_high_risk_flag", F.lit(None).cast("boolean"))
            .withColumn("bar_medium_risk_unplanned_flag", F.lit(None).cast("boolean"))
            .withColumn("bar_frailty_flag", F.lit(None).cast("boolean"))
            .withColumn("bar_claims_based_flag", F.lit(None).cast("boolean"))
        )
    if not include_pbvar:
        df = (
            df.withColumn("pbvar_a2_file_date", F.lit(None).cast("date"))
            .withColumn("pbvar_a2_present", F.lit(False))
            .withColumn("pbvar_response_codes", F.lit(None).cast("string"))
        )
    return df


@register_transform(
    "high_needs_reconciliation",
    concern="high_needs",
    output_table="reach_high_needs_reconciliation",
    description="Tie out REACH High-Needs eligibility against latest BAR flags and optional PBVAR A2 signals.",
)
def build_high_needs_reconciliation(
    eligibility: DataFrame,
    *,
    performance_year: int,
    bar: DataFrame | None = None,
    pbvar: DataFrame | None = None,
) -> DataFrame:
    """Build one High-Needs reconciliation table from warehouse-ready Spark frames."""
    result = _latest_high_needs_snapshot(eligibility, performance_year).withColumn("performance_year", F.lit(int(performance_year)))
    if bar is not None:
        result = result.join(_latest_bar_high_needs_flags(bar, performance_year), on="mbi", how="left")
    if pbvar is not None:
        result = result.join(_pbvar_a2_flags(pbvar), on="mbi", how="left")
    result = _add_missing_reconciliation_columns(result, include_bar=bar is not None, include_pbvar=pbvar is not None)
    return result.withColumn("pbvar_a2_present", F.coalesce(F.col("pbvar_a2_present"), F.lit(False))).select(
        "mbi",
        "performance_year",
        "check_date",
        "criterion_a_met_ever",
        "criterion_b_met_ever",
        "criterion_c_met_ever",
        "criterion_d_met_ever",
        "criterion_e_met_ever",
        "criterion_a_met",
        "criterion_b_met",
        "criterion_c_met",
        "criterion_d_met",
        "criterion_e_met",
        "high_needs_eligible_sticky",
        "high_needs_eligible_this_py",
        "first_eligible_py",
        "first_eligible_check_date",
        "bar_file_date",
        "bar_mobility_impairment_flag",
        "bar_high_risk_flag",
        "bar_medium_risk_unplanned_flag",
        "bar_frailty_flag",
        "bar_claims_based_flag",
        "pbvar_a2_file_date",
        "pbvar_a2_present",
        "pbvar_response_codes",
    )
