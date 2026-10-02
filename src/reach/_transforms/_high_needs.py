"""Spark transform for REACH High-Needs eligibility from warehouse-ready inputs."""

from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, Window, functions as F

from reach._expressions._high_needs import (
    CRITERION_C_MIN_UNPLANNED_ADMITS,
    criteria_any_met_expr,
    criterion_b_met_expr,
    criterion_c_score_band_expr,
    criterion_e_applicable,
    table_c_window,
    table_d_window,
    check_dates_for_py,
)
from reach._transforms._high_needs_claims import (
    build_criterion_a_flags,
    build_criterion_a_qualifying_claims,
    build_criterion_d_flags,
    build_criterion_d_qualifying_claims,
    build_criterion_e_flags,
    build_snf_home_health_days,
    count_unplanned_admissions_in_window,
)
from reach._transforms._registry import register_transform

DEFAULT_FIRST_PY = 2023


def default_performance_years(today: date | None = None) -> tuple[int, ...]:
    """Return PY2023 through the current calendar year."""
    today = today or date.today()
    return tuple(range(DEFAULT_FIRST_PY, today.year + 1))


def build_criterion_b_flags(scores: DataFrame) -> DataFrame:
    """One row per beneficiary with max risk score and criterion B decision."""
    window = Window.partitionBy("mbi").orderBy(F.desc("total_risk_score"), F.asc("model_version"))
    ranked = scores.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1)
    return (
        ranked.select("mbi", "cohort", F.col("total_risk_score").alias("max_risk_score"), F.col("model_version").alias("max_risk_score_model"))
        .withColumn("criterion_b_met", criterion_b_met_expr())
    )


def build_criterion_c_flags(scores: DataFrame, admissions: DataFrame) -> DataFrame:
    """One row per beneficiary with moderate-risk/admission-count criterion C decision."""
    max_scores = build_criterion_b_flags(scores).select("mbi", "cohort", "max_risk_score")
    return (
        max_scores.join(admissions, on="mbi", how="left")
        .withColumn("unplanned_admission_count", F.coalesce(F.col("unplanned_admission_count"), F.lit(0)))
        .withColumn("criterion_c_score_band_met", criterion_c_score_band_expr())
        .withColumn("criterion_c_admission_count_met", F.col("unplanned_admission_count") >= F.lit(CRITERION_C_MIN_UNPLANNED_ADMITS))
        .withColumn("criterion_c_met", F.col("criterion_c_score_band_met") & F.col("criterion_c_admission_count_met"))
    )


def _with_check_date(df: DataFrame, check_date: date) -> DataFrame:
    return df.withColumn("check_date", F.lit(check_date))


def _join_criteria(frames: list[DataFrame]) -> DataFrame:
    joined = reduce(lambda left, right: left.join(right, on=["mbi", "check_date"], how="full_outer"), frames)
    for letter in ("a", "b", "c", "d", "e"):
        col = f"criterion_{letter}_met"
        if col in joined.columns:
            joined = joined.withColumn(col, F.coalesce(F.col(col), F.lit(False)))
        else:
            joined = joined.withColumn(col, F.lit(False))
    return joined.withColumn("criteria_any_met", criteria_any_met_expr())


def _apply_sticky_alignment(df: DataFrame) -> DataFrame:
    within_py = Window.partitionBy("mbi", "performance_year").orderBy("check_date").rowsBetween(Window.unboundedPreceding, 0)
    before_current = Window.partitionBy("mbi", "performance_year").orderBy("check_date").rowsBetween(Window.unboundedPreceding, -1)
    across_pys = Window.partitionBy("mbi").orderBy("performance_year", "check_date").rowsBetween(Window.unboundedPreceding, 0)
    by_mbi_py = Window.partitionBy("mbi", "performance_year")
    by_mbi = Window.partitionBy("mbi")
    met_int = F.col("criteria_any_met").cast("int")
    return (
        df.withColumn("eligible_as_of_check_date", F.max(met_int).over(within_py) > 0)
        .withColumn("previously_eligible", F.coalesce(F.max(met_int).over(before_current) > 0, F.lit(False)))
        .withColumn("first_eligible_check_date", F.min(F.when(F.col("criteria_any_met"), F.col("check_date"))).over(by_mbi_py))
        .withColumn("eligible_sticky_across_pys", F.max(met_int).over(across_pys) > 0)
        .withColumn("first_ever_eligible_py", F.min(F.when(F.col("criteria_any_met"), F.col("performance_year"))).over(by_mbi))
        .withColumn("first_ever_eligible_check_date", F.min(F.when(F.col("criteria_any_met"), F.col("check_date"))).over(by_mbi))
    )


def _evaluate_one_check(
    *,
    medical_claim: DataFrame,
    mobility_codes: DataFrame,
    frailty_codes: DataFrame,
    scores: DataFrame,
    performance_year: int,
    check_date: date,
) -> DataFrame:
    table_c = table_c_window(performance_year, check_date)
    table_d = table_d_window(performance_year, check_date)
    scores_this_check = scores.where((F.col("performance_year") == performance_year) & (F.to_date("check_date") == F.lit(check_date)))
    a = _with_check_date(build_criterion_a_flags(build_criterion_a_qualifying_claims(medical_claim, mobility_codes, window=table_c)).select("mbi", "criterion_a_met"), check_date)
    admissions = count_unplanned_admissions_in_window(medical_claim, window=table_c)
    b = _with_check_date(build_criterion_b_flags(scores_this_check).select("mbi", "criterion_b_met"), check_date)
    c = _with_check_date(build_criterion_c_flags(scores_this_check, admissions).select("mbi", "criterion_c_met"), check_date)
    d = _with_check_date(build_criterion_d_flags(build_criterion_d_qualifying_claims(medical_claim, frailty_codes, window=table_d)).select("mbi", "criterion_d_met"), check_date)
    if criterion_e_applicable(performance_year):
        e_days = build_snf_home_health_days(medical_claim, window=table_c)
        e = _with_check_date(build_criterion_e_flags(e_days, performance_year=performance_year).select("mbi", "criterion_e_met"), check_date)
    else:
        e = b.select("mbi").withColumn("criterion_e_met", F.lit(False)).withColumn("check_date", F.lit(check_date))
    return _join_criteria([a, b, c, d, e]).withColumn("performance_year", F.lit(performance_year))


@register_transform(
    "high_needs_eligibility",
    concern="high_needs",
    output_table="reach_high_needs_eligibility",
    description="High-Needs criteria A-E and sticky eligibility across PY check dates.",
)
def build_high_needs_eligibility(
    medical_claim: DataFrame,
    mobility_codes: DataFrame,
    frailty_codes: DataFrame,
    hcc_risk_scores: DataFrame,
    *,
    performance_years: tuple[int, ...] | None = None,
) -> DataFrame:
    """Build one High-Needs eligibility table from warehouse-ready Spark frames."""
    years = performance_years or default_performance_years()
    per_check = [
        _evaluate_one_check(
            medical_claim=medical_claim,
            mobility_codes=mobility_codes,
            frailty_codes=frailty_codes,
            scores=hcc_risk_scores,
            performance_year=py,
            check_date=check,
        )
        for py in years
        for check in check_dates_for_py(py)
    ]
    all_checks = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), per_check)
    return _apply_sticky_alignment(all_checks).select(
        "mbi",
        "performance_year",
        "check_date",
        "criterion_a_met",
        "criterion_b_met",
        "criterion_c_met",
        "criterion_d_met",
        "criterion_e_met",
        "criteria_any_met",
        "previously_eligible",
        "eligible_as_of_check_date",
        "first_eligible_check_date",
        "eligible_sticky_across_pys",
        "first_ever_eligible_py",
        "first_ever_eligible_check_date",
    )
