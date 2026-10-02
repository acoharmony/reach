"""Patient-level secondary and tertiary coverage status for REACH copay audit."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

from reach._expressions._common import blank_to_null, trueish
from reach._expressions._copay_waiver import (
    accepted_charge_expr,
    claim_time_secondary_coverage_expr,
    claim_time_tertiary_coverage_expr,
    pseudo_coverage_artifact_expr,
    real_coverage_identifier_present_expr,
)
from reach._transforms._copay_waiver._common import _py_end, _py_start, _require_columns, _select_required, _with_hcmpi_key
from reach._transforms._copay_waiver._views import COPAY_WAIVER_COVERAGE_STATUS_VIEW, DEFAULT_ALIGNED_COLUMNS
from reach._transforms._registry import register_transform


_COVERAGE_TEXT_COLUMNS = (
    "insurance_nm",
    "insurance_superpackage_name",
    "insurance_reporting_category",
    "ins_reporting_group",
    "eligibility_class",
)

_PATIENT_INSURANCE_COLUMNS = (
    "hcmpi",
    "insurance_priority",
    "active",
    "effective_dt",
    "termination_dt",
    "insurance_package_id",
    *_COVERAGE_TEXT_COLUMNS,
)

_CHARGE_COLUMNS = (
    "hcmpi",
    "service_dt",
    "claim_status",
    "charge_status",
    "sec_insurance_class",
    "sec_insurance_group",
    "ter_insurance_class",
    "ter_insurance_group",
)


def _priority_number(column: str = "insurance_priority") -> Column:
    raw = F.trim(F.coalesce(F.col(column).cast("string"), F.lit("")))
    return F.regexp_extract(raw, r"^0*([0-9]+)", 1)


def _coverage_overlaps_performance_year(frame: DataFrame, performance_year: int) -> Column:
    _require_columns(frame, ("effective_dt", "termination_dt"), context="financial.patient_insurance coverage dates")
    start = F.to_date("effective_dt")
    end = F.to_date("termination_dt")
    return (start.isNull() | (start <= _py_end(performance_year))) & (end.isNull() | (end >= _py_start(performance_year)))


def _active_coverage_row(frame: DataFrame, performance_year: int) -> Column:
    _require_columns(frame, ("active",), context="financial.patient_insurance active flag")
    return trueish("active") & _coverage_overlaps_performance_year(frame, performance_year)


def _collect_values(condition_column: str, source_column: str) -> Column:
    value = blank_to_null(source_column)
    return F.array_join(F.array_sort(F.collect_set(F.when(F.col(condition_column), value))), "; ")


@register_transform(
    "copay_waiver_coverage_status",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_COVERAGE_STATUS_VIEW,
    description="Patient-level secondary and tertiary coverage status from financial.patient_insurance priority rows.",
)
def build_copay_waiver_coverage_status(
    aligned_population: DataFrame,
    patient_insurance: DataFrame,
    charges: DataFrame,
    *,
    performance_year: int,
) -> DataFrame:
    """Return one row per aligned patient with coverage status from insurance priority 2/3."""
    aligned = _with_hcmpi_key(
        _select_required(aligned_population, DEFAULT_ALIGNED_COLUMNS, context="aligned population for copay coverage status")
    ).dropDuplicates(["hcmpi"])
    insurance_source = _select_required(patient_insurance, _PATIENT_INSURANCE_COLUMNS, context="financial.patient_insurance for copay coverage status")
    charge_source = _select_required(charges, _CHARGE_COLUMNS, context="financial.charge for copay coverage status")

    insurance_base = (
        _with_hcmpi_key(insurance_source)
        .withColumn("insurance_priority_number", _priority_number())
        .withColumn("coverage_active_in_performance_year_flag", _active_coverage_row(insurance_source, performance_year))
        .withColumn("coverage_is_self_pay_or_no_coverage_flag", pseudo_coverage_artifact_expr(*_COVERAGE_TEXT_COLUMNS))
        .withColumn("coverage_is_real_payer_flag", real_coverage_identifier_present_expr(*_COVERAGE_TEXT_COLUMNS))
        .withColumn(
            "secondary_priority_2_coverage_row",
            F.col("coverage_active_in_performance_year_flag")
            & (F.col("insurance_priority_number") == F.lit("2"))
            & F.col("coverage_is_real_payer_flag"),
        )
        .withColumn(
            "tertiary_priority_3_coverage_row",
            F.col("coverage_active_in_performance_year_flag")
            & (F.col("insurance_priority_number") == F.lit("3"))
            & F.col("coverage_is_real_payer_flag"),
        )
        .withColumn(
            "priority_3_self_pay_or_no_coverage_row",
            F.col("coverage_active_in_performance_year_flag")
            & (F.col("insurance_priority_number") == F.lit("3"))
            & F.col("coverage_is_self_pay_or_no_coverage_flag"),
        )
    )
    insurance_rollup = insurance_base.groupBy("hcmpi").agg(
        F.max(F.col("secondary_priority_2_coverage_row").cast("int")).cast("boolean").alias("has_patient_secondary_coverage"),
        F.max(F.col("tertiary_priority_3_coverage_row").cast("int")).cast("boolean").alias("has_patient_tertiary_coverage"),
        F.max(F.col("priority_3_self_pay_or_no_coverage_row").cast("int")).cast("boolean").alias("priority_3_self_pay_or_no_coverage_flag"),
        F.countDistinct(F.when(F.col("secondary_priority_2_coverage_row"), F.col("insurance_package_id").cast("string"))).alias(
            "patient_secondary_insurance_package_count"
        ),
        F.countDistinct(F.when(F.col("tertiary_priority_3_coverage_row"), F.col("insurance_package_id").cast("string"))).alias(
            "patient_tertiary_insurance_package_count"
        ),
        _collect_values("secondary_priority_2_coverage_row", "insurance_nm").alias("patient_secondary_insurance_names"),
        _collect_values("secondary_priority_2_coverage_row", "ins_reporting_group").alias("patient_secondary_insurance_groups"),
        _collect_values("tertiary_priority_3_coverage_row", "insurance_nm").alias("patient_tertiary_insurance_names"),
        _collect_values("tertiary_priority_3_coverage_row", "ins_reporting_group").alias("patient_tertiary_insurance_groups"),
        _collect_values("priority_3_self_pay_or_no_coverage_row", "insurance_nm").alias("priority_3_self_pay_or_no_coverage_names"),
    )

    charge_rows = (
        _with_hcmpi_key(charge_source)
        .where((F.to_date("service_dt") >= _py_start(performance_year)) & (F.to_date("service_dt") <= _py_end(performance_year)))
        .where(accepted_charge_expr())
        .withColumn("claim_time_secondary_coverage_flag", claim_time_secondary_coverage_expr())
        .withColumn("claim_time_tertiary_coverage_flag", claim_time_tertiary_coverage_expr())
    )
    claim_context = charge_rows.groupBy("hcmpi").agg(
        F.sum(F.col("claim_time_secondary_coverage_flag").cast("int")).alias("claim_time_secondary_coverage_charge_count"),
        F.sum(F.col("claim_time_tertiary_coverage_flag").cast("int")).alias("claim_time_tertiary_coverage_charge_count"),
        F.max(F.col("claim_time_secondary_coverage_flag").cast("int")).cast("boolean").alias("has_claim_time_secondary_coverage"),
        F.max(F.col("claim_time_tertiary_coverage_flag").cast("int")).cast("boolean").alias("has_claim_time_tertiary_coverage"),
    )

    bool_defaults = (
        "has_patient_secondary_coverage",
        "has_patient_tertiary_coverage",
        "priority_3_self_pay_or_no_coverage_flag",
        "has_claim_time_secondary_coverage",
        "has_claim_time_tertiary_coverage",
    )
    result = (
        aligned.join(insurance_rollup, on="hcmpi", how="left")
        .join(claim_context, on="hcmpi", how="left")
        .fillna(False, subset=list(bool_defaults))
        .fillna(
            0,
            subset=[
                "patient_secondary_insurance_package_count",
                "patient_tertiary_insurance_package_count",
                "claim_time_secondary_coverage_charge_count",
                "claim_time_tertiary_coverage_charge_count",
            ],
        )
    )
    return (
        result.withColumn("has_secondary_or_tertiary_coverage_flag", F.col("has_patient_secondary_coverage") | F.col("has_patient_tertiary_coverage"))
        .withColumn("no_secondary_or_tertiary_coverage_flag", ~F.col("has_secondary_or_tertiary_coverage_flag"))
        .withColumn(
            "coverage_decision_source",
            F.when(F.col("has_patient_secondary_coverage"), F.lit("financial.patient_insurance priority 2"))
            .when(F.col("has_patient_tertiary_coverage"), F.lit("financial.patient_insurance priority 3 real payer"))
            .when(F.col("priority_3_self_pay_or_no_coverage_flag"), F.lit("priority 3 self-pay/no-coverage only"))
            .otherwise(F.lit("no active priority 2 or real priority 3 coverage")),
        )
        .withColumn("performance_year", F.lit(int(performance_year)))
        .select(
            "hcmpi",
            "has_patient_secondary_coverage",
            "has_patient_tertiary_coverage",
            "has_secondary_or_tertiary_coverage_flag",
            "no_secondary_or_tertiary_coverage_flag",
            "coverage_decision_source",
            "patient_secondary_insurance_package_count",
            "patient_secondary_insurance_names",
            "patient_secondary_insurance_groups",
            "patient_tertiary_insurance_package_count",
            "patient_tertiary_insurance_names",
            "patient_tertiary_insurance_groups",
            "priority_3_self_pay_or_no_coverage_flag",
            "priority_3_self_pay_or_no_coverage_names",
            "claim_time_secondary_coverage_charge_count",
            "claim_time_tertiary_coverage_charge_count",
            "has_claim_time_secondary_coverage",
            "has_claim_time_tertiary_coverage",
            "performance_year",
        )
    )


__all__ = ["build_copay_waiver_coverage_status"]
