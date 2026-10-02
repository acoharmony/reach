"""Spark expressions for REACH copay-waiver coverage analytics."""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, functions as F

from reach._expressions._common import ColumnOrName, as_column, blank_to_null, date_literal, safe_divide, trueish, zero_if_null
from reach._expressions._registry import register_expression


_PSEUDO_COVERAGE_PATTERN = (
    r"SELF[ -]?PAY|MOVED|MGRHOLD|MANAGER\s*HOLD|NO\s+INSURANCE|NO\s+COVERAGE|"
    r"UNSPECIFIED\s+REMIT|DO\s+NOT\s+(BILL|USE)|(^|[^A-Z0-9])NONE([^A-Z0-9]|$)"
)


def _amount(column: ColumnOrName) -> Column:
    return zero_if_null(as_column(column).cast("double"))


@register_expression("coverage_identifier_present", concern="copay_waiver")
def coverage_identifier_present_expr(*columns: ColumnOrName) -> Column:
    """True when any coverage identifier/class/group field has a non-blank value."""
    if not columns:
        return F.lit(False)
    expr = F.lit(False)
    for column in columns:
        expr = expr | blank_to_null(column).isNotNull()
    return expr


@register_expression("pseudo_coverage_artifact", concern="copay_waiver")
def pseudo_coverage_artifact_expr(*columns: ColumnOrName) -> Column:
    """True for self-pay, manager-hold, no-coverage, or remit-placeholder payer rows."""
    if not columns:
        return F.lit(False)
    text = F.upper(F.concat_ws(" ", *(F.coalesce(as_column(column).cast("string"), F.lit("")) for column in columns)))
    return text.rlike(_PSEUDO_COVERAGE_PATTERN)


@register_expression("real_coverage_identifier_present", concern="copay_waiver")
def real_coverage_identifier_present_expr(*columns: ColumnOrName) -> Column:
    """True when a payer row has coverage evidence and is not a self-pay/no-coverage artifact."""
    return coverage_identifier_present_expr(*columns) & ~pseudo_coverage_artifact_expr(*columns)


@register_expression("claim_time_secondary_coverage", concern="copay_waiver")
def claim_time_secondary_coverage_expr(
    secondary_insurance_col: ColumnOrName = "secondary_insurance_id",
    secondary_class_col: ColumnOrName = "sec_insurance_class",
    secondary_group_col: ColumnOrName = "sec_insurance_group",
) -> Column:
    """True when a charge carried descriptive secondary payer coverage on the claim."""
    return real_coverage_identifier_present_expr(secondary_class_col, secondary_group_col)


@register_expression("claim_time_tertiary_coverage", concern="copay_waiver")
def claim_time_tertiary_coverage_expr(
    tertiary_insurance_col: ColumnOrName = "tertiary_insurance_id",
    tertiary_class_col: ColumnOrName = "ter_insurance_class",
    tertiary_group_col: ColumnOrName = "ter_insurance_group",
) -> Column:
    """True when a charge carried descriptive tertiary payer coverage on the claim."""
    return real_coverage_identifier_present_expr(tertiary_class_col, tertiary_group_col)


@register_expression("claim_time_secondary_or_tertiary_coverage", concern="copay_waiver")
def claim_time_secondary_or_tertiary_coverage_expr() -> Column:
    """True when a charge carried secondary or tertiary coverage."""
    return claim_time_secondary_coverage_expr() | claim_time_tertiary_coverage_expr()


@register_expression("patient_secondary_or_tertiary_coverage", concern="copay_waiver")
def patient_secondary_or_tertiary_coverage_expr(
    priority_col: ColumnOrName = "insurance_priority",
    active_col: ColumnOrName = "active",
    insurance_name_col: ColumnOrName = "insurance_nm",
    insurance_superpackage_col: ColumnOrName = "insurance_superpackage_name",
    insurance_reporting_category_col: ColumnOrName = "insurance_reporting_category",
    insurance_reporting_group_col: ColumnOrName = "ins_reporting_group",
    eligibility_class_col: ColumnOrName = "eligibility_class",
) -> Column:
    """True for active secondary/tertiary rows that represent real payer coverage."""
    priority = F.lower(F.trim(F.coalesce(as_column(priority_col).cast("string"), F.lit(""))))
    priority_match = priority.isin("2", "02", "secondary", "secondary insurance", "3", "03", "tertiary", "tertiary insurance")
    real_coverage = real_coverage_identifier_present_expr(
        insurance_name_col,
        insurance_superpackage_col,
        insurance_reporting_category_col,
        insurance_reporting_group_col,
        eligibility_class_col,
    )
    return priority_match & trueish(active_col) & real_coverage


@register_expression("bar_supplemental_coverage", concern="copay_waiver")
def bar_supplemental_coverage_expr(supplemental_coverage_col: ColumnOrName = "SuppInsuranceFLG") -> Column:
    """True when the REACH BAR reports supplemental insurance coverage."""
    return trueish(supplemental_coverage_col)


@register_expression("coverage_overlaps_performance_year", concern="copay_waiver")
def coverage_overlaps_performance_year_expr(
    performance_year: int,
    start_col: ColumnOrName = "effective_dt",
    end_col: ColumnOrName = "termination_dt",
) -> Column:
    """True when a coverage row overlaps any day in the performance year."""
    py_start = date_literal(date(int(performance_year), 1, 1))
    py_end = date_literal(date(int(performance_year), 12, 31))
    start = F.to_date(as_column(start_col))
    end = F.to_date(as_column(end_col))
    return (start.isNull() | (start <= py_end)) & (end.isNull() | (end >= py_start))


@register_expression("accepted_charge", concern="copay_waiver")
def accepted_charge_expr(
    claim_status_col: ColumnOrName = "claim_status",
    charge_status_col: ColumnOrName = "charge_status",
) -> Column:
    """True for charges that are accepted and not explicitly held/dropped."""
    claim_status = F.upper(F.trim(F.coalesce(as_column(claim_status_col).cast("string"), F.lit(""))))
    charge_status = F.upper(F.trim(F.coalesce(as_column(charge_status_col).cast("string"), F.lit(""))))
    return (claim_status == F.lit("ACCEPTED")) & ~charge_status.isin("DROP", "DROPPED", "HOLD", "HOLDING")


@register_expression("copay_due", concern="copay_waiver")
def copay_due_expr(copay_col: ColumnOrName = "copay_amt") -> Column:
    """True when the patient-responsibility/copay amount is positive."""
    return _amount(copay_col) > F.lit(0.0)


@register_expression("copay_zeroed_or_not_collected", concern="copay_waiver")
def copay_zeroed_or_not_collected_expr(
    copay_col: ColumnOrName = "copay_amt",
    balance_col: ColumnOrName = "copay_balance_amt",
    adjusted_col: ColumnOrName = "adjusted_amt",
    owed_col: ColumnOrName = "owed_amt",
) -> Column:
    """Conservative flag for copays that appear adjusted away rather than collected."""
    copay = _amount(copay_col)
    balance = as_column(balance_col).cast("double")
    adjusted = _amount(adjusted_col)
    owed = _amount(owed_col)
    centricity_zeroed = balance.isNotNull() & (copay > 0) & (balance <= 0) & (adjusted > 0)
    athena_adjusted_proxy = balance.isNull() & (copay > 0) & (adjusted >= copay) & (owed <= 0)
    return centricity_zeroed | athena_adjusted_proxy


@register_expression("copay_zeroed_or_not_collected_amount", concern="copay_waiver")
def copay_zeroed_or_not_collected_amount_expr(
    copay_col: ColumnOrName = "copay_amt",
    balance_col: ColumnOrName = "copay_balance_amt",
    adjusted_col: ColumnOrName = "adjusted_amt",
    owed_col: ColumnOrName = "owed_amt",
) -> Column:
    """Estimated waived/not-collected copay dollars from standardized charge fields."""
    copay = _amount(copay_col)
    balance = as_column(balance_col).cast("double")
    adjusted = _amount(adjusted_col)
    unpaid_balance = F.greatest(copay - F.greatest(zero_if_null(balance), F.lit(0.0)), F.lit(0.0))
    centricity_amount = F.least(copay, F.greatest(unpaid_balance, adjusted))
    athena_amount = F.least(copay, adjusted)
    return (
        F.when(~copay_due_expr(copay_col), F.lit(0.0))
        .when(balance.isNotNull() & (balance <= 0) & (adjusted > 0), centricity_amount)
        .when(balance.isNull() & (adjusted >= copay) & (_amount(owed_col) <= 0), athena_amount)
        .otherwise(F.lit(0.0))
    )


@register_expression("alignment_period_patient_copay_payment_rate", concern="copay_waiver")
def alignment_period_patient_copay_payment_rate_expr(
    payment_col: ColumnOrName = "alignment_period_patient_copay_payment_amt",
    due_col: ColumnOrName = "alignment_period_copay_due_amt",
) -> Column:
    """Patient copay payment rate for services inside the REACH alignment window."""
    return safe_divide(payment_col, due_col)
