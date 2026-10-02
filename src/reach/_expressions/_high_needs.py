"""Spark expressions and scalar windows for REACH High-Needs eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import Column, functions as F

from reach._expressions._common import as_column, date_literal, normalize_code
from reach._expressions._registry import register_expression

CHECK_DATE_MONTH_DAY = ((1, 1), (4, 1), (7, 1), (10, 1))
HOSPITAL_INPATIENT_BILL_TYPE_PREFIX = "11"
SNF_BILL_TYPE_PREFIX = "21"
HOME_HEALTH_BILL_TYPE_PREFIX = "32"
ELECTIVE_ADMISSION_TYPE_CODE = "3"
AD_RISK_SCORE_THRESHOLD_B = 3.0
ESRD_RISK_SCORE_THRESHOLD_B = 0.35
AD_RISK_SCORE_LOWER_C = 2.0
ESRD_RISK_SCORE_LOWER_C = 0.24
CRITERION_C_MIN_UNPLANNED_ADMITS = 2
CRITERION_D_MIN_DISTINCT_SERVICE_DATES = 2
CRITERION_E_FIRST_APPLICABLE_PY = 2024
SNF_DAYS_THRESHOLD = 45
HOME_HEALTH_DAYS_THRESHOLD = 90
DIAGNOSIS_CODE_COLUMNS = tuple(f"diagnosis_code_{i}" for i in range(1, 26))


@dataclass(frozen=True)
class LookbackWindow:
    """Inclusive claim lookback interval for a check date."""

    begin: date
    end: date

    def contains(self, value: date) -> bool:
        """Return true when ``value`` is inside the interval."""
        return self.begin <= value <= self.end


def check_dates_for_py(performance_year: int) -> tuple[date, ...]:
    """Return the four quarterly High-Needs check dates for a performance year."""
    return tuple(date(performance_year, month, day) for month, day in CHECK_DATE_MONTH_DAY)


def _shift_month(anchor_year: int, anchor_month: int, delta_months: int) -> tuple[int, int]:
    zero_based = anchor_month - 1 + delta_months
    return anchor_year + zero_based // 12, zero_based % 12 + 1


def _last_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _ensure_valid_check_date(performance_year: int, check_date: date) -> None:
    valid = check_dates_for_py(performance_year)
    if check_date not in valid:
        raise ValueError(f"{check_date.isoformat()} is not a REACH High-Needs check date for PY{performance_year}.")


def _window_ending_three_months_before(check_date: date, months_spanned: int) -> LookbackWindow:
    end_year, end_month = _shift_month(check_date.year, check_date.month, -3)
    end = _last_day_of_month(end_year, end_month)
    begin_year, begin_month = _shift_month(end_year, end_month, -(months_spanned - 1))
    return LookbackWindow(begin=date(begin_year, begin_month, 1), end=end)


def table_c_window(performance_year: int, check_date: date) -> LookbackWindow:
    """Twelve-month lookback for criteria A, B, C, and E."""
    _ensure_valid_check_date(performance_year, check_date)
    return _window_ending_three_months_before(check_date, 12)


def table_d_window(performance_year: int, check_date: date) -> LookbackWindow:
    """Sixty-month lookback for criterion D."""
    _ensure_valid_check_date(performance_year, check_date)
    return _window_ending_three_months_before(check_date, 60)


@register_expression("high_needs_unplanned_admission", concern="high_needs")
def unplanned_admission_filter_expr(
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    admission_type_col: str = "admit_type_code",
) -> Column:
    """True for institutional inpatient admissions whose admit type is not elective."""
    claim_type = F.lower(F.trim(F.coalesce(as_column(claim_type_col).cast("string"), F.lit(""))))
    bill_type = F.trim(F.coalesce(as_column(bill_type_col).cast("string"), F.lit("")))
    admit_type = F.trim(F.coalesce(as_column(admission_type_col).cast("string"), F.lit("__null__")))
    return (claim_type == F.lit("institutional")) & bill_type.startswith(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX) & (admit_type != F.lit(ELECTIVE_ADMISSION_TYPE_CODE))


@register_expression("high_needs_inpatient_claim", concern="high_needs")
def inpatient_claim_expr(claim_type_col: str = "claim_type", bill_type_col: str = "bill_type_code") -> Column:
    """True for Tuva institutional hospital inpatient claim rows."""
    claim_type = F.lower(F.trim(F.coalesce(as_column(claim_type_col).cast("string"), F.lit(""))))
    bill_type = F.trim(F.coalesce(as_column(bill_type_col).cast("string"), F.lit("")))
    return (claim_type == F.lit("institutional")) & bill_type.startswith(HOSPITAL_INPATIENT_BILL_TYPE_PREFIX)


def service_days_in_window_expr(window: LookbackWindow, from_col: str, thru_col: str) -> Column:
    """Inclusive clipped service days contributed inside ``window``."""
    begin = F.greatest(F.to_date(as_column(from_col)), date_literal(window.begin))
    end = F.least(F.to_date(as_column(thru_col)), date_literal(window.end))
    return F.when(begin <= end, F.datediff(end, begin) + F.lit(1)).otherwise(F.lit(0))


@register_expression("high_needs_criterion_b", concern="high_needs")
def criterion_b_met_expr(cohort_col: str = "cohort", risk_score_col: str = "max_risk_score") -> Column:
    """Criterion B: high risk score, with ESRD and A&D thresholds."""
    cohort = F.upper(F.trim(F.coalesce(as_column(cohort_col).cast("string"), F.lit("AD"))))
    score = as_column(risk_score_col).cast("double")
    threshold = F.when(cohort == F.lit("ESRD"), F.lit(ESRD_RISK_SCORE_THRESHOLD_B)).otherwise(F.lit(AD_RISK_SCORE_THRESHOLD_B))
    return score >= threshold


@register_expression("high_needs_criterion_c_score_band", concern="high_needs")
def criterion_c_score_band_expr(cohort_col: str = "cohort", risk_score_col: str = "max_risk_score") -> Column:
    """Criterion C moderate risk-score band; upper bound is exclusive."""
    cohort = F.upper(F.trim(F.coalesce(as_column(cohort_col).cast("string"), F.lit("AD"))))
    score = as_column(risk_score_col).cast("double")
    ad_band = (score >= F.lit(AD_RISK_SCORE_LOWER_C)) & (score < F.lit(AD_RISK_SCORE_THRESHOLD_B))
    esrd_band = (score >= F.lit(ESRD_RISK_SCORE_LOWER_C)) & (score < F.lit(ESRD_RISK_SCORE_THRESHOLD_B))
    return F.when(cohort == F.lit("ESRD"), esrd_band).otherwise(ad_band)


@register_expression("high_needs_criterion_e", concern="high_needs")
def criterion_e_met_expr(snf_days_col: str = "snf_days", home_health_days_col: str = "home_health_days") -> Column:
    """Criterion E: SNF >= 45 days or home-health >= 90 days."""
    return (F.coalesce(as_column(snf_days_col).cast("int"), F.lit(0)) >= F.lit(SNF_DAYS_THRESHOLD)) | (
        F.coalesce(as_column(home_health_days_col).cast("int"), F.lit(0)) >= F.lit(HOME_HEALTH_DAYS_THRESHOLD)
    )


def criterion_e_applicable(performance_year: int) -> bool:
    """Criterion E applies only for PY2024 and later."""
    return int(performance_year) >= CRITERION_E_FIRST_APPLICABLE_PY


@register_expression("high_needs_criteria_any", concern="high_needs")
def criteria_any_met_expr(prefix: str = "criterion", letters: tuple[str, ...] = ("a", "b", "c", "d", "e")) -> Column:
    """OR the five criterion flags into a single High-Needs eligibility flag."""
    expr = F.lit(False)
    for letter in letters:
        expr = expr | F.coalesce(as_column(f"{prefix}_{letter}_met"), F.lit(False))
    return expr


def normalize_diagnosis_value(column: str) -> Column:
    """Normalize a diagnosis column for appendix-code joins."""
    return normalize_code(column)
