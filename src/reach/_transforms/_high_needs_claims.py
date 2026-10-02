"""Spark transforms for REACH High-Needs claim-based criteria."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._expressions._common import normalize_hcpcs, normalize_icd10
from reach._expressions._high_needs import (
    CRITERION_D_MIN_DISTINCT_SERVICE_DATES,
    DIAGNOSIS_CODE_COLUMNS,
    HOME_HEALTH_BILL_TYPE_PREFIX,
    LookbackWindow,
    SNF_BILL_TYPE_PREFIX,
    criterion_e_met_expr,
    inpatient_claim_expr,
    service_days_in_window_expr,
    unplanned_admission_filter_expr,
)
from reach._transforms._registry import register_transform


def _category_col(df: DataFrame):
    return F.col("category") if "category" in df.columns else F.lit(None).cast("string")


def parse_mobility_icd10_codes(mobility_codes: DataFrame) -> DataFrame:
    """Normalize Table B.6.1 mobility ICD-10 codes to one code per row."""
    if "icd10_codes" in mobility_codes.columns:
        codes = mobility_codes.select(_category_col(mobility_codes).alias("category"), F.explode(F.split(F.col("icd10_codes"), ",")).alias("_raw_code"))
    else:
        code_col = "icd10_code" if "icd10_code" in mobility_codes.columns else "code"
        codes = mobility_codes.select(_category_col(mobility_codes).alias("category"), F.col(code_col).alias("_raw_code"))
    return codes.withColumn("icd10_code", normalize_icd10("_raw_code")).where(F.col("icd10_code") != "").where(F.coalesce(F.col("category"), F.lit("")) != "x")


def parse_frailty_hcpcs_codes(frailty_codes: DataFrame) -> DataFrame:
    """Normalize Table B.6.2 frailty HCPCS codes to one code per row."""
    code_col = "hcpcs_code" if "hcpcs_code" in frailty_codes.columns else "code"
    return (
        frailty_codes.select(_category_col(frailty_codes).alias("category"), normalize_hcpcs(code_col).alias("hcpcs_code"))
        .where(F.col("hcpcs_code") != "")
        .where(F.coalesce(F.col("category"), F.lit("")) != "x")
    )


def _empty_claim_result(medical_claim: DataFrame, *columns: str) -> DataFrame:
    return medical_claim.limit(0).select(*(F.lit(None).cast("string").alias(col) for col in columns))


@register_transform("high_needs_criterion_a_claims", concern="high_needs", output_table="reach_high_needs_criterion_a_claims")
def build_criterion_a_qualifying_claims(
    medical_claim: DataFrame,
    mobility_codes: DataFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    admission_date_col: str = "admission_date",
    claim_start_date_col: str = "claim_start_date",
    diagnosis_columns: tuple[str, ...] = DIAGNOSIS_CODE_COLUMNS,
) -> DataFrame:
    """Return B.6.1 diagnosis claim matches for criterion A in one lookback window."""
    diag_cols = [col for col in diagnosis_columns if col in medical_claim.columns]
    if not diag_cols:
        return _empty_claim_result(medical_claim, "mbi", "is_inpatient", "service_date", "diagnosis_code", "category")
    is_inpatient = inpatient_claim_expr(claim_type_col=claim_type_col, bill_type_col=bill_type_col)
    service_date = F.when(is_inpatient, F.to_date(F.col(admission_date_col))).otherwise(F.to_date(F.col(claim_start_date_col)))
    dx_rows = F.explode(F.array(*[F.struct(F.lit(col).alias("dx_position"), normalize_icd10(col).alias("diagnosis_code")) for col in diag_cols]))
    claims = (
        medical_claim.withColumn("is_inpatient", is_inpatient)
        .withColumn("service_date", service_date)
        .where(F.col("service_date").between(F.lit(window.begin), F.lit(window.end)))
        .select(F.col(mbi_col).alias("mbi"), "is_inpatient", "service_date", dx_rows.alias("dx"))
        .select("mbi", "is_inpatient", "service_date", F.col("dx.diagnosis_code").alias("diagnosis_code"))
        .where(F.col("diagnosis_code") != "")
    )
    return claims.join(parse_mobility_icd10_codes(mobility_codes), on="diagnosis_code", how="inner").select("mbi", "is_inpatient", "service_date", "diagnosis_code", "category")


@register_transform("high_needs_criterion_a_flags", concern="high_needs", output_table="reach_high_needs_criterion_a_flags")
def build_criterion_a_flags(qualifying_claims: DataFrame) -> DataFrame:
    """Collapse criterion A qualifying claims to one flag row per beneficiary."""
    inpatient = F.coalesce(F.col("is_inpatient"), F.lit(False))
    return (
        qualifying_claims.groupBy("mbi")
        .agg(
            F.sum(F.when(inpatient, 1).otherwise(0)).alias("inpatient_qualifying_claim_count"),
            F.countDistinct(F.when(~inpatient, F.col("service_date"))).alias("non_inpatient_distinct_service_dates"),
            F.min("service_date").alias("first_qualifying_date"),
            F.first("diagnosis_code", ignorenulls=True).alias("first_qualifying_diagnosis"),
            F.first("category", ignorenulls=True).alias("first_qualifying_category"),
        )
        .withColumn("criterion_a_inpatient_branch_met", F.col("inpatient_qualifying_claim_count") >= 1)
        .withColumn("criterion_a_non_inpatient_branch_met", F.col("non_inpatient_distinct_service_dates") >= 2)
        .withColumn("criterion_a_met", F.col("criterion_a_inpatient_branch_met") | F.col("criterion_a_non_inpatient_branch_met"))
    )


@register_transform("high_needs_unplanned_admissions", concern="high_needs", output_table="reach_high_needs_unplanned_admissions")
def count_unplanned_admissions_in_window(
    medical_claim: DataFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    admission_date_col: str = "admission_date",
) -> DataFrame:
    """Count distinct unplanned inpatient admission dates per beneficiary."""
    return (
        medical_claim.where(unplanned_admission_filter_expr())
        .withColumn("admission_date", F.to_date(F.col(admission_date_col)))
        .where(F.col("admission_date").between(F.lit(window.begin), F.lit(window.end)))
        .select(F.col(mbi_col).alias("mbi"), "admission_date")
        .dropDuplicates(["mbi", "admission_date"])
        .groupBy("mbi")
        .agg(F.count(F.lit(1)).alias("unplanned_admission_count"))
    )


@register_transform("high_needs_criterion_d_claims", concern="high_needs", output_table="reach_high_needs_criterion_d_claims")
def build_criterion_d_qualifying_claims(
    medical_claim: DataFrame,
    frailty_codes: DataFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    claim_type_col: str = "claim_type",
    hcpcs_col: str = "hcpcs_code",
    service_date_col: str = "claim_line_start_date",
) -> DataFrame:
    """Return B.6.2 HCPCS claim matches for criterion D in one lookback window."""
    claim_type = F.lower(F.trim(F.coalesce(F.col(claim_type_col).cast("string"), F.lit(""))))
    claims = (
        medical_claim.where(claim_type != "institutional")
        .withColumn("service_date", F.to_date(F.col(service_date_col)))
        .where(F.col("service_date").between(F.lit(window.begin), F.lit(window.end)))
        .withColumn("hcpcs_code", normalize_hcpcs(hcpcs_col))
        .where(F.col("hcpcs_code") != "")
        .select(F.col(mbi_col).alias("mbi"), "service_date", "hcpcs_code")
    )
    return claims.join(parse_frailty_hcpcs_codes(frailty_codes), on="hcpcs_code", how="inner").select("mbi", "service_date", "hcpcs_code", "category")


@register_transform("high_needs_criterion_d_flags", concern="high_needs", output_table="reach_high_needs_criterion_d_flags")
def build_criterion_d_flags(qualifying_claims: DataFrame) -> DataFrame:
    """Collapse criterion D qualifying claims to one flag row per beneficiary."""
    return (
        qualifying_claims.groupBy("mbi")
        .agg(
            F.countDistinct("service_date").alias("distinct_service_dates"),
            F.min("service_date").alias("first_qualifying_date"),
            F.first("hcpcs_code", ignorenulls=True).alias("first_qualifying_hcpcs"),
            F.first("category", ignorenulls=True).alias("first_qualifying_category"),
        )
        .withColumn("criterion_d_met", F.col("distinct_service_dates") >= F.lit(CRITERION_D_MIN_DISTINCT_SERVICE_DATES))
    )


@register_transform("high_needs_snf_home_health_days", concern="high_needs", output_table="reach_high_needs_snf_home_health_days")
def build_snf_home_health_days(
    medical_claim: DataFrame,
    *,
    window: LookbackWindow,
    mbi_col: str = "person_id",
    claim_type_col: str = "claim_type",
    bill_type_col: str = "bill_type_code",
    from_col: str = "claim_line_start_date",
    thru_col: str = "claim_line_end_date",
) -> DataFrame:
    """Count SNF and home-health service days per beneficiary for criterion E."""
    bill_type = F.trim(F.coalesce(F.col(bill_type_col).cast("string"), F.lit("")))
    claim_type = F.lower(F.trim(F.coalesce(F.col(claim_type_col).cast("string"), F.lit(""))))
    facility = F.when(bill_type.startswith(SNF_BILL_TYPE_PREFIX), F.lit("SNF")).when(bill_type.startswith(HOME_HEALTH_BILL_TYPE_PREFIX), F.lit("HH"))
    days = service_days_in_window_expr(window, from_col, thru_col)
    return (
        medical_claim.where((claim_type == "institutional") & facility.isNotNull())
        .withColumn("facility", facility)
        .withColumn("days_in_window", days)
        .select(
            F.col(mbi_col).alias("mbi"),
            F.to_date(F.col(from_col)).alias("_service_from"),
            F.to_date(F.col(thru_col)).alias("_service_thru"),
            "facility",
            "days_in_window",
        )
        .dropDuplicates(["mbi", "_service_from", "_service_thru", "facility"])
        .groupBy("mbi")
        .agg(
            F.sum(F.when(F.col("facility") == "SNF", F.col("days_in_window")).otherwise(0)).alias("snf_days"),
            F.sum(F.when(F.col("facility") == "HH", F.col("days_in_window")).otherwise(0)).alias("home_health_days"),
        )
    )


@register_transform("high_needs_criterion_e_flags", concern="high_needs", output_table="reach_high_needs_criterion_e_flags")
def build_criterion_e_flags(days: DataFrame, *, performance_year: int) -> DataFrame:
    """Apply criterion E thresholds, forcing false before PY2024."""
    if performance_year < 2024:
        return days.select("mbi", "snf_days", "home_health_days").withColumn("criterion_e_met", F.lit(False))
    return days.select("mbi", "snf_days", "home_health_days").withColumn("criterion_e_met", criterion_e_met_expr())
