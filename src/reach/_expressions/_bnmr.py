"""Spark expressions for REACH BNMR reconciliation math."""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, functions as F

from reach._expressions._common import as_column
from reach._expressions._registry import register_expression

CLAIM_TYPE_LABELS = {
    "10": "HHA",
    "20": "SNF",
    "30": "SNF - Swing Beds",
    "40": "Outpatient",
    "50": "Hospice",
    "60": "Inpatient",
    "71": "Physician",
    "72": "DME/Physician",
    "81": "DMERC/non-DMEPOS",
    "82": "DMERC/DMEPOS",
}
PART_A_CODES = frozenset({"10", "20", "30", "50", "60"})
PART_B_CODES = frozenset({"40", "71", "72", "81", "82"})
CAP_FLOOR_MULTIPLIER = 0.97
CAP_CEILING_MULTIPLIER = 1.03
RISK_MONTHLY_GRAIN = ("perf_yr", "clndr_yr", "clndr_mnth", "bnmrk", "align_type", "va_cat", "bnmrk_type", "aco_id")
RISK_ANNUAL_GRAIN = ("perf_yr", "bnmrk", "align_type", "va_cat", "bnmrk_type", "aco_id")
DELIVERY_GRAIN = ("source_filename", "aco_id", "performance_year")


def _mapping_lookup(column: Column, mapping: dict[str, str]) -> Column:
    mapping_expr = F.create_map(*list(chain.from_iterable((F.lit(k), F.lit(v)) for k, v in mapping.items())))
    return mapping_expr[column]


@register_expression("bnmr_claim_type_label", concern="bnmr")
def claim_type_label_expr(claim_type_col: str = "clm_type_cd") -> Column:
    """Map CMS claim-type code to a human-readable label."""
    code = as_column(claim_type_col).cast("string")
    fallback = F.concat(F.lit("Unknown ("), F.coalesce(code, F.lit("null")), F.lit(")"))
    return F.coalesce(_mapping_lookup(code, CLAIM_TYPE_LABELS), fallback)


@register_expression("bnmr_claim_type_part", concern="bnmr")
def claim_type_part_expr(claim_type_col: str = "clm_type_cd") -> Column:
    """Map CMS claim-type code to MER/BNMR Part A, Part B, or Unknown."""
    code = as_column(claim_type_col).cast("string")
    return F.when(code.isin(*sorted(PART_A_CODES)), F.lit("Part A")).when(code.isin(*sorted(PART_B_CODES)), F.lit("Part B")).otherwise(F.lit("Unknown"))


@register_expression("bnmr_net_expenditure", concern="bnmr")
def bnmr_net_expenditure_expr() -> Column:
    """Compute BNMR net expenditure from gross less reductions plus ACO adjustment."""
    return (
        as_column("clm_pmt_amt_agg")
        - as_column("sqstr_amt_agg")
        - as_column("apa_rdctn_amt_agg")
        - as_column("ucc_amt_agg")
        - as_column("op_dsh_amt_agg")
        - as_column("cp_dsh_amt_agg")
        - as_column("op_ime_amt_agg")
        - as_column("cp_ime_amt_agg")
        - as_column("nonpbp_rdct_amt_agg")
        + as_column("aco_amt_agg_apa")
    )


@register_expression("bnmr_service_month_date", concern="bnmr")
def service_month_date_expr(year_col: str = "clndr_yr", month_col: str = "clndr_mnth") -> Column:
    """Build first-of-month service date from year/month columns."""
    return F.to_date(F.concat_ws("-", as_column(year_col).cast("string"), F.lpad(as_column(month_col).cast("string"), 2, "0"), F.lit("01")))


@register_expression("bnmr_program", concern="bnmr")
def program_expr(aco_id_col: str = "aco_id") -> Column:
    """Derive REACH/MSSP/Unknown from the ACO ID prefix."""
    aco_id = as_column(aco_id_col).cast("string")
    return F.when(aco_id.startswith("D"), F.lit("REACH")).when(aco_id.startswith("A"), F.lit("MSSP")).otherwise(F.lit("Unknown"))


@register_expression("bnmr_new_cap_format", concern="bnmr")
def is_new_cap_format_expr() -> Column:
    """True when a cap row uses the post-April-2025 capitation layout."""
    cols = (
        "aco_tcc_amt_pre_seq_actual", "aco_tcc_amt_post_seq_actual", "aco_tcc_amt_post_seq_paid",
        "aco_bpcc_amt_pre_seq_actual", "aco_bpcc_amt_post_seq_actual", "aco_bpcc_amt_post_seq_paid",
        "aco_apo_amt_pre_seq_actual", "aco_apo_amt_post_seq_actual", "aco_apo_amt_post_seq_paid",
        "aco_epcc_amt_post_seq_paid",
    )
    expr = F.lit(False)
    for col in cols:
        expr = expr | as_column(col).isNotNull()
    return expr


@register_expression("bnmr_old_cap_format", concern="bnmr")
def is_old_cap_format_expr() -> Column:
    """True when a cap row carries any old-format total column."""
    return as_column("aco_tcc_amt_total").isNotNull() | as_column("aco_bpcc_amt_total").isNotNull() | as_column("aco_epcc_amt_total_seq").isNotNull() | as_column("aco_apo_amt_total_seq").isNotNull()


def bpcc_reconcilable_expr() -> Column:
    """BPCC amount to reconcile across old and new BNMR cap layouts."""
    return F.coalesce(as_column("aco_bpcc_amt_total"), as_column("aco_bpcc_amt_post_seq_paid"))


def epcc_reconcilable_expr() -> Column:
    """EPCC amount to reconcile across old and new BNMR cap layouts."""
    return F.coalesce(as_column("aco_epcc_amt_total_seq"), as_column("aco_epcc_amt_post_seq_paid"))


def apo_reconcilable_expr() -> Column:
    """APO amount to reconcile across old and new BNMR cap layouts."""
    return F.coalesce(as_column("aco_apo_amt_total_seq"), as_column("aco_apo_amt_post_seq_paid"))


def tcc_reconcilable_expr() -> Column:
    """TCC amount to reconcile across old and new BNMR cap layouts."""
    return F.coalesce(as_column("aco_tcc_amt_total"), as_column("aco_tcc_amt_post_seq_paid"))


def payment_month_from_payment_date_expr(payment_date_col: str = "payment_date") -> Column:
    """Derive ``YYYY-MM`` payment month from PLARU payment-date text."""
    return F.date_format(F.to_date(as_column(payment_date_col), "MM/dd/yyyy"), "yyyy-MM")


def normalized_risk_score_expr(raw_col: str = "raw_risk_score", factor_col: str = "normalization_factor") -> Column:
    """Normalized risk score = raw risk score times normalization factor."""
    return as_column(raw_col).cast("double") * as_column(factor_col).cast("double")


def cap_floor_expr(ry_normalized_col: str = "ry_normalized_risk_score") -> Column:
    """REACH BNMR risk cap floor: 0.97 times RY normalized score."""
    return as_column(ry_normalized_col).cast("double") * F.lit(CAP_FLOOR_MULTIPLIER)


def cap_ceiling_expr(ry_normalized_col: str = "ry_normalized_risk_score") -> Column:
    """REACH BNMR risk cap ceiling: 1.03 times RY normalized score."""
    return as_column(ry_normalized_col).cast("double") * F.lit(CAP_CEILING_MULTIPLIER)


def capped_risk_score_expr(normalized_col: str = "expected_normalized_risk_score", floor_col: str = "expected_cap_floor", ceiling_col: str = "expected_cap_ceiling") -> Column:
    """Clamp normalized risk score to the BNMR corridor, bypassing null baselines."""
    normalized = as_column(normalized_col).cast("double")
    floor = as_column(floor_col).cast("double")
    ceiling = as_column(ceiling_col).cast("double")
    return F.when(floor.isNull() | ceiling.isNull(), normalized).otherwise(F.least(F.greatest(normalized, floor), ceiling))


def benchmark_risk_score_expr(capped_col: str = "expected_capped_risk_score", cif_col: str = "cif") -> Column:
    """Benchmark risk score = capped score times CIF."""
    return as_column(capped_col).cast("double") * as_column(cif_col).cast("double")
