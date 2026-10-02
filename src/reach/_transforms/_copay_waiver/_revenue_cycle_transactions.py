"""Transaction-grain AthenaOne revenue-cycle events for REACH copay audit."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

from reach._expressions._common import blank_to_null, zero_if_null
from reach._transforms._copay_waiver._common import (
    _py_end,
    _py_start,
    _require_columns,
    _select_required,
    _service_in_alignment_window_expr,
    _with_alignment_window,
    _with_hcmpi_key,
)
from reach._transforms._copay_waiver._views import COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW
from reach._transforms._registry import register_transform


_PATIENT_COLUMNS = (
    "hcmpi",
    "MedicareBeneficiaryID",
    "beneficiary_name",
    "beneficiary_line_1_address",
    "beneficiary_city",
    "beneficiary_usps_state_code",
    "beneficiary_zip_5",
    "BirthDate",
    "beneficiary_gender",
    "race_ethnicity",
    "lc_status_current",
    "payer_current",
    "office_name",
    "clinician_name",
    "clinician_npi",
    "latest_aligned_npi",
    "latest_aligned_tin",
    "provider_alignment_type_claim",
    "provider_alignment_type_voluntary",
    "provider_alignment_file_dt",
    "beneficiary_alignment_effective_start_date",
    "beneficiary_alignment_effective_termination_date",
    "attr_start_dt",
    "attr_end_dt",
    "reach_alignment_start_dt",
    "reach_alignment_end_dt",
    "no_secondary_or_tertiary_coverage_flag",
    "coverage_decision_source",
)

_HCMPI_MASTER_COLUMNS = ("identifiersrcfield", "identifier", "hcmpi")

_ATHENA_TRANSACTION_COLUMNS = (
    "PATIENTID",
    "POSTDATE",
    "VOIDEDDATE",
    "REVERSALFLAG",
    "TRANSACTIONTYPE",
    "TRANSACTIONTRANSFERTYPE",
    "TRANSACTIONTRANSFERINTENT",
    "TRANSACTIONREASON",
    "CUSTOMTRANSACTIONCODE",
    "TRANSACTIONMETHOD",
    "LINENOTE",
    "AMOUNT",
    "PATIENTPAYMENTID",
    "PROCEDURECODE",
    "TRANSACTIONID",
    "PARENTCHARGEID",
    "CHARGEID",
    "CLAIMID",
    "CHARGEFROMDATE",
    "CHARGETODATE",
    "OTHERMODIFIER",
    "PLACEOFSERVICE",
    "PROVIDERID",
    "SUPERVISINGPROVIDERID",
    "DEPARTMENTID",
    "TRANSACTIONCREATEDDATETIME",
    "TRANSACTIONCREATEDBY",
    "TRANSACTIONPOSTEDBY",
)


def _source_column(column: str) -> Column:
    return F.col(column)


def _source_text(column: str) -> Column:
    return F.trim(F.coalesce(_source_column(column).cast("string"), F.lit("")))


def _numeric_id(column: str) -> Column:
    raw = _source_text(column)
    digits = F.regexp_extract(raw, r"^([0-9]+)", 1)
    return F.when(F.length(digits) > 0, digits)


def _athena_id(column: str) -> Column:
    value = _numeric_id(column)
    return F.when(value.isNotNull(), F.concat(F.lit("athena."), value))


def _normalized(column: str) -> Column:
    return F.upper(F.trim(F.coalesce(_source_column(column).cast("string"), F.lit(""))))


def _hcmpi_to_athena_patient(hcmpi_master: DataFrame) -> DataFrame:
    source = _select_required(hcmpi_master, _HCMPI_MASTER_COLUMNS, context="hcmpimaster for Athena patient mapping")
    return (
        source.where(F.lower(F.trim(F.col("identifiersrcfield"))) == F.lit("athena_id"))
        .select(
            F.regexp_extract(F.trim(F.col("identifier").cast("string")), r"^([0-9]+)", 1).alias("athena_patient_id"),
            F.col("hcmpi").cast("string").alias("hcmpi"),
        )
        .where(F.col("athena_patient_id").isNotNull() & (F.length(F.col("athena_patient_id")) > 0) & F.col("hcmpi").isNotNull())
        .dropDuplicates(["athena_patient_id", "hcmpi"])
    )


def _patient_context(patient_context: DataFrame, hcmpi_master: DataFrame, *, performance_year: int) -> DataFrame:
    context = _with_alignment_window(_with_hcmpi_key(patient_context), performance_year=performance_year)
    return (
        _select_required(context, _PATIENT_COLUMNS, context="patient context for copay revenue-cycle transactions")
        .dropDuplicates(["hcmpi"])
        .join(_hcmpi_to_athena_patient(hcmpi_master), on="hcmpi", how="left")
        .where(F.col("athena_patient_id").isNotNull())
    )


@register_transform(
    "copay_waiver_revenue_cycle_transactions",
    concern="copay_waiver",
    output_table=COPAY_WAIVER_REVENUE_CYCLE_TRANSACTIONS_VIEW,
    description="Transaction-grain AthenaOne patient responsibility, payment, and REACH support events for copay audit.",
)
def build_copay_waiver_revenue_cycle_transactions(
    patient_context: DataFrame,
    athena_transactions: DataFrame,
    hcmpi_master: DataFrame,
    *,
    performance_year: int,
) -> DataFrame:
    """Return posted AthenaOne responsibility, payment, and support events for aligned patients."""
    patients = _patient_context(patient_context, hcmpi_master, performance_year=performance_year)
    tx_source = _select_required(athena_transactions, _ATHENA_TRANSACTION_COLUMNS, context="athenaone.transaction for copay revenue-cycle transactions")
    tx = tx_source.withColumn("athena_patient_id", _numeric_id("PATIENTID"))
    tx = (
        tx.withColumn("transaction_post_dt", F.to_date("POSTDATE"))
        .where(F.col("transaction_post_dt").between(_py_start(performance_year), _py_end(performance_year)))
        .where(F.col("VOIDEDDATE").isNull())
        .withColumn("_reversal_flag", _normalized("REVERSALFLAG"))
        .where((F.col("_reversal_flag") == F.lit("")) | F.col("_reversal_flag").isin("0", "N", "FALSE"))
    )

    transaction_type = _normalized("TRANSACTIONTYPE")
    transfer_type = _normalized("TRANSACTIONTRANSFERTYPE")
    transfer_intent = _normalized("TRANSACTIONTRANSFERINTENT")
    transaction_reason = _normalized("TRANSACTIONREASON")
    custom_code = _normalized("CUSTOMTRANSACTIONCODE")
    amount = zero_if_null("AMOUNT").cast("double")
    patient_transfer = transfer_type.isin("PATIENT", "PT") | transfer_intent.isin("PATIENT", "PT") | transaction_reason.isin(
        "COPAY",
        "COINSURANCE",
        "DEDUCTIBLE",
        "PATIENT",
        "PATIENTTRANSFER",
    )
    responsibility_flag = (transaction_type == F.lit("TRANSFEROUT")) & patient_transfer
    payment_flag = (transaction_type == F.lit("PAYMENT")) & (F.col("PATIENTPAYMENTID").isNotNull() | patient_transfer)
    reach_support_flag = (transaction_type == F.lit("ADJUSTMENT")) & custom_code.rlike(r"^REACH")
    relevant_event_flag = responsibility_flag | payment_flag | reach_support_flag
    procedure = blank_to_null("PROCEDURECODE")
    procedure_cd = blank_to_null(F.substring_index(procedure, ",", 1))
    event_class = (
        F.when(reach_support_flag, F.lit("cost_sharing_support_adjustment"))
        .when(responsibility_flag, F.lit("patient_responsibility_transfer"))
        .when(payment_flag, F.lit("patient_payment"))
        .otherwise(F.lit("other_revenue_cycle_event"))
    )

    detail = (
        patients.join(tx, on="athena_patient_id", how="inner")
        .withColumn("transaction_type", transaction_type)
        .withColumn("transaction_transfer_type", blank_to_null("TRANSACTIONTRANSFERTYPE"))
        .withColumn("transaction_transfer_intent", blank_to_null("TRANSACTIONTRANSFERINTENT"))
        .withColumn("transaction_reason", blank_to_null("TRANSACTIONREASON"))
        .withColumn("custom_transaction_code", blank_to_null("CUSTOMTRANSACTIONCODE"))
        .withColumn("transaction_method", blank_to_null("TRANSACTIONMETHOD"))
        .withColumn("line_note", blank_to_null("LINENOTE"))
        .withColumn("transaction_amt", amount)
        .withColumn("patient_responsibility_transfer_amt", F.when(responsibility_flag, F.abs(amount)).otherwise(F.lit(0.0)))
        .withColumn("patient_payment_amt", F.when(payment_flag, F.abs(amount)).otherwise(F.lit(0.0)))
        .withColumn("cost_sharing_support_amt", F.when(reach_support_flag, F.abs(amount)).otherwise(F.lit(0.0)))
        .withColumn("cost_sharing_support_identified_flag", reach_support_flag)
        .withColumn("revenue_cycle_event_class", event_class)
        .where(relevant_event_flag)
        .withColumn("transaction_id", _athena_id("TRANSACTIONID"))
        .withColumn("charge_id", _athena_id("PARENTCHARGEID"))
        .withColumn("transaction_charge_id", _athena_id("CHARGEID"))
        .withColumn("claim_id", _athena_id("CLAIMID"))
        .withColumn("service_from_dt", F.to_date("CHARGEFROMDATE"))
        .withColumn("service_to_dt", F.to_date("CHARGETODATE"))
        .withColumn("service_in_reach_alignment_window_flag", _service_in_alignment_window_expr("service_from_dt"))
        .withColumn("transaction_post_in_reach_alignment_window_flag", _service_in_alignment_window_expr("transaction_post_dt"))
        .withColumn("procedure_cd", procedure_cd)
        .withColumn("procedure_modifiers", blank_to_null("OTHERMODIFIER"))
        .withColumn("part_b_service_nature", F.concat_ws(" ", F.col("procedure_cd"), F.col("procedure_modifiers")))
        .withColumn("place_of_service_cd", blank_to_null("PLACEOFSERVICE"))
        .withColumn("rendering_provider_id", _numeric_id("PROVIDERID"))
        .withColumn("supervising_provider_id", _numeric_id("SUPERVISINGPROVIDERID"))
        .withColumn("billing_department_id", _numeric_id("DEPARTMENTID"))
        .withColumn("transaction_created_at", F.col("TRANSACTIONCREATEDDATETIME").cast("timestamp"))
        .withColumn("transaction_created_by", blank_to_null("TRANSACTIONCREATEDBY"))
        .withColumn("transaction_posted_by", blank_to_null("TRANSACTIONPOSTEDBY"))
        .withColumn("performance_year", F.lit(int(performance_year)))
    )
    return detail.select(
        "performance_year",
        "revenue_cycle_event_class",
        "beneficiary_name",
        "beneficiary_line_1_address",
        "beneficiary_city",
        "beneficiary_usps_state_code",
        "beneficiary_zip_5",
        "hcmpi",
        "MedicareBeneficiaryID",
        "BirthDate",
        "beneficiary_gender",
        "race_ethnicity",
        "lc_status_current",
        "payer_current",
        "office_name",
        "clinician_name",
        "clinician_npi",
        "latest_aligned_npi",
        "latest_aligned_tin",
        "provider_alignment_type_claim",
        "provider_alignment_type_voluntary",
        "provider_alignment_file_dt",
        "beneficiary_alignment_effective_start_date",
        "beneficiary_alignment_effective_termination_date",
        "attr_start_dt",
        "attr_end_dt",
        "reach_alignment_start_dt",
        "reach_alignment_end_dt",
        "service_in_reach_alignment_window_flag",
        "transaction_post_in_reach_alignment_window_flag",
        "no_secondary_or_tertiary_coverage_flag",
        "coverage_decision_source",
        "athena_patient_id",
        "transaction_id",
        "claim_id",
        "charge_id",
        "transaction_charge_id",
        "transaction_post_dt",
        "service_from_dt",
        "service_to_dt",
        "part_b_service_nature",
        "procedure_cd",
        "procedure_modifiers",
        "place_of_service_cd",
        "transaction_type",
        "transaction_transfer_type",
        "transaction_transfer_intent",
        "transaction_reason",
        "custom_transaction_code",
        "transaction_method",
        "transaction_amt",
        "patient_responsibility_transfer_amt",
        "patient_payment_amt",
        "cost_sharing_support_amt",
        "cost_sharing_support_identified_flag",
        "rendering_provider_id",
        "supervising_provider_id",
        "billing_department_id",
        "transaction_created_at",
        "transaction_created_by",
        "transaction_posted_by",
        "line_note",
    ).orderBy("hcmpi", "transaction_post_dt", "transaction_id")


__all__ = ["build_copay_waiver_revenue_cycle_transactions"]
