"""Gift-card distribution ingestion for REACH audit support."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._transforms._registry import register_transform

REACH_AUDIT_GIFTCARDS_2026_SOURCE_PATH = "/Volumes/uat_sandbox/gov_programs/bronze/reach_audit_giftcards/reach_giftcards_2026.csv"
REACH_AUDIT_GIFTCARDS_COMPLETE_SOURCE_PATH = "/Volumes/uat_sandbox/gov_programs/bronze/reach_audit_giftcards/reach_gcm_all.csv"
REACH_AUDIT_GIFTCARDS_SOURCE_PATH = REACH_AUDIT_GIFTCARDS_2026_SOURCE_PATH
REACH_AUDIT_GIFTCARDS_TABLE = "uat_sandbox.gov_programs.reach_audit_giftcards_distribution"
REACH_AUDIT_GIFTCARDS_VIEW = "reach_audit_giftcards"

_SOURCE_COLUMNS = {
    "card_type": "Card Type",
    "recipient_name": "Recipient Name",
    "recipient_contact": "Email/SMS/Address",
    "recipient_id": "Recipient ID",
    "quantity_of_gift_cards_received": "Quantity of Gift Cards Received",
    "total_face_value_of_gift_cards_received": "Total Face Value of Gift Cards Received",
    "quantity_of_gift_cards_claimed": "Quantity of Gift Cards Claimed",
    "total_face_value_of_gift_cards_claimed": "Total Face Value of Gift Cards Claimed",
}


def _source_column(df: DataFrame, label: str) -> str:
    normalized = {column.lstrip("\ufeff").strip(): column for column in df.columns}
    if label not in normalized:
        raise ValueError(f"Gift-card CSV is missing required column: {label}")
    return normalized[label]


def _raw_column(df: DataFrame, label: str):
    return F.col(f"`{_source_column(df, label)}`").cast("string")


def _integer_amount(column):
    normalized = F.regexp_replace(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"[^0-9-]", "")
    return F.when(F.length(normalized) > 0, normalized.cast("int"))


def _money_amount(column):
    text = F.trim(F.coalesce(column.cast("string"), F.lit("")))
    normalized = F.regexp_replace(text, r"[$,]", "")
    return F.when(F.length(normalized) > 0, normalized.cast("double"))


@register_transform(
    "reach_audit_giftcards_distribution",
    concern="giftcards",
    output_table=REACH_AUDIT_GIFTCARDS_TABLE,
    description="Load REACH gift-card distribution CSV from the UAT government programs bronze volume.",
)
def build_reach_audit_giftcards_distribution(spark, *, source_csv_path: str = REACH_AUDIT_GIFTCARDS_SOURCE_PATH) -> DataFrame:
    """Load the REACH gift-card CSV with all source fields preserved as typed table columns."""
    raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(source_csv_path)
    )
    card_type = _raw_column(raw, _SOURCE_COLUMNS["card_type"])
    recipient_name = _raw_column(raw, _SOURCE_COLUMNS["recipient_name"])
    recipient_contact = _raw_column(raw, _SOURCE_COLUMNS["recipient_contact"])
    recipient_id = _raw_column(raw, _SOURCE_COLUMNS["recipient_id"])
    received_qty = _raw_column(raw, _SOURCE_COLUMNS["quantity_of_gift_cards_received"])
    received_value = _raw_column(raw, _SOURCE_COLUMNS["total_face_value_of_gift_cards_received"])
    claimed_qty = _raw_column(raw, _SOURCE_COLUMNS["quantity_of_gift_cards_claimed"])
    claimed_value = _raw_column(raw, _SOURCE_COLUMNS["total_face_value_of_gift_cards_claimed"])
    return raw.select(
        F.trim(card_type).alias("card_type"),
        F.trim(recipient_name).alias("recipient_name"),
        F.trim(recipient_contact).alias("recipient_contact"),
        F.trim(recipient_id).alias("recipient_id"),
        _integer_amount(received_qty).alias("quantity_of_gift_cards_received"),
        _money_amount(received_value).alias("total_face_value_of_gift_cards_received"),
        _integer_amount(claimed_qty).alias("quantity_of_gift_cards_claimed"),
        _money_amount(claimed_value).alias("total_face_value_of_gift_cards_claimed"),
        F.lit(source_csv_path).alias("meta_source_file_path"),
        F.current_timestamp().alias("meta_ingested_at"),
    )


__all__ = [
    "REACH_AUDIT_GIFTCARDS_2026_SOURCE_PATH",
    "REACH_AUDIT_GIFTCARDS_COMPLETE_SOURCE_PATH",
    "REACH_AUDIT_GIFTCARDS_SOURCE_PATH",
    "REACH_AUDIT_GIFTCARDS_TABLE",
    "REACH_AUDIT_GIFTCARDS_VIEW",
    "build_reach_audit_giftcards_distribution",
]
