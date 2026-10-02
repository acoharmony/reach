"""Notebook assembly helpers for REACH audit gift-card distribution."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._transforms._giftcards import (
    REACH_AUDIT_GIFTCARDS_SOURCE_PATH,
    REACH_AUDIT_GIFTCARDS_TABLE,
    REACH_AUDIT_GIFTCARDS_VIEW,
    build_reach_audit_giftcards_distribution,
)


def build_reach_audit_giftcards_workbook(
    spark,
    *,
    source_csv_path: str = REACH_AUDIT_GIFTCARDS_SOURCE_PATH,
    target_table: str = REACH_AUDIT_GIFTCARDS_TABLE,
) -> dict[str, DataFrame | str]:
    """Build gift-card distribution outputs for the audit notebook."""
    distribution = build_reach_audit_giftcards_distribution(spark, source_csv_path=source_csv_path)
    distribution.createOrReplaceTempView(REACH_AUDIT_GIFTCARDS_VIEW)
    summary = distribution.agg(
        F.count("recipient_id").alias("distribution_rows"),
        F.countDistinct("recipient_id").alias("distinct_recipients"),
        F.sum("quantity_of_gift_cards_received").alias("gift_cards_received"),
        F.sum("total_face_value_of_gift_cards_received").alias("face_value_received"),
        F.sum("quantity_of_gift_cards_claimed").alias("gift_cards_claimed"),
        F.sum("total_face_value_of_gift_cards_claimed").alias("face_value_claimed"),
    )
    by_card_type = distribution.groupBy("card_type").agg(
        F.count("recipient_id").alias("distribution_rows"),
        F.countDistinct("recipient_id").alias("distinct_recipients"),
        F.sum("quantity_of_gift_cards_received").alias("gift_cards_received"),
        F.sum("total_face_value_of_gift_cards_received").alias("face_value_received"),
        F.sum("quantity_of_gift_cards_claimed").alias("gift_cards_claimed"),
        F.sum("total_face_value_of_gift_cards_claimed").alias("face_value_claimed"),
    )
    return {
        "giftcard_distribution": distribution,
        "giftcard_summary": summary,
        "giftcard_by_card_type": by_card_type,
        "source_csv_path": source_csv_path,
        "target_table": target_table,
    }


__all__ = ["build_reach_audit_giftcards_workbook"]
