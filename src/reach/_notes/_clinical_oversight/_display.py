"""HTML display helpers for clinical oversight patient review."""

from __future__ import annotations

from html import escape
from typing import Any

from pyspark.sql import DataFrame

from reach._notes._clinical_oversight._note_lookup import lookup_patient_for_note


def format_value(value: Any, *, money: bool = False) -> str:
    """Format one scalar value for notebook HTML."""
    if value is None:
        return "-"
    if money and isinstance(value, int | float):
        return f"${value:,.0f}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def tile(label: str, value: Any, *, money: bool = False) -> str:
    """Render a compact summary tile."""
    return (
        '<div style="border:1px solid #d7dde5;border-radius:6px;padding:10px 12px;background:#ffffff;">'
        f'<div style="font-size:12px;color:#52606d;line-height:1.2;">{escape(label)}</div>'
        f'<div style="font-size:20px;font-weight:650;color:#1f2933;line-height:1.35;">{escape(format_value(value, money=money))}</div>'
        '</div>'
    )


def patient_review_html(worklist: DataFrame, hcmpi: str) -> str:
    """Render a compact patient review summary for the selected HCMPI."""
    clean_hcmpi = (hcmpi or "").strip()
    if not clean_hcmpi:
        return '<div style="color:#52606d;">Set PATIENT_HCMPI, then rerun this cell.</div>'
    rows = lookup_patient_for_note(worklist, hcmpi=clean_hcmpi).limit(1).collect()
    if not rows:
        return f'<div style="color:#9b1c1c;">No active-engaged patient found for HCMPI {escape(clean_hcmpi)}.</div>'
    row = rows[0].asDict(recursive=True)
    tiles = "".join(
        [
            tile("Clinician", row.get("clinician_name")),
            tile("Panel Rank", row.get("provider_rank")),
            tile("Power Score", row.get("power_score")),
            tile("90 Day Events", row.get("total_events_90days")),
            tile("365 Day Events", row.get("total_events_365days")),
            tile("Inpatient PMPM", row.get("IP_Paid_PMPM"), money=True),
            tile("Home Health PMPM", row.get("HH_Paid_PMPM"), money=True),
            tile("Total Paid PMPM", row.get("Total_Paid_PMPM"), money=True),
            tile("Prospective RAF", row.get("Prosp_RAF_v28")),
            tile("Concurrent RAF", row.get("Concur_RAF_v28")),
        ]
    )
    detail_rows = (
        ("HCMPI", row.get("hcmpi")),
        ("MBI", row.get("MedicareBeneficiaryID")),
        ("Lifecycle", row.get("lc_status_current")),
        ("Payer", row.get("payer_current")),
        ("Office", row.get("office_name")),
        ("Alignment", row.get("alignment_journey_status")),
        ("Signature", row.get("signature_status")),
        ("Last SVA", row.get("latest_sva_date")),
        ("Last Engagement", row.get("latest_engagement_dt")),
        ("Saved Note", row.get("note_text")),
    )
    details = "".join(
        f'<tr><th style="text-align:left;padding:6px 10px;color:#52606d;border-bottom:1px solid #e5e7eb;">{escape(label)}</th>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;">{escape(format_value(value))}</td></tr>'
        for label, value in detail_rows
    )
    title = escape(format_value(row.get("hcmpi")))
    subtitle = escape(format_value(row.get("clinician_name")))
    return f"""
    <div style="font-family:system-ui,-apple-system,Segoe UI,sans-serif;color:#1f2933;max-width:1200px;">
      <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-end;margin-bottom:10px;">
        <div>
          <div style="font-size:12px;color:#52606d;text-transform:uppercase;letter-spacing:.04em;">Selected Active - Engaged Patient</div>
          <div style="font-size:26px;font-weight:700;line-height:1.25;">{title}</div>
        </div>
        <div style="font-size:16px;color:#374151;text-align:right;">{subtitle}</div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:8px;margin:10px 0 14px;">{tiles}</div>
      <table style="border-collapse:collapse;width:100%;font-size:14px;background:#ffffff;border:1px solid #e5e7eb;">{details}</table>
    </div>
    """
