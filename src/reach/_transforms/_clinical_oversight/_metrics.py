"""Clinical oversight metric definitions shared by audit cohorts."""

from __future__ import annotations

CLINICAL_OVERSIGHT_DERIVED_METRIC_LABELS = (
    ("power_score", "Power Score"),
    ("total_events_90days", "90-Day Events"),
    ("total_events_365days", "365-Day Events"),
)

CLINICAL_OVERSIGHT_UTILIZATION_METRIC_LABELS = (
    ("admits_90DAYS", "90-Day Admits"),
    ("SNF_episodes_90DAYS", "90-Day SNF"),
    ("ED_visit_90DAYS", "90-Day ED"),
    ("obs_90DAYS", "90-Day Obs"),
    ("admits_180DAYS", "180-Day Admits"),
    ("SNF_episodes_180DAYS", "180-Day SNF"),
    ("ED_visit_180DAYS", "180-Day ED"),
    ("obs_180DAYS", "180-Day Obs"),
    ("admits_365DAYS", "365-Day Admits"),
    ("SNF_episodes_365DAYS", "365-Day SNF"),
    ("ED_visit_365DAYS", "365-Day ED"),
    ("obs_365DAYS", "365-Day Obs"),
    ("admits_YTD", "YTD Admits"),
    ("SNF_episodes_YTD", "YTD SNF"),
    ("ED_visit_YTD", "YTD ED"),
    ("obs_YTD", "YTD Obs"),
)

CLINICAL_OVERSIGHT_COST_METRIC_LABELS = (
    ("IP_Paid_PMPM", "Inpatient PMPM"),
    ("OP_Paid_PMPM", "Outpatient PMPM"),
    ("PB_DME_Paid_PMPM", "Professional/DME PMPM"),
    ("SNF_Paid_PMPM", "SNF PMPM"),
    ("HH_Paid_PMPM", "Home Health PMPM"),
    ("HSP_Paid_PMPM", "Hospice PMPM"),
    ("Total_Paid_PMPM", "TCOC PMPM"),
    ("IP_Paid_Total", "Inpatient Paid"),
    ("OP_Paid_Total", "Outpatient Paid"),
    ("PB_DME_Paid_Total", "Professional/DME Paid"),
    ("SNF_Paid_Total", "SNF Paid"),
    ("HH_Paid_Total", "Home Health Paid"),
    ("HSP_Paid_Total", "Hospice Paid"),
    ("Total_Paid_Dollars", "Total Paid"),
)

CLINICAL_OVERSIGHT_RISK_METRIC_LABELS = (
    ("Prosp_RAF_v28", "Prospective RAF v28"),
    ("Concur_RAF_v28", "Concurrent RAF v28"),
    ("Member_Months", "Member Months"),
)

CLINICAL_OVERSIGHT_METRIC_LABELS = (
    *CLINICAL_OVERSIGHT_DERIVED_METRIC_LABELS,
    *CLINICAL_OVERSIGHT_UTILIZATION_METRIC_LABELS,
    *CLINICAL_OVERSIGHT_COST_METRIC_LABELS,
    *CLINICAL_OVERSIGHT_RISK_METRIC_LABELS,
)

CLINICAL_OVERSIGHT_UTILIZATION_SOURCE_COLUMNS = tuple(column for column, _label in CLINICAL_OVERSIGHT_UTILIZATION_METRIC_LABELS)
CLINICAL_OVERSIGHT_METRIC_COLUMNS = tuple(column for column, _label in CLINICAL_OVERSIGHT_METRIC_LABELS)
CLINICAL_OVERSIGHT_PANEL_METRIC_COLUMNS = (
    "power_score",
    "total_events_90days",
    "total_events_365days",
    "admits_90DAYS",
    "SNF_episodes_90DAYS",
    "ED_visit_90DAYS",
    "obs_90DAYS",
    "Total_Paid_PMPM",
    "Total_Paid_Dollars",
    "Prosp_RAF_v28",
    "Concur_RAF_v28",
)


def clinical_oversight_materialized_metric_labels() -> tuple[tuple[str, str], ...]:
    """Return the metric labels after Delta-safe materialization lowercases columns."""
    return tuple((column.lower(), label) for column, label in CLINICAL_OVERSIGHT_METRIC_LABELS)


__all__ = [
    "CLINICAL_OVERSIGHT_COST_METRIC_LABELS",
    "CLINICAL_OVERSIGHT_DERIVED_METRIC_LABELS",
    "CLINICAL_OVERSIGHT_METRIC_COLUMNS",
    "CLINICAL_OVERSIGHT_METRIC_LABELS",
    "CLINICAL_OVERSIGHT_PANEL_METRIC_COLUMNS",
    "CLINICAL_OVERSIGHT_RISK_METRIC_LABELS",
    "CLINICAL_OVERSIGHT_UTILIZATION_METRIC_LABELS",
    "CLINICAL_OVERSIGHT_UTILIZATION_SOURCE_COLUMNS",
    "clinical_oversight_materialized_metric_labels",
]
