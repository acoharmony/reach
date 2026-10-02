"""Clinical oversight transform constants."""

DASHBOARD_VIEW = "reach_clinical_oversight_dashboard"
REVIEW_WORKLIST_VIEW = "reach_clinical_oversight_review_worklist"
CLINICIAN_SUMMARY_VIEW = "reach_clinical_oversight_clinician_summary"
DISCUSSION_GROUP_CLINICIAN_NAMES = ("Jenn Datzko", "Lashiniqua Howard", "Laura Brantman Johnson")
DISCUSSION_GROUP_CLINICIANS = DISCUSSION_GROUP_CLINICIAN_NAMES
DISCUSSION_GROUP_CLINICIAN_NPIS = ("1568005569", "1922454586", "1417337247")
DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES = {
    "1568005569": "Jenn Datzko",
    "1922454586": "Lashiniqua Howard",
    "1417337247": "Laura Brantman Johnson",
}

__all__ = [
    "CLINICIAN_SUMMARY_VIEW",
    "DASHBOARD_VIEW",
    "DISCUSSION_GROUP_CLINICIAN_DISPLAY_NAMES",
    "DISCUSSION_GROUP_CLINICIAN_NAMES",
    "DISCUSSION_GROUP_CLINICIAN_NPIS",
    "DISCUSSION_GROUP_CLINICIANS",
    "REVIEW_WORKLIST_VIEW",
]
