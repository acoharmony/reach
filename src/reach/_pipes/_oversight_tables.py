"""Output table names for the REACH clinical oversight pipe."""

from __future__ import annotations

from collections.abc import Mapping

from reach._pipes._audit_tables import qualified_table
from reach._tables import reach_environment

OVERSIGHT_OUTPUT_TABLE_SUFFIXES: Mapping[str, str] = {
    "dashboard": "reach_oversight_dashboard",
    "worklist": "reach_oversight_review_worklist",
    "clinician_summary": "reach_oversight_clinician_summary",
}

OVERSIGHT_STAGE_OUTPUTS: Mapping[str, str] = {
    "clinical_oversight_dashboard": "dashboard",
    "clinical_oversight_review_worklist": "worklist",
    "clinical_oversight_clinician_summary": "clinician_summary",
}

OVERSIGHT_RUN_SUMMARY_SUFFIX = "reach_oversight_run_summary"


def oversight_table_names(uat_catalog_schema: str | None = None) -> dict[str, str]:
    """Return output-name to table-name mapping for the clinical oversight pipe."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    return {name: qualified_table(schema, suffix) for name, suffix in OVERSIGHT_OUTPUT_TABLE_SUFFIXES.items()}


def oversight_run_summary_table(uat_catalog_schema: str | None = None) -> str:
    """Return the clinical oversight run-summary table."""
    schema = uat_catalog_schema or reach_environment().uat_catalog_schema
    return qualified_table(schema, OVERSIGHT_RUN_SUMMARY_SUFFIX)


__all__ = [
    "OVERSIGHT_OUTPUT_TABLE_SUFFIXES",
    "OVERSIGHT_RUN_SUMMARY_SUFFIX",
    "OVERSIGHT_STAGE_OUTPUTS",
    "oversight_run_summary_table",
    "oversight_table_names",
]
