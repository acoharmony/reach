"""Dashboard-equivalent clinical oversight worklist transform."""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame

from reach._transforms._clinical_oversight._constants import DASHBOARD_VIEW, DISCUSSION_GROUP_CLINICIAN_NPIS
from reach._transforms._clinical_oversight._identity import filter_discussion_group
from reach._transforms._clinical_oversight._sql import DASHBOARD_SQL
from reach._transforms._registry import register_transform


@register_transform(
    "clinical_oversight_dashboard",
    concern="clinical_oversight",
    output_table=DASHBOARD_VIEW,
    description="Dashboard-equivalent top-30 aligned patient ranking per clinician.",
)
def build_clinical_oversight_dashboard(
    spark,
    *,
    clinician_npis: Iterable[str] | None = DISCUSSION_GROUP_CLINICIAN_NPIS,
) -> DataFrame:
    """Run the Lakeview dashboard SQL, filtered to the discussion group NPIs by default."""
    return filter_discussion_group(spark.sql(DASHBOARD_SQL), clinician_npis=clinician_npis)
