"""Runtime environment defaults for REACH Databricks table materialization."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ReachEnvironment:
    """Databricks defaults used by REACH package-backed audit pipes."""

    uat_catalog_schema: str = "uat_sandbox.gov_programs"
    bronze_volume_root: str = "/Volumes/uat_sandbox/gov_programs/bronze"
    wheel_workspace_path: str = "/Workspace/Users/kcorwin@harmonycares.com/reach/lib/reach-0.1.0-py3-none-any.whl"
    sql_warehouse_id: str = "a198dca258996e5a"
    serverless_environment_version: str = "5"


def reach_environment() -> ReachEnvironment:
    """Return REACH runtime defaults, allowing env overrides for Databricks Jobs."""
    return ReachEnvironment(
        uat_catalog_schema=os.getenv("REACH_UAT_CATALOG_SCHEMA", "uat_sandbox.gov_programs"),
        bronze_volume_root=os.getenv("REACH_BRONZE_VOLUME_ROOT", "/Volumes/uat_sandbox/gov_programs/bronze"),
        wheel_workspace_path=os.getenv(
            "REACH_WORKSPACE_WHEEL_PATH",
            "/Workspace/Users/kcorwin@harmonycares.com/reach/lib/reach-0.1.0-py3-none-any.whl",
        ),
        sql_warehouse_id=os.getenv("REACH_SQL_WAREHOUSE_ID", "a198dca258996e5a"),
        serverless_environment_version=os.getenv("REACH_SERVERLESS_ENVIRONMENT_VERSION", "5"),
    )


__all__ = ["ReachEnvironment", "reach_environment"]
