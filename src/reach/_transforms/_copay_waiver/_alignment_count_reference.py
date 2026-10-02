"""Census and bronze count reference for REACH alignment."""

from __future__ import annotations

from pyspark.sql import DataFrame

from reach._transforms._copay_waiver._views import REACH_ALIGNMENT_COUNT_REFERENCE_VIEW
from reach._transforms._registry import register_transform


@register_transform(
    "reach_alignment_count_reference",
    concern="copay_waiver",
    output_table=REACH_ALIGNMENT_COUNT_REFERENCE_VIEW,
    description="Reference counts comparing current REACH census to the latest beneficiary alignment overview file.",
)
def build_reach_alignment_count_reference(spark) -> DataFrame:
    """Return census and latest overview counts used to sanity-check REACH alignment."""
    return spark.sql(
        """
        WITH census_month AS (
          SELECT max(monthyear) AS monthyear
          FROM prd_silver.census.census
          WHERE monthyear <= date_trunc('month', current_date())
        ),
        census_reach AS (
          SELECT c.hcmpi, c.monthyear, c.lc_status_current
          FROM prd_silver.census.census c
          CROSS JOIN census_month m
          WHERE c.monthyear = m.monthyear
            AND upper(trim(coalesce(c.payer_current, ''))) = 'REACH'
            AND c.hcmpi IS NOT NULL
        ),
        mbi_by_hcmpi AS (
          SELECT hcmpi, max(identifier) AS beneficiary_mbi_id
          FROM prd_silver.census.hcmpimaster
          WHERE identifiersrcfield = 'member_mbi'
          GROUP BY hcmpi
        ),
        overview_file AS (
          SELECT max(meta_file_dt) AS meta_file_dt
          FROM prd_bronze.reach_ingested.beneficiary_alignment_report_overview
        ),
        overview AS (
          SELECT DISTINCT o.beneficiary_mbi_id, o.meta_file_dt
          FROM prd_bronze.reach_ingested.beneficiary_alignment_report_overview o
          CROSS JOIN overview_file f
          WHERE o.meta_file_dt = f.meta_file_dt
        ),
        census_counts AS (
          SELECT
            'current_reach_census' AS count_source,
            'prd_silver.census.census' AS source_table,
            cast(max(monthyear) AS string) AS source_as_of,
            count(DISTINCT hcmpi) AS source_count,
            count(DISTINCT CASE WHEN lower(lc_status_current) = 'active - engaged' THEN hcmpi END) AS active_engaged_hcmpis,
            count(DISTINCT hcmpi) AS mapped_hcmpis
          FROM census_reach
        ),
        overview_counts AS (
          SELECT
            'latest_beneficiary_alignment_report_overview' AS count_source,
            'prd_bronze.reach_ingested.beneficiary_alignment_report_overview' AS source_table,
            cast(max(o.meta_file_dt) AS string) AS source_as_of,
            count(DISTINCT o.beneficiary_mbi_id) AS source_count,
            cast(NULL AS BIGINT) AS active_engaged_hcmpis,
            count(DISTINCT c.hcmpi) AS mapped_hcmpis
          FROM overview o
          LEFT JOIN mbi_by_hcmpi m
            ON o.beneficiary_mbi_id = m.beneficiary_mbi_id
          LEFT JOIN census_reach c
            ON m.hcmpi = c.hcmpi
        ),
        provider_alignment_raw AS (
          SELECT
            mbi_id,
            prvdr_npi_num_clm_or_va,
            coalesce(
              try_to_date(
                concat(
                  '20', substr(regexp_extract(filenm, '[.]D([0-9]{6})[.]', 1), 1, 2),
                  '-', substr(regexp_extract(filenm, '[.]D([0-9]{6})[.]', 1), 3, 2),
                  '-', substr(regexp_extract(filenm, '[.]D([0-9]{6})[.]', 1), 5, 2)
                ),
                'yyyy-MM-dd'
              ),
              to_date(try_to_timestamp(insertdatetime)),
              to_date(meta_load_dt)
            ) AS provider_alignment_file_dt
          FROM prd_bronze.reach_ingested.provider_alignment_report_r2history
          WHERE mbi_id IS NOT NULL
            AND prvdr_npi_num_clm_or_va IS NOT NULL
        ),
        provider_alignment_file AS (
          SELECT max(provider_alignment_file_dt) AS provider_alignment_file_dt
          FROM provider_alignment_raw
        ),
        provider_alignment_counts AS (
          SELECT
            'latest_provider_alignment_report_r2history' AS count_source,
            'prd_bronze.reach_ingested.provider_alignment_report_r2history' AS source_table,
            cast(max(p.provider_alignment_file_dt) AS string) AS source_as_of,
            count(DISTINCT p.mbi_id) AS source_count,
            cast(NULL AS BIGINT) AS active_engaged_hcmpis,
            count(DISTINCT c.hcmpi) AS mapped_hcmpis
          FROM provider_alignment_raw p
          CROSS JOIN provider_alignment_file f
          LEFT JOIN mbi_by_hcmpi m
            ON p.mbi_id = m.beneficiary_mbi_id
          LEFT JOIN census_reach c
            ON m.hcmpi = c.hcmpi
          WHERE p.provider_alignment_file_dt = f.provider_alignment_file_dt
        )
        SELECT * FROM census_counts
        UNION ALL
        SELECT * FROM overview_counts
        UNION ALL
        SELECT * FROM provider_alignment_counts
        """
    )
