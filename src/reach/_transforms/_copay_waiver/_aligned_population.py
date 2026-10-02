"""Default REACH aligned-population denominator."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from reach._transforms._copay_waiver._views import REACH_ALIGNED_POPULATION_VIEW
from reach._transforms._registry import register_transform


@register_transform(
    "reach_census_aligned_population",
    concern="copay_waiver",
    output_table=REACH_ALIGNED_POPULATION_VIEW,
    description="Current REACH census population enriched with latest beneficiary-overview and provider-alignment context.",
)
def build_reach_census_aligned_population(spark, *, active_engaged_only: bool = False) -> DataFrame:
    """Load the current REACH population from census; bronze REACH tables only enrich it."""
    aligned = spark.sql(
        """
        WITH census_month AS (
          SELECT max(monthyear) AS monthyear
          FROM prd_silver.census.census
          WHERE monthyear <= date_trunc('month', current_date())
        ),
        census_reach AS (
          SELECT
            c.hcmpi,
            c.monthyear AS census_monthyear,
            c.lc_status_current,
            c.lifecycle_status,
            c.payer_current,
            c.payer_type_current,
            c.payer_type,
            c.payer,
            c.assigned_insurance_nm,
            c.assigned_ins_reporting_group,
            c.product,
            c.program_nm,
            c.cohort_name,
            c.office_name,
            c.primary_provider_npi,
            c.primary_provider_npi_current,
            c.first_engagement_dt,
            c.latest_engagement_dt,
            c.attr_start_dt,
            c.attr_end_dt,
            c.census_PayerEligible,
            c.active_panel
          FROM prd_silver.census.census c
          CROSS JOIN census_month m
          WHERE c.monthyear = m.monthyear
            AND upper(trim(coalesce(c.payer_current, ''))) = 'REACH'
            AND c.hcmpi IS NOT NULL
        ),
        mbi_by_hcmpi AS (
          SELECT hcmpi, max(identifier) AS MedicareBeneficiaryID
          FROM prd_silver.census.hcmpimaster
          WHERE identifiersrcfield = 'member_mbi'
          GROUP BY hcmpi
        ),
        overview_file AS (
          SELECT max(meta_file_dt) AS overview_file_dt
          FROM prd_bronze.reach_ingested.beneficiary_alignment_report_overview
        ),
        latest_overview AS (
          SELECT *
          FROM (
            SELECT
              o.*,
              row_number() OVER (
                PARTITION BY o.beneficiary_mbi_id
                ORDER BY o.meta_file_dt DESC NULLS LAST, o.meta_load_dt DESC NULLS LAST
              ) AS rn
            FROM prd_bronze.reach_ingested.beneficiary_alignment_report_overview o
            CROSS JOIN overview_file f
            WHERE o.meta_file_dt = f.overview_file_dt
          )
          WHERE rn = 1
        ),
        provider_alignment_raw AS (
          SELECT
            mbi_id,
            prvdr_npi_num_clm_or_va,
            prvdr_tin_num_clm_or_va,
            algn_type_clm,
            algn_type_va,
            filenm,
            filetype,
            datasource,
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
            ) AS provider_alignment_file_dt,
            try_to_timestamp(insertdatetime) AS provider_alignment_insert_ts,
            meta_load_dt AS provider_alignment_load_ts
          FROM prd_bronze.reach_ingested.provider_alignment_report_r2history
          WHERE mbi_id IS NOT NULL
            AND prvdr_npi_num_clm_or_va IS NOT NULL
        ),
        latest_provider_alignment AS (
          SELECT
            mbi_id,
            regexp_replace(trim(cast(prvdr_npi_num_clm_or_va AS string)), '[^0-9]', '') AS latest_aligned_npi,
            prvdr_tin_num_clm_or_va AS latest_aligned_tin,
            algn_type_clm AS provider_alignment_type_claim,
            algn_type_va AS provider_alignment_type_voluntary,
            provider_alignment_file_dt,
            filenm AS provider_alignment_file_name,
            'prd_bronze.reach_ingested.provider_alignment_report_r2history' AS provider_alignment_source
          FROM (
            SELECT
              *,
              row_number() OVER (
                PARTITION BY mbi_id
                ORDER BY provider_alignment_file_dt DESC NULLS LAST,
                  provider_alignment_insert_ts DESC NULLS LAST,
                  provider_alignment_load_ts DESC NULLS LAST
              ) AS rn
            FROM provider_alignment_raw
          )
          WHERE rn = 1
        ),
        provider_names AS (
          SELECT provider_npi, clinician_name
          FROM (
            SELECT
              provider_npi,
              clinician_name,
              row_number() OVER (
                PARTITION BY provider_npi
                ORDER BY try_cast(regexp_replace(coalesce(cast(performance_year AS string), ''), '[^0-9]', '') AS INT) DESC NULLS LAST,
                  effective_end_date DESC NULLS LAST,
                  effective_start_date DESC NULLS LAST
              ) AS rn
            FROM (
              SELECT
                regexp_replace(trim(cast(individual_npi AS string)), '[^0-9]', '') AS provider_npi,
                CASE
                  WHEN length(trim(concat_ws(' ', individual_first_name, individual_last_name))) > 0
                    THEN trim(concat_ws(' ', individual_first_name, individual_last_name))
                  WHEN length(trim(coalesce(provider_legal_business_name, ''))) > 0
                    THEN trim(provider_legal_business_name)
                  ELSE regexp_replace(trim(cast(individual_npi AS string)), '[^0-9]', '')
                END AS clinician_name,
                performance_year,
                effective_start_date,
                effective_end_date
              FROM uat_sandbox.gov_programs.participant_list
              WHERE individual_npi IS NOT NULL
            )
            WHERE provider_npi <> ''
          )
          WHERE rn = 1
        )
        SELECT
          c.census_monthyear,
          'prd_silver.census.census' AS alignment_source,
          c.hcmpi,
          m.MedicareBeneficiaryID,
          CASE
            WHEN length(trim(concat_ws(' ', o.beneficiary_first_name, o.beneficiary_last_name))) > 0
              THEN trim(concat_ws(' ', o.beneficiary_first_name, o.beneficiary_last_name))
          END AS beneficiary_name,
          o.beneficiary_first_name,
          o.beneficiary_last_name,
          o.beneficiary_line_1_address,
          o.beneficiary_city,
          o.beneficiary_usps_state_code,
          o.beneficiary_zip_5,
          coalesce(
            p.latest_aligned_npi,
            regexp_replace(trim(cast(c.primary_provider_npi AS string)), '[^0-9]', ''),
            regexp_replace(trim(cast(c.primary_provider_npi_current AS string)), '[^0-9]', '')
          ) AS clinician_npi,
          coalesce(
            pn.clinician_name,
            coalesce(
              p.latest_aligned_npi,
              regexp_replace(trim(cast(c.primary_provider_npi AS string)), '[^0-9]', ''),
              regexp_replace(trim(cast(c.primary_provider_npi_current AS string)), '[^0-9]', '')
            )
          ) AS clinician_name,
          c.primary_provider_npi,
          c.primary_provider_npi_current,
          p.latest_aligned_npi,
          p.latest_aligned_tin,
          p.provider_alignment_type_claim,
          p.provider_alignment_type_voluntary,
          p.provider_alignment_file_dt,
          p.provider_alignment_file_name,
          p.provider_alignment_source,
          c.lc_status_current,
          c.lifecycle_status,
          c.payer_current,
          c.payer_type_current,
          c.payer_type,
          c.payer,
          c.assigned_insurance_nm,
          c.assigned_ins_reporting_group,
          c.product,
          c.program_nm,
          c.cohort_name,
          c.office_name,
          c.first_engagement_dt,
          c.latest_engagement_dt,
          c.attr_start_dt,
          c.attr_end_dt,
          c.census_PayerEligible,
          c.active_panel,
          o.meta_file_dt AS overview_file_dt,
          o.beneficiary_mbi_id IS NOT NULL AS overview_aligned_flag,
          o.beneficiary_alignment_effective_start_date,
          o.beneficiary_alignment_effective_termination_date,
          coalesce(
            try_to_date(cast(o.beneficiary_alignment_effective_start_date AS string), 'M/d/yyyy'),
            try_to_date(cast(o.beneficiary_alignment_effective_start_date AS string), 'yyyy-MM-dd'),
            to_date(c.attr_start_dt)
          ) AS reach_alignment_start_dt,
          coalesce(
            try_to_date(cast(o.beneficiary_alignment_effective_termination_date AS string), 'M/d/yyyy'),
            try_to_date(cast(o.beneficiary_alignment_effective_termination_date AS string), 'yyyy-MM-dd'),
            to_date(c.attr_end_dt)
          ) AS reach_alignment_end_dt,
          o.beneficiary_gender,
          o.race_ethnicity,
          o.beneficiary_age,
          CASE
            WHEN o.beneficiary_date_of_birth RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' THEN make_date(
              CASE
                WHEN CAST(element_at(split(o.beneficiary_date_of_birth, '/'), 3) AS INT) <= CAST(date_format(current_date(), 'yy') AS INT)
                THEN 2000 + CAST(element_at(split(o.beneficiary_date_of_birth, '/'), 3) AS INT)
                ELSE 1900 + CAST(element_at(split(o.beneficiary_date_of_birth, '/'), 3) AS INT)
              END,
              CAST(element_at(split(o.beneficiary_date_of_birth, '/'), 1) AS INT),
              CAST(element_at(split(o.beneficiary_date_of_birth, '/'), 2) AS INT)
            )
            ELSE coalesce(
              try_to_date(o.beneficiary_date_of_birth, 'M/d/yyyy'),
              try_to_date(o.beneficiary_date_of_birth, 'yyyy-MM-dd')
            )
          END AS BirthDate
        FROM census_reach c
        LEFT JOIN mbi_by_hcmpi m
          ON c.hcmpi = m.hcmpi
        LEFT JOIN latest_overview o
          ON m.MedicareBeneficiaryID = o.beneficiary_mbi_id
        LEFT JOIN latest_provider_alignment p
          ON m.MedicareBeneficiaryID = p.mbi_id
        LEFT JOIN provider_names pn
          ON coalesce(
            p.latest_aligned_npi,
            regexp_replace(trim(cast(c.primary_provider_npi AS string)), '[^0-9]', ''),
            regexp_replace(trim(cast(c.primary_provider_npi_current AS string)), '[^0-9]', '')
          ) = pn.provider_npi
        """
    ).dropDuplicates(["hcmpi"])
    if active_engaged_only:
        aligned = aligned.where(F.lower(F.col("lc_status_current")) == F.lit("active - engaged"))
    return aligned

