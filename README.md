# REACH

Spark-focused Databricks helpers for REACH analytic workbooks and warehouse outputs.

This package follows the same shape as the LEAD project:

- `_expressions` is the central registry of reusable, pure Spark column expressions.
- `_transforms` is the central registry of Spark DataFrame builders. Each transform is grouped by concern and is intended to create one output table.
- `_pipes` is lightweight pipe metadata for warehouse-facing analytic outputs.
- `_notes` owns notebook/workbook state in concern-specific packages; it does not aggregate unrelated notebook surfaces.

Current analytic coverage includes clinical oversight workbook views, copay-waiver/secondary-coverage analysis, HEDR eligibility, High-Needs eligibility/reconciliation, BNMR reconciliation, and small REACH quality summary helpers.

File parsing, delivery provenance, and bronze/file transforms are intentionally not in this package. Those are owned by the importer layer (`~/edp-importer`); REACH consumes warehouse-ready Spark tables.

The clinical oversight workbook preserves the Lakeview dashboard's patient ranking query, filters it by the individual NPIs for Jenn Datzko, Lashiniqua Howard, and Laura Brantman Johnson, and adds a persistent Delta notes table for patient review status, notes, and next actions. Note entry only requires an HCMPI or MBI because workbook context fills the remaining patient/provider fields. If an ignored `discussed.txt` file is present beside the notebook/repo, the workbook can seed missing notes for those already-discussed patients without embedding the raw identifiers in tracked source.

The copay-waiver workbook helper is separate from clinical oversight and defaults to the current REACH census population (`payer_current = REACH`). It displays the latest `prd_bronze.reach_ingested.beneficiary_alignment_report_overview` count as a reference, uses `prd_bronze.reach_ingested.provider_alignment_report_r2history` for REACH provider alignment, and keeps census as the primary population source. It uses OSLaunch RCM tables as context and AthenaOne transactions as the support source of truth: `prd_silver.financial.patient_insurance` is the coverage decision source, with active performance-year priority `2` rows treated as secondary coverage and real non-self-pay priority `3` rows treated as tertiary coverage; `prd_silver.financial.charge` provides Part B service context and charge-level copay proxies; `prd_athena_bronze.athenaone.transaction` provides posted patient-responsibility, patient-payment, and REACH-coded Cost-Sharing Support adjustment events; `prd_athena_bronze.financials.activityfact` provides Athena rollup context; `prd_silver.patient_metrics.allpayer_cost_util_completed` and `prd_silver.patient_metrics.patient_registry_util_cost` provide the same latest-12-month spend, RAF, and utilization metrics used by the clinical oversight report; and `prd_silver.quality.patient_quality_metrics` provides patient-level measure diffs.

Build the wheel with uv:

```bash
uv build --wheel
```

## Audit Pipes

The audit workbooks are backed by wheel entry points so Databricks Jobs can populate durable UAT tables before notebook review.

```bash
uv run aco reach_audit
uv run aco reach_audit --pipe copay
uv run aco reach_audit --pipe giftcards
uv run aco reach_oversight
```

Default UAT outputs are under `uat_sandbox.gov_programs`:

- `reach_audit_copay_aligned_population`
- `reach_audit_copay_alignment_count_reference`
- `reach_audit_copay_coverage_status`
- `reach_audit_copay_athena_patient_copay_activity`
- `reach_audit_copay_revenue_cycle_transactions`
- `reach_audit_copay_patient_responsibility`
- `reach_audit_copay_patient_profile`
- `reach_audit_copay_patient_cost_context`
- `reach_audit_copay_population_summary`
- `reach_audit_copay_segment_breakdown`
- `reach_audit_copay_support_service_detail`
- `reach_audit_copay_no_secondary_due_not_waived_patients`
- `reach_audit_copay_cohort_diffs`
- `reach_audit_copay_measure_diffs`
- `reach_audit_copay_stage_manifest`
- `reach_audit_copay_stage_metrics`
- `reach_audit_copay_check_results`
- `reach_audit_copay_service_trace`
- `reach_audit_copay_run_summary`
- `reach_audit_giftcards_distribution`
- `reach_audit_giftcards_run_summary`
- `reach_oversight_dashboard`
- `reach_oversight_review_worklist`
- `reach_oversight_clinician_summary`
- `reach_oversight_run_summary`

Pipe runs fail by default when any selected output table is empty, after writing row counts to the run summary table. Audit notebooks are read-only answer sheets over the materialized tables; they do not run the pipes.

