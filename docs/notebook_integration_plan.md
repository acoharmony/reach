# Reach Notebook Integration Plan

This plan captures reusable logic and concepts found in `/opt/s3/data/notebooks`
that should be integrated into the `~/reach` project. The preferred approach is
to promote durable logic into `src/reach` and keep `nb/` notebooks as thin
marimo workbook shells.

## Current Reach Shape

The Reach project already follows a useful split:

- `src/reach/_expressions`: reusable Spark column expressions.
- `src/reach/_transforms`: Spark DataFrame builders and analytic outputs.
- `src/reach/_pipes`: warehouse-facing materialization pipelines.
- `src/reach/_notes`: notebook/workbook helpers and stateful workbook behavior.
- `nb/`: marimo workbooks that should mostly orchestrate package logic.

File parsing, raw delivery provenance, and bronze/file transforms should stay
outside Reach unless Reach is only consuming already-materialized outputs.

## Candidate Source Notebooks

### 1. Alignment Foundation

Source notebooks:

- `/opt/s3/data/notebooks/consolidated_alignments.py`
- `/opt/s3/data/notebooks/beneficiary_attribution_by_tin.py`
- `/opt/s3/data/notebooks/voluntary_alignment_beneficiaries.py`
- `/opt/s3/data/notebooks/ny_reach_dashboard.py`

Concepts to integrate:

- Latest alignment year-month detection from wide `ym_YYYYMM_*` columns.
- Current REACH population selection and living-beneficiary filters.
- Current attribution TIN/provider resolution across REACH, MSSP, voluntary,
  and aligned provider fields.
- Beneficiary-by-TIN summary and death-status split.
- Alignment trend views across historical months.
- Quarterly SVA contribution analysis.
- Final-quarter forecast logic for active REACH population.
- Transition analysis between REACH, MSSP, both, and neither.
- NY Reach filtered cohort builder and dashboard summaries.

Reach targets:

- Add reusable expressions to `src/reach/_expressions/_alignment.py`.
- Add durable builders to `src/reach/_transforms/_alignment.py`.
- Add notebook display helpers under `src/reach/_notes/_alignment/`.
- Add a thin workbook such as `nb/reach_alignment_workbook.py`.

### 2. Provider, TIN, and NPI Panels

Source notebooks:

- `/opt/s3/data/notebooks/practitioner_panel.py`
- `/opt/s3/data/notebooks/practitioner_panel_databricks.py`
- `/opt/s3/data/notebooks/provider_tins_npis.py`
- `/opt/s3/data/notebooks/preferred_providers.py`

Concepts to integrate:

- Participant list parsing by provider category, provider type, TIN, NPI, and
  organization/individual provider identity.
- TIN/NPI normalization and grouped provider reference views.
- Attribution diagnostics for missing or inferred provider TINs.
- Practitioner-level and TIN-level claim panel summaries.
- QP-relevant non-facility RVU, allowed amount, paid amount, and patient counts.
- Preferred-provider facility rollups and facility-beneficiary detail.
- Export-ready TIN/NPI maps.

Reach targets:

- Add `src/reach/_transforms/_provider_panel.py`.
- Add expression helpers to `src/reach/_expressions/_participant.py` or a new
  provider expression module if the logic grows.
- Add pipe stages if the panel outputs need durable UAT tables.
- Add notebook helpers under `src/reach/_notes/_provider_panel/`.

### 3. High-Cost and Clinical Review Workflows

Source notebooks:

- `/opt/s3/data/notebooks/reach_high_cost.py`
- `/opt/s3/data/notebooks/patient.py`
- `/opt/s3/data/notebooks/analytics.py`

Concepts to integrate:

- High-cost patient enrichment from BAR, beneficiary metrics, PALMR, provider
  reference data, and readmission counts.
- Ranking tables for total spend, inpatient admissions, ER visits, readmissions,
  SNF spend, and home-health spend.
- Provider picker and provider-scoped high-cost panels.
- Single-patient lookup by MBI/HCMPI with identity, demographics, alignment,
  conditions, medical claims, pharmacy claims, and yearly spend.
- PMPM, service-category, readmission, and year-over-year analytic summaries.

Reach targets:

- Extend existing clinical oversight transforms under
  `src/reach/_transforms/_clinical_oversight/`.
- Add patient lookup helpers under `src/reach/_notes/_clinical_oversight/` if
  the audience is clinical review.
- Add a separate `src/reach/_notes/_patient/` package if the lookup becomes a
  general-purpose patient inspector.
- Consider durable pipe outputs for high-cost ranking tables if repeated use is
  expected.

### 4. Quality Dashboard Expansion

Source notebook:

- `/opt/s3/data/notebooks/cms_quality_measures.py`

Concepts to integrate:

- QTLQR and BLQQR aggregation.
- ACR, UAMCC, and DAH measure presentation.
- Exclusion long-format transforms.
- Live-vs-delivered tie-outs.
- Measure matrix views.
- Denominator eligibility diagnostics and PY-alignment gating checks.

Reach targets:

- Extend `src/reach/_expressions/_quality.py`.
- Extend `src/reach/_transforms/_quality.py`.
- Add workbook helpers under `src/reach/_notes/_quality/`.
- Extend `warehouse_analytics_pipe()` if quality outputs should materialize.

### 5. BNMR Analysis Views

Source notebook:

- `/opt/s3/data/notebooks/reach_bnmr_analysis.py`

Concepts to integrate:

- Delivery timeline.
- Claims spend across deliveries.
- County benchmark rates.
- USPCC benchmark trend.
- Normalization factor summaries.
- Capitation aggregate views.
- Stop-loss county exposure.
- Report parameter inventory.
- Risk beneficiary counts and eligible months.
- Silver table inventory and table explorer concepts.

Reach targets:

- Extend `src/reach/_transforms/_bnmr.py` where calculations are durable.
- Add notebook-facing helpers under `src/reach/_notes/_bnmr/`.
- Keep exploratory table-browser behavior in notebooks, not core transforms.

### 6. High-Needs Reconciliation Views

Source notebooks:

- `/opt/s3/data/notebooks/high_needs_reconciliation.py`
- `/opt/s3/data/notebooks/high_needs_bar_recall_diagnostic.py`

Concepts to integrate:

- Criterion-level reconciliation summaries.
- Composite high-needs breakdown.
- First-ever eligible breakdown.
- Criterion drilldowns.
- Per-beneficiary inspector and row detail.
- BAR tie-out.
- Recall residual attribution.
- PBVAR A2 tie-out.

Reach targets:

- Existing high-needs logic lives in `src/reach/_expressions/_high_needs.py`,
  `src/reach/_transforms/_high_needs.py`, and
  `src/reach/_transforms/_high_needs_reconciliation.py`.
- Add presentation helpers under `src/reach/_notes/_high_needs/`.
- Add or update a thin high-needs notebook in `nb/`.

### 7. HEDR Workbook Polish

Source notebook:

- `/opt/s3/data/notebooks/reach_hedr_eligibility.py`

Concepts to integrate:

- Compliance status tiles.
- Denominator/numerator summaries.
- Missing required field breakdown.
- Alignment timing view.
- Operational action list for remediation and monitoring.

Reach targets:

- Existing HEDR logic lives in `src/reach/_expressions/_hedr.py` and
  `src/reach/_transforms/_hedr.py`.
- Add HEDR workbook helpers under `src/reach/_notes/_hedr/`.
- Add a thin `nb/reach_hedr_eligibility.py` workbook if needed.

### 8. Gift Card Audit Enrichment

Source notebooks:

- `/opt/s3/data/notebooks/gift_card_analysis.py`
- `/opt/s3/data/notebooks/jan_26_giftcards.py`
- `/opt/s3/data/notebooks/gift_card_order_address_verification_june_2026.py`

Concepts to integrate:

- GCM, BAR, HDAI source overlap.
- Deceased beneficiary checks.
- AWV comparison and mismatch review.
- Missing address checks.
- Three-source comparison.
- Mailing-list construction.
- Distribution summaries.
- Duplicate analysis.
- Direct delivery formatting.
- Address normalization and direct-vs-REACH address verification.

Reach targets:

- Extend `src/reach/_transforms/_giftcards.py`.
- Extend `src/reach/_notes/_giftcards/_workbook.py`.
- Keep one-off June 2026-specific file labels configurable rather than hard
  coding them into reusable logic.

### 9. Delivery Provenance

Source notebook:

- `/opt/s3/data/notebooks/reach_delivery_provenance.py`

Concepts to integrate:

- Report delivery status by schema/report type.
- Expected-vs-actual delivery lag distribution.
- Late and early outlier views.
- Cadence summaries.
- Delivered-without-schedule checks.
- Report-type drilldown.

Reach targets:

- Do not move importer/file parsing logic into Reach.
- Add a consumer-only workbook helper under `src/reach/_notes/_delivery/` if
  Reach users need the dashboard.
- Read from upstream importer-produced provenance tables only.

## Implementation Sequence

1. Build the alignment foundation.
   - Add current alignment and attribution helpers.
   - Add transition and quarterly SVA summaries.
   - Add tests for month-column detection and attribution precedence.

2. Build provider panel transforms.
   - Normalize participant provider identity.
   - Add TIN/NPI rollups and attribution diagnostics.
   - Add preferred-provider facility summaries.

3. Extend clinical oversight with high-cost views.
   - Add high-cost enrichment and ranking transforms.
   - Wire provider-scoped views into notebook helpers.
   - Decide whether patient lookup should live inside clinical oversight or as
     a standalone patient inspector.

4. Expand quality and HEDR notebook helpers.
   - Promote durable quality calculations.
   - Keep visual presentation and table explorers in `_notes`.
   - Add live/delivered tie-out helpers only when their source tables are stable.

5. Add BNMR and high-needs presentation helpers.
   - Reuse existing BNMR/high-needs transforms.
   - Move only repeated calculations into transforms.
   - Keep ad hoc table inventory and drilldown UI in notebooks.

6. Enrich gift card audit logic.
   - Add address verification and source-overlap checks.
   - Keep date-specific campaign configuration external.

7. Add delivery provenance as a consumer view.
   - Do not import file-ingestion ownership into Reach.
   - Consume stable upstream provenance outputs.

## Testing Guidance

- Unit-test pure expression behavior where possible.
- Use small Spark DataFrame fixtures for transforms that encode business rules.
- Treat wide alignment-month column detection, attribution precedence, coverage
  windows, denominator eligibility, and address normalization as high-risk
  logic requiring explicit tests.
- Keep workbook helpers thin enough that they mostly need smoke checks rather
  than exhaustive tests.

## Migration Rule of Thumb

Move logic into `src/reach` when it is repeated, business-critical, warehouse
facing, or likely to be audited. Leave logic in `nb/` when it is UI-only,
exploratory, one-off campaign-specific, or purely presentation oriented.
