# Human Test Plan: QA Dashboard

**Generated:** 2026-03-11
**Implementation plan:** `docs/implementation-plans/2026-03-10-qa-dashboard/`
**Automated test coverage:** 18/18 acceptance criteria covered

## Prerequisites

- Python 3.12+ with `uv` installed
- `uv run pytest` passing (406 tests, 0 failures)
- A test dataset with at least 3 SCDM tables (e.g., demographic, encounter, diagnosis) containing:
  - Mixed pass/warn/fail/note severity results
  - At least one column with completeness below 95%
  - At least one table with zero failures
  - Cross-table checks configured (2+ tables sharing a key like PatID)
- A valid `config.toml` pointing to the test dataset with `run_l1 = true` and `run_l2 = true`

## Phase 1: Generate Dashboard Output

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `uv run scdm-qa run config.toml` | Command completes with exit code 0 or 1. Output directory (from `[options] output_dir`) is created. |
| 1.2 | List the output directory contents | Contains `index.html`, one `{table_key}.html` per configured table, and `cross_table.html` if L2 checks ran. |
| 1.3 | Disconnect from the network (airplane mode or disable Wi-Fi) | Network is off. |
| 1.4 | Open `index.html` in a browser | Page loads fully with no broken resources, no console errors. All charts, tables, and stat cards render. |
| 1.5 | Re-enable network | Done. |

## Phase 2: Index Page Visual Verification

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Open `index.html` in a browser. Inspect the donut chart. | **AC1.2**: Four segments visible: Pass (green), Fail (red), Warn (yellow/amber), Note (grey). Segment sizes are proportional to check counts. |
| 2.2 | Hover over each donut segment. | Tooltip shows the severity label, count, and percentage. Values match the stat cards on the page. |
| 2.3 | Inspect the horizontal bar chart. | **AC1.3**: One bar per table. Bars sorted worst-first (lowest pass rate at top). |
| 2.4 | Compare bar colours. | Tables near 0% pass rate are red, near 50% are yellow, near 100% are green. Gradient is smooth. |
| 2.5 | Compare bar lengths to the pass rate values displayed. | Bar lengths are proportional to the pass rate percentage. |

## Phase 3: Detail Page Interactive Verification

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | On `index.html`, click a table row in the summary grid. | **AC2.1**: Browser navigates to `{table_key}.html`. |
| 3.2 | On the detail page, type "Fail" in the Severity header filter. | **AC2.4**: Only rows with severity "Fail" remain visible. |
| 3.3 | Type "col_vals_not_null" in the Type header filter (with Severity filter still active). | **AC2.4**: Rows filter further. Filters compose correctly. |
| 3.4 | Type a partial string (e.g., "PatID") in the Column header filter. | **AC2.4**: Rows filter to those with "PatID" in the column field. |
| 3.5 | Type a partial phrase in the Description header filter. | **AC2.4**: Free-text filtering works on description content. |
| 3.6 | Clear all filters. Click the "Download CSV" button. | **AC2.5**: A CSV file downloads containing all validation check rows. Column headers match the table columns. |
| 3.7 | Apply a severity filter (e.g., "Fail"). Click "Download CSV" again. | **AC2.5**: Downloaded CSV contains only the filtered rows. |
| 3.8 | Sort by a column (click column header), then download CSV. | **AC2.5**: Sort order is preserved in the CSV output. |

## Phase 4: Profiling Table Verification

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Open a detail page for a table with profiling data. Scroll to the profiling section. | **AC2.6**: Profiling table visible with columns: Column, Type, Completeness, Distinct, Min, Max. |
| 4.2 | Find a column with completeness below 95%. | **AC2.6**: That row's completeness cell has a red/pink background. |
| 4.3 | Find a column with completeness at or above 95%. | **AC2.6**: No special highlighting. Normal background. |

## Phase 5: Failing Rows Verification

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | Open a detail page for a table with failing checks. Scroll to the failing rows section. | **AC2.7**: Each failing check has its own collapsible section. |
| 5.2 | Click a collapsible section header. | **AC2.7**: Section expands to show a Tabulator table of sample failing rows. |
| 5.3 | Click the same header again. | **AC2.7**: Section collapses. |
| 5.4 | Expand two different sections simultaneously. | **AC2.7**: Both are independently expanded. |
| 5.5 | In an expanded failing row section, click its CSV download button. | **AC2.8**: A CSV file downloads with the check label in the filename. |
| 5.6 | Expand a second failing row section. Click its CSV download button. | **AC2.8**: A different CSV downloads containing rows specific to that check only. |

## End-to-End: Full Pipeline with Mixed Results

1. Configure `config.toml` with 3+ tables and both `run_l1 = true`, `run_l2 = true`.
2. Run `uv run scdm-qa run config.toml`.
3. Open `index.html` — verify stat cards show correct totals across all tables and cross-table checks.
4. Verify donut chart proportions match the stat card numbers.
5. Verify bar chart includes all tables plus `cross_table`, sorted worst-first.
6. Click into each detail page — verify header stats (total rows, chunks, pass rate) are correct.
7. On the `cross_table` detail page — verify no profiling section is visible, validation table shows L2 checks.
8. Click "Back to Index" link on a detail page — verify navigation returns to `index.html`.
9. Run `uv run scdm-qa serve <output_dir>` — verify the serve command starts and `index.html` is accessible.

## End-to-End: L1-Only and L2-Only Modes

1. Run `uv run scdm-qa run config.toml --l1-only`.
2. Open `index.html` — verify no `cross_table` entry. No `cross_table.html` file.
3. Run `uv run scdm-qa run config.toml --l2-only`.
4. Open `index.html` — verify only `cross_table` entry exists. No per-table detail pages.

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| qa-dashboard.AC1.1 | test_dashboard.py::test_html_contains_stat_cards_elements | — |
| qa-dashboard.AC1.2 | — | Phase 2, steps 2.1-2.2 |
| qa-dashboard.AC1.3 | — | Phase 2, steps 2.3-2.5 |
| qa-dashboard.AC1.4 | test_dashboard.py::test_all_pass_produces_valid_html | — |
| qa-dashboard.AC1.5 | test_dashboard.py::test_cross_table_only_produces_valid_html | — |
| qa-dashboard.AC2.1 | test_dashboard.py::test_summary_table_navigates_to_table_page | Phase 3, step 3.1 |
| qa-dashboard.AC2.2 | test_dashboard.py::test_detail_page_contains_stat_cards | — |
| qa-dashboard.AC2.3 | test_dashboard.py::test_detail_page_json_contains_all_step_fields | — |
| qa-dashboard.AC2.4 | — | Phase 3, steps 3.2-3.5 |
| qa-dashboard.AC2.5 | — | Phase 3, steps 3.6-3.8 |
| qa-dashboard.AC2.6 | — | Phase 4, steps 4.1-4.3 |
| qa-dashboard.AC2.7 | — | Phase 5, steps 5.1-5.4 |
| qa-dashboard.AC2.8 | — | Phase 5, steps 5.5-5.6 |
| qa-dashboard.AC2.9 | test_dashboard.py::test_failing_rows_section_hidden_when_no_failures | — |
| qa-dashboard.AC2.10 | test_dashboard.py::test_profiling_section_hidden_when_empty | — |
| qa-dashboard.AC3.1 | test_dashboard.py::test_cross_table_detail_page_created | — |
| qa-dashboard.AC3.2 | test_reporting.py::test_cross_table_same_format | — |
| qa-dashboard.AC4.1 | test_serialise.py::test_ac4_1_schema_version_set | — |
| qa-dashboard.AC4.2 | test_serialise.py::test_ac4_2_failing_rows_truncated | — |
| qa-dashboard.AC4.3 | test_serialise.py::test_ac4_3_none_fields_handled_gracefully | — |
| qa-dashboard.AC5.1 | test_serialise.py::test_ac5_1_json_serialisable | — |
| qa-dashboard.AC5.2 | test_reporting.py::test_html_self_contained | Phase 1, step 1.4 |
| qa-dashboard.AC5.3 | test_reporting.py::test_html_self_contained | Phase 1, steps 1.3-1.4 |
| qa-dashboard.AC6.1 | test_pipeline_phases.py::test_cross_table_entry_in_index | E2E, step 9 |
| qa-dashboard.AC6.2 | test_pipeline_phases.py::test_cross_table_report_file_created | Phase 1, step 1.2 |
