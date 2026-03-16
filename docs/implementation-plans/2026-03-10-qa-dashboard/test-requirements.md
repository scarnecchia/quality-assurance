# Test Requirements: QA Dashboard

## Automated Tests

| AC ID | Description | Test Type | Expected Test File | Phase |
|-------|-------------|-----------|-------------------|-------|
| qa-dashboard.AC1.1 | Index page displays total check count, overall pass rate, and total failure count as stat cards | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC1.4 | Index page renders correctly when all checks pass (zero failures, donut shows 100% pass) | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC1.5 | Index page renders correctly when only cross-table checks exist (no L1 tables) | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC2.1 | Clicking a table row in the index grid navigates to the correct detail page | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC2.2 | Detail page header shows table name, total rows, chunks processed, overall pass rate | integration | tests/test_dashboard.py | 4 |
| qa-dashboard.AC2.3 | Tabulator validation table displays all StepResult fields | integration | tests/test_dashboard.py | 4 |
| qa-dashboard.AC2.9 | Detail page renders correctly when a table has zero failures | integration | tests/test_dashboard.py | 5 |
| qa-dashboard.AC2.10 | Detail page for cross_table has no profiling section (ProfilingResult is empty) | integration | tests/test_dashboard.py | 5 |
| qa-dashboard.AC3.1 | Cross-table checks appear as a table entry in the index grid alongside L1 per-table entries | integration | tests/test_dashboard.py | 4 |
| qa-dashboard.AC3.2 | Cross-table detail page shows L2 check results in the same Tabulator format as L1 detail pages | integration | tests/test_reporting.py | 6 |
| qa-dashboard.AC4.1 | JSON output includes schema_version field set to "1.0" | unit | tests/test_serialise.py | 1 |
| qa-dashboard.AC4.2 | Failing rows in JSON are truncated to max_failing_rows limit | unit | tests/test_serialise.py | 1 |
| qa-dashboard.AC4.3 | Serialisation handles null check_id and null severity gracefully (renders as empty/dash) | unit | tests/test_serialise.py | 1 |
| qa-dashboard.AC5.1 | JSON data embedded in script blocks, not external files | unit | tests/test_serialise.py | 1 |
| qa-dashboard.AC5.2 | Tabulator JS+CSS and Plotly JS are inlined in each HTML file (no CDN links) | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC5.3 | HTML files open correctly in a browser without network access | integration | tests/test_dashboard.py | 3 |
| qa-dashboard.AC6.1 | scdm-qa serve serves the new dashboard files without changes to the serve command | integration | tests/test_pipeline_phases.py | 6 |
| qa-dashboard.AC6.2 | scdm-qa run produces new dashboard files in the configured output_dir | integration | tests/test_pipeline_phases.py | 6 |

### Automated Test Details

**tests/test_serialise.py (Phase 1 — unit tests)**
- AC4.1: Assert `serialise_run()` output dict contains `"schema_version": "1.0"` and a valid ISO `generated_at` timestamp.
- AC4.2: Create a StepResult with a 20-row failing_rows DataFrame, call `serialise_step()` with `max_failing_rows=5`, assert output `failing_rows` list has exactly 5 entries.
- AC4.3: Create a StepResult with `check_id=None` and `severity=None`, assert serialised dict contains both keys with `None` values (not omitted, not KeyError).
- AC5.1: Assert `json.dumps(serialise_run(...))` succeeds without TypeError, confirming all values are JSON-serialisable.

**tests/test_dashboard.py (Phases 3-5 — integration tests)**
- AC1.1: Render dashboard with known inputs, extract embedded JSON from `index.html`, assert `summary.total_checks`, `summary.total_failures`, and `summary.by_severity.pass` match expected counts. Assert HTML contains `stat-total-checks`, `stat-pass-rate`, `stat-total-failures` element IDs.
- AC1.4: Render dashboard where all steps have `n_failed=0`, assert embedded JSON has `total_failures: 0` and `by_severity.pass` equals total checks.
- AC1.5: Render dashboard with only a `cross_table` entry (`total_rows=0`, `chunks_processed=0`), assert `index.html` renders without error.
- AC2.1: Assert rendered `index.html` contains `table_key + ".html"` as a link target or in the Tabulator `rowClick` handler for each table.
- AC2.2: Extract embedded JSON from detail page, assert `validation.total_rows` and `validation.chunks_processed` match input ValidationResult values.
- AC2.3: Assert embedded JSON `validation.steps` entries contain all StepResult fields: `check_id`, `assertion_type`, `column`, `description`, `n_passed`, `n_failed`, `pass_rate`, `severity`. Assert field values match input.
- AC2.9: Render detail page where all steps have `n_failed=0`, assert HTML renders without error and all step `failing_rows` in embedded JSON are empty lists.
- AC2.10: Render detail page for `cross_table` with `ProfilingResult(columns=())`, assert embedded JSON `profiling.columns` is `[]`.
- AC3.1: Render dashboard with both L1 and cross-table results, assert `index.html` embedded JSON `tables` dict contains `"cross_table"` key and `cross_table.html` is created.
- AC5.2: Assert rendered HTML does not contain `https://unpkg.com`, `https://cdn.`, or other CDN URL patterns. Assert HTML contains `new Tabulator` and `Plotly.newPlot` (confirming inlined JS).
- AC5.3: Assert rendered HTML contains no `<link rel="stylesheet" href="http` or `<script src="http` tags (no external resource fetches required). This is the automatable portion; actual offline browser rendering is verified manually.

**tests/test_reporting.py (Phase 6 — integration tests, rewritten)**
- AC3.2: Render dashboard with cross-table results, assert `cross_table.html` contains same Tabulator column structure (`validation-table` div, `headerFilter`, `download("csv"`) as L1 detail pages.

**tests/test_pipeline_phases.py (Phase 6 — integration tests, updated)**
- AC6.1: Assert serve command exists and accepts an output directory argument (no filename assumptions beyond static file serving). Verify `index.html` is produced by the pipeline (the serve command's expected entry point).
- AC6.2: Run pipeline integration test, assert output directory contains `index.html` and per-table `{table_key}.html` files with embedded JSON `dashboard-data` script blocks.

## Human Verification

| AC ID | Description | Justification | Verification Approach |
|-------|-------------|---------------|----------------------|
| qa-dashboard.AC1.2 | Plotly donut chart shows pass/warn/fail/note breakdown by check count with correct proportions | Visual rendering — Python tests can verify the JSON data and the presence of the `Plotly.newPlot` call, but cannot verify that the donut chart renders with correct proportions, colours, and labels in a browser. | 1. Run `scdm-qa run` against a dataset with mixed pass/warn/fail/note results. 2. Open `index.html` in a browser. 3. Verify the donut chart displays four segments (Pass, Fail, Warn, Note) with proportions matching the summary counts. 4. Hover over each segment and confirm the tooltip shows correct label, percentage, and count. |
| qa-dashboard.AC1.3 | Plotly horizontal bar chart shows pass rate per table, sorted worst-first, colour-scaled red-green | Visual rendering — automated tests can verify JSON data contains per-table pass rates, but cannot verify sort order is visually correct, that the colour gradient renders as red-to-green, or that bar lengths are proportional. | 1. Run `scdm-qa run` against a dataset with at least 3 tables having different pass rates. 2. Open `index.html` in a browser. 3. Verify bars are sorted worst-first (lowest pass rate at top). 4. Verify colour gradient: tables near 0% are red, near 50% are yellow, near 100% are green. 5. Verify bar lengths are proportional to pass rate values. |
| qa-dashboard.AC2.4 | Header filters on each column allow filtering by severity, assertion type, column name, and free text on description | Interactive behaviour — automated tests can verify the `headerFilter: true` property exists in the Tabulator config, but cannot verify the filters actually work in a browser (typing filters rows, dropdowns appear, etc.). | 1. Open a detail page in a browser. 2. Type in the Severity header filter — verify rows filter to matching severity. 3. Type in the Type header filter — verify rows filter to matching assertion type. 4. Type in the Column header filter — verify rows filter to matching column name. 5. Type a partial string in the Description header filter — verify free-text filtering works. 6. Combine multiple filters — verify they compose correctly. |
| qa-dashboard.AC2.5 | CSV download button exports the current filtered/sorted view of the validation table | Interactive behaviour — automated tests can verify the download button exists and the `download("csv"` call is present, but cannot verify the actual file download triggers correctly or that the CSV content matches the filtered view. | 1. Open a detail page in a browser. 2. Click "Download CSV" with no filters — verify a CSV file downloads containing all validation check rows. 3. Apply a severity filter (e.g., "Fail"), click "Download CSV" again — verify the downloaded CSV contains only the filtered rows. 4. Sort by a column, download — verify sort order is preserved in CSV. |
| qa-dashboard.AC2.6 | Profiling table shows column stats with completeness below 95% highlighted in red | Visual rendering — automated tests can verify the `low-completeness` CSS class is defined and that completeness values below 95% exist in the JSON, but cannot verify the red highlighting actually renders visually. | 1. Run `scdm-qa run` against a dataset where at least one column has completeness below 95%. 2. Open the detail page for that table. 3. Verify the profiling table is visible with columns: Column, Type, Completeness, Distinct, Min, Max. 4. Verify cells with completeness below 95% have a red/pink background. 5. Verify cells at or above 95% have no special highlighting. |
| qa-dashboard.AC2.7 | Failing row sections are collapsible, one per failing check, showing sample rows in a Tabulator table | Interactive behaviour — automated tests can verify the HTML structure includes `collapsible-header` elements and that failing row data exists in the JSON, but cannot verify the collapse/expand interaction works in a browser. | 1. Open a detail page for a table with failing checks. 2. Verify each failing check has its own collapsible section with the check label and failure count. 3. Click a section header — verify it expands to show a Tabulator table of sample failing rows. 4. Click the header again — verify it collapses. 5. Verify multiple sections can be independently expanded/collapsed. |
| qa-dashboard.AC2.8 | Each failing row section has its own CSV download button | Interactive behaviour — automated tests can verify the button elements exist in the HTML structure, but cannot verify each button downloads the correct CSV for its specific failing check section. | 1. Open a detail page with multiple failing checks. 2. Expand the first failing row section. 3. Click its CSV download button — verify a CSV downloads with rows from that specific check only. 4. Expand a second failing row section. 5. Click its CSV download button — verify a different CSV downloads with rows from that check. 6. Verify filenames include the check label. |
