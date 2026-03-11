# QA Dashboard Design

## Summary

The SCDM QA tool currently generates HTML reports using `great-tables` — a Python-first library that produces static, non-interactive output. This design replaces those reports with a self-contained interactive dashboard: one index page and one detail page per validated table, all generated at the end of each pipeline run.

The approach keeps generation fully server-side (Python + Jinja2 templates) while moving all interactivity client-side. Validation and profiling results are first serialised to a versioned JSON schema, then injected directly into HTML files as embedded `<script>` blocks. Two JavaScript libraries — Tabulator for filterable/sortable/exportable data tables and Plotly for charts — are vendored and inlined into each file, making the output completely self-contained: no network access required, shareable as a zip. The implementation is phased across six stages: serialisation layer → vendor assets and base template → index page → detail page validation checks → profiling and failing-row drill-down → pipeline wiring and removal of old reporting code.

## Definition of Done

Replace the current hand-rolled great-tables HTML reports with a modern interactive dashboard built on static HTML + embedded JSON data + Tabulator + Plotly.js.

1. **Replace current HTML reports** with a modern interactive dashboard using static HTML + embedded JSON data + Tabulator + Plotly.js
2. **Index page** has a health scorecard (overall pass/fail/warn counts, pass-rate donut, severity breakdown) plus a table-by-table summary grid with click-through to detail pages
3. **Detail pages** show per-table validation checks (filterable/sortable), profiling stats, charts, and failing row drill-down with CSV export
4. **L1 and L2 results combined** in one unified dashboard (cross-table checks appear alongside per-table checks)
5. **Self-contained** — all JS/CSS bundled inline, no external CDN data calls, shareable as a zip of HTML files
6. **Existing `scdm-qa serve` command** continues to work (just serves the new files)

Out of scope for this design (future work): Streamlit, trend analysis across runs, pointblank native reporting. However, the JSON data schema should be stable and versioned to support cross-run comparison later.

## Acceptance Criteria

### qa-dashboard.AC1: Index page scorecard and charts
- **qa-dashboard.AC1.1 Success:** Index page displays total check count, overall pass rate, and total failure count as stat cards
- **qa-dashboard.AC1.2 Success:** Plotly donut chart shows pass/warn/fail/note breakdown by check count with correct proportions
- **qa-dashboard.AC1.3 Success:** Plotly horizontal bar chart shows pass rate per table, sorted worst-first, colour-scaled red→green
- **qa-dashboard.AC1.4 Failure:** Index page renders correctly when all checks pass (zero failures, donut shows 100% pass)
- **qa-dashboard.AC1.5 Edge:** Index page renders correctly when only cross-table checks exist (no L1 tables)

### qa-dashboard.AC2: Detail page validation checks
- **qa-dashboard.AC2.1 Success:** Clicking a table row in the index grid navigates to the correct detail page
- **qa-dashboard.AC2.2 Success:** Detail page header shows table name, total rows, chunks processed, overall pass rate
- **qa-dashboard.AC2.3 Success:** Tabulator validation table displays all StepResult fields (check_id, assertion_type, column, description, n_passed, n_failed, pass_rate, severity)
- **qa-dashboard.AC2.4 Success:** Header filters on each column allow filtering by severity, assertion type, column name, and free text on description
- **qa-dashboard.AC2.5 Success:** CSV download button exports the current filtered/sorted view of the validation table
- **qa-dashboard.AC2.6 Success:** Profiling table shows column stats with completeness below 95% highlighted in red
- **qa-dashboard.AC2.7 Success:** Failing row sections are collapsible, one per failing check, showing sample rows in a Tabulator table
- **qa-dashboard.AC2.8 Success:** Each failing row section has its own CSV download button
- **qa-dashboard.AC2.9 Failure:** Detail page renders correctly when a table has zero failures (no failing row sections shown)
- **qa-dashboard.AC2.10 Edge:** Detail page for cross_table has no profiling section (ProfilingResult is empty)

### qa-dashboard.AC3: L1 and L2 combined
- **qa-dashboard.AC3.1 Success:** Cross-table checks appear as a table entry in the index grid alongside L1 per-table entries
- **qa-dashboard.AC3.2 Success:** Cross-table detail page shows L2 check results in the same Tabulator format as L1 detail pages

### qa-dashboard.AC4: JSON serialisation
- **qa-dashboard.AC4.1 Success:** JSON output includes schema_version field set to "1.0"
- **qa-dashboard.AC4.2 Success:** Failing rows in JSON are truncated to max_failing_rows limit
- **qa-dashboard.AC4.3 Edge:** Serialisation handles null check_id and null severity gracefully (renders as empty/dash)

### qa-dashboard.AC5: Self-contained HTML
- **qa-dashboard.AC5.1 Success:** JSON data embedded in `<script type="application/json">` blocks, not external files
- **qa-dashboard.AC5.2 Success:** Tabulator JS+CSS and Plotly JS are inlined in each HTML file (no CDN links)
- **qa-dashboard.AC5.3 Success:** HTML files open correctly in a browser without network access

### qa-dashboard.AC6: Backward compatibility
- **qa-dashboard.AC6.1 Success:** `scdm-qa serve <output-dir>` serves the new dashboard files without changes to the serve command
- **qa-dashboard.AC6.2 Success:** `scdm-qa run <config.toml>` produces new dashboard files in the configured output_dir

## Glossary

- **StepResult**: A frozen dataclass representing the outcome of a single validation check step — carries fields like `check_id`, `severity`, `n_passed`, `n_failed`, and `pass_rate`.
- **ValidationResult**: The aggregate output of all validation steps run against a single table, composed of a list of `StepResult` entries.
- **ProfilingResult**: Per-column statistics collected during a validation run (completeness, distinct count, min/max, type). Empty for cross-table checks.
- **L1 / L2**: Validation tiers. L1 is per-table (format, type, completeness, code checks). L2 is cross-table (referential integrity, consistency between tables).
- **cross_table**: The synthetic table key used in the dashboard to group all L2 cross-table check results as if they were a single table entry.
- **Tabulator**: An open-source JavaScript library for interactive, filterable, sortable, exportable data tables. Replaces `great-tables` for the client-side rendering layer.
- **Plotly.js**: A JavaScript charting library used here for the pass-rate donut and per-table horizontal bar charts on the index page.
- **great-tables**: The Python library currently used to render validation and profiling data as static HTML tables. Being removed as a direct dependency in this design.
- **Jinja2**: A Python templating engine. Already used in the codebase for the index page; this design extends it to all dashboard templates.
- **Vendored assets**: Third-party JS/CSS files downloaded once, committed to the repo, and inlined into HTML at generation time — eliminating CDN dependencies.
- **`schema_version`**: A field in the JSON output (`"1.0"`) that allows future tooling to detect and handle format changes, particularly for cross-run trend analysis.
- **`save_dashboard()`**: The new reporting entry point exposed by `dashboard.py`, called once by the pipeline after all L1 and L2 validation completes.
- **`serialise_run()`**: The top-level serialisation function in `serialise.py` that converts all `ValidationResult` + `ProfilingResult` pairs into a single JSON-serialisable dict.
- **Data injection pattern**: The technique of embedding JSON data inside `<script type="application/json">` tags in HTML, then reading and parsing it client-side — avoids separate data fetches and keeps files self-contained.
- **pointblank**: A Python validation library used to express validation rules. Its native reporting is explicitly out of scope for this design.
- **PackageLoader**: A Jinja2 loader that reads templates from within an installed Python package, used here so templates are included in the distributed build.

## Architecture

Static HTML dashboard generated by Python at the end of the validation pipeline. Each report run produces a set of HTML files in the output directory: one index page and one detail page per validated table (including cross-table checks). All interactivity is client-side via embedded JavaScript.

**Generation flow:**

1. Pipeline completes L1 (per-table) and L2 (cross-table) validation, collecting `ValidationResult` + `ProfilingResult` pairs
2. `save_dashboard()` serialises all results to JSON via standalone `to_dict()` functions in `serialise.py`
3. Jinja2 templates render HTML with JSON embedded in `<script type="application/json">` blocks
4. Vendored JS/CSS assets (Tabulator, Plotly) are read from `vendor/` and inlined into each HTML file
5. Files written to output directory, ready for `scdm-qa serve` or direct browser access

**Key components:**

- **`src/scdm_qa/reporting/serialise.py`** — Converts frozen dataclasses (`ValidationResult`, `ProfilingResult`, `StepResult`, `ColumnProfile`) to JSON-serialisable dicts. Handles `pl.DataFrame` → list-of-dicts conversion for failing rows. Adds `schema_version` field for future cross-run compatibility.
- **`src/scdm_qa/reporting/dashboard.py`** — Replaces `builder.py` as the main reporting entry point. Loads Jinja2 templates, reads vendor assets, renders HTML files. Exposes `save_dashboard(output_dir, results, config)` called by pipeline.
- **`src/scdm_qa/reporting/templates/`** — Jinja2 templates: `base.html` (shared layout with inlined assets), `index.html` (scorecard + table grid), `detail.html` (per-table checks, profiling, failing rows).
- **`src/scdm_qa/reporting/vendor/`** — Vendored third-party assets: `tabulator.min.js` (~120KB), `tabulator.min.css` (~50KB), `plotly-basic.min.js` (~1.1MB). Version-tracked via `VERSIONS.md`.

**Data flow:**

```
Pipeline
  ├─ L1 per-table: ValidationResult + ProfilingResult (per table)
  ├─ L2 cross-table: ValidationResult (table_key="cross_table")
  └─ calls save_dashboard()
       ├─ serialise_run() → full JSON (summary + per-table data)
       ├─ render index.html (summary JSON, no failing rows)
       └─ render {table_key}.html (per-table JSON with failing rows)
```

**Index page layout:**
- Health scorecard header: three stat cards (total checks, overall pass rate, total failures) + Plotly donut chart (pass/warn/fail/note breakdown by check count)
- Plotly horizontal bar chart: pass rate per table, sorted worst-first, colour-scaled red→green
- Tabulator summary grid: one row per table with [Table, Rows, Checks, Failures, Pass Rate, Worst Severity]. Rows link to detail pages. Sortable and filterable.

**Detail page layout:**
- Header: table name, total rows, chunks processed, overall pass rate for this table
- Validation checks: Tabulator table with columns [Check ID, Assertion Type, Column, Description, Passed, Failed, Pass Rate, Severity]. Header filters on each column. Severity-based row colouring. CSV download button.
- Profiling: Tabulator table with columns [Column, Type, Completeness, Distinct Count, Min, Max]. Completeness below 95% highlighted.
- Failing rows: collapsible sections per failing check, each containing a Tabulator table of the sample rows + CSV download. Only rendered for checks with `n_failed > 0`.

**Data injection pattern:** Each template contains a `<script type="application/json" id="dashboard-data">` block. Python serialises the appropriate JSON and Jinja2 injects it. Client-side JS reads via `JSON.parse(document.getElementById('dashboard-data').textContent)` and hydrates all Tabulator tables and Plotly charts.

## Existing Patterns

Investigation found the current reporting module at `src/scdm_qa/reporting/`:

- **`builder.py`** — builds HTML via great-tables `GT` objects + string concatenation. No templates. Functions: `build_validation_table()`, `build_profiling_table()`, `build_failing_rows_table()`, `save_table_report()`.
- **`index.py`** — uses a single inline Jinja2 template string (`_INDEX_TEMPLATE`) for the index page. `ReportSummary` is a `TypedDict`.

**Patterns followed by this design:**
- Jinja2 is already a dependency and used for the index page — this design extends that to all templates
- `ReportSummary` TypedDict pattern is reasonable for template data — the new JSON schema serves the same role but is richer
- Pipeline calls reporting after validation completes — same pattern, just deferred to after all tables finish

**Patterns diverged from:**
- great-tables `GT` objects replaced by Tabulator.js — GT lacks interactivity (no filtering, sorting, or export) and doesn't support the dashboard UX we need
- String concatenation for HTML replaced by proper Jinja2 templates — the current `builder.py` mixes Python logic with HTML fragments, which doesn't scale to a full dashboard
- Per-table-as-it-finishes reporting replaced by all-at-once — the index page needs complete summary stats (donut chart, overall counts) which require all tables to have finished

<!-- START_PHASE_1 -->
## Implementation Phases

### Phase 1: Data Serialisation Layer
**Goal:** Convert validation and profiling results to a versioned JSON schema

**Components:**
- `src/scdm_qa/reporting/serialise.py` — `serialise_step()`, `serialise_validation()`, `serialise_profiling()`, `serialise_run()` functions that convert frozen dataclasses to JSON-serialisable dicts
- JSON schema version `"1.0"` with top-level structure: `schema_version`, `generated_at`, `tables` (keyed by table_key), `summary` (aggregate counts by severity)

**Dependencies:** None (first phase)

**Done when:** Unit tests verify correct JSON output for ValidationResult, ProfilingResult, StepResult, ColumnProfile including edge cases (empty tables, all-pass, all-fail, null check_ids, failing rows truncation). Covers `qa-dashboard.AC4.1`, `qa-dashboard.AC4.2`, `qa-dashboard.AC5.1`.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Vendor Assets & Base Template
**Goal:** Set up vendored JS/CSS and the shared HTML template

**Components:**
- `src/scdm_qa/reporting/vendor/tabulator.min.js`, `tabulator.min.css`, `plotly-basic.min.js` — downloaded and committed
- `src/scdm_qa/reporting/vendor/VERSIONS.md` — tracks vendored versions
- `src/scdm_qa/reporting/templates/base.html` — shared layout template that inlines vendor assets via Jinja2 blocks
- `pyproject.toml` — package-data config to include `templates/` and `vendor/` in builds

**Dependencies:** None (parallel with Phase 1)

**Done when:** `uv build` includes template and vendor files in the package. Base template renders valid HTML with inlined Tabulator and Plotly assets. Covers `qa-dashboard.AC5.2`, `qa-dashboard.AC5.3`.
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Index Page Dashboard
**Goal:** Build the index page with scorecard, charts, and table summary grid

**Components:**
- `src/scdm_qa/reporting/templates/index.html` — extends base template. Health scorecard (stat cards + Plotly donut), per-table bar chart, Tabulator summary grid with click-through links
- `src/scdm_qa/reporting/dashboard.py` — `save_dashboard()` entry point (initially renders index only). Loads templates via `jinja2.PackageLoader`, reads vendor assets, injects summary JSON
- Dashboard CSS (in base template or separate) — grid layout, severity colour tokens, responsive sizing

**Dependencies:** Phase 1 (serialisation), Phase 2 (base template + vendor assets)

**Done when:** Index page renders with embedded JSON, displays scorecard with correct aggregate counts, shows donut chart and bar chart, summary grid is filterable/sortable and links to detail pages. Covers `qa-dashboard.AC1.1` through `qa-dashboard.AC1.5`, `qa-dashboard.AC2.1`.
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Detail Page — Validation Checks
**Goal:** Build the per-table detail page with interactive validation check table

**Components:**
- `src/scdm_qa/reporting/templates/detail.html` — extends base template. Table header, Tabulator validation checks table with header filters, severity row colouring, CSV download
- `src/scdm_qa/reporting/dashboard.py` — extended to render one detail page per table key

**Dependencies:** Phase 3 (dashboard.py scaffolding, base template)

**Done when:** Detail pages render with all StepResult data. Tabulator table supports filtering by severity/assertion type/column, sorting on all columns, CSV export. Severity-based row colouring works. Covers `qa-dashboard.AC2.2` through `qa-dashboard.AC2.5`, `qa-dashboard.AC3.1`.
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Detail Page — Profiling & Failing Rows
**Goal:** Add profiling stats and failing row drill-down to detail pages

**Components:**
- `src/scdm_qa/reporting/templates/detail.html` — extended with profiling Tabulator table and collapsible failing row sections
- Profiling table: columns [Column, Type, Completeness, Distinct Count, Min, Max] with low-completeness highlighting
- Failing rows: one collapsible section per failing check, each with Tabulator table + CSV download. Only rendered when `n_failed > 0`.

**Dependencies:** Phase 4 (detail page scaffolding)

**Done when:** Profiling data displays correctly with completeness highlighting. Failing row sections expand/collapse, show correct sample data, and export to CSV. Covers `qa-dashboard.AC2.6`, `qa-dashboard.AC2.7`, `qa-dashboard.AC2.8`.
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Pipeline Integration & Migration
**Goal:** Wire the new dashboard into the pipeline and remove old reporting code

**Components:**
- `src/scdm_qa/pipeline.py` — replace `save_table_report()` + `save_index()` calls with single `save_dashboard()` call after all L1+L2 complete
- `src/scdm_qa/reporting/__init__.py` — export `save_dashboard` instead of old functions
- `src/scdm_qa/reporting/builder.py` — removed
- `src/scdm_qa/reporting/index.py` — removed (ReportSummary TypedDict no longer needed)
- `tests/test_reporting.py` — rewritten for new dashboard module
- `tests/test_pipeline.py` — updated reporting assertions

**Dependencies:** Phase 5 (complete dashboard rendering)

**Done when:** `uv run scdm-qa run config.toml` produces new dashboard HTML files. `scdm-qa serve` serves them. Old reporting code is removed. All existing tests pass with updated assertions. Covers `qa-dashboard.AC3.2`, `qa-dashboard.AC6.1`, `qa-dashboard.AC6.2`.
<!-- END_PHASE_6 -->

## Additional Considerations

**Future trend analysis:** The JSON schema includes `schema_version` and `generated_at` specifically to enable cross-run comparison. A future design can read multiple JSON outputs and render trend charts without changing the serialisation format.

**File sizes:** Each HTML file is ~1.3MB (dominated by inlined Plotly.js). For 14 tables + index + cross_table = 16 files ≈ 21MB uncompressed. A zip compresses to ~2-3MB due to Plotly deduplication across files.

**great-tables removal:** `great-tables` is removed as a direct dependency. pointblank still pulls it in transitively, but our reporting code no longer imports it.
