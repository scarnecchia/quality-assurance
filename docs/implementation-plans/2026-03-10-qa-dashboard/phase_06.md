# QA Dashboard Implementation Plan — Phase 6: Pipeline Integration & Migration

**Goal:** Wire the new dashboard into the pipeline, replace per-table reporting with a single `save_dashboard()` call, and remove old reporting code.

**Architecture:** Modify `pipeline.py` to collect `(ValidationResult, ProfilingResult)` tuples from all L1 and L2 outcomes, then call `save_dashboard()` once after all validation completes. Remove `builder.py` and `index.py`. Update `__init__.py` exports. Adapt existing tests to the new reporting interface.

**Tech Stack:** Python (pipeline wiring, test updates)

**Scope:** 6 phases from original design (phase 6 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC3: L1 and L2 combined
- **qa-dashboard.AC3.2 Success:** Cross-table detail page shows L2 check results in the same Tabulator format as L1 detail pages

### qa-dashboard.AC6: Backward compatibility
- **qa-dashboard.AC6.1 Success:** `scdm-qa serve <output-dir>` serves the new dashboard files without changes to the serve command
- **qa-dashboard.AC6.2 Success:** `scdm-qa run <config.toml>` produces new dashboard files in the configured output_dir

---

<!-- START_TASK_1 -->
### Task 1: Modify pipeline.py to use save_dashboard

**Verifies:** qa-dashboard.AC6.2

**Files:**
- Modify: `src/scdm_qa/pipeline.py`

**Implementation:**

The key change: instead of calling `save_table_report()` per table and `save_index()` at the end, collect all results and call `save_dashboard()` once.

1. **Replace imports** at the top of `pipeline.py`:

   Remove:
   ```python
   from scdm_qa.reporting.builder import save_table_report
   from scdm_qa.reporting.index import ReportSummary, make_report_summary, save_index
   ```

   Add:
   ```python
   from scdm_qa.reporting.dashboard import save_dashboard
   ```

2. **Replace `report_summaries` with `dashboard_results`** in `run_pipeline()`:

   Remove line 56:
   ```python
   report_summaries: list[ReportSummary] = []
   ```

   Add:
   ```python
   dashboard_results: list[tuple[ValidationResult, ProfilingResult]] = []
   ```

3. **In the L1 per-table loop** (currently lines 71-110), replace the `save_table_report` and `make_report_summary` calls:

   Where currently:
   ```python
   if outcome.validation_result and outcome.profiling_result:
       save_table_report(...)
       report_summaries.append(make_report_summary(...))
   elif outcome.profiling_result:
       empty_vr = ValidationResult(...)
       save_table_report(...)
       report_summaries.append(make_report_summary(...))
   ```

   Replace with:
   ```python
   if outcome.validation_result and outcome.profiling_result:
       dashboard_results.append((outcome.validation_result, outcome.profiling_result))
   elif outcome.profiling_result:
       empty_vr = ValidationResult(
           table_key=table_key,
           table_name=outcome.profiling_result.table_name,
           steps=(),
           total_rows=outcome.profiling_result.total_rows,
           chunks_processed=0,
       )
       dashboard_results.append((empty_vr, outcome.profiling_result))
   ```

4. **In the L2 cross-table section** (currently lines 146-167), replace the `save_table_report` and `make_report_summary` calls:

   Where currently:
   ```python
   empty_profiling = ProfilingResult(...)
   save_table_report(...)
   report_summaries.append(make_report_summary(...))
   ```

   Replace with:
   ```python
   empty_profiling = ProfilingResult(
       table_key="cross_table",
       table_name="Cross-Table Checks",
       total_rows=0,
       columns=(),
   )
   dashboard_results.append((cross_table_vr, empty_profiling))
   ```

5. **Replace the final `save_index()` call** (currently line 173-174):

   Where currently:
   ```python
   if report_summaries:
       save_index(config.output_dir, report_summaries)
   ```

   Replace with:
   ```python
   if dashboard_results:
       save_dashboard(
           config.output_dir,
           dashboard_results,
           max_failing_rows=config.max_failing_rows,
       )
   ```

The `ProfilingResult` import already exists (line 10). The `ValidationResult` import already exists (line 27). No new imports needed beyond `save_dashboard`.

**Verification:**
Run: `python -c "from scdm_qa.pipeline import run_pipeline; print('ok')"`
Expected: `ok`

**Commit:** `feat: wire save_dashboard into pipeline, replacing per-table reporting`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update reporting __init__.py exports

**Files:**
- Modify: `src/scdm_qa/reporting/__init__.py`

**Implementation:**

Update the reporting package's public API. Remove old exports and keep the new ones.

Current `__init__.py`:
```python
from scdm_qa.reporting.builder import save_table_report
from scdm_qa.reporting.index import make_report_summary, save_index

__all__ = ["save_table_report", "save_index", "make_report_summary"]
```

Replace with:
```python
from scdm_qa.reporting.dashboard import save_dashboard
from scdm_qa.reporting.serialise import serialise_run

__all__ = ["save_dashboard", "serialise_run"]
```

**Verification:**
Run: `python -c "from scdm_qa.reporting import save_dashboard, serialise_run; print('ok')"`
Expected: `ok`

**Commit:** `refactor: update reporting package exports for dashboard`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Remove old reporting modules

**Files:**
- Delete: `src/scdm_qa/reporting/builder.py`
- Delete: `src/scdm_qa/reporting/index.py`

**Implementation:**

Remove the two old reporting files that are no longer imported:

```bash
git rm src/scdm_qa/reporting/builder.py
git rm src/scdm_qa/reporting/index.py
```

Before removing, verify nothing else imports them:

```bash
grep -r "from scdm_qa.reporting.builder" src/ tests/
grep -r "from scdm_qa.reporting.index" src/ tests/
```

Expected: Only `test_reporting.py` imports from these (which will be rewritten in Task 5). `pipeline.py` no longer imports them (changed in Task 1). `__init__.py` no longer imports them (changed in Task 2).

**Verification:**
Run: `python -c "import scdm_qa.reporting; print('ok')"`
Expected: `ok` (package still imports, just with new exports)

**Commit:** `refactor: remove old great-tables reporting modules`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Remove great-tables from pyproject.toml dependencies

**Files:**
- Modify: `pyproject.toml`

**Implementation:**

Remove `great-tables>=0.21,<1` from the `[project] dependencies` list in `pyproject.toml`. The design explicitly states "great-tables is removed as a direct dependency." pointblank still pulls it transitively, but our code no longer imports it.

In `pyproject.toml`, remove this line from the dependencies array:
```toml
    "great-tables>=0.21,<1",
```

**Verification:**
Run: `uv sync`
Expected: Syncs without errors (great-tables still installed transitively via pointblank)

Run: `grep great-tables pyproject.toml`
Expected: No matches

**Commit:** `chore: remove great-tables from direct dependencies`

<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Rewrite test_reporting.py for new dashboard

**Verifies:** qa-dashboard.AC3.2, qa-dashboard.AC6.2

**Files:**
- Modify: `tests/test_reporting.py` (full rewrite)

**Implementation:**

The existing `test_reporting.py` tests `save_table_report`, `save_index`, and `make_report_summary` — all of which are removed. Rewrite to test the new `save_dashboard` interface.

Replace all imports:
```python
from scdm_qa.reporting.dashboard import save_dashboard
from scdm_qa.reporting.serialise import serialise_run
```

Keep the `_make_validation_result` and `_make_profiling_result` helper factories (same pattern, updated to include `check_id` and `severity`).

Rewrite test classes to cover:

**TestSaveDashboard:**
- `test_creates_index_and_detail_files` — produces `index.html` and `{table_key}.html`
- `test_html_contains_dashboard_data_script` — embedded JSON in `<script type="application/json">` block
- `test_html_self_contained` — no external CDN links (`https://` not in HTML for JS/CSS resources)

**TestSaveDashboard__CrossTable:**
- `test_cross_table_detail_page_created` — `cross_table.html` exists with L2 check data
- `test_cross_table_same_format` — cross-table detail page contains same Tabulator structure as L1 pages (AC3.2)
- `test_cross_table_no_profiling` — cross-table detail page has empty profiling columns in JSON

**TestSaveDashboard__EdgeCases:**
- `test_empty_results` — empty results list produces no output files (or just an empty dashboard)
- `test_all_passing` — all checks pass, index shows 100% pass rate
- `test_with_failing_rows` — failing rows appear in detail page JSON

**TestSaveDashboard__ProfileOnly:**
- `test_profile_only_produces_dashboard` — When results contain only profiling data (empty ValidationResult with `steps=()`), `save_dashboard` still produces valid HTML files. This covers the `scdm-qa profile` command path where `profile_only=True`.

These tests replace and supersede the existing `TestSaveTableReport`, `TestSaveTableReport__CheckID`, `TestSaveIndex`, and `TestSaveTableReport__EmptyProfiling` classes.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_reporting.py -v`
Expected: All tests pass

**Commit:** `test: rewrite test_reporting.py for dashboard interface`

<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Update pipeline tests for new reporting

**Verifies:** qa-dashboard.AC6.1, qa-dashboard.AC6.2

**Files:**
- Modify: `tests/test_pipeline_phases.py` (update reporting assertions)

**Implementation:**

The `TestCrossTableReporting` class (lines 586-707) checks for specific HTML content from the old great-tables output. The new dashboard produces different HTML structure but the same semantic content. Update assertions:

1. `test_cross_table_report_file_created` (line 616):
   - Still checks `cross_table.html` exists — OK, same filename
   - Checks `"Cross-Table Checks" in html` — OK, still present in new template
   - Checks `"Validation" in html` — May need to change to `"Validation Checks"` or just `"validation"` depending on new template. The detail template has `<h2>Validation Checks</h2>`, so update assertion to match.

2. `test_cross_table_entry_in_index` (line 648):
   - Checks `index.html` exists — OK
   - Checks `"Cross-Table Checks" in html` — OK, present in JSON data
   - Checks `"cross_table.html" in html` — OK, present in Tabulator rowClick handler
   - Checks `"1" in html` — Fragile assertion, but still true (failure count in JSON)

3. `test_cross_table_no_profiling_section` (line 680):
   - Checks `"<h2>Data Profile</h2>" not in html` — The new template always includes the profiling div (hidden by JS when columns are empty). Update assertion to verify profiling columns are empty in the embedded JSON:
   ```python
   import json
   import re

   html = report_path.read_text()
   match = re.search(
       r'<script type="application/json" id="dashboard-data">(.*?)</script>',
       html, re.DOTALL,
   )
   data = json.loads(match.group(1))
   assert data["profiling"]["columns"] == []
   ```

The implementor should read the actual HTML output of the new dashboard to verify which assertions need adjustment. The key is: test the same semantic behavior, not the exact HTML structure.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_pipeline_phases.py -v`
Expected: All tests pass

**Commit:** `test: update pipeline tests for new dashboard reporting`

<!-- END_TASK_6 -->

<!-- START_TASK_7 -->
### Task 7: Verify serve command works with new dashboard

**Verifies:** qa-dashboard.AC6.1

**Files:**
- No file changes expected

**Implementation:**

The `scdm-qa serve` command serves static files from the output directory. Read `src/scdm_qa/cli.py` to verify the serve command implementation. It should use Python's `http.server` or similar to serve the directory contents. Since the new dashboard produces the same file types (HTML files in the same output directory), the serve command should work without changes.

Verify by:
1. Read `cli.py` serve command implementation
2. Confirm it just serves static files from a directory (no filename assumptions)
3. If it hardcodes `index.html` as the default page, confirm the new dashboard still produces `index.html`

If the serve command makes no assumptions about filenames beyond serving a directory, no changes are needed.

**Verification:**
Run: `uv run pytest -v` (full test suite)
Expected: All tests pass

Run: `uv run scdm-qa serve --help`
Expected: Shows help for serve command, confirming it exists and works

**Commit:** (no commit needed if no changes required — skip this step)

<!-- END_TASK_7 -->

<!-- START_TASK_8 -->
### Task 8: Full test suite verification

**Files:**
- No file changes

**Implementation:**

Run the complete test suite to verify nothing is broken by the migration.

**Verification:**
Run: `uv run pytest -v`
Expected: All 316+ tests pass (existing tests + new dashboard tests)

If any tests fail, investigate and fix before committing. Common issues:
- Imports referencing removed modules (`builder`, `index`)
- HTML assertions checking old great-tables output format
- Missing vendor assets in test environment

**Commit:** (no commit unless fixes needed)

<!-- END_TASK_8 -->
