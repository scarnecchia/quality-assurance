# QA Dashboard Implementation Plan — Phase 4: Detail Page — Validation Checks

**Goal:** Build the per-table detail page with interactive validation check table, header filters, severity colouring, and CSV export.

**Architecture:** Create `detail.html` Jinja2 template extending `base.html`. Extend `save_dashboard()` in `dashboard.py` to render one detail page per table key with per-table JSON embedded. The detail page uses Tabulator for the validation checks table with header filters on each column and CSV download.

**Tech Stack:** Python (Jinja2, serialise.py), JavaScript (Tabulator with headerFilter, CSV download, rowFormatter)

**Scope:** 6 phases from original design (phase 4 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC2: Detail page validation checks
- **qa-dashboard.AC2.2 Success:** Detail page header shows table name, total rows, chunks processed, overall pass rate
- **qa-dashboard.AC2.3 Success:** Tabulator validation table displays all StepResult fields (check_id, assertion_type, column, description, n_passed, n_failed, pass_rate, severity)
- **qa-dashboard.AC2.4 Success:** Header filters on each column allow filtering by severity, assertion type, column name, and free text on description
- **qa-dashboard.AC2.5 Success:** CSV download button exports the current filtered/sorted view of the validation table

### qa-dashboard.AC3: L1 and L2 combined
- **qa-dashboard.AC3.1 Success:** Cross-table checks appear as a table entry in the index grid alongside L1 per-table entries

---

<!-- START_TASK_1 -->
### Task 1: Create detail.html template

**Files:**
- Create: `src/scdm_qa/reporting/templates/detail.html`

**Implementation:**

Create `detail.html` extending `base.html`. The template receives context variables:
- `page_title` (str): the table name
- `dashboard_json` (str): raw JSON string of per-table data (validation + profiling + failing rows)

Template structure:

```html
{% extends "base.html" %}

{% block content %}
<a href="index.html" class="back-link">← Back to Index</a>

<h1>{{ page_title }}</h1>

<!-- Header stats -->
<div class="stat-cards">
    <div class="stat-card">
        <div class="label">Total Rows</div>
        <div class="value" id="stat-total-rows">—</div>
    </div>
    <div class="stat-card">
        <div class="label">Chunks Processed</div>
        <div class="value" id="stat-chunks">—</div>
    </div>
    <div class="stat-card">
        <div class="label">Pass Rate</div>
        <div class="value" id="stat-pass-rate">—</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Checks</div>
        <div class="value" id="stat-checks">—</div>
    </div>
</div>

<!-- Validation checks section -->
<div class="section">
    <h2>Validation Checks</h2>
    <button class="download-btn" id="download-validation-csv">Download CSV</button>
    <div id="validation-table"></div>
</div>

<!-- Profiling section placeholder (Phase 5) -->
<div class="section" id="profiling-section" style="display:none;">
    <h2>Data Profile</h2>
    <div id="profiling-table"></div>
</div>

<!-- Failing rows placeholder (Phase 5) -->
<div class="section" id="failing-rows-section" style="display:none;">
    <h2>Failing Rows</h2>
    <div id="failing-rows-container"></div>
</div>

<!-- Embedded JSON data -->
<script type="application/json" id="dashboard-data">{{ dashboard_json|safe }}</script>
{% endblock %}

{% block extra_js %}
<script>
(function() {
    var raw = document.getElementById("dashboard-data").textContent;
    var data = JSON.parse(raw);
    var validation = data.validation;
    var steps = validation.steps;

    // --- Header stats ---
    var totalChecks = steps.length;
    var passed = steps.filter(function(s) { return s.n_failed === 0; }).length;
    var passRate = totalChecks > 0
        ? ((passed / totalChecks) * 100).toFixed(1) + "%"
        : "—";

    document.getElementById("stat-total-rows").textContent =
        validation.total_rows.toLocaleString();
    document.getElementById("stat-chunks").textContent =
        validation.chunks_processed.toLocaleString();
    document.getElementById("stat-pass-rate").textContent = passRate;
    document.getElementById("stat-checks").textContent = totalChecks;

    // --- Validation checks table ---
    var tableData = steps.map(function(s) {
        return {
            check_id: s.check_id || "—",
            assertion_type: s.assertion_type,
            column: s.column,
            description: s.description,
            n_passed: s.n_passed,
            n_failed: s.n_failed,
            pass_rate: (s.pass_rate * 100).toFixed(1),
            severity: s.severity || "—"
        };
    });

    var severityRank = { "Fail": 3, "Warn": 2, "Note": 1, "—": 0 };

    var validationTable = new Tabulator("#validation-table", {
        data: tableData,
        layout: "fitColumns",
        columns: [
            {
                title: "Check ID",
                field: "check_id",
                width: 100,
                headerFilter: true,
                sorter: "string"
            },
            {
                title: "Type",
                field: "assertion_type",
                width: 160,
                headerFilter: true,
                sorter: "string"
            },
            {
                title: "Column",
                field: "column",
                width: 130,
                headerFilter: true,
                sorter: "string"
            },
            {
                title: "Description",
                field: "description",
                headerFilter: true,
                sorter: "string"
            },
            {
                title: "Passed",
                field: "n_passed",
                width: 90,
                sorter: "number",
                hozAlign: "right",
                formatter: function(cell) {
                    return cell.getValue().toLocaleString();
                }
            },
            {
                title: "Failed",
                field: "n_failed",
                width: 90,
                sorter: "number",
                hozAlign: "right",
                formatter: function(cell) {
                    var v = cell.getValue();
                    if (v > 0) cell.getElement().classList.add("severity-fail");
                    return v.toLocaleString();
                }
            },
            {
                title: "Pass Rate",
                field: "pass_rate",
                width: 100,
                sorter: "number",
                hozAlign: "right",
                formatter: function(cell) {
                    return cell.getValue() + "%";
                }
            },
            {
                title: "Severity",
                field: "severity",
                width: 100,
                headerFilter: true,
                sorter: function(a, b) {
                    return (severityRank[a] || 0) - (severityRank[b] || 0);
                },
                formatter: function(cell) {
                    var v = cell.getValue();
                    var cls = { "Fail": "severity-fail", "Warn": "severity-warn", "Note": "severity-note" };
                    if (cls[v]) cell.getElement().classList.add(cls[v]);
                    return v;
                }
            }
        ],
        rowFormatter: function(row) {
            var sev = row.getData().severity;
            if (sev === "Fail" && row.getData().n_failed > 0) {
                row.getElement().style.backgroundColor = "#fff5f5";
            } else if (sev === "Warn" && row.getData().n_failed > 0) {
                row.getElement().style.backgroundColor = "#fffbf0";
            }
        },
        initialSort: [
            { column: "severity", dir: "desc" },
            { column: "n_failed", dir: "desc" }
        ]
    });

    // CSV download
    document.getElementById("download-validation-csv").addEventListener("click", function() {
        validationTable.download("csv", "validation-checks.csv");
    });
})();
</script>
{% endblock %}
```

Key design decisions:
- Header stats computed client-side from step data (same source of truth as table)
- Severity column has a custom sorter to rank Fail > Warn > Note > none
- Row background tinting for failing rows with Fail/Warn severity
- Profiling and failing rows sections are hidden placeholders (Phase 5 will show them)
- `check_id` and `severity` display "—" when null (AC4.3 display handling)

**Verification:**
Run: `cat src/scdm_qa/reporting/templates/detail.html | head -3`
Expected: `{% extends "base.html" %}`

**Commit:** `feat: add detail.html template with validation checks table`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Extend save_dashboard() to render detail pages

**Verifies:** qa-dashboard.AC2.2, qa-dashboard.AC2.3, qa-dashboard.AC3.1

**Files:**
- Modify: `src/scdm_qa/reporting/dashboard.py`

**Implementation:**

Extend `save_dashboard()` to render one detail page per table key after the index page. The function should iterate over the serialised run data and render a detail page for each table.

After the index page rendering (existing code from Phase 3), add:

```python
# Render detail pages
for table_key, table_data in run_data["tables"].items():
    detail_json = json.dumps(table_data)
    table_name = table_data["validation"]["table_name"]
    detail_html = _render_page(
        "detail.html",
        page_title=table_name,
        dashboard_json=detail_json,
    )
    detail_path = output_dir / f"{table_key}.html"
    detail_path.write_text(detail_html, encoding="utf-8")
```

The `run_data` variable already exists from the index page rendering — it's the output of `serialise_run()`. Each detail page gets the full per-table data (including `failing_rows` for use in Phase 5).

Note: The `_render_page` function must handle the `|safe` filter for `dashboard_json` in the detail template as well (same pattern as index.html).

**Verification:**
Run: Create a quick test:
```python
python -c "
from pathlib import Path
from scdm_qa.reporting.dashboard import save_dashboard
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
import polars as pl
import tempfile

with tempfile.TemporaryDirectory() as td:
    out = Path(td)
    vr = ValidationResult('demo', 'Demo Table', (
        StepResult(1, 'col_vals_not_null', 'PatID', 'not null', 98, 2, None, '122', 'Fail'),
    ), 100, 1)
    pr = ProfilingResult('demo', 'Demo Table', 100, ())
    save_dashboard(out, [(vr, pr)])
    assert (out / 'index.html').exists()
    assert (out / 'demo.html').exists()
    print('index + detail pages rendered')
"
```
Expected: `index + detail pages rendered`

**Commit:** `feat: extend save_dashboard to render per-table detail pages`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for detail page rendering

**Verifies:** qa-dashboard.AC2.2, qa-dashboard.AC2.3, qa-dashboard.AC2.4, qa-dashboard.AC2.5, qa-dashboard.AC3.1

**Files:**
- Modify: `tests/test_dashboard.py` (add new test class)

**Testing:**

Add a `TestDetailPage` class to the existing `tests/test_dashboard.py` (created in Phase 3). Reuse the same `_make_*` helper factories.

Tests must verify each AC listed above:

- **qa-dashboard.AC2.2** — Detail page HTML contains stat cards for total rows, chunks processed, pass rate. Verify the embedded JSON has correct `validation.total_rows`, `validation.chunks_processed` values matching the input ValidationResult.
- **qa-dashboard.AC2.3** — Embedded JSON `validation.steps` array contains entries with all StepResult fields: `check_id`, `assertion_type`, `column`, `description`, `n_passed`, `n_failed`, `pass_rate`, `severity`. Verify field values match input.
- **qa-dashboard.AC2.4** — HTML contains `headerFilter: true` or `headerFilter` in the Tabulator column definitions for severity, assertion_type, column, and description fields.
- **qa-dashboard.AC2.5** — HTML contains `download("csv"` call wired to the download button.
- **qa-dashboard.AC3.1** — When results include a cross-table entry (`table_key="cross_table"`), both `index.html` and `cross_table.html` are produced. The index page JSON `tables` dict contains a `"cross_table"` key.

Additional test cases:
- Detail page for a table with multiple steps: all steps appear in the JSON.
- Detail page filename matches `{table_key}.html`.
- Detail page HTML contains the back link to `index.html`.
- Multiple tables in results produce corresponding detail pages.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_dashboard.py -v`
Expected: All tests pass

**Commit:** `test: add detail page validation checks tests`

<!-- END_TASK_3 -->
