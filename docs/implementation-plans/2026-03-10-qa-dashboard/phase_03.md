# QA Dashboard Implementation Plan — Phase 3: Index Page Dashboard

**Goal:** Build the index page with health scorecard, Plotly charts, and Tabulator summary grid.

**Architecture:** Create `index.html` Jinja2 template extending `base.html`. Implement `save_dashboard()` in `dashboard.py` to serialise results, render index page with embedded JSON, and write to output directory. The index page uses client-side JavaScript to hydrate Plotly charts and a Tabulator summary table from embedded JSON data.

**Tech Stack:** Python (Jinja2, serialise.py from Phase 1), JavaScript (Plotly.js donut + horizontal bar, Tabulator summary grid)

**Scope:** 6 phases from original design (phase 3 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC1: Index page scorecard and charts
- **qa-dashboard.AC1.1 Success:** Index page displays total check count, overall pass rate, and total failure count as stat cards
- **qa-dashboard.AC1.2 Success:** Plotly donut chart shows pass/warn/fail/note breakdown by check count with correct proportions
- **qa-dashboard.AC1.3 Success:** Plotly horizontal bar chart shows pass rate per table, sorted worst-first, colour-scaled red→green
- **qa-dashboard.AC1.4 Failure:** Index page renders correctly when all checks pass (zero failures, donut shows 100% pass)
- **qa-dashboard.AC1.5 Edge:** Index page renders correctly when only cross-table checks exist (no L1 tables)

### qa-dashboard.AC2: Detail page validation checks (partial — navigation only)
- **qa-dashboard.AC2.1 Success:** Clicking a table row in the index grid navigates to the correct detail page

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->

<!-- START_TASK_1 -->
### Task 1: Create index.html template

**Files:**
- Create: `src/scdm_qa/reporting/templates/index.html`

**Implementation:**

Create `index.html` extending `base.html`. The template receives one context variable:
- `page_title` (str): "Index"

The template embeds JSON data in a `<script type="application/json" id="dashboard-data">` block. The JSON is injected by the Python render function (dashboard.py) via a Jinja2 variable `dashboard_json` which is the raw JSON string of the summary data (no per-table failing rows).

Template structure:

```html
{% extends "base.html" %}

{% block content %}
<h1>SCDM-QA Dashboard</h1>

<!-- Stat cards -->
<div class="stat-cards">
    <div class="stat-card">
        <div class="label">Total Checks</div>
        <div class="value" id="stat-total-checks">—</div>
    </div>
    <div class="stat-card">
        <div class="label">Pass Rate</div>
        <div class="value" id="stat-pass-rate">—</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Failures</div>
        <div class="value severity-fail" id="stat-total-failures">—</div>
    </div>
</div>

<!-- Charts row -->
<div class="charts-row">
    <div class="chart-container" id="donut-chart"></div>
    <div class="chart-container" id="bar-chart"></div>
</div>

<!-- Summary table -->
<h2>Tables</h2>
<div id="summary-table"></div>

<!-- Embedded JSON data -->
<script type="application/json" id="dashboard-data">{{ dashboard_json|safe }}</script>
{% endblock %}

{% block extra_js %}
<script>
(function() {
    var raw = document.getElementById("dashboard-data").textContent;
    var data = JSON.parse(raw);
    var summary = data.summary;
    var tables = data.tables;

    // --- Stat cards ---
    var totalChecks = summary.total_checks;
    var totalFailures = summary.total_failures;
    var bySeverity = summary.by_severity;
    var passRate = totalChecks > 0
        ? ((bySeverity.pass / totalChecks) * 100).toFixed(1) + "%"
        : "—";

    document.getElementById("stat-total-checks").textContent = totalChecks.toLocaleString();
    document.getElementById("stat-pass-rate").textContent = passRate;
    document.getElementById("stat-total-failures").textContent = totalFailures.toLocaleString();

    // --- Donut chart ---
    var severityLabels = ["Pass", "Fail", "Warn", "Note"];
    var severityValues = [
        bySeverity.pass || 0,
        bySeverity.Fail || 0,
        bySeverity.Warn || 0,
        bySeverity.Note || 0
    ];
    var severityColors = ["#28a745", "#dc3545", "#ffc107", "#6c757d"];

    Plotly.newPlot("donut-chart", [{
        values: severityValues,
        labels: severityLabels,
        type: "pie",
        hole: 0.5,
        marker: { colors: severityColors },
        hoverinfo: "label+percent+value",
        textinfo: "label+value"
    }], {
        title: "Check Results",
        height: 350,
        margin: { t: 40, b: 20, l: 20, r: 20 },
        showlegend: true
    }, { displayModeBar: false });

    // --- Bar chart: pass rate per table, sorted worst-first ---
    var tableKeys = Object.keys(tables);
    var barData = tableKeys.map(function(key) {
        var t = tables[key];
        var v = t.validation;
        var steps = v.steps;
        var total = steps.length;
        var passed = steps.filter(function(s) { return s.n_failed === 0; }).length;
        var rate = total > 0 ? (passed / total) * 100 : 100;
        return { name: v.table_name, rate: rate, key: key };
    });
    barData.sort(function(a, b) { return a.rate - b.rate; });

    // Colour scale: red (0%) → yellow (50%) → green (100%)
    function rateToColor(rate) {
        if (rate < 50) {
            var t = rate / 50;
            var r = 220; var g = Math.round(53 + t * (199 - 53));
            return "rgb(" + r + "," + g + ",69)";
        } else {
            var t2 = (rate - 50) / 50;
            var r2 = Math.round(255 - t2 * (255 - 40)); var g2 = Math.round(193 + t2 * (167 - 193));
            return "rgb(" + r2 + "," + g2 + ",69)";
        }
    }

    Plotly.newPlot("bar-chart", [{
        type: "bar",
        x: barData.map(function(d) { return d.rate; }),
        y: barData.map(function(d) { return d.name; }),
        orientation: "h",
        marker: { color: barData.map(function(d) { return rateToColor(d.rate); }) },
        hovertemplate: "%{y}: %{x:.1f}%<extra></extra>"
    }], {
        title: "Pass Rate by Table",
        xaxis: { title: "Pass Rate (%)", range: [0, 105] },
        yaxis: { automargin: true },
        height: Math.max(250, barData.length * 35 + 80),
        margin: { l: 150, r: 20, t: 40, b: 40 }
    }, { displayModeBar: false });

    // --- Summary table ---
    var tableRows = tableKeys.map(function(key) {
        var t = tables[key];
        var v = t.validation;
        var steps = v.steps;
        var total = steps.length;
        var failures = steps.reduce(function(sum, s) { return sum + s.n_failed; }, 0);
        var passed = steps.filter(function(s) { return s.n_failed === 0; }).length;
        var rate = total > 0 ? ((passed / total) * 100).toFixed(1) : "100.0";

        // Worst severity among failing steps
        var severityRank = { "Fail": 3, "Warn": 2, "Note": 1 };
        var worstSeverity = "—";
        steps.forEach(function(s) {
            if (s.n_failed > 0 && s.severity) {
                if (!worstSeverity || worstSeverity === "—" ||
                    (severityRank[s.severity] || 0) > (severityRank[worstSeverity] || 0)) {
                    worstSeverity = s.severity;
                }
            }
        });

        return {
            table_key: key,
            table_name: v.table_name,
            total_rows: v.total_rows,
            checks: total,
            failures: failures,
            pass_rate: parseFloat(rate),
            worst_severity: worstSeverity
        };
    });

    new Tabulator("#summary-table", {
        data: tableRows,
        layout: "fitColumns",
        columns: [
            { title: "Table", field: "table_name", headerFilter: true, sorter: "string" },
            { title: "Rows", field: "total_rows", sorter: "number", formatter: function(cell) {
                return cell.getValue().toLocaleString();
            }},
            { title: "Checks", field: "checks", sorter: "number" },
            { title: "Failures", field: "failures", sorter: "number", formatter: function(cell) {
                var v = cell.getValue();
                if (v > 0) cell.getElement().classList.add("severity-fail");
                return v.toLocaleString();
            }},
            { title: "Pass Rate", field: "pass_rate", sorter: "number", formatter: function(cell) {
                return cell.getValue().toFixed(1) + "%";
            }},
            { title: "Worst Severity", field: "worst_severity", headerFilter: true, sorter: "string", formatter: function(cell) {
                var v = cell.getValue();
                var cls = { "Fail": "severity-fail", "Warn": "severity-warn", "Note": "severity-note" };
                if (cls[v]) cell.getElement().classList.add(cls[v]);
                return v;
            }}
        ],
        initialSort: [{ column: "pass_rate", dir: "asc" }],
        rowClick: function(e, row) {
            window.location.href = row.getData().table_key + ".html";
        }
    });
})();
</script>
{% endblock %}
```

Key design decisions:
- JSON data embedded via `{{ dashboard_json }}` — Jinja2 injects raw JSON string (must use `autoescape=False` for this variable or use `|safe` filter)
- Stat cards populated from `summary` object in JSON
- Donut uses `summary.by_severity` counts
- Bar chart computes pass rate per table from step-level data, sorted worst-first
- `rateToColor()` provides red→yellow→green gradient
- Summary table uses Tabulator with header filters, click-through to `{table_key}.html`
- Bar chart height scales with number of tables

**Verification:**
Run: `cat src/scdm_qa/reporting/templates/index.html | head -3`
Expected: `{% extends "base.html" %}`

**Commit:** `feat: add index.html dashboard template with scorecard, charts, and summary grid`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement save_dashboard() for index page rendering

**Verifies:** qa-dashboard.AC1.1, qa-dashboard.AC1.2, qa-dashboard.AC1.3, qa-dashboard.AC1.4, qa-dashboard.AC1.5, qa-dashboard.AC2.1

**Files:**
- Modify: `src/scdm_qa/reporting/dashboard.py` (replace the placeholder `save_dashboard`)

**Implementation:**

Replace the `NotImplementedError` placeholder in `save_dashboard()` with actual index page rendering. The function should:

1. Call `serialise_run(results, max_failing_rows=max_failing_rows)` to get the full JSON dict
2. Create a summary-only version of the JSON (strip `failing_rows` from each step to keep the index page small):
   ```python
   import copy
   import json

   run_data = serialise_run(results, max_failing_rows=max_failing_rows)

   # Strip failing rows for index page (they're only needed on detail pages)
   index_data = copy.deepcopy(run_data)
   for table_data in index_data["tables"].values():
       for step in table_data["validation"]["steps"]:
           step["failing_rows"] = []

   index_json = json.dumps(index_data)
   ```

3. Render the index page:
   ```python
   output_dir.mkdir(parents=True, exist_ok=True)
   html = _render_page("index.html", page_title="Index", dashboard_json=index_json)
   index_path = output_dir / "index.html"
   index_path.write_text(html, encoding="utf-8")
   ```

4. Return `output_dir` (the directory, not the file — detail pages will be added in Phase 4).

**Important:** The Jinja2 environment has `autoescape=True`. The `dashboard_json` variable contains raw JSON that must NOT be HTML-escaped. The template uses `{{ dashboard_json|safe }}` to bypass autoescape for this variable only. No changes needed to the Python render function — the `|safe` filter in the template handles it.

Add imports to `dashboard.py`:
```python
import copy
import json
from scdm_qa.reporting.serialise import serialise_run
```

Update the function signature to match:
```python
def save_dashboard(
    output_dir: Path,
    results: list[tuple[ValidationResult, ProfilingResult]],
    *,
    max_failing_rows: int = 500,
) -> Path:
```

**Verification:**
Run: `python -c "from scdm_qa.reporting.dashboard import save_dashboard; print('ok')"`
Expected: `ok`

**Commit:** `feat: implement save_dashboard index page rendering`

<!-- END_TASK_2 -->

<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->

<!-- START_TASK_3 -->
### Task 3: Export save_dashboard from reporting package

**Files:**
- Modify: `src/scdm_qa/reporting/__init__.py`

**Implementation:**

Add `save_dashboard` to the reporting package exports alongside existing ones. The current `__init__.py` exports `save_table_report`, `save_index`, `make_report_summary`. Add:

```python
from scdm_qa.reporting.dashboard import save_dashboard
```

And add `"save_dashboard"` to `__all__`.

**Verification:**
Run: `python -c "from scdm_qa.reporting import save_dashboard; print('ok')"`
Expected: `ok`

**Commit:** `feat: export save_dashboard from reporting package`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for index page rendering

**Verifies:** qa-dashboard.AC1.1, qa-dashboard.AC1.2, qa-dashboard.AC1.3, qa-dashboard.AC1.4, qa-dashboard.AC1.5, qa-dashboard.AC2.1

**Files:**
- Create: `tests/test_dashboard.py`

**Testing:**

Follow the existing test pattern from `tests/test_reporting.py` — module-level `_make_*` helper factories, class-based test organisation. Reuse similar factory pattern but with `check_id` and `severity` fields populated.

Helper factories needed:
- `_make_step(*, n_passed=98, n_failed=2, check_id="122", severity="Fail", failing_rows=None)` — creates a StepResult with configurable fields
- `_make_validation_result(*, table_key="demographic", steps=None, with_failures=False)` — creates a ValidationResult
- `_make_profiling_result(*, table_key="demographic")` — creates a ProfilingResult
- `_make_results_pair(*, table_key="demographic", with_failures=False)` — returns `(ValidationResult, ProfilingResult)` tuple

Tests must verify each AC listed above:

- **qa-dashboard.AC1.1** — `save_dashboard` produces `index.html` in output dir. The HTML contains `stat-total-checks`, `stat-pass-rate`, `stat-total-failures` elements. The embedded JSON contains correct `summary.total_checks`, `summary.total_failures`, and `summary.by_severity.pass` counts.
- **qa-dashboard.AC1.2** — Embedded JSON `summary.by_severity` contains correct counts for Fail, Warn, Note, and pass categories. HTML contains `donut-chart` div and `Plotly.newPlot` call.
- **qa-dashboard.AC1.3** — HTML contains `bar-chart` div. Embedded JSON `tables` dict has entries for each table key passed to `save_dashboard`.
- **qa-dashboard.AC1.4** — When all steps have `n_failed=0`, `save_dashboard` produces valid HTML. Embedded JSON has `total_failures: 0` and `by_severity.pass` equals total checks.
- **qa-dashboard.AC1.5** — When results list contains only a cross-table entry (`table_key="cross_table"`, `total_rows=0`, `chunks_processed=0`), `save_dashboard` produces valid HTML without errors.
- **qa-dashboard.AC2.1** — HTML contains `table_key + ".html"` in the Tabulator rowClick handler or as a link target for each table in the results.

Additional test cases:
- Embedded JSON has no `failing_rows` data (stripped for index page) — verify all step `failing_rows` are empty lists.
- `save_dashboard` with empty results list produces valid HTML with zero counts.
- Returned path is the output directory.
- HTML contains inlined Tabulator and Plotly assets (check for presence of `new Tabulator` and `Plotly.newPlot`).
- **JSON special character round-trip:** Create a StepResult with `description="values < 0 & > 100"` and verify the embedded JSON in the rendered HTML can be parsed back via `json.loads()` with the description intact. This tests that the `|safe` filter correctly bypasses HTML autoescaping for the JSON data block.

To verify JSON content in tests, extract it from the HTML:
```python
import json
import re

html = (tmp_path / "index.html").read_text()
match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
data = json.loads(match.group(1))
```

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_dashboard.py -v`
Expected: All tests pass

**Commit:** `test: add index page dashboard tests`

<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_B -->
