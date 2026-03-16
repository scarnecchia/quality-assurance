# QA Dashboard Implementation Plan — Phase 5: Detail Page — Profiling & Failing Rows

**Goal:** Add profiling stats table and failing row drill-down to detail pages.

**Architecture:** Extend `detail.html` template to populate the profiling and failing rows sections (currently hidden placeholders from Phase 4). Profiling data rendered as a Tabulator table with completeness highlighting. Failing rows rendered as collapsible sections — one per failing check — each containing a Tabulator table with CSV download. All data already embedded in the per-table JSON from Phase 4.

**Tech Stack:** JavaScript (Tabulator conditional formatting, DOM manipulation for collapsible sections)

**Scope:** 6 phases from original design (phase 5 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC2: Detail page validation checks (profiling and failing rows)
- **qa-dashboard.AC2.6 Success:** Profiling table shows column stats with completeness below 95% highlighted in red
- **qa-dashboard.AC2.7 Success:** Failing row sections are collapsible, one per failing check, showing sample rows in a Tabulator table
- **qa-dashboard.AC2.8 Success:** Each failing row section has its own CSV download button
- **qa-dashboard.AC2.9 Failure:** Detail page renders correctly when a table has zero failures (no failing row sections shown)
- **qa-dashboard.AC2.10 Edge:** Detail page for cross_table has no profiling section (ProfilingResult is empty)

---

<!-- START_TASK_1 -->
### Task 1: Extend detail.html with profiling and failing rows JavaScript

**Verifies:** qa-dashboard.AC2.6, qa-dashboard.AC2.7, qa-dashboard.AC2.8, qa-dashboard.AC2.9, qa-dashboard.AC2.10

**Files:**
- Modify: `src/scdm_qa/reporting/templates/detail.html`

**Implementation:**

Extend the `extra_js` block in `detail.html` to populate the profiling section and failing rows section. The data is already embedded in the JSON from Phase 4's `save_dashboard()`. The profiling data is at `data.profiling` and failing rows are in each step's `failing_rows` array.

Add the following JavaScript after the existing validation checks code (inside the same IIFE):

```javascript
    // --- Profiling section ---
    var profiling = data.profiling;
    if (profiling && profiling.columns && profiling.columns.length > 0) {
        document.getElementById("profiling-section").style.display = "block";

        var profilingData = profiling.columns.map(function(col) {
            return {
                name: col.name,
                col_type: col.col_type,
                completeness: col.completeness_pct,
                distinct_count: col.distinct_count,
                min_value: col.min_value || "—",
                max_value: col.max_value || "—"
            };
        });

        new Tabulator("#profiling-table", {
            data: profilingData,
            layout: "fitColumns",
            columns: [
                { title: "Column", field: "name", headerFilter: true, sorter: "string" },
                { title: "Type", field: "col_type", width: 100, headerFilter: true, sorter: "string" },
                {
                    title: "Completeness",
                    field: "completeness",
                    width: 120,
                    sorter: "number",
                    hozAlign: "right",
                    formatter: function(cell) {
                        var v = cell.getValue();
                        if (v < 95) {
                            cell.getElement().classList.add("low-completeness");
                        }
                        return v.toFixed(1) + "%";
                    }
                },
                {
                    title: "Distinct",
                    field: "distinct_count",
                    width: 100,
                    sorter: "number",
                    hozAlign: "right",
                    formatter: function(cell) {
                        return cell.getValue().toLocaleString();
                    }
                },
                { title: "Min", field: "min_value", width: 120, sorter: "string" },
                { title: "Max", field: "max_value", width: 120, sorter: "string" }
            ]
        });
    }

    // --- Failing rows sections ---
    var failingSteps = steps.filter(function(s) {
        return s.n_failed > 0 && s.failing_rows && s.failing_rows.length > 0;
    });

    if (failingSteps.length > 0) {
        document.getElementById("failing-rows-section").style.display = "block";
        var container = document.getElementById("failing-rows-container");

        failingSteps.forEach(function(step, idx) {
            var checkLabel = step.check_id || ("Step " + step.step_index);
            var sectionId = "failing-rows-" + idx;
            var tableId = "failing-table-" + idx;

            // Create collapsible section
            var header = document.createElement("div");
            header.className = "collapsible-header";
            header.textContent = checkLabel + ": " + step.description +
                " (" + step.n_failed.toLocaleString() + " failures)";

            var body = document.createElement("div");
            body.className = "collapsible-body";
            body.id = sectionId;

            // Download button
            var downloadBtn = document.createElement("button");
            downloadBtn.className = "download-btn";
            downloadBtn.textContent = "Download CSV";
            downloadBtn.id = "download-failing-" + idx;
            body.appendChild(downloadBtn);

            // Table container
            var tableDiv = document.createElement("div");
            tableDiv.id = tableId;
            body.appendChild(tableDiv);

            container.appendChild(header);
            container.appendChild(body);

            // Toggle collapse
            header.addEventListener("click", function() {
                header.classList.toggle("open");
                body.classList.toggle("open");
            });

            // Build columns from failing row keys
            var rows = step.failing_rows;
            var columns = [];
            if (rows.length > 0) {
                columns = Object.keys(rows[0]).map(function(key) {
                    return { title: key, field: key, headerFilter: true, sorter: "string" };
                });
            }

            var failingTable = new Tabulator("#" + tableId, {
                data: rows,
                layout: "fitColumns",
                columns: columns,
                maxHeight: 400
            });

            // CSV download for this section
            downloadBtn.addEventListener("click", function() {
                failingTable.download("csv", checkLabel + "-failing-rows.csv");
            });
        });
    }
```

Also update the profiling section div to remove `style="display:none;"` default — let the JS control visibility. Actually keep it as `display:none` by default so it doesn't flash empty content. The JS shows it when data exists.

Key design decisions:
- Profiling section only shown when `profiling.columns` is non-empty (cross_table has empty columns — AC2.10)
- Completeness below 95% gets `low-completeness` CSS class (red background, defined in base.html)
- Failing rows section only shown when at least one step has `n_failed > 0` AND `failing_rows` non-empty (AC2.9)
- Each failing section is independently collapsible
- Columns for failing row tables derived dynamically from the row data keys
- Each section has its own CSV download button (AC2.8)

**Verification:**
Run: `grep -c "profiling-section" src/scdm_qa/reporting/templates/detail.html`
Expected: At least 2 (the div and the JS reference)

**Commit:** `feat: add profiling table and collapsible failing rows to detail page`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Tests for profiling and failing rows rendering

**Verifies:** qa-dashboard.AC2.6, qa-dashboard.AC2.7, qa-dashboard.AC2.8, qa-dashboard.AC2.9, qa-dashboard.AC2.10

**Files:**
- Modify: `tests/test_dashboard.py` (add new test classes)

**Testing:**

Add `TestDetailPageProfiling` and `TestDetailPageFailingRows` classes to `tests/test_dashboard.py`. Extend the existing helper factories if needed — particularly `_make_profiling_result` should accept a `columns` parameter to test empty vs populated profiling.

Tests must verify each AC listed above:

- **qa-dashboard.AC2.6** — When profiling data has a column with completeness below 95% (e.g., `null_count=10, total_count=100` → 90%), the embedded JSON contains `completeness_pct: 90.0`. HTML contains `low-completeness` CSS class definition and `profiling-table` div. When profiling has columns, the profiling section is present in the HTML.
- **qa-dashboard.AC2.7** — When steps have `n_failed > 0` and `failing_rows` is non-empty, the embedded JSON contains `failing_rows` arrays with row data. HTML contains `collapsible-header` class and `failing-rows-container` div.
- **qa-dashboard.AC2.8** — HTML contains `download-failing-` button IDs and `download("csv"` calls for failing row sections.
- **qa-dashboard.AC2.9** — When all steps have `n_failed=0`, the detail page HTML still renders. The embedded JSON steps all have empty `failing_rows`. The failing rows section div exists but stays hidden (controlled by JS).
- **qa-dashboard.AC2.10** — When `table_key="cross_table"` and profiling has empty `columns=()`, the embedded JSON `profiling.columns` is an empty array. The profiling section div exists but stays hidden.

Additional test cases:
- Multiple failing steps: each produces a separate collapsible section in the HTML structure.
- Failing rows truncation: when failing_rows DataFrame has more rows than `max_failing_rows`, the serialised data is truncated (this is tested in Phase 1, but verify end-to-end).

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_dashboard.py -v`
Expected: All tests pass

**Commit:** `test: add profiling and failing rows detail page tests`

<!-- END_TASK_2 -->
