# L1 & L2 Validation Checks Implementation Plan — Phase 4

**Goal:** Implement L2 date ordering check 226 to detect rows where date column A occurs after date column B.

**Architecture:** New `check_date_ordering()` function in `global_checks.py`. Iterates chunks, filters for rows where both dates are non-null and col_a > col_b. Returns one StepResult per date pair. Date pair definitions stored in `checks.py` as a new `DateOrderingDef` dataclass.

**Tech Stack:** Python 3.12+, polars, pytest

**Scope:** 8 phases from original design (phase 4 of 8)

**Codebase verified:** 2026-03-10

**Deviation from design:** The design plan lists LAB (Order_dt > Lab_dt, Lab_dt > Result_dt) as date ordering pairs. However, codebase investigation confirms the `laboratory` table schema only contains 4 columns: PatID, LabID, MS_Test_Name, Result_Type. The Order_dt, Lab_dt, and Result_dt columns do not exist in the current SCDM spec. **LAB date ordering checks are omitted.** Only encounter (ADate > DDate) and enrollment (Enr_Start > Enr_End) pairs are implemented.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC2: L2 checks detect cross-record data quality issues
- **l1-l2-checks.AC2.1 Success:** Check 226 flags rows where date_a > date_b for all configured pairs
- **l1-l2-checks.AC2.6 Failure:** Check 226 does not flag rows where either date is null

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Add DateOrderingDef and date pair registry to checks.py

**Verifies:** None (infrastructure for Task 2)

**Files:**
- Modify: `src/scdm_qa/schemas/models.py`
- Modify: `src/scdm_qa/schemas/checks.py`

**Implementation:**

In `src/scdm_qa/schemas/models.py`, add a new frozen dataclass after `L1CheckDef`:

```python
@dataclass(frozen=True)
class DateOrderingDef:
    check_id: str       # "226"
    table_key: str      # e.g. "encounter"
    date_a: str         # column that should be <= date_b
    date_b: str         # column that should be >= date_a
    severity: str       # "Fail" | "Warn"
    description: str    # human-readable, e.g. "ADate <= DDate"
```

In `src/scdm_qa/schemas/checks.py`, add the date ordering definitions and accessor:

```python
from scdm_qa.schemas.models import L1CheckDef, DateOrderingDef

# Check 226: Date ordering violations
# Source: SAS lkp_all_l2 where CheckID=226
DATE_ORDERING_DEFS: tuple[DateOrderingDef, ...] = (
    DateOrderingDef("226", "encounter", "ADate", "DDate", "Fail", "ADate <= DDate"),
    DateOrderingDef("226", "enrollment", "Enr_Start", "Enr_End", "Fail", "Enr_Start <= Enr_End"),
)


def get_date_ordering_checks_for_table(table_key: str) -> tuple[DateOrderingDef, ...]:
    """Return date ordering check definitions for a given table."""
    return tuple(d for d in DATE_ORDERING_DEFS if d.table_key == table_key)
```

**Verification:**

Run: `uv run python -c "from scdm_qa.schemas.checks import get_date_ordering_checks_for_table; print(get_date_ordering_checks_for_table('encounter'))"`
Expected: Prints the encounter DateOrderingDef.

**Commit:** `feat: add DateOrderingDef and date pair registry for check 226`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement check_date_ordering() in global_checks.py

**Verifies:** l1-l2-checks.AC2.1, l1-l2-checks.AC2.6

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Add import:

```python
from scdm_qa.schemas.checks import get_date_ordering_checks_for_table, get_not_populated_checks_for_table
```

Add new function:

```python
def check_date_ordering(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Check 226: Detect rows where date_a > date_b.

    Rows where either date is null are skipped (not flagged).
    Returns one StepResult per configured date pair.
    """
    ordering_defs = get_date_ordering_checks_for_table(schema.table_key)
    if not ordering_defs:
        return []

    # Accumulate per-pair counts across chunks
    pair_failed: dict[str, int] = {}
    pair_passed: dict[str, int] = {}
    pair_failing_rows: dict[str, list[pl.DataFrame]] = {}
    pair_failing_count: dict[str, int] = {}
    total_rows = 0

    for pair_def in ordering_defs:
        key = f"{pair_def.date_a}>{pair_def.date_b}"
        pair_failed[key] = 0
        pair_passed[key] = 0
        pair_failing_rows[key] = []
        pair_failing_count[key] = 0

    for chunk in chunks:
        total_rows += chunk.height
        for pair_def in ordering_defs:
            if pair_def.date_a not in chunk.columns or pair_def.date_b not in chunk.columns:
                continue

            key = f"{pair_def.date_a}>{pair_def.date_b}"

            # Filter to rows where both dates are non-null
            both_present = chunk.filter(
                pl.col(pair_def.date_a).is_not_null() & pl.col(pair_def.date_b).is_not_null()
            )

            violations = both_present.filter(
                pl.col(pair_def.date_a) > pl.col(pair_def.date_b)
            )

            pair_failed[key] += violations.height
            pair_passed[key] += both_present.height - violations.height

            if violations.height > 0 and pair_failing_count[key] < max_failing_rows:
                remaining = max_failing_rows - pair_failing_count[key]
                sample = violations.head(remaining)
                pair_failing_rows[key].append(sample)
                pair_failing_count[key] += sample.height

    results: list[StepResult] = []
    for pair_def in ordering_defs:
        key = f"{pair_def.date_a}>{pair_def.date_b}"
        failing = None
        if pair_failing_rows[key]:
            failing = pl.concat(pair_failing_rows[key])

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="date_ordering",
                column=f"{pair_def.date_a}, {pair_def.date_b}",
                description=f"{pair_def.description} (check {pair_def.check_id})",
                n_passed=pair_passed[key],
                n_failed=pair_failed[key],
                failing_rows=failing,
                check_id=pair_def.check_id,
                severity=pair_def.severity,
            )
        )

    return results
```

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_date_ordering for L2 check 226`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Wire check_date_ordering into pipeline.py

**Verifies:** l1-l2-checks.AC2.1

**Files:**
- Modify: `src/scdm_qa/pipeline.py:16`

**Implementation:**

Update import:

```python
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness, check_not_populated, check_date_ordering
```

In `_process_table()`, after the `check_not_populated` block, add:

```python
    # L2 check: date ordering (check 226)
    date_order_reader = create_reader(file_path, chunk_size=config.chunk_size)
    date_order_steps = check_date_ordering(schema, date_order_reader.chunks(), max_failing_rows=config.max_failing_rows)
    global_steps.extend(date_order_steps)
```

**Verification:**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

**Commit:** `feat: wire check_date_ordering into pipeline`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for check_date_ordering

**Verifies:** l1-l2-checks.AC2.1, l1-l2-checks.AC2.6, l1-l2-checks.AC4.1

**Files:**
- Modify: `tests/test_global_checks.py`

**Implementation:**

Add new test classes to `tests/test_global_checks.py`.

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC2.1:** Create an encounter-schema iterator with rows where ADate > DDate. Call `check_date_ordering`. Assert the result has n_failed > 0 and check_id="226".

- **l1-l2-checks.AC2.6:** Create rows where ADate is null or DDate is null. Assert those rows are NOT counted as failures (n_failed == 0 for null-date rows).

- **l1-l2-checks.AC4.1:** Assert check_id="226" is on all returned StepResults.

- **Clean data test:** All rows have ADate <= DDate. Assert n_failed == 0.

- **Multiple chunks:** Violations spread across chunks accumulate correctly.

- **No date ordering defs:** Call with a table that has no date ordering checks (e.g., demographic). Assert empty list returned.

Use `iter([df])` to provide chunk iterators. Construct a minimal TableSchema with `table_key="encounter"` or use `get_schema("encounter")`. For simpler tests, create inline DataFrames with just the date columns plus PatID.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for L2 date ordering check 226`
<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_A -->
