# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC3: CLI + config phase isolation
- **cross-table-code-checks.AC3.1 Success:** `--l1-only` runs only per-table validation, skips cross-table
- **cross-table-code-checks.AC3.2 Success:** `--l2-only` runs only cross-table validation, skips per-table
- **cross-table-code-checks.AC3.3 Success:** Default (no flags) runs both L1 and L2
- **cross-table-code-checks.AC3.4 Failure:** `--l1-only --l2-only` together raises error
- **cross-table-code-checks.AC3.5 Success:** TOML `run_l1`/`run_l2` options control phase execution
- **cross-table-code-checks.AC3.6 Success:** CLI flags override TOML config values

---

## Phase 1: DuckDB Required + Config/CLI Extensions

This is an infrastructure + functionality phase. DuckDB becomes a required dependency, and `QAConfig` and CLI gain L1/L2 phase isolation controls.

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Move DuckDB to required dependencies

**Files:**
- Modify: `pyproject.toml:10-27`

**Implementation:**

Move the `duckdb>=1,<2` entry from `[project.optional-dependencies]` into the `dependencies` list under `[project]`. Remove the now-empty `[project.optional-dependencies]` section entirely.

After the change, the `dependencies` list should be:

```toml
dependencies = [
    "typer>=0.24,<1",
    "structlog>=25,<26",
    "polars>=1.38,<2",
    "pointblank>=0.6,<1",
    "pyreadstat>=1.3,<2",
    "great-tables>=0.21,<1",
    "jinja2>=3,<4",
    "pyarrow>=23.0.1",
    "duckdb>=1,<2",
]
```

**Verification:**

Run: `uv sync`
Expected: Installs without errors, duckdb resolves as a required dependency.

Run: `uv run python -c "import duckdb; print(duckdb.__version__)"`
Expected: Prints version (1.x).

**Commit:** `chore: move duckdb from optional to required dependency`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Verify existing tests still pass after dependency change

**Verification:**

Run: `uv run pytest`
Expected: All existing tests pass. No regressions from moving duckdb.

<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-5) -->
<!-- START_TASK_3 -->
### Task 3: Add run_l1/run_l2 fields to QAConfig and load_config()

**Verifies:** cross-table-code-checks.AC3.5

**Files:**
- Modify: `src/scdm_qa/config.py:8-76`
- Test: `tests/test_config.py`

**Implementation:**

Add two new boolean fields to the `QAConfig` frozen dataclass (after `verbose`):

```python
run_l1: bool = True
run_l2: bool = True
```

In `load_config()`, extract these from the `options` dict following the existing `verbose` pattern (line 56):

```python
run_l1 = options.get("run_l1", True)
run_l2 = options.get("run_l2", True)
```

Pass both to the `QAConfig` constructor.

**Testing:**

Tests must verify each AC listed above:
- cross-table-code-checks.AC3.5: Config with `run_l1 = false` and `run_l2 = true` parses correctly (and vice versa). Default config (no run_l1/run_l2 in TOML) defaults both to `True`.

Follow the existing pattern in `tests/test_config.py` — write TOML strings to tmp files, parse with `load_config()`, assert field values.

**Verification:**

Run: `uv run pytest tests/test_config.py -v`
Expected: All tests pass including new ones.

**Commit:** `feat: add run_l1/run_l2 config options for phase isolation`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add --l1-only and --l2-only CLI flags

**Verifies:** cross-table-code-checks.AC3.1, cross-table-code-checks.AC3.2, cross-table-code-checks.AC3.3, cross-table-code-checks.AC3.4, cross-table-code-checks.AC3.6

**Files:**
- Modify: `src/scdm_qa/cli.py:23-48`
- Test: `tests/test_cli.py`

**Implementation:**

Add two new options to the `run` command signature:

```python
l1_only: Annotated[bool, typer.Option("--l1-only", help="Run only per-table (L1) validation")] = False,
l2_only: Annotated[bool, typer.Option("--l2-only", help="Run only cross-table (L2) validation")] = False,
```

Before calling `run_pipeline`, validate mutual exclusion and apply overrides:

```python
if l1_only and l2_only:
    typer.echo("error: --l1-only and --l2-only are mutually exclusive", err=True)
    raise typer.Exit(code=2)
```

The `_load_and_configure` helper needs to return a config that can be modified. Since `QAConfig` is frozen, use `dataclasses.replace()` to create a new instance with overridden values:

```python
from dataclasses import replace

if l1_only:
    cfg = replace(cfg, run_l1=True, run_l2=False)
elif l2_only:
    cfg = replace(cfg, run_l1=False, run_l2=True)
```

Note: `run_pipeline` does not yet use `run_l1`/`run_l2` — that wiring happens in Phase 6. This phase establishes the config plumbing only.

**Testing:**

Tests must verify:
- cross-table-code-checks.AC3.1: `--l1-only` sets `run_l1=True, run_l2=False` on the config
- cross-table-code-checks.AC3.2: `--l2-only` sets `run_l1=False, run_l2=True` on the config
- cross-table-code-checks.AC3.3: No flags leaves both `True`
- cross-table-code-checks.AC3.4: `--l1-only --l2-only` together exits with code 2 and error message
- cross-table-code-checks.AC3.6: CLI `--l1-only` overrides TOML `run_l1 = true, run_l2 = true`

Follow the existing `TestRunCommand` pattern in `tests/test_cli.py` — use `CliRunner`, create temp config and data files, invoke with flags, assert exit codes and output.

For testing the config override behavior (AC3.6), create a TOML with `run_l1 = true` and `run_l2 = true`, invoke with `--l1-only`, and verify the pipeline receives `run_l2=False`. This may require inspecting the config via a mock or spy on `run_pipeline`.

**Verification:**

Run: `uv run pytest tests/test_cli.py -v`
Expected: All tests pass including new ones.

**Commit:** `feat: add --l1-only and --l2-only CLI flags with mutual exclusion`

<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Full test suite verification

**Verification:**

Run: `uv run pytest`
Expected: All tests pass. No regressions.

**Commit:** No commit needed — verification only.

<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->
