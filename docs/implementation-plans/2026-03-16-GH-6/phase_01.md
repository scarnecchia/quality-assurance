# Streaming SAS-to-Parquet Conversion Implementation Plan

**Goal:** Replace the in-memory SAS-to-Parquet concat with streaming writes via `pyarrow.parquet.ParquetWriter`, keeping memory bounded to one chunk at a time.

**Architecture:** A canonical `pyarrow.Schema` is built from SCDM `TableSchema`/`ColumnDef` definitions (Numeric -> float64, Character -> utf8). Each SAS chunk is cast to this schema and written as a Parquet row group. Unknown tables fall back to first-chunk inference.

**Tech Stack:** Python 3.12+, pyarrow (ParquetWriter, Schema), polars (DataFrame chunks), pyreadstat (SAS reading), pytest

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-6.AC3: Canonical schema resolution (partial — schema builder only; integration verified in Phase 2)
- **GH-6.AC3.1 Success:** Known SCDM table key resolves to canonical pyarrow.Schema from spec

---

## Phase 1: Canonical Arrow Schema Builder

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Implement `build_arrow_schema` function

**Verifies:** GH-6.AC3.1

**Files:**
- Modify: `src/scdm_qa/validation/cross_table.py` (add function after line 20, before `run_cross_table_checks`)

**Implementation:**

Add a pure function `build_arrow_schema` that converts a `TableSchema` into a `pyarrow.Schema`. The mapping is:

- `ColumnDef.col_type == "Numeric"` -> `pa.float64()`
- `ColumnDef.col_type == "Character"` -> `pa.utf8()`
- Nullability from `ColumnDef.missing_allowed`

The function accepts an optional `data_columns` parameter — a sequence of column names present in the actual data. When provided, the returned schema includes:
1. Canonical-typed fields for columns that exist in both the spec and the data (in data order)
2. Columns in data but not in spec are **not** included (the caller handles merging with inferred types at write time)

When `data_columns` is `None`, all spec columns are returned in spec order.

This function is a pure transformation (Functional Core) — no I/O, no logging, no side effects.

Add the following top-level imports to `cross_table.py` (alongside the existing `from scdm_qa.schemas import get_schema`):

```python
import pyarrow as pa
from scdm_qa.schemas.models import TableSchema

_SCDM_TYPE_MAP: dict[str, pa.DataType] = {
    "Numeric": pa.float64(),
    "Character": pa.utf8(),
}


def build_arrow_schema(
    table_schema: TableSchema,
    *,
    data_columns: tuple[str, ...] | None = None,
) -> pa.Schema:
    """Build a canonical pyarrow.Schema from an SCDM TableSchema.

    Args:
        table_schema: SCDM table definition with column types and nullability.
        data_columns: If provided, only include spec columns present in this
            sequence, ordered to match data column order. Columns in data but
            not in spec are excluded (caller merges inferred types separately).

    Returns:
        pyarrow.Schema with canonical types for SCDM columns.

    Raises:
        ValueError: If a ColumnDef has an unrecognised col_type.
    """
    col_lookup = {c.name: c for c in table_schema.columns}

    if data_columns is not None:
        ordered_names = [n for n in data_columns if n in col_lookup]
    else:
        ordered_names = [c.name for c in table_schema.columns]

    fields: list[pa.Field] = []
    for name in ordered_names:
        col_def = col_lookup[name]
        arrow_type = _SCDM_TYPE_MAP.get(col_def.col_type)
        if arrow_type is None:
            raise ValueError(
                f"unrecognised SCDM col_type {col_def.col_type!r} "
                f"for column {col_def.name!r}"
            )
        fields.append(pa.field(name, arrow_type, nullable=col_def.missing_allowed))

    return pa.schema(fields)
```

**Testing:**

Tests go in `tests/test_cross_table_engine.py` (the existing test file for cross-table validation logic).

Tests must verify:
- GH-6.AC3.1: Call `build_arrow_schema(get_schema("demographic"))` and assert the returned schema has the correct number of fields, correct pyarrow types (float64 for Numeric columns, utf8 for Character columns), and correct nullability matching `missing_allowed`.
- `data_columns` filtering: Pass a subset of column names and verify only those columns appear in the schema, in the data column order.
- `data_columns` with extra columns not in spec: Verify they are excluded from the returned schema.
- Unknown `col_type`: Construct a `ColumnDef` with `col_type="Unknown"` inside a `TableSchema` and assert `ValueError` is raised.

Use real `get_schema()` lookups (project convention: never mock schema loading). Construct custom `TableSchema` / `ColumnDef` instances only for edge cases not representable with real SCDM tables.

Follow the existing test class pattern:

```python
class TestBuildArrowSchema:
    def test_known_table_produces_correct_types(self) -> None: ...
    def test_numeric_columns_map_to_float64(self) -> None: ...
    def test_character_columns_map_to_utf8(self) -> None: ...
    def test_nullability_matches_missing_allowed(self) -> None: ...
    def test_data_columns_filters_and_orders(self) -> None: ...
    def test_data_columns_excludes_non_spec_columns(self) -> None: ...
    def test_unknown_col_type_raises_value_error(self) -> None: ...
```

**Verification:**

Run: `uv run pytest tests/test_cross_table_engine.py -v -k TestBuildArrowSchema`
Expected: All tests pass

**Commit:** `feat: add build_arrow_schema for canonical SCDM-to-pyarrow type mapping (GH-6)`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Verify all existing tests still pass

**Verifies:** None (regression check)

**Files:** None (read-only verification)

**Verification:**

Run: `uv run pytest`
Expected: All 416+ tests pass, no regressions

**Commit:** No commit needed — verification only
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
