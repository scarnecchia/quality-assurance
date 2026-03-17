# Architectural Review: SCDM-QA Pipeline

This review evaluates the `scdm-qa` repository from the perspective of a seasoned data quality (DQ) engineer experienced in building large-scale pipelines for TB-sized healthcare datasets.

## Executive Summary
The tech stack (**Polars**, **DuckDB**, **Pointblank**) is modern and well-chosen for high-performance data validation. However, the current orchestration layer contains several architectural "time bombs" that will lead to performance degradation or system failure (OOM) as data scales from gigabytes to terabytes.

---

## 1. Critical Bottleneck: Multiple Data Scans
**Finding:** The pipeline currently performs up to 6 independent scans of the same source file for different validation stages (L1, Uniqueness, Sort Order, Global Checks).
*   **The Problem:** Reading 100GB+ SAS files is extremely I/O and CPU intensive. Repeatedly reading the same data is the single greatest performance drain in the current system.
*   **Recommendation:** Move to a **"Single-Pass" Chunk-Consumer Architecture**. The `TableValidator` should open a single reader and "broadcast" each chunk to a set of registered **Accumulators** (Profiling, Schema, Uniqueness, Sort Order).

## 2. Memory Safety: SAS Handling (OOM Risk)
**Finding:** The `_convert_sas_to_parquet` function in `cross_table.py` collects all chunks into a list and calls `pl.concat(chunks)`.
*   **The Problem:** This loads the **entire dataset into RAM** before writing it to Parquet. This will crash with an Out-of-Memory (OOM) error on any dataset larger than available system memory.
*   **Recommendation:** Implement a **Streaming Parquet Writer**. Use `pyarrow.parquet.ParquetWriter` to sink each chunk to disk immediately as it is read, ensuring memory usage stays constant regardless of table size.

## 3. Maintainability: Fragile Metadata Mapping
**Finding:** Validation logic (`build_validation`) and reporting metadata (`_build_step_descriptions`) are implemented as two separate, mirrored imperative functions.
*   **The Problem:** This is a "Shotgun Surgery" code smell. Adding a new check requires updating logic in multiple files/functions. If they drift, reporting will be incorrect or the system will crash.
*   **Recommendation:** Implement a **Declarative Check Registry**. Define check types, IDs, and parameters in a single source of truth (e.g., a registry class or config file). Both the validation engine and the reporting engine should consume this registry.

## 4. Scalability: Global and Cross-Table Checks
**Finding:** Uniqueness checks (`_uniqueness_in_memory`) load all key columns into memory.
*   **The Problem:** For tables with billions of rows, even just the keys will exceed RAM.
*   **Recommendation:** Standardize on **DuckDB** for all "Global" and L2 checks. DuckDB is designed for disk-backed out-of-core execution. Use `LEFT ANTI JOIN` for RI checks and `GROUP BY/HAVING` for uniqueness to leverage DuckDB's optimized join/agg engines.

## 5. Architectural Cleanliness: God Function
**Finding:** `src/scdm_qa/pipeline.py` is becoming a "God Function," managing everything from I/O and orchestration to reporting and exit codes.
*   **Recommendation:** Extract a `TableValidator` class to encapsulate the L1 lifecycle (reading, accumulating, global checking) and a `CrossTableValidator` for L2 logic. The pipeline should act only as a high-level orchestrator.

---

## Summary of Action Items
1.  **Refactor to Chunk-Consumer pattern** to eliminate redundant I/O.
2.  **Fix SAS-to-Parquet conversion** to use streaming writers.
3.  **Consolidate check definitions** into a declarative registry.
4.  **Maximize DuckDB usage** for any check requiring data cross-referencing or global aggregation.
