"""Tests for DuckDB connection factory with resource constraints."""

from __future__ import annotations

from pathlib import Path

import pytest


duckdb = pytest.importorskip("duckdb")

from scdm_qa.validation.duckdb_utils import create_connection


class TestCreateConnection:
    def test_applies_memory_limit(self) -> None:
        conn = create_connection(memory_limit="512MB", threads=2)
        try:
            result = conn.execute("SELECT current_setting('memory_limit')").fetchone()
            assert result is not None
            # DuckDB reports in MiB internally; 512MB ≈ 488.2 MiB
            assert "MiB" in result[0] or "MB" in result[0]
        finally:
            conn.close()

    def test_applies_threads(self) -> None:
        conn = create_connection(memory_limit="512MB", threads=3)
        try:
            result = conn.execute("SELECT current_setting('threads')").fetchone()
            assert result is not None
            assert int(result[0]) == 3
        finally:
            conn.close()

    def test_applies_temp_directory(self, tmp_path: Path) -> None:
        conn = create_connection(
            memory_limit="512MB", threads=2, temp_directory=tmp_path,
        )
        try:
            result = conn.execute("SELECT current_setting('temp_directory')").fetchone()
            assert result is not None
            assert str(tmp_path) in result[0]
        finally:
            conn.close()

    def test_no_temp_directory_by_default(self) -> None:
        conn = create_connection(memory_limit="512MB", threads=2)
        try:
            # Should not raise — just uses DuckDB's default
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()

    def test_connection_is_functional(self) -> None:
        conn = create_connection(memory_limit="512MB", threads=1)
        try:
            result = conn.execute("SELECT 42 AS answer").fetchone()
            assert result[0] == 42
        finally:
            conn.close()
