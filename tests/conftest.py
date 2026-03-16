"""Pytest configuration and fixtures for SCDM QA tests."""

from __future__ import annotations

import logging

import pytest

from scdm_qa.logging import configure_logging


@pytest.fixture(autouse=True)
def _configure_logging_for_tests() -> None:
    """Auto-configure logging for all tests to enable caplog capture."""
    configure_logging()
