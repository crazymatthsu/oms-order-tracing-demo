"""Shared fixtures: the default mock book, its settings and its oracle reconciliation."""

from __future__ import annotations

import faulthandler
import sys

import pytest

from oms_trace import mockdata, reference


@pytest.hookimpl(trylast=True)
def pytest_unconfigure():
    """Keep faulthandler off over an embedded Deephaven JVM (see tests/test_dag_embedded.py)."""
    deephaven_server = sys.modules.get("deephaven_server")
    if deephaven_server is not None and getattr(deephaven_server.Server, "instance", None) is not None:
        faulthandler.disable()


@pytest.fixture(scope="session")
def book() -> mockdata.Book:
    return mockdata.generate(seed=42, families=24)


@pytest.fixture(scope="session")
def settings(book):
    return mockdata.settings_for(book)


@pytest.fixture(scope="session")
def oracle(book, settings) -> reference.Result:
    return reference.reconcile(book.rows, settings, book.now)
