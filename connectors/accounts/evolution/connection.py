"""SQL Server connection handling for Accounts/Evolution source databases.

This module is the only place that should know how to open a connection to
an Evolution SQL Server database. GE and TLS are two databases on the same
server, reached with the same driver and credentials -- callers pass only
the target database name, so there is exactly one connection code path for
both companies (config.settings.EvolutionSettings.sources).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pyodbc

from config.settings import EvolutionSettings

logger = logging.getLogger(__name__)


def build_connection_string(database: str, settings: EvolutionSettings) -> str:
    """Build an encrypted ODBC connection string for one Evolution database."""
    return (
        f"DRIVER={{{settings.driver}}};"
        f"SERVER={settings.server};"
        f"DATABASE={database};"
        f"UID={settings.username};"
        f"PWD={settings.password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )


@contextmanager
def sql_server_connection(database: str, settings: EvolutionSettings) -> Iterator[pyodbc.Connection]:
    """Open one Evolution SQL Server connection and guarantee it is closed.

    A bounded `timeout` is always passed to `pyodbc.connect` so a source
    server that is unreachable fails fast instead of hanging the job. The
    connection is closed in `finally` regardless of how the `with` block
    exits (success, extraction error, or KeyboardInterrupt).
    """
    connection_string = build_connection_string(database, settings)
    connection = pyodbc.connect(connection_string, timeout=settings.connect_timeout_seconds)
    try:
        yield connection
    finally:
        connection.close()
