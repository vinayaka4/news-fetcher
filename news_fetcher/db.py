from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable


class HybridRow:
    """A database row supporting both numeric and column-name access."""
    def __init__(self, values: tuple[Any, ...], columns: list[str]):
        self._values = values
        self._columns = columns
        self._mapping = dict(zip(columns, values))

    def __getitem__(self, key: int | str) -> Any:
        return self._values[key] if isinstance(key, int) else self._mapping[key]

    def __iter__(self):
        return iter(self._values)

    def keys(self):
        return self._columns


class PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self.rowcount = cursor.rowcount

    def _row(self, row):
        if row is None:
            return None
        columns = [item.name for item in self._cursor.description] if self._cursor.description else []
        return HybridRow(tuple(row), columns)

    def fetchone(self):
        return self._row(self._cursor.fetchone())

    def fetchall(self):
        return [self._row(row) for row in self._cursor.fetchall()]

    def __iter__(self):
        for row in self._cursor:
            yield self._row(row)


class PostgresConnection:
    backend = "postgres"

    def __init__(self, connection):
        self._connection = connection
        self.row_factory = None

    @staticmethod
    def _sql(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, parameters: Iterable[Any] = ()) -> PostgresCursor:
        cursor = self._connection.cursor()
        cursor.execute(self._sql(sql), tuple(parameters))
        return PostgresCursor(cursor)

    def executescript(self, script: str) -> None:
        cursor = self._connection.cursor()
        for statement in script.split(";"):
            if statement.strip():
                cursor.execute(statement)
        cursor.close()

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def database_target(default: str = "news.db") -> str:
    return os.getenv("DATABASE_URL") or os.getenv("NEWS_DATABASE") or default


def connect(target: str | Path | None = None):
    value = str(target or database_target())
    if value.startswith(("postgres://", "postgresql://")):
        import psycopg
        return PostgresConnection(psycopg.connect(value))
    connection = sqlite3.connect(value)
    connection.row_factory = sqlite3.Row
    return connection


def backend(connection) -> str:
    return getattr(connection, "backend", "sqlite")
