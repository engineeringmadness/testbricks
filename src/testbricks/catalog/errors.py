"""Errors raised by the SparkMock CSV catalog."""


class SparkMockError(Exception):
    """Base error for SparkMock catalog and table operations."""


class InvalidTableNameError(SparkMockError, ValueError):
    """Raised when a table name is not ``schema.table``."""


class SchemaMismatchError(SparkMockError, ValueError):
    """Raised when append cannot align new columns with an existing table."""
