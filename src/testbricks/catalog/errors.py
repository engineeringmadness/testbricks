"""Errors raised by the SparkProxy CSV catalog."""


class SparkProxyError(Exception):
    """Base error for SparkProxy catalog and table operations."""


class InvalidTableNameError(SparkProxyError, ValueError):
    """Raised when a table name is not ``schema.table``."""


class SchemaMismatchError(SparkProxyError, ValueError):
    """Raised when append cannot align new columns with an existing table."""
