"""Errors raised by the CSV catalog layer.

All catalog errors derive from both ``TestbricksError`` and the hierarchy
below. ``InvalidTableNameError`` and ``SchemaMismatchError`` additionally
inherit ``ValueError`` because callers legitimately treat invalid table
names and schema mismatches as value-level input errors.
"""

from testbricks.errors import TestbricksError


class SparkProxyError(TestbricksError):
    """Base error for SparkProxy catalog and table operations."""


class InvalidTableNameError(SparkProxyError, ValueError):
    """Raised when a table name is not ``schema.table``."""


class SchemaMismatchError(SparkProxyError, ValueError):
    """Raised when append cannot align new columns with an existing table."""
