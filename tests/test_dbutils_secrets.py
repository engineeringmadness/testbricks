import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from testbricks.dbutils import DbutilsError, dbutils


@pytest.fixture(autouse=True)
def reset_secrets():
    for name in list(os.environ):
        if name.startswith("DBUTILS_SECRET_"):
            os.environ.pop(name)
    yield
    for name in list(os.environ):
        if name.startswith("DBUTILS_SECRET_"):
            os.environ.pop(name)


class TestGet:
    def test_get_returns_env_var_value(self):
        os.environ["DBUTILS_SECRET_jdbc_password"] = "secret-value"
        assert dbutils.secrets.get("jdbc", "password") == "secret-value"

    def test_get_missing_secret_raises(self):
        with pytest.raises(DbutilsError, match="does not exist"):
            dbutils.secrets.get("jdbc", "password")

    def test_env_var_accessible_outside_dbutils(self):
        os.environ["DBUTILS_SECRET_my-scope_api-key"] = "token"
        assert dbutils.secrets.get("my-scope", "api-key") == "token"
        assert os.getenv("DBUTILS_SECRET_my-scope_api-key") == "token"


class TestGetBytes:
    def test_get_bytes_returns_utf8_encoded_value(self):
        os.environ["DBUTILS_SECRET_jdbc_password"] = "secret-value"
        assert dbutils.secrets.getBytes("jdbc", "password") == b"secret-value"

    def test_get_bytes_missing_secret_raises(self):
        with pytest.raises(DbutilsError, match="does not exist"):
            dbutils.secrets.getBytes("jdbc", "password")
