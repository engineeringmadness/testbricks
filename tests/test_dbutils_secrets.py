import os

import pytest

from testbricks.dbutils import DbutilsError, dbutils
from testbricks.dbutils.secrets import SecretMetadata, SecretScope


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


class TestList:
    def test_list_returns_keys_for_scope(self):
        os.environ["DBUTILS_SECRET_jdbc_password"] = "secret-value"
        os.environ["DBUTILS_SECRET_jdbc_user"] = "alice"
        os.environ["DBUTILS_SECRET_other_token"] = "tok"
        listed = dbutils.secrets.list("jdbc")
        assert listed == [SecretMetadata(key="password"), SecretMetadata(key="user")]

    def test_list_unknown_scope_returns_empty(self):
        os.environ["DBUTILS_SECRET_jdbc_password"] = "secret-value"
        assert dbutils.secrets.list("missing") == []


class TestListScopes:
    def test_list_scopes_returns_unique_scope_names(self):
        os.environ["DBUTILS_SECRET_jdbc_password"] = "secret-value"
        os.environ["DBUTILS_SECRET_jdbc_user"] = "alice"
        os.environ["DBUTILS_SECRET_kv_api-key"] = "token"
        assert dbutils.secrets.listScopes() == [
            SecretScope(name="jdbc"),
            SecretScope(name="kv"),
        ]

    def test_list_scopes_empty_when_no_secrets(self):
        assert dbutils.secrets.listScopes() == []
