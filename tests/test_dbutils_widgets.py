import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from mock.dbutils import DbutilsError, configure, dbutils


@pytest.fixture(autouse=True)
def reset_widgets():
    dbutils.widgets.removeAll()
    yield
    dbutils.widgets.removeAll()


class TestText:
    def test_text_sets_env_var(self):
        assert dbutils.widgets.text("env", "dev") is None
        assert dbutils.widgets.get("env") == "dev"
        assert os.environ["env"] == "dev"

    def test_text_overwrites_existing_env(self):
        os.environ["env"] = "prod"
        dbutils.widgets.text("env", "dev")
        assert dbutils.widgets.get("env") == "dev"
        assert os.environ["env"] == "dev"


class TestDropdown:
    def test_dropdown_valid_default(self):
        assert dbutils.widgets.dropdown("mode", "dev", ["dev", "prod"]) is None
        assert dbutils.widgets.get("mode") == "dev"
        assert os.environ["mode"] == "dev"

    def test_dropdown_invalid_default_raises(self):
        with pytest.raises(DbutilsError, match="not in choices"):
            dbutils.widgets.dropdown("mode", "staging", ["dev", "prod"])


class TestGet:
    def test_get_unregistered_raises(self):
        with pytest.raises(DbutilsError, match="does not exist"):
            dbutils.widgets.get("missing")


class TestRemove:
    def test_remove_deletes_env_and_registry(self):
        dbutils.widgets.text("env", "dev")
        assert dbutils.widgets.remove("env") is None
        assert "env" not in os.environ
        with pytest.raises(DbutilsError, match="does not exist"):
            dbutils.widgets.get("env")

    def test_remove_unregistered_returns_none(self):
        assert dbutils.widgets.remove("missing") is None


class TestRemoveAll:
    def test_remove_all_clears_only_widgets(self):
        os.environ["unrelated"] = "keep"
        dbutils.widgets.text("env", "dev")
        dbutils.widgets.text("region", "us")

        assert dbutils.widgets.removeAll() is None

        assert "env" not in os.environ
        assert "region" not in os.environ
        assert os.environ["unrelated"] == "keep"


class TestEnvAccess:
    def test_env_var_accessible_outside_dbutils(self):
        dbutils.widgets.text("env", "dev")
        assert os.getenv("env") == "dev"
