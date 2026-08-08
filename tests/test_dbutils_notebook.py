import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from mock.dbutils import DbutilsError, configure, dbutils
from mock.notebook_exceptions import NotebookExit


@pytest.fixture(autouse=True)
def reset_dbutils(tmp_path):
    configure(str(tmp_path), source_dir=str(tmp_path))
    yield
    dbutils.widgets.removeAll()


class TestNotebookExit:
    def test_exit_raises_notebook_exit(self):
        with pytest.raises(NotebookExit) as exc_info:
            dbutils.notebook.exit("val")

        assert exc_info.value.value == "val"

    def test_exit_converts_non_string_values(self):
        with pytest.raises(NotebookExit) as exc_info:
            dbutils.notebook.exit(42)

        assert exc_info.value.value == "42"


class TestNotebookRun:
    def test_run_returns_exit_value(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text(
            'dbutils.notebook.exit("result")\n', encoding="utf-8"
        )
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            result = dbutils.notebook.run("./child", 60)

        assert result == "result"

    def test_run_returns_empty_when_no_exit(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text("VALUE = 1\n", encoding="utf-8")
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            result = dbutils.notebook.run("./child", 60)

        assert result == ""

    def test_run_isolated_namespace(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text("CHILD_ONLY = True\n", encoding="utf-8")
        parent = tmp_path / "parent.py"
        parent.write_text("PARENT_VALUE = 1\n", encoding="utf-8")

        namespace = {"PARENT_VALUE": 1}
        with dbutils.executor.caller_context(str(parent)):
            dbutils.notebook.run("./child", 60)

        assert "CHILD_ONLY" not in namespace

    def test_run_passes_arguments_via_env(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text(
            'import os\nRESULT = os.environ["env"]\n', encoding="utf-8"
        )
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            dbutils.notebook.run("./child", 60, {"env": "prod"})

        assert os.environ["env"] == "prod"

    def test_run_arguments_override_widget_default(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text(
            'dbutils.widgets.text("env", "dev")\n'
            'RESULT = dbutils.widgets.get("env")\n',
            encoding="utf-8",
        )
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            dbutils.notebook.run("./child", 60, {"env": "prod"})

        assert os.environ["env"] == "prod"

    def test_run_relative_path(self, tmp_path):
        helpers = tmp_path / "helpers"
        helpers.mkdir()
        (helpers / "setup.py").write_text(
            'dbutils.notebook.exit("ok")\n', encoding="utf-8"
        )
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            result = dbutils.notebook.run("./helpers/setup", 60)

        assert result == "ok"

    def test_run_workspace_path(self, tmp_path):
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "child.py").write_text(
            'dbutils.notebook.exit("workspace")\n', encoding="utf-8"
        )
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            result = dbutils.notebook.run("/Workspace/project/child", 60)

        assert result == "workspace"

    def test_run_missing_notebook_raises(self, tmp_path):
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            with pytest.raises(DbutilsError, match="Notebook not found"):
                dbutils.notebook.run("./missing", 60)

    def test_timeout_accepted_not_enforced(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text("pass\n", encoding="utf-8")
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            result = dbutils.notebook.run("./child", 0)

        assert result == ""

    def test_workspace_path_without_source_dir_raises(self, tmp_path):
        configure(str(tmp_path), source_dir=None)
        parent = tmp_path / "parent.py"

        with dbutils.executor.caller_context(str(parent)):
            with pytest.raises(DbutilsError, match="source_dir not configured"):
                dbutils.notebook.run("/Workspace/project/child", 60)
