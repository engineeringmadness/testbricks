import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from testbricks.dbutils import DbutilsError, configure, dbutils


@pytest.fixture(autouse=True)
def reset_dbutils(tmp_path):
    configure(str(tmp_path), source_dir=str(tmp_path))
    dbutils.jobs.taskValues.clear()
    yield
    dbutils.jobs.taskValues.clear()
    dbutils.widgets.removeAll()


class TestTaskValuesSetGet:
    def test_set_and_get_within_current_task(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            dbutils.jobs.taskValues.set(key="region", value="eu")
            assert (
                dbutils.jobs.taskValues.get(taskKey="producer", key="region") == "eu"
            )

    def test_get_task_key_alias(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            dbutils.jobs.taskValues.set(key="region", value="eu")
            assert (
                dbutils.jobs.taskValues.get(task_key="producer", key="region") == "eu"
            )

    def test_non_string_value_is_stringified(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            dbutils.jobs.taskValues.set(key="count", value=3)
            assert dbutils.jobs.taskValues.get(taskKey="producer", key="count") == "3"

    def test_missing_key_raises_in_job(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            with pytest.raises(DbutilsError, match="not found"):
                dbutils.jobs.taskValues.get(taskKey="producer", key="missing")

    def test_default_used_when_key_missing_in_job(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            assert (
                dbutils.jobs.taskValues.get(
                    taskKey="producer", key="missing", default="fallback"
                )
                == "fallback"
            )

    def test_debug_value_used_outside_job(self):
        assert (
            dbutils.jobs.taskValues.get(
                taskKey="producer", key="region", debugValue="local"
            )
            == "local"
        )

    def test_set_updates_environ(self):
        with dbutils.jobs.taskValues.current_task("producer"):
            dbutils.jobs.taskValues.set(key="bridge_key", value="from_task")
        assert os.environ["bridge_key"] == "from_task"
        os.environ.pop("bridge_key", None)

    def test_get_requires_task_key(self):
        with pytest.raises(DbutilsError, match="taskKey"):
            dbutils.jobs.taskValues.get(key="k")


class TestTaskValuesIsolation:
    def test_isolated_sets_visible_after_context_exits(self):
        store = dbutils.jobs.taskValues
        with store.current_task("parent"):
            with store.isolated_context():
                store.set(key="from_child", value="secret")
                assert store.get(taskKey="parent", key="from_child") == "secret"
            assert store.get(taskKey="parent", key="from_child") == "secret"

    def test_isolated_sets_not_committed_if_context_raises(self):
        store = dbutils.jobs.taskValues
        with store.current_task("parent"):
            with pytest.raises(RuntimeError):
                with store.isolated_context():
                    store.set(key="from_child", value="secret")
                    raise RuntimeError("boom")
            with pytest.raises(DbutilsError, match="not found"):
                store.get(taskKey="parent", key="from_child")

    def test_isolated_notebook_run_commits_on_return(self, tmp_path):
        child = tmp_path / "child.py"
        child.write_text(
            'dbutils.jobs.taskValues.set(key="from_child", value="ok")\n',
            encoding="utf-8",
        )
        parent = tmp_path / "parent.py"
        store = dbutils.jobs.taskValues
        with store.current_task("parent"), dbutils.executor.caller_context(
            str(parent)
        ):
            dbutils.notebook.run("./child", 60)
            assert store.get(taskKey="parent", key="from_child") == "ok"
