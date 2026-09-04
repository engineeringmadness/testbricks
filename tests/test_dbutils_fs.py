import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from testbricks.dbutils import DbutilsError, configure, dbutils
from testbricks.dbutils.fs import FileInfo
from testbricks.dbutils.path_resolver import PathResolver
from testbricks.local_workflow_runner import LocalWorkflowRunner


@pytest.fixture(autouse=True)
def reset_dbutils(tmp_path):
    configure(str(tmp_path))
    yield


class TestConfiguration:
    def test_configure_sets_base_path(self, tmp_path):
        resolver = PathResolver()
        resolver.configure(str(tmp_path))
        assert resolver.is_configured
        assert resolver.resolve("dbfs:/data/file.csv") == os.path.join(
            str(tmp_path), "data", "file.csv"
        )

    def test_unconfigured_fs_operation_raises(self):
        from testbricks.dbutils.dbutils_mock import DbutilsMock

        unconfigured = DbutilsMock()
        with pytest.raises(DbutilsError, match="not configured"):
            unconfigured.fs.mkdirs("dbfs:/data")


class TestPathResolver:
    @pytest.mark.parametrize(
        "input_path,expected_suffix",
        [
            ("dbfs:/data/file.csv", os.path.join("data", "file.csv")),
            ("/dbfs/data/file.csv", os.path.join("data", "file.csv")),
            ("/mnt/vol/data/file.csv", os.path.join("vol", "data", "file.csv")),
            ("/data/file.csv", os.path.join("data", "file.csv")),
            ("data/file.csv", os.path.join("data", "file.csv")),
        ],
    )
    def test_path_resolver_strips_prefixes(self, tmp_path, input_path, expected_suffix):
        configure(str(tmp_path))
        resolver = PathResolver()
        resolver.configure(str(tmp_path))
        assert resolver.resolve(input_path) == os.path.join(str(tmp_path), expected_suffix)


class TestMkdirs:
    def test_mkdirs_creates_nested_dirs(self, tmp_path):
        configure(str(tmp_path))
        assert dbutils.fs.mkdirs("dbfs:/a/b/c") is True
        assert os.path.isdir(tmp_path / "a" / "b" / "c")


class TestCp:
    def test_cp_file(self, tmp_path):
        configure(str(tmp_path))
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        source_file = source_dir / "file.csv"
        source_file.write_text("a,b\n1,2\n", encoding="utf-8")

        assert dbutils.fs.cp("dbfs:/data/file.csv", "dbfs:/backup/file.csv") is True

        dest_file = tmp_path / "backup" / "file.csv"
        assert dest_file.exists()
        assert dest_file.read_text(encoding="utf-8") == source_file.read_text(encoding="utf-8")

    def test_cp_dir_requires_recurse(self, tmp_path):
        configure(str(tmp_path))
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        (source_dir / "nested.txt").write_text("x", encoding="utf-8")

        with pytest.raises(DbutilsError, match="recurse is False"):
            dbutils.fs.cp("dbfs:/data", "dbfs:/backup")

    def test_cp_dir_recursive(self, tmp_path):
        configure(str(tmp_path))
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        (source_dir / "nested.txt").write_text("x", encoding="utf-8")

        assert dbutils.fs.cp("dbfs:/data", "dbfs:/backup", recurse=True) is True
        assert (tmp_path / "backup" / "nested.txt").read_text(encoding="utf-8") == "x"


class TestMv:
    def test_mv_file(self, tmp_path):
        configure(str(tmp_path))
        source_dir = tmp_path / "data"
        source_dir.mkdir()
        source_file = source_dir / "file.csv"
        source_file.write_text("content", encoding="utf-8")

        assert dbutils.fs.mv("dbfs:/data/file.csv", "dbfs:/moved/file.csv") is True

        assert not source_file.exists()
        assert (tmp_path / "moved" / "file.csv").read_text(encoding="utf-8") == "content"


class TestRm:
    def test_rm_file(self, tmp_path):
        configure(str(tmp_path))
        target = tmp_path / "file.txt"
        target.write_text("x", encoding="utf-8")

        assert dbutils.fs.rm("dbfs:/file.txt") is True
        assert not target.exists()

    def test_rm_dir_non_recursive_empty(self, tmp_path):
        configure(str(tmp_path))
        target = tmp_path / "empty_dir"
        target.mkdir()

        assert dbutils.fs.rm("dbfs:/empty_dir") is True
        assert not target.exists()

    def test_rm_dir_non_recursive_nonempty(self, tmp_path):
        configure(str(tmp_path))
        target = tmp_path / "nonempty_dir"
        target.mkdir()
        (target / "file.txt").write_text("x", encoding="utf-8")

        with pytest.raises(DbutilsError, match="failed to remove"):
            dbutils.fs.rm("dbfs:/nonempty_dir")

    def test_rm_dir_recursive(self, tmp_path):
        configure(str(tmp_path))
        target = tmp_path / "dir"
        target.mkdir()
        (target / "file.txt").write_text("x", encoding="utf-8")

        assert dbutils.fs.rm("dbfs:/dir", recurse=True) is True
        assert not target.exists()


class TestLs:
    def test_ls_lists_files_and_directories(self, tmp_path):
        configure(str(tmp_path))
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "nested").mkdir()
        file_path = tmp_path / "data" / "file.csv"
        file_path.write_text("abc", encoding="utf-8")

        entries = dbutils.fs.ls("dbfs:/data")
        by_name = {entry.name: entry for entry in entries}

        assert set(by_name) == {"file.csv", "nested/"}
        file_info = by_name["file.csv"]
        assert isinstance(file_info, FileInfo)
        assert file_info.path == "dbfs:/data/file.csv"
        assert file_info.size == 3
        assert isinstance(file_info.modificationTime, int)
        assert file_info.modificationTime >= 0

        dir_info = by_name["nested/"]
        assert dir_info.path == "dbfs:/data/nested"
        assert dir_info.size == 0

    def test_ls_file_returns_single_entry(self, tmp_path):
        configure(str(tmp_path))
        (tmp_path / "file.txt").write_text("x", encoding="utf-8")
        entries = dbutils.fs.ls("dbfs:/file.txt")
        assert len(entries) == 1
        assert entries[0].name == "file.txt"
        assert entries[0].path == "dbfs:/file.txt"
        assert entries[0].size == 1

    def test_ls_missing_directory_raises(self, tmp_path):
        configure(str(tmp_path))
        with pytest.raises(DbutilsError, match="cannot list missing directory"):
            dbutils.fs.ls("dbfs:/does-not-exist")

    def test_ls_empty_directory(self, tmp_path):
        configure(str(tmp_path))
        (tmp_path / "empty").mkdir()
        assert dbutils.fs.ls("dbfs:/empty") == []


class TestNoOpStubs:
    def test_unimplemented_fs_method_returns_true(self, tmp_path):
        configure(str(tmp_path))
        assert dbutils.fs.head("dbfs:/file.txt") is True

    def test_unimplemented_submodule_returns_true(self, tmp_path):
        configure(str(tmp_path))
        assert dbutils.jobs.taskValues.get(key="k") is True


class TestWorkflowRunnerIntegration:
    def test_workflow_runner_injects_dbutils(self, tmp_path):
        import json

        source_dir = tmp_path / "notebooks"
        source_dir.mkdir()
        store_dir = tmp_path / "store"
        store_dir.mkdir()

        workflow = {
            "tasks": [
                {
                    "task_key": "task_1",
                    "notebook_task": {"notebook_path": "/Workspace/any/main"},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        marker = store_dir / "created_by_dbutils"
        (source_dir / "main.py").write_text(
            'dbutils.fs.mkdirs("dbfs:/workflow_dir")\n'
            f'open(r"{marker}", "w").write("ok")\n',
            encoding="utf-8",
        )

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(store_dir))
        runner.run_workflow()

        assert (store_dir / "workflow_dir").is_dir()
        assert marker.read_text(encoding="utf-8") == "ok"
