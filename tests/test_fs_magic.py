import pytest

from testbricks.notebook_executor import (
    transform_fs_commands,
    transform_run_commands,
    transform_sh_commands,
)


class TestTransformFsCommands:
    def test_inline_ls(self):
        source = "# %fs ls /mnt/data\nprint('done')\n"
        transformed = transform_fs_commands(source)
        assert "# %fs" not in transformed
        assert "dbutils.fs.ls('/mnt/data')" in transformed
        assert "print('done')" in transformed

    def test_magic_inline(self):
        source = "# MAGIC %fs ls dbfs:/data\n"
        transformed = transform_fs_commands(source)
        assert "dbutils.fs.ls('dbfs:/data')" in transformed

    def test_put_with_overwrite_token(self):
        source = "# %fs put dbfs:/out.txt hello true\n"
        transformed = transform_fs_commands(source)
        assert "dbutils.fs.put('dbfs:/out.txt', 'hello', overwrite=True)" in transformed

    def test_empty_block_is_noop(self):
        source = "# MAGIC %fs\nprint('after')\n"
        transformed = transform_fs_commands(source)
        assert "dbutils.fs." not in transformed
        assert "print('after')" in transformed

    def test_invalid_command_raises(self):
        with pytest.raises(ValueError, match="Invalid %fs command"):
            transform_fs_commands("# %fs 123 /path\n")

    def test_run_and_sh_transforms_still_apply(self):
        source = "# %run ./helpers/setup\n# %sh echo hi\n# %fs ls dbfs:/\n"
        after_run = transform_run_commands(source, "main.py")
        transformed = transform_fs_commands(transform_sh_commands(after_run))
        assert "__run_notebook__('./helpers/setup')" in transformed
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed
        assert "dbutils.fs.ls('dbfs:/')" in transformed


class TestExecFileFsMagic:
    def test_executes_fs_ls_and_continues(self, tmp_path, notebook_executor):
        (tmp_path / "listed").mkdir()
        notebook = tmp_path / "main.py"
        notebook.write_text("# %fs ls dbfs:/\nAFTER = True\n", encoding="utf-8")
        namespace = notebook_executor.namespace(str(notebook))
        notebook_executor.exec_file(str(notebook), namespace, top_level=True)
        assert namespace["AFTER"] is True

    def test_executes_fs_put(self, tmp_path, notebook_executor):
        notebook = tmp_path / "main.py"
        notebook.write_text(
            "# %fs put dbfs:/from_magic.txt hello\n",
            encoding="utf-8",
        )
        namespace = notebook_executor.namespace(str(notebook))
        notebook_executor.exec_file(str(notebook), namespace, top_level=True)
        assert (tmp_path / "from_magic.txt").read_text(encoding="utf-8") == "hello"
