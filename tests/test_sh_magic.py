from unittest.mock import patch

import pytest

from testbricks.notebook_exceptions import NotebookExit, ShellCommandError
from testbricks.notebook_executor import (
    run_shell,
    transform_run_commands,
    transform_sh_commands,
)


class TestShellCommandError:
    def test_is_runtime_error_not_notebook_exit(self):
        err = ShellCommandError("bash failed", returncode=1)
        assert isinstance(err, RuntimeError)
        assert not isinstance(err, NotebookExit)
        assert err.returncode == 1
        assert "bash failed" in str(err)


class TestTransformShCommands:
    def test_inline_comment(self):
        source = "# %sh echo hi\nprint('done')\n"
        transformed = transform_sh_commands(source)
        assert "# %sh" not in transformed
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed
        assert "print('done')" in transformed

    def test_magic_inline_comment(self):
        source = "# MAGIC %sh echo hi\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed

    def test_multiline_magic_block(self):
        source = "# MAGIC %sh\n# MAGIC echo a\n# MAGIC echo b\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__('echo a\\necho b', fail_on_error=False)" in transformed

    def test_fail_on_error_flag(self):
        source = "# MAGIC %sh -e\n# MAGIC echo hello\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__('echo hello', fail_on_error=True)" in transformed

    def test_unknown_flag_raises(self):
        with pytest.raises(ValueError, match="Unknown %sh flag"):
            transform_sh_commands("# %sh -x echo hi\n")

    def test_empty_block_is_noop(self):
        source = "# MAGIC %sh\nprint('after')\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__" not in transformed
        assert "print('after')" in transformed

    def test_non_magic_is_single_line(self):
        source = "# %sh echo hi\n# echo skipped\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed
        assert "# echo skipped" in transformed

    def test_run_transform_still_applies(self):
        source = "# %run ./helpers/setup\n# %sh echo hi\n"
        after_run = transform_run_commands(source, "main.py")
        transformed = transform_sh_commands(after_run)
        assert "__run_notebook__('./helpers/setup')" in transformed
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed

    def test_new_magic_ends_sh_block(self):
        source = "# MAGIC %sh echo hi\n# MAGIC %run ./other\n"
        transformed = transform_sh_commands(source)
        assert "__run_shell__('echo hi', fail_on_error=False)" in transformed
        assert "# MAGIC %run ./other" in transformed


class TestRunShell:
    def test_prints_stdout(self, capsys):
        run_shell("echo hello")
        captured = capsys.readouterr()
        assert "hello" in captured.out

    def test_nonzero_without_fail_on_error_continues(self):
        run_shell("exit 7", fail_on_error=False)

    def test_nonzero_with_fail_on_error_raises(self, capsys):
        with pytest.raises(ShellCommandError) as exc_info:
            run_shell("echo fail_out; echo fail_err >&2; exit 3", fail_on_error=True)
        captured = capsys.readouterr()
        assert "fail_out" in captured.out
        assert "fail_err" in captured.err
        assert exc_info.value.returncode == 3
        assert "3" in str(exc_info.value)

    def test_uses_process_cwd(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        run_shell("pwd")
        captured = capsys.readouterr()
        assert str(tmp_path) in captured.out

    def test_inherits_environ(self, monkeypatch, capsys):
        monkeypatch.setenv("TESTBRICKS_SH_VAR", "from_env")
        run_shell("echo $TESTBRICKS_SH_VAR")
        captured = capsys.readouterr()
        assert "from_env" in captured.out

    def test_missing_bash_raises(self):
        with patch(
            "testbricks.notebook_executor.subprocess.run",
            side_effect=FileNotFoundError("bash"),
        ):
            with pytest.raises(ShellCommandError, match="bash"):
                run_shell("echo hi")


class TestExecFileShMagic:
    def test_executes_sh_and_continues(self, tmp_path, notebook_executor, capsys):
        notebook = tmp_path / "main.py"
        notebook.write_text(
            "# %sh echo from_sh\nAFTER = True\n",
            encoding="utf-8",
        )
        namespace = {"__name__": "__main__"}
        notebook_executor.exec_file(str(notebook), namespace, top_level=True)
        captured = capsys.readouterr()
        assert "from_sh" in captured.out
        assert namespace["AFTER"] is True

    def test_dash_e_stops_later_python(self, tmp_path, notebook_executor):
        notebook = tmp_path / "main.py"
        notebook.write_text(
            "# %sh -e exit 1\nAFTER = True\n",
            encoding="utf-8",
        )
        namespace = {"__name__": "__main__"}
        with pytest.raises(ShellCommandError):
            notebook_executor.exec_file(str(notebook), namespace, top_level=True)
        assert "AFTER" not in namespace

    def test_percent_run_child_can_use_sh(self, tmp_path, notebook_executor, capsys):
        (tmp_path / "child.py").write_text("# %sh echo from_child\n", encoding="utf-8")
        main = tmp_path / "main.py"
        main.write_text("# %run ./child\nAFTER = True\n", encoding="utf-8")
        namespace = {"__name__": "__main__", "__file__": str(main)}
        namespace["__run_notebook__"] = lambda path: notebook_executor.run_shared(path, namespace)
        notebook_executor.exec_file(str(main), namespace, top_level=True)
        captured = capsys.readouterr()
        assert "from_child" in captured.out
        assert namespace["AFTER"] is True

    def test_isolated_run_propagates_shell_error(self, tmp_path, notebook_executor):
        (tmp_path / "child.py").write_text("# %sh -e exit 2\n", encoding="utf-8")
        parent = tmp_path / "parent.py"
        with notebook_executor.caller_context(str(parent)):
            with pytest.raises(ShellCommandError):
                notebook_executor.run_isolated("./child")
