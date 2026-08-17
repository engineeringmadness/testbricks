import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from testbricks.notebook_exceptions import NotebookExit, ShellCommandError
from testbricks.notebook_executor import transform_run_commands, transform_sh_commands


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

