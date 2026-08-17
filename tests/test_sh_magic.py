import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from testbricks.notebook_exceptions import NotebookExit, ShellCommandError


class TestShellCommandError:
    def test_is_runtime_error_not_notebook_exit(self):
        err = ShellCommandError("bash failed", returncode=1)
        assert isinstance(err, RuntimeError)
        assert not isinstance(err, NotebookExit)
        assert err.returncode == 1
        assert "bash failed" in str(err)
