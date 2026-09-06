from testbricks.errors import TestbricksError


class NotebookExit(BaseException):
    """Notebook-level exit raised by ``dbutils.notebook.exit``.

    Deliberately derives from ``BaseException`` so that user notebook code
    (and retry loops) cannot swallow it with a plain ``except Exception``.
    """

    def __init__(self, value: str):
        self.value = value
        super().__init__(value)


class ShellCommandError(TestbricksError, RuntimeError):
    def __init__(self, message, returncode=None):
        self.returncode = returncode
        super().__init__(message)
