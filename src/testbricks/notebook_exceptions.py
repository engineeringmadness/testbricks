class NotebookExit(BaseException):
    def __init__(self, value: str):
        self.value = value
        super().__init__(value)


class ShellCommandError(RuntimeError):
    def __init__(self, message, returncode=None):
        self.returncode = returncode
        super().__init__(message)
