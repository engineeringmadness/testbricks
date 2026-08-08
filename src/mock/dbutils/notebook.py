from mock.notebook_exceptions import NotebookExit


class NotebookMock:
    def __init__(self, executor):
        self._executor = executor

    def exit(self, value):
        raise NotebookExit(str(value))

    def run(self, path, timeout_seconds, arguments=None):
        return self._executor.run_isolated(path, arguments=arguments or {})
