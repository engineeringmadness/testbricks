from testbricks.notebook_exceptions import NotebookExit


class NotebookMock:
    def __init__(self, dbutils_mock):
        self._dbutils = dbutils_mock

    @property
    def _executor(self):
        return self._dbutils.executor

    def exit(self, value):
        raise NotebookExit(str(value))

    def run(self, path, timeout_seconds, arguments=None):
        return self._executor.run_isolated(path, arguments=arguments)
