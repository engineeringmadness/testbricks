from .fs import FsMock
from .noop import NoOpModule
from .notebook import NotebookMock
from .path_resolver import PathResolver
from .secrets import SecretsMock
from .widgets import WidgetsMock


class DbutilsMock:
    def __init__(self):
        from testbricks.notebook_executor import NotebookExecutor

        self._path_resolver = PathResolver()
        self._source_dir = None
        self._executor = NotebookExecutor(self)
        self.fs = FsMock(self._path_resolver)
        self.secrets = SecretsMock()
        self.widgets = WidgetsMock()
        self.notebook = NotebookMock(self._executor)
        self.jobs = NoOpModule()

    @property
    def source_dir(self):
        return self._source_dir

    @property
    def executor(self):
        return self._executor

    def configure(self, base_path, source_dir=None):
        self._path_resolver.configure(base_path)
        self._source_dir = source_dir

    def help(self, module=None):
        return True

    def __getattr__(self, name):
        return NoOpModule()
