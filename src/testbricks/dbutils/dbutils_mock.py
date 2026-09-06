from .data import DataMock
from .fs import FsMock
from .jobs import JobsMock
from .library import LibraryMock
from .noop import NoOpModule
from .notebook import NotebookMock
from .path_resolver import PathResolver
from .secrets import SecretsMock
from .widgets import WidgetsMock


class DbutilsMock:
    def __init__(self):
        self._path_resolver = PathResolver()
        self._source_dir = None
        self._notebook_executor = None
        self.fs = FsMock(self._path_resolver)
        self.secrets = SecretsMock()
        self.widgets = WidgetsMock()
        self.notebook = NotebookMock(self)
        self.jobs = JobsMock()
        self.library = LibraryMock()
        self.data = DataMock()

    @property
    def source_dir(self):
        return self._source_dir

    @property
    def path_resolver(self):
        return self._path_resolver

    @property
    def executor(self):
        # Lazy seam: notebook_executor imports dbutils submodules at module
        # scope, so constructing the executor here would create an import
        # cycle (dbutils package -> dbutils_mock -> notebook_executor ->
        # dbutils package). Importing on first use breaks the cycle.
        if self._notebook_executor is None:
            from testbricks.notebook_executor import NotebookExecutor

            self._notebook_executor = NotebookExecutor(self)
        return self._notebook_executor

    def configure(self, base_path, source_dir=None):
        self._path_resolver.configure(base_path, source_dir)
        self._source_dir = source_dir

    def __getattr__(self, name):
        return NoOpModule()
