from .fs import FsMock
from .noop import NoOpModule
from .path_resolver import PathResolver


class DbutilsMock:
    def __init__(self):
        self._path_resolver = PathResolver()
        self.fs = FsMock(self._path_resolver)
        self.secrets = NoOpModule()
        self.widgets = NoOpModule()
        self.notebook = NoOpModule()
        self.jobs = NoOpModule()

    def configure(self, base_path):
        self._path_resolver.configure(base_path)

    def help(self, module=None):
        return True

    def __getattr__(self, name):
        return NoOpModule()
