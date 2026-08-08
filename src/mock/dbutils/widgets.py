import os

from .errors import DbutilsError


class WidgetsMock:
    def __init__(self):
        self._registry: set[str] = set()

    def text(self, name, default, label=None):
        os.environ[name] = str(default)
        self._registry.add(name)
        return None

    def dropdown(self, name, default, choices, label=None):
        if default not in choices:
            raise DbutilsError(
                f"Default value '{default}' is not in choices {list(choices)}"
            )
        os.environ[name] = str(default)
        self._registry.add(name)
        return None

    def get(self, name):
        if name not in self._registry:
            raise DbutilsError(f"Widget '{name}' does not exist")
        return os.environ[name]

    def remove(self, name):
        if name not in self._registry:
            return None
        os.environ.pop(name, None)
        self._registry.discard(name)
        return None

    def removeAll(self):
        for name in list(self._registry):
            os.environ.pop(name, None)
        self._registry.clear()
        return None
