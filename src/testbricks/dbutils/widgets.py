import os
from contextlib import contextmanager
from contextvars import ContextVar

from .errors import DbutilsError

_argument_overrides: ContextVar[frozenset[str] | None] = ContextVar(
    "argument_overrides", default=None
)


@contextmanager
def argument_override_context(keys):
    token = _argument_overrides.set(frozenset(keys))
    try:
        yield
    finally:
        _argument_overrides.reset(token)


class WidgetsMock:
    def __init__(self):
        self._registry: set[str] = set()

    def _set_widget_value(self, name, value):
        overrides = _argument_overrides.get()
        if overrides is None or name not in overrides:
            os.environ[name] = str(value)

    def _register(self, name, value):
        self._set_widget_value(name, value)
        self._registry.add(name)
        return None

    def text(self, name, default, label=None):
        return self._register(name, default)

    def dropdown(self, name, default, choices, label=None):
        if default not in choices:
            raise DbutilsError(
                f"Default value '{default}' is not in choices {list(choices)}"
            )
        return self._register(name, default)

    def combobox(self, name, default, choices, label=None):
        return self.dropdown(name, default, choices, label)

    def multiselect(self, name, default, choices, label=None):
        selected = [part.strip() for part in str(default).split(",")]
        selected = [part for part in selected if part]
        if not selected:
            selected = [default]
        for value in selected:
            if value not in choices:
                raise DbutilsError(
                    f"Default value '{default}' is not in choices {list(choices)}"
                )
        return self._register(name, default)

    def get(self, name):
        if name not in self._registry:
            raise DbutilsError(f"Widget '{name}' does not exist")
        return os.environ[name]

    def getAll(self):
        return {name: os.environ[name] for name in self._registry}

    def getArgument(self, name, optional=None):
        if name in self._registry:
            return self.get(name)
        if optional is not None:
            return str(optional)
        return self.get(name)

    def remove(self, name):
        if name in self._registry:
            os.environ.pop(name, None)
            self._registry.discard(name)
        return None

    def removeAll(self):
        for name in list(self._registry):
            os.environ.pop(name, None)
        self._registry.clear()
        return None
