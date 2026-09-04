import os
from contextlib import contextmanager
from contextvars import ContextVar

from .errors import DbutilsError

_MISSING = object()
_current_task_key: ContextVar[str | None] = ContextVar(
    "task_values_current_task", default=None
)


def _stringify(value):
    return value if isinstance(value, str) else str(value)


class TaskValuesStore:
    """In-memory `dbutils.jobs.taskValues` store for a single workflow run.

    Shared-namespace workflow tasks write through immediately. Isolated
    `dbutils.notebook.run()` calls buffer `set` until the child returns.
    """

    def __init__(self):
        self.clear()

    def clear(self):
        self._committed: dict[str, dict[str, str]] = {}
        self._buffers: list[dict[str, dict[str, str]]] = []

    @contextmanager
    def current_task(self, task_key):
        token = _current_task_key.set(task_key)
        try:
            yield
        finally:
            _current_task_key.reset(token)

    @contextmanager
    def isolated_context(self):
        self._buffers.append({})
        try:
            yield
        except BaseException:
            self._buffers.pop()
            raise
        inner = self._buffers.pop()
        parent = self._buffers[-1] if self._buffers else self._committed
        for task_key, values in inner.items():
            parent.setdefault(task_key, {}).update(values)

    def _write_target(self):
        return self._buffers[-1] if self._buffers else self._committed

    def _read_values(self, task_key):
        merged = dict(self._committed.get(task_key, {}))
        current = _current_task_key.get()
        if task_key == current:
            for buffer in self._buffers:
                merged.update(buffer.get(task_key, {}))
        return merged

    def set(self, key=None, value=None, *, update_env=True):
        if key is None:
            raise DbutilsError("taskValues.set requires 'key'")
        if value is None:
            raise DbutilsError("taskValues.set requires 'value'")
        task_key = _current_task_key.get()
        if not task_key:
            task_key = ""
        rendered = _stringify(value)
        self._write_target().setdefault(task_key, {})[str(key)] = rendered
        if update_env:
            os.environ[str(key)] = rendered
        return None

    def get(
        self,
        taskKey=None,
        key=None,
        default=_MISSING,
        debugValue=_MISSING,
        task_key=None,
    ):
        resolved_task = taskKey if taskKey is not None else task_key
        if resolved_task is None:
            raise DbutilsError("taskValues.get requires 'taskKey'")
        if key is None:
            raise DbutilsError("taskValues.get requires 'key'")

        values = self._read_values(resolved_task)
        if key in values:
            return values[key]

        in_job = _current_task_key.get() is not None
        if not in_job and debugValue is not _MISSING:
            return _stringify(debugValue)
        if default is not _MISSING:
            return _stringify(default)
        raise DbutilsError(
            f"Task value '{key}' not found for task '{resolved_task}'"
        )


class JobsMock:
    def __init__(self):
        self.taskValues = TaskValuesStore()
