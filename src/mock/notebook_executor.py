import os
import re
from contextlib import contextmanager
from contextvars import ContextVar

from mock.notebook_exceptions import NotebookExit

RUN_COMMAND_PATTERN = re.compile(
    r"^\s*#\s*(?:MAGIC\s+)?%run\s+(.+?)\s*$",
    re.MULTILINE,
)

WORKSPACE_PREFIXES = ("/Workspace/", "/Repos/")

_caller_file: ContextVar[str | None] = ContextVar("caller_file", default=None)


def parse_run_path(raw_path, file_path):
    path = raw_path.strip()
    if len(path) >= 2 and path[0] == path[-1] and path[0] in ("'", '"'):
        path = path[1:-1]
    path = path.strip()
    if not path:
        raise ValueError(f"Empty %run path in notebook '{file_path}'")
    return path


def transform_run_commands(source, file_path):
    def replace(match):
        relative_path = parse_run_path(match.group(1), file_path)
        return f"__run_notebook__({relative_path!r})"

    return RUN_COMMAND_PATTERN.sub(replace, source)


class NotebookExecutor:
    def __init__(self, dbutils_mock):
        self._dbutils = dbutils_mock

    @contextmanager
    def _caller_context(self, caller_file):
        token = _caller_file.set(caller_file)
        try:
            yield
        finally:
            _caller_file.reset(token)

    @contextmanager
    def caller_context(self, caller_file):
        with self._caller_context(caller_file):
            yield

    def resolve_path(self, path, caller_file=None):
        from mock.dbutils.errors import DbutilsError

        normalized_path = path.strip()
        if len(normalized_path) >= 2 and normalized_path[0] == normalized_path[-1]:
            if normalized_path[0] in ("'", '"'):
                normalized_path = normalized_path[1:-1].strip()

        if normalized_path.startswith("/"):
            source_dir = self._dbutils.source_dir
            if source_dir is None:
                raise DbutilsError(
                    "source_dir not configured — required for workspace paths"
                )
            remainder = normalized_path
            for prefix in WORKSPACE_PREFIXES:
                if remainder.startswith(prefix):
                    remainder = remainder[len(prefix) :]
                    break
            remainder = remainder.lstrip("/")
            notebook_path = os.path.join(source_dir, remainder)
        else:
            if caller_file is None:
                caller_file = _caller_file.get()
            if not caller_file:
                raise DbutilsError(
                    "caller file not set — cannot resolve relative notebook path"
                )
            notebook_path = os.path.join(
                os.path.dirname(caller_file), normalized_path
            )

        notebook_path = os.path.normpath(notebook_path)
        if not notebook_path.endswith(".py"):
            notebook_path += ".py"

        if not os.path.exists(notebook_path):
            raise DbutilsError(f"Notebook not found: {notebook_path}")

        return notebook_path

    def _execute_source(self, source, file_path, global_namespace, local_namespace):
        transformed = transform_run_commands(source, file_path)
        exec(compile(transformed, file_path, "exec"), global_namespace, local_namespace)

    def exec_file(self, file_path, namespace, *, top_level=False):
        with self._caller_context(file_path):
            with open(file_path, encoding="utf-8") as notebook_file:
                source = notebook_file.read()
            namespace["__file__"] = file_path
            try:
                self._execute_source(source, file_path, namespace, namespace)
            except NotebookExit as exc:
                if top_level:
                    return exc.value
                raise
        return None

    def run_shared(self, path, namespace):
        caller_file = namespace.get("__file__")
        notebook_path = self.resolve_path(path, caller_file=caller_file)
        with self._caller_context(notebook_path):
            self.exec_file(notebook_path, namespace, top_level=False)

    def run_isolated(self, path, arguments=None):
        caller_file = _caller_file.get()
        notebook_path = self.resolve_path(path, caller_file=caller_file)
        arguments = arguments or {}

        from mock.dbutils.widgets import argument_override_context

        for key, value in arguments.items():
            os.environ[key] = str(value)

        namespace = {
            "__name__": "__main__",
            "__file__": notebook_path,
            "dbutils": self._dbutils,
        }
        namespace["__run_notebook__"] = lambda run_path: self.run_shared(
            run_path, namespace
        )

        with self._caller_context(notebook_path):
            with argument_override_context(arguments.keys()):
                try:
                    self.exec_file(notebook_path, namespace, top_level=False)
                except NotebookExit as exc:
                    return exc.value
        return ""
