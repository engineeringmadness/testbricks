import os
import re
import subprocess
import sys
from contextlib import contextmanager
from contextvars import ContextVar

from testbricks.notebook_exceptions import NotebookExit, ShellCommandError

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


SH_START_PATTERN = re.compile(r"^(\s*)#\s*(?:MAGIC\s+)?%sh(?:\s+(.*))?\s*$")
MAGIC_START_PATTERN = re.compile(r"^\s*#\s*MAGIC\s+%sh")
MAGIC_BODY_PATTERN = re.compile(r"^\s*#\s*MAGIC\s+(.*)$")


def _parse_sh_remainder(remainder):
    fail_on_error = False
    if remainder is None:
        return fail_on_error, ""
    tokens = remainder.split()
    script_start = None
    for index, token in enumerate(tokens):
        if token.startswith("-"):
            if token == "-e":
                fail_on_error = True
            else:
                raise ValueError(f"Unknown %sh flag: {token}")
        else:
            script_start = index
            break
    if script_start is None:
        return fail_on_error, ""
    parts = remainder.split(maxsplit=script_start)
    inline = parts[-1] if parts else ""
    return fail_on_error, inline.strip()


def transform_sh_commands(source):
    lines = source.splitlines(keepends=True)
    output = []
    index = 0
    while index < len(lines):
        line = lines[index]
        newline = "\n" if line.endswith("\n") else ""
        match = SH_START_PATTERN.match(line.rstrip("\n"))
        if not match:
            output.append(line)
            index += 1
            continue
        indent, remainder = match.group(1), match.group(2)
        fail_on_error, inline = _parse_sh_remainder(remainder)
        script_lines = []
        if inline:
            script_lines.append(inline)
        started_with_magic = bool(MAGIC_START_PATTERN.match(line))
        index += 1
        if started_with_magic:
            while index < len(lines):
                body_match = MAGIC_BODY_PATTERN.match(lines[index].rstrip("\n"))
                if not body_match:
                    break
                body = body_match.group(1)
                if body.lstrip().startswith("%"):
                    break
                script_lines.append(body)
                index += 1
        script = "\n".join(script_lines)
        if script.strip():
            output.append(
                f"{indent}__run_shell__({script!r}, fail_on_error={fail_on_error}){newline}"
            )
    return "".join(output)


def run_shell(script, fail_on_error=False):
    try:
        completed = subprocess.run(
            ["bash", "-c", script],
            cwd=os.getcwd(),
            env=os.environ.copy(),
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ShellCommandError("bash not found") from exc

    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    if fail_on_error and completed.returncode != 0:
        snippet = (completed.stderr or completed.stdout or "").strip()
        snippet = snippet[-200:]
        raise ShellCommandError(
            f"Command failed with exit code {completed.returncode}: {snippet}",
            returncode=completed.returncode,
        )


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
        from testbricks.dbutils.errors import DbutilsError

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
        transformed = transform_sh_commands(transformed)
        exec(compile(transformed, file_path, "exec"), global_namespace, local_namespace)

    def exec_file(self, file_path, namespace, *, top_level=False):
        with self._caller_context(file_path):
            with open(file_path, encoding="utf-8") as notebook_file:
                source = notebook_file.read()
            namespace["__file__"] = file_path
            namespace["__run_shell__"] = run_shell
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

        from testbricks.dbutils.widgets import argument_override_context

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
