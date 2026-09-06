import os
import re
import shlex
import subprocess
import sys
from contextlib import contextmanager
from contextvars import ContextVar

from testbricks.catalog.identifier import strip_wrappers
from testbricks.dbutils.errors import DbutilsError
from testbricks.dbutils.widgets import argument_override_context, seeded_environ
from testbricks.notebook_exceptions import NotebookExit, ShellCommandError

RUN_COMMAND_PATTERN = re.compile(
    r"^\s*#\s*(?:MAGIC\s+)?%run\s+(.+?)\s*$",
    re.MULTILINE,
)
SH_START_PATTERN = re.compile(r"^(\s*)#\s*(?:MAGIC\s+)?%sh(?:\s+(.*))?\s*$")
FS_START_PATTERN = re.compile(r"^(\s*)#\s*(?:MAGIC\s+)?%fs(?:\s+(.*))?\s*$")
MAGIC_BODY_PATTERN = re.compile(r"^\s*#\s*MAGIC\s+(.*)$")

_caller_file: ContextVar[str | None] = ContextVar("caller_file", default=None)


def _strip_matching_quotes(value):
    return strip_wrappers(value, "'\"").strip()


def parse_run_path(raw_path, file_path):
    path = _strip_matching_quotes(raw_path)
    if not path:
        raise DbutilsError(f"Empty %run path in notebook '{file_path}'")
    return path


def transform_run_commands(source, file_path):
    def replace(match):
        return f"__run_notebook__({parse_run_path(match.group(1), file_path)!r})"

    return RUN_COMMAND_PATTERN.sub(replace, source)


def _parse_sh_remainder(remainder):
    if not remainder:
        return False, ""
    tokens = remainder.split()
    fail_on_error = False
    script_start = None
    for index, token in enumerate(tokens):
        if not token.startswith("-"):
            script_start = index
            break
        if token != "-e":
            raise ValueError(f"Unknown %sh flag: {token}")
        fail_on_error = True
    if script_start is None:
        return fail_on_error, ""
    inline = remainder.split(maxsplit=script_start)[-1]
    return fail_on_error, inline.strip()


def _collect_magic_lines(lines, index):
    """Collect consecutive ``# MAGIC <body>`` lines starting at ``index``.

    Stops at the first non-magic line or at a body that starts another magic
    command (``%run``, ``%sh``, ...). Returns ``(bodies, next_index)``.
    """
    bodies = []
    while index < len(lines):
        body_match = MAGIC_BODY_PATTERN.match(lines[index].rstrip("\n"))
        if not body_match:
            break
        body = body_match.group(1)
        if body.lstrip().startswith("%"):
            break
        bodies.append(body)
        index += 1
    return bodies, index


def _newline_for(line):
    return "\n" if line.endswith("\n") else ""


def transform_sh_commands(source):
    lines = source.splitlines(keepends=True)
    output = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = SH_START_PATTERN.match(line.rstrip("\n"))
        if not match:
            output.append(line)
            index += 1
            continue
        indent, remainder = match.groups()
        fail_on_error, inline = _parse_sh_remainder(remainder)
        script_lines = [inline] if inline else []
        started_with_magic = "MAGIC" in line.split("%sh", 1)[0]
        index += 1
        if started_with_magic:
            bodies, index = _collect_magic_lines(lines, index)
            script_lines.extend(bodies)
        script = "\n".join(script_lines)
        if script.strip():
            output.append(
                f"{indent}__run_shell__({script!r}, fail_on_error={fail_on_error})"
                f"{_newline_for(line)}"
            )
    return "".join(output)


def _fs_python_call(command, args):
    if not command.isidentifier():
        raise ValueError(f"Invalid %fs command: {command}")
    overwrite = None
    if command == "put" and args and args[-1].lower() in ("true", "false"):
        overwrite = args[-1].lower() == "true"
        args = args[:-1]
    rendered = ", ".join(repr(arg) for arg in args)
    if overwrite is None:
        return f"dbutils.fs.{command}({rendered})"
    prefix = f"{rendered}, " if rendered else ""
    return f"dbutils.fs.{command}({prefix}overwrite={overwrite})"


def transform_fs_commands(source):
    lines = source.splitlines(keepends=True)
    output = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = FS_START_PATTERN.match(line.rstrip("\n"))
        if not match:
            output.append(line)
            index += 1
            continue
        indent, remainder = match.groups()
        parts = shlex.split(remainder or "")
        started_with_magic = "MAGIC" in line.split("%fs", 1)[0]
        index += 1
        if started_with_magic:
            bodies, index = _collect_magic_lines(lines, index)
            if bodies:
                parts.extend(shlex.split("\n".join(bodies)))
        if not parts:
            continue
        command, *args = parts
        output.append(f"{indent}{_fs_python_call(command, args)}{_newline_for(line)}")
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
        snippet = (completed.stderr or completed.stdout or "").strip()[-200:]
        raise ShellCommandError(
            f"Command failed with exit code {completed.returncode}: {snippet}",
            returncode=completed.returncode,
        )


class NotebookExecutor:
    def __init__(self, dbutils_mock):
        self._dbutils = dbutils_mock

    @contextmanager
    def caller_context(self, caller_file):
        token = _caller_file.set(caller_file)
        try:
            yield
        finally:
            _caller_file.reset(token)

    def namespace(self, file_path, extra=None):
        ns = {
            "__name__": "__main__",
            "__file__": file_path,
            "dbutils": self._dbutils,
            "__run_shell__": run_shell,
        }
        ns["__run_notebook__"] = lambda path: self.run_shared(path, ns)
        if extra:
            ns.update(extra)
        return ns

    def resolve_path(self, path, caller_file=None):
        return self._dbutils.path_resolver.resolve_notebook(
            _strip_matching_quotes(path),
            caller_file if caller_file is not None else _caller_file.get(),
        )

    def exec_file(self, file_path, namespace, *, top_level=False):
        with self.caller_context(file_path):
            with open(file_path, encoding="utf-8") as notebook_file:
                source = notebook_file.read()
            namespace["__file__"] = file_path
            namespace["__run_shell__"] = run_shell
            transformed = transform_fs_commands(
                transform_sh_commands(transform_run_commands(source, file_path))
            )
            try:
                exec(compile(transformed, file_path, "exec"), namespace, namespace)
            except NotebookExit as exc:
                if top_level:
                    return exc.value
                raise
        return None

    def run_shared(self, path, namespace):
        notebook_path = self.resolve_path(path, caller_file=namespace.get("__file__"))
        self.exec_file(notebook_path, namespace, top_level=False)

    def run_isolated(self, path, arguments=None, extra=None):
        arguments = arguments or {}
        notebook_path = self.resolve_path(path, caller_file=_caller_file.get())

        namespace = self.namespace(notebook_path, extra=extra)
        with (
            argument_override_context(arguments.keys()),
            seeded_environ(arguments),
            self._dbutils.jobs.taskValues.isolated_context(),
        ):
            try:
                self.exec_file(notebook_path, namespace, top_level=False)
            except NotebookExit as exc:
                return exc.value
        return ""
