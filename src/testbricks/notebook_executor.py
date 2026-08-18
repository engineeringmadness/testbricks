import os
import re
import subprocess
import sys
from contextlib import contextmanager
from contextvars import ContextVar

from testbricks.dbutils.path_resolver import strip_known_prefix
from testbricks.notebook_exceptions import NotebookExit, ShellCommandError

RUN_COMMAND_PATTERN = re.compile(
    r"^\s*#\s*(?:MAGIC\s+)?%run\s+(.+?)\s*$",
    re.MULTILINE,
)
SH_START_PATTERN = re.compile(r"^(\s*)#\s*(?:MAGIC\s+)?%sh(?:\s+(.*))?\s*$")
MAGIC_BODY_PATTERN = re.compile(r"^\s*#\s*MAGIC\s+(.*)$")
WORKSPACE_PREFIXES = ("/Workspace/", "/Repos/")

_caller_file: ContextVar[str | None] = ContextVar("caller_file", default=None)


def _strip_matching_quotes(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1].strip()
    return value


def parse_run_path(raw_path, file_path):
    path = _strip_matching_quotes(raw_path)
    if not path:
        raise ValueError(f"Empty %run path in notebook '{file_path}'")
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
            newline = "\n" if line.endswith("\n") else ""
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
        from testbricks.dbutils.errors import DbutilsError

        normalized_path = _strip_matching_quotes(path)
        if normalized_path.startswith("/"):
            source_dir = self._dbutils.source_dir
            if source_dir is None:
                raise DbutilsError(
                    "source_dir not configured — required for workspace paths"
                )
            remainder = strip_known_prefix(normalized_path, WORKSPACE_PREFIXES).lstrip("/")
            notebook_path = os.path.join(source_dir, remainder)
        else:
            caller_file = caller_file or _caller_file.get()
            if not caller_file:
                raise DbutilsError(
                    "caller file not set — cannot resolve relative notebook path"
                )
            notebook_path = os.path.join(os.path.dirname(caller_file), normalized_path)

        notebook_path = os.path.normpath(notebook_path)
        if not notebook_path.endswith(".py"):
            notebook_path += ".py"
        if not os.path.exists(notebook_path):
            raise DbutilsError(f"Notebook not found: {notebook_path}")
        return notebook_path

    def exec_file(self, file_path, namespace, *, top_level=False):
        with self.caller_context(file_path):
            with open(file_path, encoding="utf-8") as notebook_file:
                source = notebook_file.read()
            namespace["__file__"] = file_path
            namespace["__run_shell__"] = run_shell
            transformed = transform_sh_commands(transform_run_commands(source, file_path))
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

    def run_isolated(self, path, arguments=None):
        from testbricks.dbutils.widgets import argument_override_context

        arguments = arguments or {}
        notebook_path = self.resolve_path(path, caller_file=_caller_file.get())
        for key, value in arguments.items():
            os.environ[key] = str(value)

        namespace = self.namespace(notebook_path)
        with argument_override_context(arguments.keys()):
            try:
                self.exec_file(notebook_path, namespace, top_level=False)
            except NotebookExit as exc:
                return exc.value
        return ""
