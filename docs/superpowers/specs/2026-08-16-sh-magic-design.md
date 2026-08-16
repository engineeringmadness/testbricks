# Databricks `%sh` Magic Design

**Date:** 2026-08-16  
**Status:** Awaiting spec review  
**Scope:** Parse and execute Databricks `%sh` comment magics in notebooks run by `NotebookExecutor` / `LocalWorkflowRunner`, delegating to `subprocess` + `bash -c`.

**Out of scope:**

- IPython/Databricks live cell magics (`%sh` as a real magic, not a comment)
- `%sql`, `%md`, and other magics
- Windows `cmd` / PowerShell
- Command timeouts
- Capturing shell output into Python variables
- Changing process cwd to the notebook directory

## Goal

Databricks notebooks often shell out with `%sh` (install tools, `ls`, `cat`, small scripts). Testbricks already rewrites `# %run` / `# MAGIC %run` before `exec`. This feature adds the same for `%sh`, using `subprocess` so exported `.py` notebooks keep working locally.

The README already lists `%sh` as planned alongside `%run`.

## Requirements Summary

| Decision | Choice |
|---|---|
| Source forms | Comment lines only: `# %sh ...` and `# MAGIC %sh ...`, including multi-line `# MAGIC` body lines |
| Working directory | Current process cwd (`os.getcwd()`), not the notebook file directory |
| Shell | `subprocess.run(["bash", "-c", script], ...)` |
| Environment | Inherit `os.environ` (widgets / workflow `base_parameters` already live there) |
| Output | Print captured stdout to `sys.stdout`, stderr to `sys.stderr` |
| Non-zero exit | Continue unless the block is `%sh -e`, then raise |
| Architecture | Transform comments to `__run_shell__(...)` (same pattern as `%run` → `__run_notebook__`) |
| Public API | None. No new package export. Helper is injected into the notebook namespace only. |

## Architecture

```
src/testbricks/
  notebook_executor.py   # %run transform (existing); %sh transform; run_shell; inject __run_shell__
  notebook_exceptions.py # ShellCommandError (new)
  local_workflow_runner.py  # unchanged exec path; inherits helper via NotebookExecutor
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `transform_sh_commands(source)` | Rewrite `%sh` comment blocks to `__run_shell__(script, fail_on_error=...)` |
| `run_shell(script, fail_on_error=False)` | Run `bash -c`, print streams, raise on `-e` + non-zero exit |
| `ShellCommandError` | Failure from `%sh -e` or missing `bash`; not `NotebookExit`, not `DbutilsError` |
| `NotebookExecutor.exec_file` | After `%run` transform, apply `%sh` transform; bind `__run_shell__` on the namespace before `exec` |
| `LocalWorkflowRunner` | No second exec path. Top-level tasks, `%run` children, and `dbutils.notebook.run()` all go through `NotebookExecutor` |

`transform_run_commands` stays as-is. Tests import `transform_sh_commands` from `notebook_executor` (no re-export from `local_workflow_runner` unless a caller already depends on that pattern).

### Why inject in `exec_file`

`__run_notebook__` is bound in `run_workflow` and `run_isolated` separately. `__run_shell__` is bound inside `exec_file` so top-level, shared `%run`, and isolated `notebook.run()` cannot forget it. Re-binding on nested `exec_file` is idempotent.

## Parsing

A **block** starts on a full-line comment matching:

```text
^\s*#\s*(?:MAGIC\s+)?%sh(?:\s+|$)
```

Same idea as `%run`: only whole-line comments are rewritten. `%sh` inside a Python string or after code on the same line is left alone.

### Flags

After `%sh`, tokens that start with `-` are flags.

- `-e` sets `fail_on_error=True`. Repeated `-e` is still true.
- Any other flag (for example `-x`) raises `ValueError` at transform time. Do not pass unknown flags through to bash.
- The first token that does not start with `-` begins the script (inline remainder of the start line).

Examples:

| Source start line | `fail_on_error` | Inline script |
|---|---|---|
| `# %sh echo hi` | false | `echo hi` |
| `# MAGIC %sh -e ls /tmp` | true | `ls /tmp` |
| `# MAGIC %sh -e` | true | (empty; body may follow) |
| `# MAGIC %sh` | false | (empty; body may follow) |

### Multi-line body

Databricks exports multi-line cells as consecutive `# MAGIC` lines.

- Continuation lines: immediately following lines matching `^\s*#\s*MAGIC\s+(.*)$` whose body does **not** start with `%` (a new magic ends the block).
- The captured body (after `# MAGIC `) is appended as the next script line.
- A non-MAGIC line, a new `%` magic, or end of file ends the block.
- `# %sh` **without** `MAGIC` is single-line only. A following `# echo hi` is a normal Python comment, not shell. This matches how Testbricks treats `# %run` today (one line).

### Empty script

If the joined script is empty or whitespace-only, emit **no** `__run_shell__` call (delete the magic comment lines). Do not start a subprocess. `%sh -e` with an empty body is still a no-op (nothing failed).

### Transform output

Use `repr` for the script string (same as `%run` paths):

```python
# MAGIC %sh -e
# MAGIC echo hello
```

becomes:

```python
__run_shell__('echo hello', fail_on_error=True)
```

Join body lines with `\n`. Do not append an extra trailing newline after the last body line. Two MAGIC body lines `echo a` and `echo b` become `'echo a\necho b'`.

Inline-only:

```python
# %sh echo hi
```

becomes:

```python
__run_shell__('echo hi', fail_on_error=False)
```

`transform_run_commands` runs first, then `transform_sh_commands`.

## Data Flow

```
Notebook source
  → transform_run_commands
  → transform_sh_commands
  → exec_file binds namespace["__run_shell__"] = run_shell
  → compile + exec
  → run_shell(script, fail_on_error)
  → subprocess.run(
        ["bash", "-c", script],
        cwd=os.getcwd(),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
      )
  → write stdout to sys.stdout if non-empty
  → write stderr to sys.stderr if non-empty
  → if fail_on_error and returncode != 0: raise ShellCommandError
  → otherwise continue Python after the block
```

Relative paths in the script (`ls ./scripts`) resolve against process cwd, not `os.path.dirname(__file__)`.

## Error Handling

| Case | Behavior |
|---|---|
| `%sh`, exit ≠ 0 | Print stdout/stderr; continue. Do not raise. |
| `%sh -e`, exit ≠ 0 | Print stdout/stderr, then raise `ShellCommandError` including return code and a short stdout/stderr snippet. Stops the current notebook. |
| `bash` not found (`FileNotFoundError`) | Raise `ShellCommandError`. Missing interpreter is not a soft failure. |
| Empty `%sh` / `%sh -e` | No-op; no process. |
| Unknown flag after `%sh` | `ValueError` at transform time. |
| `%sh -e` in a `%run` child | `ShellCommandError` propagates in the shared namespace and stops the caller file. |
| `%sh -e` in `dbutils.notebook.run()` | `ShellCommandError` propagates out of `run()` (not converted to an exit value). |
| Top-level workflow task | Uncaught `ShellCommandError` fails that task the same way any other exception does today. |

`ShellCommandError` is a `RuntimeError`. It is not `NotebookExit` (that is only `dbutils.notebook.exit`) and not `DbutilsError`.

## Testing

Keep transform tests separate from execution tests (same split as `%run`). Prefer a dedicated `tests/test_sh_magic.py` so `test_local_workflow_runner.py` does not grow another magic suite.

### Transform

- `# %sh echo hi` and `# MAGIC %sh echo hi` → `__run_shell__('echo hi', fail_on_error=False)`
- Multi-line `# MAGIC %sh` plus following `# MAGIC` body lines join with newlines
- `%sh -e` sets `fail_on_error=True`
- Unknown flag → `ValueError`
- Empty block → no `__run_shell__` call
- Python after the block is unchanged
- A file with both `%run` and `%sh` still transforms `%run`

### Runner (real `bash`)

Do not mock `subprocess` except for the missing-`bash` case.

- Success: stdout visible via `capsys`
- Failure without `-e`: later Python in the same file ran
- Failure with `-e`: `ShellCommandError`; later Python did not run
- `cwd` is process cwd (`os.chdir` in the test, `pwd` in the script)
- Child notebook via `%run` can contain `%sh`
- `os.environ` key is visible as `$VAR` in bash
- Missing `bash`: `ShellCommandError` (mock `subprocess.run` to raise `FileNotFoundError`, or patch `run_shell`'s executable)

## Implementation notes

- `subprocess.run` uses `check=False`; inspect `returncode` so non-`-e` failures can continue.
- Copy `os.environ` so the child cannot mutate the parent mapping object; keys still match the parent at spawn time.
- Print captured text as-is (do not add an extra newline if bash already ended with one).
- YAGNI: no timeout argument, no `executable=` besides argv `bash`, no Windows branch.

## Non-goals (explicit)

- `%sh` as IPython magic in an interactive kernel
- Streaming bytes before the process exits (`capture_output=True` is enough for local tests)
- Mapping Databricks `/dbfs` paths inside the shell (use `dbutils.fs` / existing path resolver from Python)
