# dbutils.fs Mock Design

**Date:** 2026-08-08  
**Status:** Approved for implementation  
**Scope:** `dbutils` proxy with `dbutils.fs` module — `cp`, `mv`, `rm`, `mkdirs`

## Goal

Build a local proxy for Databricks `dbutils` so notebook code can run in CI without a Databricks cluster. The first slice implements four `dbutils.fs` file operations; all other `dbutils` methods and `dbutils.fs` methods are no-ops that return `True`.

This complements the existing `SparkMock`, which already proxies `spark.read.table`, `spark.write.table`, and `spark.sql` against local CSV files.

## Requirements Summary

| Decision | Choice |
|---|---|
| Path mapping | Share `base_path` with `SparkMock`; strip `dbfs:`, `/dbfs/`, `/mnt/` prefixes |
| Access pattern | Global `dbutils` import; `LocalWorkflowRunner` auto-injects into notebook namespace |
| Unimplemented methods | No-op returning `True` |
| Configuration | Explicit `configure(base_path)`; runner calls it at workflow start |
| Errors | Custom `DbutilsError` wrapping underlying Python exceptions |

## Architecture

```
src/mock/dbutils/
  __init__.py          # exports `dbutils` singleton + configure()
  dbutils_mock.py      # DbutilsMock root object
  fs.py                # FsMock with cp, mv, rm, mkdirs
  path_resolver.py     # prefix stripping + base_path join
  noop.py              # NoOpModule for unimplemented submodules/methods
  errors.py            # DbutilsError
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `PathResolver` | Strip Databricks path prefixes; join remainder to `base_path`; prevent path traversal |
| `FsMock` | Implement `cp`, `mv`, `rm`, `mkdirs` via `shutil`/`os`; return `True` on success |
| `NoOpModule` | Catch-all for unimplemented `dbutils.*` methods; any call returns `True` |
| `DbutilsMock` | Root object exposing `.fs` (real) and stub submodules |
| `configure()` | Module-level function that sets `base_path` on the singleton's path resolver |

### Data Flow

```
Notebook / Test
    → dbutils.fs.cp("dbfs:/data/file.csv", "dbfs:/backup/file.csv")
    → FsMock.cp()
    → PathResolver.resolve()  →  ./store/data/file.csv, ./store/backup/file.csv
    → shutil.copy2()
    → return True
```

## Path Resolution

`PathResolver` normalizes Databricks-style paths to local paths under `base_path`.

### Prefix Stripping

| Input prefix | Example input | Resolved local path (`base_path=./store`) |
|---|---|---|
| `dbfs:/` | `dbfs:/data/file.csv` | `./store/data/file.csv` |
| `/dbfs/` | `/dbfs/data/file.csv` | `./store/data/file.csv` |
| `/mnt/` | `/mnt/vol/data/file.csv` | `./store/vol/data/file.csv` |
| *(none)* | `/data/file.csv` or `data/file.csv` | `./store/data/file.csv` |

### Rules

- Leading slashes on the path remainder are stripped before joining to `base_path`.
- `configure()` must be called before any `fs` operation; otherwise raise `DbutilsError("dbutils not configured — call configure(base_path) first")`.
- Resolved paths are normalized with `os.path.normpath`.
- Resolved paths must remain under `base_path` (reject `..` traversal that escapes the root).
- `/Volumes/...` paths are not specially handled in v1. They resolve as literal paths under `base_path` (e.g. `/Volumes/main/default/my-volume/data.csv` → `./store/Volumes/main/default/my-volume/data.csv`).

## `fs` Operations

All four methods match Databricks signatures and return `True` on success.

| Method | Signature | Implementation |
|---|---|---|
| `cp` | `(from_path, to_path, recurse=False)` | File → `shutil.copy2`; directory + `recurse=True` → `shutil.copytree`; directory + `recurse=False` → `DbutilsError` |
| `mv` | `(from_path, to_path, recurse=False)` | `cp` then `rm` on source (Databricks semantics: copy + delete) |
| `rm` | `(path, recurse=False)` | File → `os.remove`; empty directory → `os.rmdir`; non-empty directory + `recurse=True` → `shutil.rmtree`; non-empty directory + `recurse=False` → `DbutilsError` |
| `mkdirs` | `(path)` | `os.makedirs(resolved_path, exist_ok=True)` |

### Parent Directory Handling

- `cp` / `mv`: create parent directories of the destination if they do not exist.
- `mkdirs`: idempotent — no error if the directory already exists.

### Error Handling

All `OSError` / `FileNotFoundError` from `shutil`/`os` are caught and re-raised as `DbutilsError` with the original exception chained:

```python
raise DbutilsError("failed to copy ...") from exc
```

## No-Op Stubs

Unimplemented `dbutils` submodules and methods return `True` for any call.

### Stubbed Submodules (v1)

| Submodule | Real Databricks methods | Mock behavior |
|---|---|---|
| `dbutils.secrets` | `get`, `list`, `listScopes` | All calls → `True` |
| `dbutils.widgets` | `text`, `dropdown`, `get`, `remove` | All calls → `True` |
| `dbutils.notebook` | `exit`, `run` | All calls → `True` |
| `dbutils.jobs` | `taskValues` | All calls → `True` |
| `dbutils.fs` *(unimplemented methods)* | `ls`, `head`, `put`, `mount`, etc. | All calls → `True` |

### `NoOpModule` Behavior

- `dbutils.help()` returns `True`.
- Nested access like `dbutils.jobs.taskValues.get()` works via recursive `NoOpModule` (each attribute access returns another `NoOpModule` until a method is called).

## Configuration and Usage

### Tests

```python
from mock.dbutils import configure, dbutils

configure("./store")
dbutils.fs.mkdirs("dbfs:/data/raw")
dbutils.fs.cp("dbfs:/data/raw/file.csv", "dbfs:/data/backup/file.csv")
```

### `LocalWorkflowRunner` Integration

**Constructor change:**

```python
LocalWorkflowRunner(source_dir, workflow_json_path, base_path)
```

**At `run_workflow()` start:**

1. Call `configure(base_path)` on the global dbutils singleton.
2. Inject `dbutils` into `execution_globals` alongside `__run_notebook__`.

```python
from mock.dbutils import configure, dbutils

def run_workflow(self):
    configure(self.base_path)
    execution_globals = {
        "__name__": "__main__",
        "dbutils": dbutils,
        "__run_notebook__": lambda path: self._run_notebook(path, execution_globals),
    }
    ...
```

Notebooks can use `dbutils.fs.cp(...)` with zero changes. Existing `LocalWorkflowRunner` callers must pass `base_path` (breaking change — only `tests/test_local_workflow_runner.py` is affected today).

## Testing Plan

New test file: `tests/test_dbutils_fs.py`

| Test | Verifies |
|---|---|
| `test_configure_sets_base_path` | `configure()` stores path; unconfigured calls raise `DbutilsError` |
| `test_path_resolver_strips_prefixes` | `dbfs:/`, `/dbfs/`, `/mnt/` all resolve correctly |
| `test_mkdirs_creates_nested_dirs` | `mkdirs("dbfs:/a/b/c")` creates dirs under `base_path` |
| `test_cp_file` | Single file copy works; destination exists |
| `test_cp_dir_requires_recurse` | Copying directory without `recurse=True` raises `DbutilsError` |
| `test_cp_dir_recursive` | Directory copy with `recurse=True` copies all contents |
| `test_mv_file` | Source removed; destination exists after move |
| `test_rm_file` | File deleted |
| `test_rm_dir_non_recursive_empty` | Empty directory removed |
| `test_rm_dir_non_recursive_nonempty` | Raises `DbutilsError` |
| `test_rm_dir_recursive` | `recurse=True` removes directory and contents |
| `test_unimplemented_fs_method_returns_true` | `dbutils.fs.ls("dbfs:/")` returns `True` |
| `test_unimplemented_submodule_returns_true` | `dbutils.secrets.get("scope", "key")` returns `True` |
| `test_workflow_runner_injects_dbutils` | Runner injects `dbutils` and calls `configure()` |

Tests use the existing `tmp_path` pytest fixture for isolated filesystem state.

## Out of Scope (v1)

- `dbutils.fs.ls`, `head`, `put`, `mount`, `mounts`, `unmount`, `updateMount`, `refreshMounts`
- `/Volumes/` prefix special handling
- Cross-filesystem semantics beyond local `shutil`/`os`
- `spark` injection by `LocalWorkflowRunner` (future work)
- R, Scala, or non-Python runtimes
