import os

from .errors import DbutilsError

DBFS_PREFIXES = ("dbfs:/", "/dbfs/", "/mnt/")
WORKSPACE_PREFIXES = ("/Workspace/", "/Repos/")


def strip_known_prefix(path, prefixes):
    for prefix in prefixes:
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


class PathResolver:
    """Resolves ``dbutils.fs`` paths (against ``base_path``) and notebook paths
    (against ``source_dir`` or the calling notebook) into local filesystem paths."""

    def __init__(self):
        self._base_path = None
        self._source_dir = None

    def configure(self, base_path, source_dir=None):
        self._base_path = os.path.abspath(base_path) if base_path is not None else None
        self._source_dir = os.path.abspath(source_dir) if source_dir else None

    @property
    def is_configured(self):
        return self._base_path is not None

    def resolve(self, path):
        """Resolve a dbutils.fs-style path under the configured base_path."""
        if not self.is_configured:
            raise DbutilsError("dbutils not configured — call configure(base_path) first")

        remainder = strip_known_prefix(path, DBFS_PREFIXES).lstrip("/")
        resolved = os.path.normpath(os.path.join(self._base_path, remainder))
        base = os.path.normpath(self._base_path)

        if resolved != base and not resolved.startswith(base + os.sep):
            raise DbutilsError(f"path escapes base_path: {path}")

        return resolved

    def resolve_notebook(self, path, caller_file=None):
        """Resolve a %run / notebook.run-style path.

        Workspace-style absolute paths (``/Workspace/...``, ``/Repos/...``) are
        rooted at the configured ``source_dir`` (with a containment check);
        relative paths are resolved against the calling notebook's directory.
        """
        if path.startswith("/"):
            if self._source_dir is None:
                raise DbutilsError("source_dir not configured — required for workspace paths")
            remainder = strip_known_prefix(path, WORKSPACE_PREFIXES).lstrip("/")
            notebook_path = self._require_within_source_dir(
                os.path.join(self._source_dir, remainder), path
            )
        else:
            if not caller_file:
                raise DbutilsError("caller file not set — cannot resolve relative notebook path")
            notebook_path = os.path.normpath(os.path.join(os.path.dirname(caller_file), path))

        if not notebook_path.endswith(".py"):
            notebook_path += ".py"
        if not os.path.exists(notebook_path):
            raise DbutilsError(f"Notebook not found: {notebook_path}")
        return notebook_path

    def _require_within_source_dir(self, notebook_path, original_path):
        resolved = os.path.normpath(notebook_path)
        source_dir = os.path.normpath(self._source_dir)
        if resolved != source_dir and not resolved.startswith(source_dir + os.sep):
            raise DbutilsError(f"path escapes source_dir: {original_path}")
        return resolved
