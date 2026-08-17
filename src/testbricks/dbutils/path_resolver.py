import os

from .errors import DbutilsError

_PREFIXES = ("dbfs:/", "/dbfs/", "/mnt/")


def strip_known_prefix(path, prefixes):
    for prefix in prefixes:
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


class PathResolver:
    def __init__(self):
        self._base_path = None

    def configure(self, base_path):
        self._base_path = os.path.abspath(base_path)

    @property
    def is_configured(self):
        return self._base_path is not None

    def resolve(self, path):
        if not self.is_configured:
            raise DbutilsError(
                "dbutils not configured — call configure(base_path) first"
            )

        remainder = strip_known_prefix(path, _PREFIXES).lstrip("/")
        resolved = os.path.normpath(os.path.join(self._base_path, remainder))
        base = os.path.normpath(self._base_path)

        if resolved != base and not resolved.startswith(base + os.sep):
            raise DbutilsError(f"path escapes base_path: {path}")

        return resolved
