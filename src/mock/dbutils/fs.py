import os
import shutil

from .errors import DbutilsError
from .noop import _noop
from .path_resolver import PathResolver

_IMPLEMENTED_METHODS = frozenset({"cp", "mv", "rm", "mkdirs"})


class FsMock:
    def __init__(self, path_resolver: PathResolver):
        self._path_resolver = path_resolver

    def __getattr__(self, name):
        if name in _IMPLEMENTED_METHODS:
            raise AttributeError(name)
        return _noop

    def help(self, command=None):
        return True

    def cp(self, from_path, to_path, recurse=False):
        source = self._resolve(from_path)
        destination = self._resolve(to_path)

        try:
            if os.path.isdir(source):
                if not recurse:
                    raise DbutilsError(
                        f"source is a directory and recurse is False: {from_path}"
                    )
                if os.path.exists(destination):
                    raise DbutilsError(f"destination already exists: {to_path}")
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
                shutil.copytree(source, destination)
            else:
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
                shutil.copy2(source, destination)
        except DbutilsError:
            raise
        except (OSError, FileNotFoundError) as exc:
            raise DbutilsError(
                f"failed to copy {from_path} to {to_path}"
            ) from exc

        return True

    def mv(self, from_path, to_path, recurse=False):
        self.cp(from_path, to_path, recurse=recurse)
        self.rm(from_path, recurse=recurse)
        return True

    def rm(self, path, recurse=False):
        target = self._resolve(path)

        try:
            if os.path.isfile(target) or os.path.islink(target):
                os.remove(target)
            elif os.path.isdir(target):
                if recurse:
                    shutil.rmtree(target)
                else:
                    os.rmdir(target)
            else:
                raise FileNotFoundError(target)
        except DbutilsError:
            raise
        except (OSError, FileNotFoundError) as exc:
            raise DbutilsError(f"failed to remove {path}") from exc

        return True

    def mkdirs(self, path):
        target = self._resolve(path)

        try:
            os.makedirs(target, exist_ok=True)
        except (OSError, FileNotFoundError) as exc:
            raise DbutilsError(f"failed to create directory {path}") from exc

        return True

    def _resolve(self, path):
        return self._path_resolver.resolve(path)
