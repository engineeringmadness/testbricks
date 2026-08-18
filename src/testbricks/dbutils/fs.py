import os
import shutil

from .errors import DbutilsError
from .noop import NoOpModule
from .path_resolver import PathResolver


class FsMock:
    def __init__(self, path_resolver: PathResolver):
        self._path_resolver = path_resolver

    def __getattr__(self, name):
        return NoOpModule()

    def help(self, command=None):
        return True

    def cp(self, from_path, to_path, recurse=False):
        source = self._resolve(from_path)
        destination = self._resolve(to_path)

        def _copy():
            if os.path.isdir(source):
                if not recurse:
                    raise DbutilsError(
                        f"source is a directory and recurse is False: {from_path}"
                    )
                if os.path.exists(destination):
                    raise DbutilsError(f"destination already exists: {to_path}")
            self._ensure_parent(destination)
            if os.path.isdir(source):
                shutil.copytree(source, destination)
            else:
                shutil.copy2(source, destination)

        return self._os_call(
            f"failed to copy {from_path} to {to_path}", _copy
        )

    def mv(self, from_path, to_path, recurse=False):
        self.cp(from_path, to_path, recurse=recurse)
        self.rm(from_path, recurse=recurse)
        return True

    def rm(self, path, recurse=False):
        target = self._resolve(path)

        def _remove():
            if os.path.isfile(target) or os.path.islink(target):
                os.remove(target)
            elif os.path.isdir(target):
                shutil.rmtree(target) if recurse else os.rmdir(target)
            else:
                raise FileNotFoundError(target)

        return self._os_call(f"failed to remove {path}", _remove)

    def mkdirs(self, path):
        target = self._resolve(path)
        return self._os_call(
            f"failed to create directory {path}",
            lambda: os.makedirs(target, exist_ok=True),
        )

    def _resolve(self, path):
        return self._path_resolver.resolve(path)

    def _ensure_parent(self, destination):
        os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

    @staticmethod
    def _os_call(message, action):
        try:
            action()
        except DbutilsError:
            raise
        except (OSError, FileNotFoundError) as exc:
            raise DbutilsError(message) from exc
        return True
