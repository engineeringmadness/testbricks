import os
import shutil
from collections import namedtuple

from .errors import DbutilsError
from .noop import NoOpModule
from .path_resolver import PathResolver

FileInfo = namedtuple("FileInfo", ["path", "name", "size", "modificationTime"])


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

    def put(self, file, contents, overwrite=False):
        target = self._resolve(file)
        if os.path.exists(target) and not overwrite:
            raise DbutilsError(f"file already exists: {file}")

        def _write():
            self._ensure_parent(target)
            with open(target, "w", encoding="utf-8") as handle:
                handle.write("" if contents is None else str(contents))

        return self._os_call(f"failed to put {file}", _write)

    def ls(self, path):
        target = self._resolve(path)
        if not os.path.exists(target):
            raise DbutilsError(f"cannot list missing directory: {path}")
        if os.path.isfile(target):
            return [self._file_info(path, os.path.basename(target.rstrip("/")), target)]

        entries = []
        for name in sorted(os.listdir(target)):
            child = os.path.join(target, name)
            display_path = self._join_display_path(path, name)
            display_name = f"{name}/" if os.path.isdir(child) else name
            entries.append(self._file_info(display_path, display_name, child))
        return entries

    @staticmethod
    def _join_display_path(parent, name):
        if parent.endswith("/"):
            return f"{parent}{name}"
        return f"{parent}/{name}"

    @staticmethod
    def _file_info(display_path, name, local_path):
        size = 0 if os.path.isdir(local_path) else os.path.getsize(local_path)
        mtime_ms = int(os.path.getmtime(local_path) * 1000)
        return FileInfo(
            path=display_path,
            name=name,
            size=size,
            modificationTime=mtime_ms,
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
