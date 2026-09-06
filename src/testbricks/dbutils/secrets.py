import os
from collections import namedtuple

from .errors import DbutilsError

_SECRET_PREFIX = "DBUTILS_SECRET_"

SecretMetadata = namedtuple("SecretMetadata", ["key"])
SecretScope = namedtuple("SecretScope", ["name"])


def _secret_env_name(scope, key):
    return f"{_SECRET_PREFIX}{scope}_{key}"


def _iter_secret_entries():
    for name, _value in os.environ.items():
        if not name.startswith(_SECRET_PREFIX):
            continue
        remainder = name[len(_SECRET_PREFIX) :]
        if not remainder or "_" not in remainder:
            continue
        scope, key = remainder.split("_", 1)
        if scope and key:
            yield scope, key


class SecretsMock:
    def get(self, scope, key):
        env_name = _secret_env_name(scope, key)
        if env_name not in os.environ:
            raise DbutilsError(f"Secret for scope '{scope}' and key '{key}' does not exist")
        return os.environ[env_name]

    def getBytes(self, scope, key):
        return self.get(scope, key).encode("utf-8")

    def list(self, scope):
        keys = sorted(
            {key for listed_scope, key in _iter_secret_entries() if listed_scope == scope}
        )
        return [SecretMetadata(key=key) for key in keys]

    def listScopes(self):
        scopes = sorted({scope for scope, _key in _iter_secret_entries()})
        return [SecretScope(name=scope) for scope in scopes]
