import os

from .errors import DbutilsError

_SECRET_PREFIX = "DBUTILS_SECRET_"


def _secret_env_name(scope, key):
    return f"{_SECRET_PREFIX}{scope}_{key}"


class SecretsMock:
    def get(self, scope, key):
        env_name = _secret_env_name(scope, key)
        if env_name not in os.environ:
            raise DbutilsError(
                f"Secret for scope '{scope}' and key '{key}' does not exist"
            )
        return os.environ[env_name]

    def getBytes(self, scope, key):
        return self.get(scope, key).encode("utf-8")
