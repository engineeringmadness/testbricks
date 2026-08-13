from .dbutils_mock import DbutilsMock
from .errors import DbutilsError

dbutils = DbutilsMock()


def configure(base_path, source_dir=None):
    dbutils.configure(base_path, source_dir)
