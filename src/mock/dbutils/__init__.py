from .dbutils_mock import DbutilsMock
from .errors import DbutilsError

dbutils = DbutilsMock()


def configure(base_path):
    dbutils.configure(base_path)
