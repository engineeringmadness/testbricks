import os
import sys

# PySpark workers on Windows need an explicit Python executable path.
# On this CI/dev box `python3` is not available, but `python` is.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
