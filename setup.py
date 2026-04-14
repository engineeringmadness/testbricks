from setuptools import setup, find_packages

setup(
    name="testbrick",
    version="0.1.0",
    description="A set of proxy objects to facilitate testing of Databricks notebooks in CI/CD pipelines",
    author="Karan Gupta",
    author_email="gkaran184@gmail.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark",
        "pandas",
        "pyarrow",
        "numpy",
        "py4j",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
)