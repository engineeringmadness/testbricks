#!/usr/bin/env bash
set -euo pipefail

cd /workspace

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"
export PYSPARK_PYTHON=python3.14
export PYSPARK_DRIVER_PYTHON=python3.14

python3.14 -m pip install --upgrade pip
python3.14 -m pip install -r requirements.txt
python3.14 -m pip install -e .
