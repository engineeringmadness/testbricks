#!/bin/bash
set -e

echo "Building testbrick package..."

# Clean previous builds
rm -rf build/ dist/ *.egg-info/

# Install build dependencies
pip install setuptools wheel twine

# Build the package
python setup.py sdist bdist_wheel

# Publish to PyPI
echo "Publishing to PyPI..."
twine upload dist/*

echo "Package published successfully!"