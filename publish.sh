#!/bin/bash
set -e

echo "Building testbrick package..."

# Clean previous builds
rm -rf build/ dist/ *.egg-info/

# Install build dependencies
pip install build twine

# Build the package (sdist + wheel) via pyproject.toml
python -m build

# Publish to PyPI
echo "Publishing to PyPI..."
twine upload dist/*

echo "Package published successfully!"