#!/bin/bash

# SPMT Bootstrap and Test Script
# Automatically sets up the project and runs tests

set -e  # Exit on error

echo "======================================"
echo "SPMT: Bootstrap and Test"
echo "======================================"
echo ""

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "Error: pyproject.toml not found. Please run this script from the project root."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1

# Install package with dev dependencies
echo "📦 Installing SPMT with dev dependencies..."
pip install -e ".[dev]" > /dev/null 2>&1
echo "✅ Installation complete"

# Run pytest
echo ""
echo "🧪 Running tests..."
pytest tests/ -v --tb=short

echo ""
echo "======================================"
echo "✅ Bootstrap and tests completed!"
echo "======================================"
