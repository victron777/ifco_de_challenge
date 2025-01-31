#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Activate the virtual environment (if used)
# source /app/venv/bin/activate

# Run unit tests
echo "Running unit tests..."
python -m unittest discover -s tests -v

# Run the main application
echo "Running the main application..."
python src/main.py