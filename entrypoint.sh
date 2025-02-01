#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Set Python environment variables for PySpark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Set PYTHONPATH to include the project root directory
export PYTHONPATH=/app

# Activate the virtual environment (if used)
# source /app/venv/bin/activate

# Run unit tests
echo "Running unit tests..."
python -m unittest discover -s tests -v

# Run the main application
echo "Running the main application..."
python src/main.py

## Run the Streamlit dashboard
echo "Starting the Streamlit dashboard..."
exec streamlit run src/dashboard.py --server.port=8501 --server.address=0.0.0.0
