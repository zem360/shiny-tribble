#!/bin/bash

# Exit immediately if a command fails
set -e

# 1. Setup Virtual Environment
echo "------------------------------------------"
echo "Creating Virtual Environment..."
python3 -m venv venv
source venv/bin/activate

# 2. Install Dependencies
echo "Installing Requirements..."
pip install --upgrade pip
pip install -r requirements.txt
echo "Registering Jupyter Kernel..."
python -m ipykernel install --user --name=mfg_pipeline --display-name "Manufacturing Project"

echo "Setup complete. To explore data, run: jupyter lab"

# 3. Set PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)

# 4. Run Tests
echo "------------------------------------------"
echo "Running Pytest..."
python -m pytest tests/

# 5. Run Pipeline
echo "------------------------------------------"
echo "Executing Manufacturing Pipeline..."
python src/pipeline.py

echo "------------------------------------------"
echo "Process Complete! Output saved to data/output/"