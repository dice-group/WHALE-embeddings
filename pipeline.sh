#!/usr/bin/env bash

# echo "▶️  Starting data download..."
# bash Crawler/download_data.sh

echo "▶️  Creating Python environment..."

# Enable conda commands in this script
eval "$(conda shell.bash hook)"

# Create (if missing) and activate the 'dice' env
conda create -n dice python=3.10.13 --no-default-packages -y || true
conda activate dice

# Install Python deps
pip install --upgrade pip
pip install tqdm dicee

echo "▶️  Extracting domain-based datasets (1 core)..."
python3 Crawler/domain_extraction.py --num_core 1

echo "▶️  Scheduling training jobs..."
# Pass through any flags you need, e.g. -b or -s
bash Training/schedule_jobs.sh

echo "✅ Pipeline complete."
