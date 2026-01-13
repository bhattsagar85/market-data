#!/bin/bash
set -e

# Define the timeframes you want to process
TIMEFRAMES=("5m" "10m" "1d")

echo "Starting Full Rebuild Process... 🏗️"

for TF in "${TIMEFRAMES[@]}"; do
    echo "------------------------------------------"
    echo "⏳ Processing timeframe: $TF"
    
    # 1. Build the Vector Features
    echo "🚀 Rebuilding Vector $TF index"
    PYTHONPATH=src python src/features/feature_builder_$TF.py
    
    # 2. Build the FAISS Index
    echo "🚀 Rebuilding FAISS $TF index"
    PYTHONPATH=src python src/vector_store/build_index_$TF.py
    
    echo "✅ Timeframe $TF completed"
done

echo "------------------------------------------"
echo "🎉 ALL INDEXES REBUILT SUCCESSFULLY"